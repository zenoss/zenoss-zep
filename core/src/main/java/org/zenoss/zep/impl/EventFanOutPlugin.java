/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.amqp.AmqpConnectionManager;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.ExchangeConfiguration;
import org.zenoss.amqp.ZenossQueueConfig;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.plugins.EventPostIndexContext;
import org.zenoss.zep.plugins.EventPostIndexPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EventFanOutPlugin extends EventPostIndexPlugin {

    private static final Logger logger = LoggerFactory.getLogger(EventFanOutPlugin.class);
    private static final String ROUTING_KEY_PREFIX = "zenoss.zenevent.";

    private AmqpConnectionManager amqpConnectionManager;
    
    private final ExchangeConfiguration exchangeConfiguration;

    public EventFanOutPlugin() throws IOException {
        this.exchangeConfiguration = ZenossQueueConfig.getConfig().getExchange(
                "$ProcessedZenEvents");
    }

    public void setAmqpConnectionManager(AmqpConnectionManager amqpConnectionManager) {
        this.amqpConnectionManager = amqpConnectionManager;
    }

    @Override
    public void startBatch(EventPostIndexContext context) throws Exception {
        context.setPluginState(this, new ArrayList<EventSummary>(context.getIndexLimit()));
    }

    @Override
    public void processEvent(EventSummary eventSummary, EventPostIndexContext context) throws ZepException {
        List<EventSummary> events = (List<EventSummary>) context.getPluginState(this);
        events.add(eventSummary);
    }

    @Override
    public void endBatch(EventPostIndexContext context) throws Exception {
        List<EventSummary> events = (List<EventSummary>) context.getPluginState(this);
        AmqpException lastException = null;
        int exceptionCount = 0;
        for (EventSummary eventSummary : events) {
            final String eventClass = eventSummary.getOccurrence(0).getEventClass();
            try {
                logger.debug("Publishing event to fan-out exchange: {}", eventSummary);
                this.amqpConnectionManager.publish(this.exchangeConfiguration,
                        ROUTING_KEY_PREFIX + sanitizeEventClass(eventClass), eventSummary);
            } catch (AmqpException e) {
                lastException = e;
                exceptionCount++;
            }
        }
        context.setPluginState(this, null);
        if (exceptionCount > 0 && lastException != null) {
            throw new ZepException(lastException);
        }
    }

    private static String sanitizeEventClass(String eventClass) {
        final int length = eventClass.length();
        final StringBuilder sb = new StringBuilder(length);
        int startIndex = 0;
        if (eventClass.charAt(0) == '/') {
            startIndex = 1;
        }
        while (startIndex < length) {
            char ch = Character.toLowerCase(eventClass.charAt(startIndex));
            if (ch == '/') {
                sb.append('.');
            } else if (ch == ' ') {
                sb.append('_');
            } else {
                sb.append(ch);
            }
            ++startIndex;
        }
        return sb.toString();
    }
}
