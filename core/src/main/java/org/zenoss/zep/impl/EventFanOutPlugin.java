/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.amqp.AmqpConnectionManager;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.ExchangeConfiguration;
import org.zenoss.amqp.ZenossQueueConfig;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.plugins.EventPostIndexPlugin;

import java.io.IOException;

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
    public void processEvent(EventSummary eventSummary) throws ZepException {
        final String eventClass = eventSummary.getOccurrence(0).getEventClass();
        try {
            logger.debug("Publishing event to fan-out exchange: {}", eventSummary);
            this.amqpConnectionManager.publish(this.exchangeConfiguration,
                    ROUTING_KEY_PREFIX + sanitizeEventClass(eventClass), eventSummary);
        } catch (AmqpException e) {
            throw new ZepException(e);
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
