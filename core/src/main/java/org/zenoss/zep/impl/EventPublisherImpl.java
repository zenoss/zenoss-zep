/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import org.zenoss.amqp.AmqpConnectionManager;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.ExchangeConfiguration;
import org.zenoss.amqp.ZenossQueueConfig;
import org.zenoss.protobufs.zep.Zep.RawEvent;
import org.zenoss.zep.EventPublisher;
import org.zenoss.zep.ZepException;

import java.io.IOException;

/**
 * Raw event publisher.
 */
public class EventPublisherImpl implements EventPublisher {

    private final ExchangeConfiguration exchangeConfiguration;
    private AmqpConnectionManager connectionManager;

    public EventPublisherImpl() throws IOException {
        this.exchangeConfiguration = ZenossQueueConfig.getConfig().getExchange("$RawZenEvents");
    }

    public void setAmqpConnectionManager(AmqpConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public void publishRawEvent(RawEvent rawEvent) throws ZepException {
        try {
            this.connectionManager.publish(exchangeConfiguration, createRoutingKey(rawEvent), rawEvent);
        } catch (AmqpException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }

    private static String createRoutingKey(RawEvent rawEvent) {
        final StringBuilder sb = new StringBuilder("zenoss.zenevent");
        final String eventClass = rawEvent.getEventClass();
        if (eventClass.isEmpty()) {
            sb.append(".unknown");
        }
        else {
            sb.append(eventClass.replace('/', '.').toLowerCase());
        }
        return sb.toString();
    }
}
