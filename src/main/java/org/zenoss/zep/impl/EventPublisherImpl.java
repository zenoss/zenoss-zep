/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.impl;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.amqp.AmqpConnectionManager;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.BatchPublisher;
import org.zenoss.amqp.ExchangeConfiguration;
import org.zenoss.amqp.ZenossQueueConfig;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.EventPublisher;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepUtils;

import com.google.protobuf.Message;

public class EventPublisherImpl implements EventPublisher {

    private static final Logger logger = LoggerFactory
            .getLogger(EventPublisherImpl.class);

    private AmqpConnectionManager amqpConnectionManager;

    private BatchPublisher<Message> publisher;

    private final ExchangeConfiguration exchangeConfiguration;

    public EventPublisherImpl() throws IOException {
        this.exchangeConfiguration = ZenossQueueConfig.getConfig().getExchange(
                "$ProcessedZenEvents");
    }

    public void setAmqpConnectionManager(
            AmqpConnectionManager amqpConnectionManager) {
        this.amqpConnectionManager = amqpConnectionManager;
    }

    @Override
    public void addEvent(EventSummary event) throws ZepException {
        if (this.publisher == null) {
            try {
                this.publisher = this.amqpConnectionManager
                        .createBatchPublisher(this.exchangeConfiguration);
            } catch (AmqpException e) {
                ZepUtils.close(this.publisher);
                this.publisher = null;
                throw new ZepException(e.getLocalizedMessage(), e);
            }
        }
        final String eventClass = event.getOccurrence(0).getEventClass();
        try {
            logger.debug("Publishing event to fan-out exchange: {}", event);
            this.publisher.publish(event, "zenoss.zenevent."
                    + sanitizeEventClass(eventClass));
        } catch (AmqpException e) {
            ZepUtils.close(this.publisher);
            this.publisher = null;
            throw new ZepException(e);
        }
    }

    @Override
    public void publish() throws ZepException {
        try {
            this.publisher.commit();
        } catch (AmqpException e) {
            ZepUtils.close(this.publisher);
            this.publisher = null;
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }

    private static String sanitizeEventClass(String eventClass) {
        StringBuilder sb = new StringBuilder(eventClass.length());
        int startIndex = 0;
        if (eventClass.charAt(0) == '/') {
            startIndex = 1;
        }
        while (startIndex < eventClass.length()) {
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
