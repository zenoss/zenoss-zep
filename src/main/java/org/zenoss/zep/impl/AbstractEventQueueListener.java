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
import org.springframework.dao.TransientDataAccessException;
import org.zenoss.amqp.AmqpConnectionManager;
import org.zenoss.amqp.Consumer;
import org.zenoss.amqp.Message;
import org.zenoss.amqp.QueueConfiguration;
import org.zenoss.amqp.QueueListener;
import org.zenoss.amqp.ZenossQueueConfig;

public abstract class AbstractEventQueueListener extends QueueListener {

    private static final Logger logger = LoggerFactory.getLogger(AbstractEventQueueListener.class);

    abstract protected String getQueueIdentifier();

    public void setConnectionManager(AmqpConnectionManager connectionManager)
            throws IOException {
        QueueConfiguration queueCfg = ZenossQueueConfig.getConfig().getQueue(
                this.getQueueIdentifier());
        logger.info("Listening for messages on queue: {}", queueCfg.getQueue()
                .getName());
        connectionManager.addListener(ZenossQueueConfig.getConfig()
                .getQueue(this.getQueueIdentifier()), this);
    }

    private static boolean isTransientException(Exception e) {
        boolean isTransient = false;
        Throwable t = e;
        while (t != null) {
            if (t instanceof TransientDataAccessException) {
                isTransient = true;
                break;
            }
            t = t.getCause();
        }
        return isTransient;
    }

    @Override
    protected void receive(Message<com.google.protobuf.Message> message,
            Consumer<com.google.protobuf.Message> consumer) throws Exception {
        try {
            handle(message.getBody());
            consumer.ackMessage(message);
        } catch (Exception e) {
            if (isTransientException(e)) {
                /* Re-queue the message if we get a temporary database failure */
                logger.warn("Transient database exception: {}", e);
                logger.info("Requeueing message due to transient failure: {}",
                        message);
                consumer.rejectMessage(message, true);
            } else if (!message.getEnvelope().isRedeliver()) {
                /* Attempt one redelivery of the message */
                logger.info("Requeueing message: {}", message);
                consumer.rejectMessage(message, true);
                throw e;
            } else {
                /* TODO: Dead letter queue or other safety net? */
                logger.warn("Rejecting message: {}", message);
                consumer.rejectMessage(message, false);
                throw e;
            }
        }
    }

}
