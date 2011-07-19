/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.TransientDataAccessException;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.Consumer;
import org.zenoss.amqp.Message;
import org.zenoss.amqp.QueueListener;

import java.util.concurrent.ExecutorService;

public abstract class AbstractQueueListener extends QueueListener {

    private static final Logger logger = LoggerFactory.getLogger(AbstractQueueListener.class);

    protected ExecutorService executorService;

    protected abstract String getQueueIdentifier();

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    protected boolean isTransientException(Exception e) {
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

    protected void rejectMessage(Consumer<?> consumer, Message<?> message, boolean requeue) {
        try {
            consumer.rejectMessage(message, requeue);
        } catch (AmqpException e) {
            logger.warn("Failed rejecting message", e);
        }
    }

    @Override
    protected void receive(final Message<com.google.protobuf.Message> message,
            final Consumer<com.google.protobuf.Message> consumer) throws Exception {
        this.executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    handle(message.getBody());
                    consumer.ackMessage(message);
                } catch (Exception e) {
                    if (isTransientException(e)) {
                        /* Re-queue the message if we get a temporary database failure */
                        logger.debug("Transient database exception", e);
                        logger.debug("Re-queueing message due to transient failure: {}", message);
                        rejectMessage(consumer, message, true);
                    } else if (!message.getEnvelope().isRedeliver()) {
                        /* Attempt one redelivery of the message */
                        logger.debug("First failure processing message: " + message, e);
                        rejectMessage(consumer, message, true);
                    } else {
                        /* TODO: Dead letter queue or other safety net? */
                        logger.warn("Failed processing message: " + message, e);
                        rejectMessage(consumer, message, false);
                    }
                }
            }
        });
    }
}
