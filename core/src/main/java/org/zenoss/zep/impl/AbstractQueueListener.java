/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.impl;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.TransientDataAccessException;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.Consumer;
import org.zenoss.amqp.Message;
import org.zenoss.amqp.QueueListener;
import org.zenoss.zep.ZepUtils;
import org.zenoss.zep.dao.impl.DaoUtils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

public abstract class AbstractQueueListener extends QueueListener {

    private static final Logger logger = LoggerFactory.getLogger(AbstractQueueListener.class);

    protected ExecutorService executorService;

    @Autowired
    protected MetricRegistry metricRegistry;

    private final String ackMessageTimerName     = this.getClass().getName() + ".ackMessage";
    private final String handleMessageTimerName  = this.getClass().getName() + ".handleMessage";
    private final String receiveMessageTimerName = this.getClass().getName() + ".receiveMessage";
    private final String rejectMessageTimerName  = this.getClass().getName() + ".rejectMessage";

    protected abstract String getQueueIdentifier();


    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }
    protected void rejectMessage(final Consumer<?> consumer, final Message<?> message, final boolean requeue) {
        try {
            metricRegistry.timer(rejectMessageTimerName).time(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    try {
                        consumer.rejectMessage(message, requeue);
                    } catch (AmqpException e) {
                        logger.warn("Failed rejecting message", e);
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void receive(final Message<com.google.protobuf.Message> message,
            final Consumer<com.google.protobuf.Message> consumer) throws Exception {
        metricRegistry.timer(receiveMessageTimerName).time(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                AbstractQueueListener.this.executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            DaoUtils.deadlockRetry(new Callable<Object>() {
                                @Override
                                public Object call() throws Exception {
                                    metricRegistry.timer(handleMessageTimerName).time(new Callable<Object>() {
                                        @Override
                                        public Object call() throws Exception {
                                            handle(message.getBody());
                                            return null;
                                        }
                                    });
                                    return null;
                                }
                            });
                            metricRegistry.timer(ackMessageTimerName).time(new Callable<Object>() {
                                @Override
                                public Object call() throws Exception {
                                    consumer.ackMessage(message);
                                    return null;
                                }
                            });
                        } catch (Exception e) {
                            if (ZepUtils.isExceptionOfType(e, TransientDataAccessException.class)) {
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
                return null;
            }
        });
    }
}
