/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.amqp.Consumer;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.dao.EventSummaryBaseDao;

/**
 * Used to import migrated events into ZEP.
 */
public class MigratedEventQueueListener extends AbstractEventQueueListener {

    private static final Logger logger = LoggerFactory.getLogger(MigratedEventQueueListener.class);

    private final String queueIdentifier;
    private EventSummaryBaseDao eventSummaryBaseDao;

    public MigratedEventQueueListener(String queueIdentifier) {
        this.queueIdentifier = queueIdentifier;
    }

    public void setEventSummaryBaseDao(EventSummaryBaseDao eventSummaryBaseDao) {
        this.eventSummaryBaseDao = eventSummaryBaseDao;
    }

    @Override
    protected String getQueueIdentifier() {
        return this.queueIdentifier;
    }

    @Override
    public void handle(Message message) throws Exception {
        EventSummary summary = (EventSummary) message;
        this.eventSummaryBaseDao.importEvent(summary);
    }

    @Override
    protected void receive(org.zenoss.amqp.Message<Message> message, Consumer<Message> consumer) throws Exception {
        try {
            handle(message.getBody());
            consumer.ackMessage(message);
        } catch (Exception e) {
            if (isTransientException(e)) {
                /* Re-queue the message if we get a temporary database failure */
                logger.debug("Transient database exception", e);
                logger.debug("Re-queueing message due to transient failure: {}", message);
                rejectMessage(consumer, message, true);
            } else {
                /* TODO: Dead letter queue or other safety net? */
                logger.warn("Failed processing message: " + message, e);
                rejectMessage(consumer, message, false);
            }
        }
    }
}
