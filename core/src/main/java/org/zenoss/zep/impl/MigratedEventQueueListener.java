/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.impl;

import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.Channel;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventStoreDao;
import org.zenoss.zep.dao.EventSummaryBaseDao;

/**
 * Used to import migrated events into ZEP.
 */
public class MigratedEventQueueListener extends AbstractQueueListener {

    private static final Logger logger = LoggerFactory.getLogger(MigratedEventQueueListener.class);

    private String queueIdentifier;
    private int prefetchCount = 100;
    private EventSummaryBaseDao eventSummaryBaseDao;
    private EventStoreDao eventStoreDao;

    public void setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
    }

    public void setEventSummaryBaseDao(EventSummaryBaseDao eventSummaryBaseDao) {
        this.eventSummaryBaseDao = eventSummaryBaseDao;
    }

    public void setEventStoreDao(EventStoreDao eventStoreDao) {
        this.eventStoreDao = eventStoreDao;
    }

    public void setQueueIdentifier(String queueIdentifier) {
        this.queueIdentifier = queueIdentifier;
    }

    @Override
    protected void configureChannel(Channel channel) throws AmqpException {
        logger.debug("Using prefetch count: {} for queue: {}", this.prefetchCount, getQueueIdentifier());
        channel.setQos(0, this.prefetchCount);
    }

    @Override
    protected String getQueueIdentifier() {
        return this.queueIdentifier;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void handle(Message message) throws Exception {
        EventSummary summary = (EventSummary) message;
        try {
            // ZEN-5286 - Avoid duplicate UUIDs stored in event_summary/event_archive.
            // This check is prone to race conditions but this is worked around by having a single threaded
            // executor processing all migrated events (in both the summary and archive queues). Multi-instance
            // ZEP (if implemented) should come up with a better protection for this to ensure that duplicate
            // events are never imported into the system.
            if (this.eventStoreDao.findByUuid(summary.getUuid()) != null) {
                throw new DuplicateKeyException("Found existing event");
            }
            this.eventSummaryBaseDao.importEvent(summary);
        } catch (DuplicateKeyException e) {
            // Create event summary entry - if we get a duplicate key exception just skip importing this event as it
            // either has already been imported or there is already an active event with the same fingerprint.
            logger.info("Event with UUID {} already exists in database - skipping", summary.getUuid());
        }
    }
}
