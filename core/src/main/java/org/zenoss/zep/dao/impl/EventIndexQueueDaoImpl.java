/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventIndexHandler;
import org.zenoss.zep.dao.EventIndexQueueDao;
import org.zenoss.zep.dao.IndexQueueID;
import org.zenoss.zep.events.EventIndexQueueSizeEvent;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;

/**
 * Implementation of EventIndexQueueDao.
 */
public class EventIndexQueueDaoImpl implements EventIndexQueueDao, ApplicationEventPublisherAware {

    private final IndexDaoDelegate  indexDaoDelegate;
    private ApplicationEventPublisher applicationEventPublisher;

    private MetricRegistry metrics;
    private Counter indexedCounter;
    private long lastQueueSize = -1;

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    public EventIndexQueueDaoImpl(IndexDaoDelegate indexDaoDelegate) {
        this.indexDaoDelegate = indexDaoDelegate;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public List<IndexQueueID> indexEvents(final EventIndexHandler handler, final int limit) throws ZepException {
        return indexEvents(handler, limit, -1L);
    }

    @Resource(name = "metrics")
    public void setBean(MetricRegistry metrics) {
        this.metrics = metrics;
        String metricName = "";
        String baseName = this.getClass().getCanonicalName();
        metricName = MetricRegistry.name(baseName, indexDaoDelegate.getQueueName(), "size");
        this.indexedCounter = metrics.counter(MetricRegistry.name(baseName, indexDaoDelegate.getQueueName(), "indexed"));
        this.metrics.register(metricName, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return lastQueueSize;
            }
        });
    }

    @Override
    @TransactionalRollbackAllExceptions
    public List<IndexQueueID> indexEvents(final EventIndexHandler handler, final int limit,
                                          final long maxUpdateTime) throws ZepException {


        PollEvents pollEvents = indexDaoDelegate.pollEvents(limit, maxUpdateTime).invoke();
        List<EventSummary> indexed = pollEvents.getIndexed();
        Set<String> deleted = pollEvents.getDeleted();
        List<IndexQueueID> eventsIDs = pollEvents.getIndexQueueIds();

        if (!indexed.isEmpty()) {
            try {
                handler.prepareToHandle(indexed);
            } catch (Exception e) {
                throw new RuntimeException(e.getLocalizedMessage(), e);
            }
        }
        for (EventSummary summary : indexed) {
            try {
                handler.handle(summary);
            } catch (Exception e) {
                throw new RuntimeException(e.getLocalizedMessage(), e);
            }
        }
        for (String iqUuid : deleted) {
            try {
                handler.handleDeleted(iqUuid);
            } catch (Exception e) {
                throw new RuntimeException(e.getLocalizedMessage(), e);
            }
        }
        if (!eventsIDs.isEmpty()) {
            try {
                handler.handleComplete();
            } catch (Exception e) {
                throw new ZepException(e.getLocalizedMessage(), e);
            }
        }

        // publish current size of event_*_index_queue
        this.lastQueueSize = indexDaoDelegate.getQueueLength();
        this.applicationEventPublisher.publishEvent(
                new EventIndexQueueSizeEvent(this, indexDaoDelegate.getQueueName(), this.lastQueueSize, limit)
        );

        this.indexedCounter.inc(eventsIDs.size());
        return eventsIDs;
    }

    @Override
    public void queueEvents(List<String> uuids, long timestamp) {
        indexDaoDelegate.queueEvents(uuids, timestamp);
    }

    @Override
    public long getQueueLength() {
        return indexDaoDelegate.getQueueLength();
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void deleteIndexQueueIds(List<IndexQueueID> queueIds) throws ZepException {
        indexDaoDelegate.deleteIndexQueueIds(queueIds);
    }

    public static interface PollEvents {
        List<IndexQueueID> getIndexQueueIds();

        List<EventSummary> getIndexed();

        Set<String> getDeleted();

        PollEvents invoke() throws ZepException;
    }


}
