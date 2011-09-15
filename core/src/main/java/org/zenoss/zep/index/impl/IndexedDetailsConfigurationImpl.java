/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.index.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventDetailsConfigDao;
import org.zenoss.zep.events.IndexDetailsUpdatedEvent;
import org.zenoss.zep.events.IndexRebuildRequiredEvent;
import org.zenoss.zep.index.IndexedDetailsConfiguration;

import java.util.Map;

/**
 * Used to maintain the current configuration for the indexed event details.
 */
public class IndexedDetailsConfigurationImpl implements IndexedDetailsConfiguration,
        ApplicationListener<IndexDetailsUpdatedEvent>, ApplicationEventPublisherAware {
    
    private static final Logger logger = LoggerFactory.getLogger(IndexedDetailsConfigurationImpl.class);

    private EventDetailsConfigDao eventDetailsConfigDao;

    private volatile boolean initialized = false;
    private volatile Map<String, EventDetailItem> eventDetailItemsByName;
    private ApplicationEventPublisher applicationEventPublisher;

    public void setEventDetailsConfigDao(EventDetailsConfigDao eventDetailsConfigDao) {
        this.eventDetailsConfigDao = eventDetailsConfigDao;
    }

    @Override
    public Map<String, EventDetailItem> getEventDetailItemsByName() throws ZepException {
        if (!initialized) {
            synchronized (this) {
                this.eventDetailsConfigDao.init();
                initialized = true;
            }
        }
        if (eventDetailItemsByName == null) {
            synchronized (this) {
                if (eventDetailItemsByName == null) {
                    this.eventDetailItemsByName = eventDetailsConfigDao.getEventDetailItemsByName();
                    for (EventDetailItem item : this.eventDetailItemsByName.values()) {
                        logger.info("Indexed event detail: {} ({})", item.getKey(), item.getType());
                    }
                }
            }
        }
        return this.eventDetailItemsByName;
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public void onApplicationEvent(IndexDetailsUpdatedEvent event) {
        this.eventDetailItemsByName = null;
        this.applicationEventPublisher.publishEvent(new IndexRebuildRequiredEvent(this));
    }
}
