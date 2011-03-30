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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.scheduling.TaskScheduler;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.ZepConfig;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.ConfigDao;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventDao;
import org.zenoss.zep.dao.EventDetailsConfigDao;
import org.zenoss.zep.dao.EventStoreDao;
import org.zenoss.zep.dao.Purgable;
import org.zenoss.zep.events.IndexDetailsUpdatedEvent;
import org.zenoss.zep.events.ZepConfigUpdatedEvent;
import org.zenoss.zep.events.ZepEvent;
import org.zenoss.zep.index.EventIndexer;

import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Represents core application logic for ZEP, including the scheduled aging and
 * purging of events.
 */
public class Application implements ApplicationListener<ZepEvent> {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    @Autowired
    private TaskScheduler scheduler;
    private ScheduledFuture<?> eventSummaryAger = null;
    private ScheduledFuture<?> eventSummaryArchiver = null;
    private ScheduledFuture<?> eventArchivePurger = null;
    private ScheduledFuture<?> eventPurger = null;
    private ScheduledFuture<?> eventIndexerFuture = null;
    private ZepConfig oldConfig = null;
    private ZepConfig config;

    private int indexIntervalSeconds = 1;

    private final boolean enableIndexing;

    private ConfigDao configDao;
    private EventDao eventDao;
    private EventStoreDao eventStoreDao;
    private EventArchiveDao eventArchiveDao;
    private EventIndexer eventIndexer;
    private EventDetailsConfigDao eventDetailsConfigDao;

    public Application(boolean enableIndexing) {
        this.enableIndexing = enableIndexing;
    }

    public void setEventStoreDao(EventStoreDao eventStoreDao) {
        this.eventStoreDao = eventStoreDao;
    }

    public void setEventArchiveDao(EventArchiveDao eventArchiveDao) {
        this.eventArchiveDao = eventArchiveDao;
    }

    public void setConfigDao(ConfigDao configDao) {
        this.configDao = configDao;
    }

    public void setEventDao(EventDao eventDao) {
        this.eventDao = eventDao;
    }
    
    public void setEventIndexer(EventIndexer eventIndexer) {
        this.eventIndexer = eventIndexer;
    }

    public void setIndexIntervalSeconds(int indexIntervalSeconds) {
        this.indexIntervalSeconds = indexIntervalSeconds;
    }

    public void setEventDetailsConfigDao(EventDetailsConfigDao eventDetailsConfigDao) {
        this.eventDetailsConfigDao = eventDetailsConfigDao;
    }

    public void init() throws ZepException {
        logger.info("Initializing ZEP");
        this.config = configDao.getConfig();

        // Initialize the default event details
        this.eventDetailsConfigDao.init();
        
        /*
         * We must initialize partitions first to ensure events have a partition
         * where they can be created before we start processing the queue. This
         * init method is run before the event processor starts due to a hard
         * dependency in the Spring config on this.
         */
        initializePartitions();
        startEventSummaryAging();
        startEventSummaryArchiving();
        startEventArchivePurging();
        startEventPurging();
        startEventIndexer();
        logger.info("Completed ZEP initialization");
    }

    public void shutdown() throws InterruptedException {
        cancelFuture(this.eventIndexerFuture);
        this.eventIndexerFuture = null;

        cancelFuture(this.eventPurger);
        this.eventPurger = null;

        cancelFuture(this.eventArchivePurger);
        this.eventArchivePurger = null;

        cancelFuture(this.eventSummaryArchiver);
        this.eventSummaryArchiver = null;

        cancelFuture(this.eventSummaryAger);
        this.eventSummaryAger = null;
    }

    private void cancelFuture(Future<?> future) {
        if (future != null) {
            future.cancel(false);
            try {
                future.get();
            } catch (CancellationException e) {
                // Expected exception - we just canceled above
            } catch (ExecutionException e) {
                logger.warn("exception", e);
            } catch (InterruptedException e) {
                logger.debug("Interrupted", e);
            }
        }
    }

    private void initializePartitions() throws ZepException {
        eventArchiveDao.initializePartitions();
        eventDao.initializePartitions();
    }

    private void startEventIndexer() throws ZepException {
        cancelFuture(this.eventIndexerFuture);
        this.eventIndexerFuture = null;

        // Rebuild the index if necessary.
        this.eventIndexer.init();

        if (this.enableIndexing) {
            logger.info("Starting event indexing at interval: {} second(s)", this.indexIntervalSeconds);
            Date startTime = new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(this.indexIntervalSeconds));
            this.eventIndexerFuture = scheduler.scheduleWithFixedDelay(
                    new ThreadRenamingRunnable(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                final long now = System.currentTimeMillis();
                                int numIndexed;
                                while ((numIndexed = eventIndexer.index(now)) > 0) {
                                    logger.debug("Indexed {} events", numIndexed);
                                }
                            } catch (TransientDataAccessException e) {
                                logger.debug("Transient failure indexing events", e);
                            } catch (Exception e) {
                                logger.warn("Failed to index events", e);
                            }
                        }
                    }, "ZEP_EVENT_INDEXER"), startTime, TimeUnit.SECONDS.toMillis(this.indexIntervalSeconds));
        }
    }

    private void startEventSummaryAging() {
        final int duration = config.getEventAgeIntervalMinutes();
        final EventSeverity severity = config.getEventAgeDisableSeverity();
        if (oldConfig != null
                && duration == oldConfig.getEventAgeIntervalMinutes()
                && severity == oldConfig.getEventAgeDisableSeverity()) {
            logger.info("Event aging configuration not changed.");
            return;
        }

        cancelFuture(this.eventSummaryAger);
        this.eventSummaryAger = null;

        if (duration > 0) {
            logger.info("Starting event aging at interval: {} minute(s)", duration);
            Date startTime = new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1));
            this.eventSummaryAger = scheduler.scheduleWithFixedDelay(
                    new ThreadRenamingRunnable(new Runnable() {
                        @Override
                        public void run() {
                            logger.info("Aging events");
                            try {
                                int numAged;
                                do {
                                    numAged = eventStoreDao.ageEvents(duration,
                                            TimeUnit.MINUTES, severity,
                                            100);
                                } while (numAged > 0);
                            } catch (Exception e) {
                                logger.warn("Failed to age events", e);
                            }
                        }
                    }, "ZEP_EVENT_AGER"), startTime, TimeUnit.MINUTES.toMillis(duration));
        } else {
            logger.info("Event aging disabled");
        }
    }

    private void startEventSummaryArchiving() {
        final int duration = config.getEventArchiveIntervalDays();
        if (oldConfig != null && duration == oldConfig.getEventArchiveIntervalDays()) {
            logger.info("Event archiving configuration not changed.");
            return;
        }

        cancelFuture(this.eventSummaryArchiver);
        this.eventSummaryArchiver = null;
        if (duration > 0) {
            logger.info("Starting event archiving at interval: {} days(s)",
                    duration);
            this.eventSummaryArchiver = scheduler.scheduleWithFixedDelay(
                    new ThreadRenamingRunnable(new Runnable() {
                        @Override
                        public void run() {
                            logger.info("Archiving events");
                            try {
                                int numArchived;
                                do {
                                    numArchived = eventStoreDao.archive(
                                            duration,
                                            TimeUnit.DAYS, 100);
                                } while (numArchived > 0);
                            } catch (Exception e) {
                                logger.warn("Failed to archive events", e);
                            }
                        }
                    }, "ZEP_EVENT_ARCHIVER"), TimeUnit.DAYS.toMillis(1));
        } else {
            logger.info("Event archiving disabled");
        }
    }

    private ScheduledFuture<?> purge(final Purgable purgable,
                                     final int purgeDuration, final TimeUnit purgeUnit, long delayInMs,
                                     String threadName) {
        final Date startTime = new Date(System.currentTimeMillis() + 60000L);
        return scheduler.scheduleWithFixedDelay(
                new ThreadRenamingRunnable(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            purgable.purge(purgeDuration, purgeUnit);
                        } catch (Exception e) {
                            logger.warn("Failed purging", e);
                        }
                    }
                }, threadName), startTime, delayInMs);
    }

    private void startEventArchivePurging() {
        final int duration = config.getEventArchivePurgeIntervalDays();
        if (oldConfig != null
                && duration == oldConfig.getEventArchivePurgeIntervalDays()) {
            logger.info("Event archive purging configuration not changed.");
            return;
        }
        cancelFuture(this.eventArchivePurger);
        this.eventArchivePurger = purge(eventStoreDao, duration,
                TimeUnit.DAYS,
                eventArchiveDao.getPartitionIntervalInMs(),
                "ZEP_EVENT_ARCHIVE_PURGER");
    }

    private void startEventPurging() {
        final int duration = config.getEventOccurrencePurgeIntervalDays();
        if (oldConfig != null
                && duration == oldConfig.getEventOccurrencePurgeIntervalDays()) {
            logger.info("Event occurrence purging configuration not changed.");
            return;
        }
        cancelFuture(this.eventPurger);
        this.eventPurger = purge(eventDao, duration,
                TimeUnit.DAYS,
                eventDao.getPartitionIntervalInMs(), "ZEP_EVENT_PURGER");
    }

    @Override
    public synchronized void onApplicationEvent(ZepEvent event) {
        if (event instanceof ZepConfigUpdatedEvent) {
            ZepConfigUpdatedEvent configUpdatedEvent = (ZepConfigUpdatedEvent) event;
            this.config = configUpdatedEvent.getConfig();
            logger.info("Configuration changed: {}", this.config);
            startEventSummaryAging();
            startEventSummaryArchiving();
            startEventArchivePurging();
            startEventPurging();
            this.oldConfig = config;
        }
        else if (event instanceof IndexDetailsUpdatedEvent) {
            try {
                startEventIndexer();
            } catch (ZepException e) {
                logger.warn("Failed to restart event indexing", e);
            }
        }
    }
}
