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

import static org.zenoss.zep.ConfigConstants.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.ConfigDao;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventDao;
import org.zenoss.zep.dao.EventStoreDao;
import org.zenoss.zep.dao.Purgable;
import org.zenoss.zep.events.ConfigUpdatedEvent;
import org.zenoss.zep.index.EventIndexer;

/**
 * Represents core application logic for ZEP, including the scheduled aging and
 * purging of events.
 */
public class Application implements ApplicationListener<ConfigUpdatedEvent> {

    private static final Logger logger = LoggerFactory
            .getLogger(Application.class);

    private final int indexInterval;
    private ScheduledThreadPoolExecutor threadPool = null;
    private ScheduledFuture<?> eventSummaryAger = null;
    private ScheduledFuture<?> eventSummaryArchiver = null;
    private ScheduledFuture<?> eventArchivePurger = null;
    private ScheduledFuture<?> eventPurger = null;
    private ScheduledFuture<?> eventIndexerFuture = null;
    private AppConfig oldConfig = null;
    private AppConfig config;

    private ConfigDao configDao;
    private EventDao eventDao;
    private EventStoreDao eventStoreDao;
    private EventArchiveDao eventArchiveDao;
    private EventIndexer eventIndexer;

    public Application(int indexInterval) {
        this.indexInterval = indexInterval;
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

    public synchronized void init() throws ZepException {
        logger.info("Initializing ZEP");
        this.config = new AppConfig(configDao.getConfig());
        threadPool = new ScheduledThreadPoolExecutor(5);
        threadPool.setKeepAliveTime(30L, TimeUnit.SECONDS);
        threadPool.allowCoreThreadTimeOut(true);
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
    }

    public synchronized void shutdown() throws InterruptedException {
        cancelFuture(this.eventSummaryAger);
        this.eventSummaryAger = null;

        cancelFuture(this.eventSummaryArchiver);
        this.eventSummaryArchiver = null;

        cancelFuture(this.eventArchivePurger);
        this.eventArchivePurger = null;

        cancelFuture(this.eventPurger);
        this.eventPurger = null;

        cancelFuture(this.eventIndexerFuture);
        this.eventIndexer = null;

        this.threadPool.shutdownNow();
        this.threadPool.awaitTermination(0L, TimeUnit.SECONDS);
    }

    private void cancelFuture(Future<?> future) {
        if (future != null) {
            future.cancel(true);
            try {
                future.get();
            } catch (CancellationException e) {
                logger.debug("Canceled future", e);
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

    private void startEventIndexer() {
        cancelFuture(this.eventIndexerFuture);
        this.eventIndexerFuture = null;

        logger.info("Starting event indexing at interval: {} second(s)",
                this.indexInterval);
        this.eventIndexerFuture = this.threadPool.scheduleWithFixedDelay(
                new ThreadRenamingRunnable(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            eventIndexer.index();
                        } catch (Exception e) {
                            logger.warn("Failed to index events", e);
                            /* If we fail to index, make sure we retry again next time */
                            eventIndexer.markSummaryDirty();
                            eventIndexer.markArchiveDirty();
                        }
                    }
                }, "ZEP_EVENT_INDEXING_THREAD"), this.indexInterval,
                this.indexInterval, TimeUnit.SECONDS);
    }

    private void startEventSummaryAging() {
        final int duration = config.getEventSummaryAgingInterval();
        final EventSeverity severity = config.getEventSummaryAgingSeverity();
        if (oldConfig != null
                && duration == oldConfig.getEventSummaryAgingInterval()
                && severity == oldConfig.getEventSummaryAgingSeverity()) {
            logger.info("Event aging configuration not changed.");
            return;
        }

        cancelFuture(this.eventSummaryAger);
        this.eventSummaryAger = null;

        if (duration > 0) {
            logger.info("Starting event aging at interval: {} minute(s)",
                    duration);
            this.eventSummaryAger = this.threadPool.scheduleWithFixedDelay(
                    new ThreadRenamingRunnable(new Runnable() {
                        @Override
                        public void run() {
                            logger.info("Aging events");
                            try {
                                int numAged;
                                do {
                                    numAged = eventStoreDao.ageEvents(duration,
                                            EVENT_AGE_INTERVAL_UNIT, severity,
                                            100);
                                } while (numAged > 0);
                            } catch (Exception e) {
                                logger.warn("Failed to age events", e);
                            }
                        }
                    }, "ZEP_EVENT_AGING_THREAD"), 1L, duration,
                    TimeUnit.MINUTES);
        } else {
            logger.info("Event aging disabled");
        }
    }

    private void startEventSummaryArchiving() {
        final int duration = config.getEventSummaryArchiveInterval();
        if (oldConfig != null
                && duration == oldConfig.getEventSummaryArchiveInterval()) {
            logger.info("Event archiving configuration not changed.");
            return;
        }

        cancelFuture(this.eventSummaryArchiver);
        this.eventSummaryArchiver = null;
        if (duration > 0) {
            logger.info("Starting event archiving at interval: {} days(s)",
                    duration);
            this.eventSummaryArchiver = this.threadPool.scheduleWithFixedDelay(
                    new ThreadRenamingRunnable(new Runnable() {
                        @Override
                        public void run() {
                            logger.info("Archiving events");
                            try {
                                int numArchived;
                                do {
                                    numArchived = eventStoreDao.archive(
                                            duration,
                                            EVENT_ARCHIVE_INTERVAL_UNIT, 100);
                                } while (numArchived > 0);
                            } catch (Exception e) {
                                logger.warn("Failed to archive events", e);
                            }
                        }
                    }, "ZEP_EVENT_ARCHIVING_THREAD"), 0L, 1L,
                    EVENT_ARCHIVE_INTERVAL_UNIT);
        } else {
            logger.info("Event archiving disabled");
        }
    }

    private ScheduledFuture<?> purge(final Purgable purgable,
            final int purgeDuration, final TimeUnit purgeUnit, long delayInMs,
            String threadName) {
        return this.threadPool.scheduleWithFixedDelay(
                new ThreadRenamingRunnable(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            purgable.purge(purgeDuration, purgeUnit);
                        } catch (Exception e) {
                            logger.warn("Failed purging", e);
                        }
                    }
                }, threadName), 60000L, delayInMs, TimeUnit.MILLISECONDS);
    }

    private void startEventArchivePurging() {
        final int duration = config.getEventArchivePurgingInterval();
        if (oldConfig != null
                && duration == oldConfig.getEventArchivePurgingInterval()) {
            logger.info("Event archive purging configuration not changed.");
            return;
        }
        cancelFuture(this.eventArchivePurger);
        this.eventArchivePurger = purge(eventStoreDao, duration,
                EVENT_ARCHIVE_PURGE_INTERVAL_UNIT,
                eventArchiveDao.getPartitionIntervalInMs(),
                "EVENT_ARCHIVE_PURGER");
    }

    private void startEventPurging() {
        final int duration = config.getEventOccurrencePurgingInterval();
        if (oldConfig != null
                && duration == oldConfig.getEventOccurrencePurgingInterval()) {
            logger.info("Event occurrence purging configuration not changed.");
            return;
        }
        cancelFuture(this.eventPurger);
        this.eventPurger = purge(eventDao, duration,
                EVENT_OCCURRENCE_PURGE_INTERVAL_UNIT,
                eventDao.getPartitionIntervalInMs(), "EVENT_PURGER");
    }

    @Override
    public synchronized void onApplicationEvent(ConfigUpdatedEvent event) {
        logger.info("Configuration changed: {}", event.getConfig());
        if (event.isMultiple()) {
            this.config = new AppConfig(event.getConfig());
        } else {
            Map.Entry<String, String> cfg = event.getConfig().entrySet()
                    .iterator().next();
            String key = cfg.getKey();
            String value = cfg.getValue();
            if (value == null) {
                this.config.remove(key);
            } else {
                this.config.put(key, value);
            }
        }
        startEventSummaryAging();
        startEventSummaryArchiving();
        startEventArchivePurging();
        startEventPurging();
        this.oldConfig = new AppConfig(config.getConfig());
    }

    private static class AppConfig {
        private final Map<String, String> config;

        public AppConfig(Map<String, String> config) {
            this.config = new HashMap<String, String>(config);
        }

        public Map<String, String> getConfig() {
            return config;
        }

        public String remove(String key) {
            return this.config.remove(key);
        }

        public void put(String key, String val) {
            this.config.put(key, val);
        }

        public int getEventSummaryAgingInterval() {
            return getConfigInt(config, CONFIG_EVENT_AGE_INTERVAL_MINUTES,
                    DEFAULT_EVENT_AGE_INTERVAL_MINUTES);
        }

        public EventSeverity getEventSummaryAgingSeverity() {
            EventSeverity severity = null;
            String strAgingSeverity = config
                    .get(CONFIG_EVENT_AGE_DISABLE_SEVERITY);
            if (strAgingSeverity != null) {
                try {
                    severity = EventSeverity.valueOf(strAgingSeverity);
                } catch (Exception e) {
                    logger.warn("Invalid event aging value: {}", strAgingSeverity);
                }
            }
            if (severity == null) {
                severity = DEFAULT_EVENT_AGE_DISABLE_SEVERITY;
            }
            return severity;
        }

        public int getEventSummaryArchiveInterval() {
            return getConfigInt(config, CONFIG_EVENT_ARCHIVE_INTERVAL_DAYS,
                    DEFAULT_EVENT_ARCHIVE_INTERVAL_DAYS);
        }

        public int getEventArchivePurgingInterval() {
            return getConfigInt(config,
                    CONFIG_EVENT_ARCHIVE_PURGE_INTERVAL_DAYS,
                    DEFAULT_EVENT_ARCHIVE_PURGE_INTERVAL_DAYS);
        }

        public int getEventOccurrencePurgingInterval() {
            return getConfigInt(config,
                    CONFIG_EVENT_OCCURRENCE_PURGE_INTERVAL_DAYS,
                    DEFAULT_EVENT_OCCURRENCE_PURGE_INTERVAL_DAYS);
        }

        private static int getConfigInt(Map<String, String> config,
                String configName, int defaultValue) {
            int value = defaultValue;
            String strValue = config.get(configName);
            if (strValue != null) {
                try {
                    value = Integer.valueOf(strValue);
                } catch (NumberFormatException nfe) {
                    logger.warn("Invalid value {} for configuration entry {}",
                            strValue, configName);
                }
            }
            return value;
        }
    }

    /**
     * Renames threads while they are running, then restores the name back to
     * the original.
     */
    private static class ThreadRenamingRunnable implements Runnable {

        private final Runnable runnable;
        private final String name;

        public ThreadRenamingRunnable(Runnable runnable, String name) {
            if (runnable == null || name == null) {
                throw new NullPointerException();
            }
            this.runnable = runnable;
            this.name = name;
        }

        @Override
        public void run() {
            final String previousName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(this.name);
            } catch (SecurityException e) {
                logger.debug("Exception changing name", e);
            }
            try {
                this.runnable.run();
            } finally {
                try {
                    Thread.currentThread().setName(previousName);
                } catch (SecurityException e) {
                    logger.debug("Exception changing name", e);
                }
            }
        }
    }
}
