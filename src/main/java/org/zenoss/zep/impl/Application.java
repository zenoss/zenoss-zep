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
import org.springframework.scheduling.TaskScheduler;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.ConfigDao;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventDao;
import org.zenoss.zep.dao.EventStoreDao;
import org.zenoss.zep.dao.Purgable;
import org.zenoss.zep.events.ConfigUpdatedEvent;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.zenoss.zep.ConfigConstants.*;

/**
 * Represents core application logic for ZEP, including the scheduled aging and
 * purging of events.
 */
public class Application implements ApplicationListener<ConfigUpdatedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    @Autowired
    private TaskScheduler scheduler;
    private ScheduledFuture<?> eventSummaryAger = null;
    private ScheduledFuture<?> eventSummaryArchiver = null;
    private ScheduledFuture<?> eventArchivePurger = null;
    private ScheduledFuture<?> eventPurger = null;
    private AppConfig oldConfig = null;
    private AppConfig config;

    private ConfigDao configDao;
    private EventDao eventDao;
    private EventStoreDao eventStoreDao;
    private EventArchiveDao eventArchiveDao;

    public Application() {
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

    public void init() throws ZepException {
        logger.info("Initializing ZEP");
        this.config = new AppConfig(configDao.getConfig());
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
    }

    public void shutdown() throws InterruptedException {
        cancelFuture(this.eventSummaryAger);
        this.eventSummaryAger = null;

        cancelFuture(this.eventSummaryArchiver);
        this.eventSummaryArchiver = null;

        cancelFuture(this.eventArchivePurger);
        this.eventArchivePurger = null;

        cancelFuture(this.eventPurger);
        this.eventPurger = null;
    }

    private void cancelFuture(Future<?> future) {
        if (future != null) {
            future.cancel(true);
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
                                            EVENT_AGE_INTERVAL_UNIT, severity,
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
                                            EVENT_ARCHIVE_INTERVAL_UNIT, 100);
                                } while (numArchived > 0);
                            } catch (Exception e) {
                                logger.warn("Failed to archive events", e);
                            }
                        }
                    }, "ZEP_EVENT_ARCHIVER"), EVENT_ARCHIVE_INTERVAL_UNIT.toMillis(1));
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
                "ZEP_EVENT_ARCHIVE_PURGER");
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
                eventDao.getPartitionIntervalInMs(), "ZEP_EVENT_PURGER");
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

}
