/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.zenoss.amqp.AmqpConnectionManager;
import org.zenoss.amqp.QueueConfig;
import org.zenoss.amqp.QueueConfiguration;
import org.zenoss.amqp.ZenossQueueConfig;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.ZepConfig;
import org.zenoss.zep.HeartbeatProcessor;
import org.zenoss.zep.PluginService;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.ConfigDao;
import org.zenoss.zep.dao.DBMaintenanceService;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventStoreDao;
import org.zenoss.zep.dao.EventTimeDao;
import org.zenoss.zep.dao.Purgable;
import org.zenoss.zep.dao.impl.DaoUtils;
import org.zenoss.zep.events.ZepConfigUpdatedEvent;
import org.zenoss.zep.index.EventIndexRebuilder;
import org.zenoss.zep.index.EventIndexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents core application logic for ZEP, including the scheduled aging and
 * purging of events.
 */
public class Application implements ApplicationEventPublisherAware, ApplicationContextAware, ApplicationListener<ApplicationEvent> {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private ThreadPoolTaskScheduler scheduler;
    private ScheduledFuture<?> eventSummaryAger = null;
    private ScheduledFuture<?> eventSummaryArchiver = null;
    private ScheduledFuture<?> eventArchivePurger = null;
    private ScheduledFuture<?> eventTimePurger = null;
    private ScheduledFuture<?> heartbeatFuture = null;
    private ScheduledFuture<?> dbMaintenanceFuture = null;
    private ZepConfig oldConfig = null;
    private ZepConfig config;

    private int heartbeatIntervalSeconds = 60;

    private long dbMaintenanceIntervalMinutes = 3600;

    private AmqpConnectionManager amqpConnectionManager;
    private ConfigDao configDao;
    private EventStoreDao eventStoreDao;
    private EventArchiveDao eventArchiveDao;
    private EventTimeDao eventTimeDao;
    private EventIndexer eventSummaryIndexer;
    private EventIndexRebuilder eventSummaryRebuilder;
    private EventIndexer eventArchiveIndexer;
    private EventIndexRebuilder eventArchiveRebuilder;
    private DBMaintenanceService dbMaintenanceService;
    private HeartbeatProcessor heartbeatProcessor;
    private PluginService pluginService;
    private ApplicationContext applicationContext;
    private ExecutorService queueExecutor;
    private ExecutorService migratedExecutor;

    private List<String> queueListeners = new ArrayList<String>();
    private ApplicationEventPublisher applicationEventPublisher;

    public void setEventStoreDao(EventStoreDao eventStoreDao) {
        this.eventStoreDao = eventStoreDao;
    }

    public void setEventArchiveDao(EventArchiveDao eventArchiveDao) {
        this.eventArchiveDao = eventArchiveDao;
    }

    public void setEventTimeDao(EventTimeDao eventTimeDao) {
        this.eventTimeDao = eventTimeDao;
    }

    public void setAmqpConnectionManager(AmqpConnectionManager amqpConnectionManager) {
        this.amqpConnectionManager = amqpConnectionManager;
    }

    public void setConfigDao(ConfigDao configDao) {
        this.configDao = configDao;
    }

    public void setEventSummaryIndexer(EventIndexer eventSummaryIndexer) {
        this.eventSummaryIndexer = eventSummaryIndexer;
    }

    public void setEventSummaryRebuilder(EventIndexRebuilder eventSummaryRebuilder) {
        this.eventSummaryRebuilder = eventSummaryRebuilder;
    }

    public void setEventArchiveIndexer(EventIndexer eventArchiveIndexer) {
        this.eventArchiveIndexer = eventArchiveIndexer;
    }

    public void setEventArchiveRebuilder(EventIndexRebuilder eventArchiveRebuilder) {
        this.eventArchiveRebuilder = eventArchiveRebuilder;
    }

    public void setHeartbeatProcessor(HeartbeatProcessor heartbeatProcessor) {
        this.heartbeatProcessor = heartbeatProcessor;
    }

    public void setHeartbeatIntervalSeconds(int heartbeatIntervalSeconds) {
        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    }

    public void setPluginService(PluginService pluginService) {
        this.pluginService = pluginService;
    }

    public void setQueueExecutor(ExecutorService queueExecutor) {
        this.queueExecutor = queueExecutor;
    }

    public void setScheduler(ThreadPoolTaskScheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void setDbMaintenanceService(DBMaintenanceService dbMaintenanceService) {
        this.dbMaintenanceService = dbMaintenanceService;
    }

    public void setDbMaintenanceIntervalMinutes(long dbMaintenanceIntervalMinutes) {
        this.dbMaintenanceIntervalMinutes = dbMaintenanceIntervalMinutes;
    }

    public void setMigratedExecutor(ExecutorService migratedExecutor) {
        this.migratedExecutor = migratedExecutor;
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }


    private final AtomicBoolean appInit = new AtomicBoolean(false);

    private void init() {
        if (!appInit.compareAndSet(false, true)) {
            logger.info("ZEP already initialized");
            return;
        }

        logger.info("Initializing ZEP");
        long sleep = 1;
        boolean done = false;
        try {
            while (!done) {
                try {
                    this.config = configDao.getConfig();
                    done = true;
                } catch (Exception e) {
                    logger.warn("Could not get config dao; {}; {}", e.getMessage(), e.getCause() != null ? e.getMessage() : "");
                    Thread.sleep(Math.min(sleep, 5000));
                    sleep = sleep * 2;
                }
            }
            if (!done) {
                logger.error("Could not start ZEP");
            }
            //publish ZepConfigUpdatedEvent so everyting has the freshly loaded config
            this.applicationEventPublisher.publishEvent(new ZepConfigUpdatedEvent(this, config));

        /*
         * We must initialize partitions first to ensure events have a partition
         * where they can be created before we start processing the queue. This
         * init method is run before the event processor starts due to a hard
         * dependency in the Spring config on this.
         */
            this.pluginService.initializePlugins();
            initializePartitions();
            this.eventSummaryRebuilder.init();
            this.eventArchiveRebuilder.init();
            startEventSummaryAging();
            startEventSummaryArchiving();
            startEventArchivePurging();
            startEventTimePurging();
            startDbMaintenance();
            startHeartbeatProcessing();
            startQueueListeners();
            logger.info("Completed ZEP initialization");
        } catch (Exception e) {
            logger.error("Could not start ZEP", e);
        }
    }

    private static void stopExecutor(ExecutorService executorService) {
        executorService.shutdown();
        try {
            executorService.awaitTermination(0L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
        }
    }

    public void shutdown() throws ZepException, InterruptedException {
        this.scheduler.shutdown();

        try {
            this.scheduler.getScheduledExecutor().awaitTermination(0L, TimeUnit.SECONDS);
            logger.info("Scheduled tasks finished");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        this.eventSummaryIndexer.shutdown();
        this.eventSummaryRebuilder.shutdown();
        this.eventArchiveIndexer.shutdown();
        this.eventArchiveRebuilder.shutdown();

        stopQueueListeners();

        stopExecutor(this.queueExecutor);
        stopExecutor(this.migratedExecutor);

        this.amqpConnectionManager.shutdown();

        this.pluginService.shutdown();
    }

    private void cancelFuture(Future<?> future) {
        if (future != null) {
            future.cancel(true);
            try {
                future.get();
            } catch (ExecutionException e) {
                logger.warn("exception", e);
            } catch (InterruptedException e) {
                logger.debug("Interrupted", e);
            } catch (CancellationException e) {
                /* Expected - we just canceled above */
            }
        }
    }

    private void initializePartitions() throws ZepException {
        eventArchiveDao.initializePartitions();
        eventTimeDao.initializePartitions();
    }

    private void startEventSummaryAging() {

        final int duration = config.getEventAgeIntervalMinutes();
        final EventSeverity severity = config.getEventAgeDisableSeverity();
        final boolean inclusive = config.getEventAgeSeverityInclusive();
        final long agingIntervalMilliseconds = config.getAgingIntervalMilliseconds();
        final int agingLimit = config.getAgingLimit();

        if (oldConfig != null
                && duration == oldConfig.getEventAgeIntervalMinutes()
                && severity == oldConfig.getEventAgeDisableSeverity()
                && inclusive == oldConfig.getEventAgeSeverityInclusive()
                && agingIntervalMilliseconds == oldConfig.getAgingIntervalMilliseconds()
                && agingLimit == oldConfig.getAgingLimit()
                ) {
            logger.info("Event aging configuration not changed.");
            return;
        }

        cancelFuture(this.eventSummaryAger);
        this.eventSummaryAger = null;

        if (duration > 0) {
            logger.info("Starting event aging at interval: {} milliseconds(s), inclusive severity: {}",
                    agingIntervalMilliseconds, inclusive);
            Date startTime = new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1));
            this.eventSummaryAger = scheduler.scheduleWithFixedDelay(
                    new ThreadRenamingRunnable(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                final int numAged = DaoUtils.deadlockRetry(new Callable<Integer>() {
                                    @Override
                                    public Integer call() throws Exception {
                                        return eventStoreDao.ageEvents(duration, TimeUnit.MINUTES, severity,
                                                agingLimit, inclusive);
                                    }
                                });
                                if (numAged > 0) {
                                    logger.debug("Aged {} events", numAged);
                                }
                            } catch (TransientDataAccessException e) {
                                logger.debug("Failed to age events", e);
                            } catch (Exception e) {
                                logger.warn("Failed to age events", e);
                            }
                        }
                    }, "ZEP_EVENT_AGER"), startTime, agingIntervalMilliseconds);
        } else {
            logger.info("Event aging disabled");
        }
    }

    private void startEventSummaryArchiving() {

        final int duration = config.getEventArchiveIntervalMinutes();
        final long archiveIntervalMilliseconds = config.getArchiveIntervalMilliseconds();
        final int archiveLimit = config.getArchiveLimit();


        if (oldConfig != null
                && duration == oldConfig.getEventArchiveIntervalMinutes()
                && archiveIntervalMilliseconds == oldConfig.getArchiveIntervalMilliseconds()
                && archiveLimit == oldConfig.getArchiveLimit()
                ) {
            logger.info("Event archiving configuration not changed.");
            return;
        }

        logger.info("Validating that event_summary table is in a good state");
        try {
            dbMaintenanceService.validateEventSummaryState();
        } catch (Exception e) {
            logger.error("There was an error validating the event_summary table: {} ", e.toString());
            System.exit(1);
        }

        final Date startTime = new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1));
        cancelFuture(this.eventSummaryArchiver);
        this.eventSummaryArchiver = null;
        if (duration > 0) {
            logger.info("Starting event archiving at interval: {} milliseconds(s)", archiveIntervalMilliseconds);
            this.eventSummaryArchiver = scheduler.scheduleWithFixedDelay(
                    new ThreadRenamingRunnable(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                final int numArchived = DaoUtils.deadlockRetry(new Callable<Integer>() {
                                    @Override
                                    public Integer call() throws Exception {
                                        return eventStoreDao.archive(duration, TimeUnit.MINUTES, archiveLimit);
                                    }
                                });
                                if (numArchived > 0) {
                                    logger.debug("Archived {} events", numArchived);
                                    eventArchiveIndexer.index();
                                }
                            } catch (TransientDataAccessException e) {
                                logger.debug("Failed to archive events", e);
                            } catch (Exception e) {
                                logger.error("Failed to archive events", e);
                            }
                        }
                    }, "ZEP_EVENT_ARCHIVER"), startTime, archiveIntervalMilliseconds);
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


    private void startEventTimePurging() {
        final int duration = config.getEventTimePurgeIntervalDays();
        if (oldConfig != null
                && duration == oldConfig.getEventTimePurgeIntervalDays()) {
            logger.info("Event Times purging configuration not changed.");
            return;
        }
        cancelFuture(this.eventTimePurger);
        this.eventTimePurger = purge(eventTimeDao, duration, TimeUnit.DAYS,
                eventTimeDao.getPartitionIntervalInMs(), "ZEP_EVENT_TIME_PURGER");
    }

    private void startDbMaintenance() {
        cancelFuture(this.dbMaintenanceFuture);
        this.dbMaintenanceFuture = null;

        if (this.dbMaintenanceIntervalMinutes <= 0) {
            logger.info("Database table optimization disabled.");
            return;
        }
        logger.info("Starting database table optimization at interval: {} minutes(s)",
                this.dbMaintenanceIntervalMinutes);
        final Date startTime = new Date(System.currentTimeMillis() +
                TimeUnit.MINUTES.toMillis(this.dbMaintenanceIntervalMinutes));
        this.dbMaintenanceFuture = scheduler.scheduleWithFixedDelay(new ThreadRenamingRunnable(new Runnable() {
            @Override
            public void run() {
                try {
                    logger.debug("Optimizing database tables");
                    dbMaintenanceService.optimizeTables();
                    logger.debug("Completed optimizing database tables");
                } catch (Exception e) {
                    logger.warn("Failed to optimize database tables", e);
                }
            }
        }, "ZEP_DATABASE_MAINTENANCE"), startTime, TimeUnit.MINUTES.toMillis(this.dbMaintenanceIntervalMinutes));
    }

    private void startHeartbeatProcessing() {
        cancelFuture(this.heartbeatFuture);
        this.heartbeatFuture = null;

        Date startTime = new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1));
        this.heartbeatFuture = scheduler.scheduleWithFixedDelay(new ThreadRenamingRunnable(new Runnable() {
            @Override
            public void run() {
                logger.debug("Processing heartbeats");
                try {
                    heartbeatProcessor.sendHeartbeatEvents();
                } catch (Exception e) {
                    logger.warn("Failed to process heartbeat events", e);
                }
            }
        }, "ZEP_HEARTBEAT_PROCESSOR"), startTime, TimeUnit.SECONDS.toMillis(this.heartbeatIntervalSeconds));
    }

    private void startQueueListeners() throws ZepException {
        QueueConfig queueConfig;
        try {
            queueConfig = ZenossQueueConfig.getConfig();
        } catch (IOException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
        Collection<AbstractQueueListener> queueListeners =
                applicationContext.getBeansOfType(AbstractQueueListener.class).values();
        for (AbstractQueueListener queueListener : queueListeners) {
            QueueConfiguration queue = queueConfig.getQueue(queueListener.getQueueIdentifier());
            this.queueListeners.add(this.amqpConnectionManager.addListener(queue, queueListener));
        }
    }

    private void stopQueueListeners() {
        for (String listenerId : this.queueListeners) {
            this.amqpConnectionManager.removeListener(listenerId);
        }
        this.queueListeners.clear();
    }

    public void onApplicationEvent(ApplicationEvent event) {

        if (event instanceof ZepConfigUpdatedEvent) {
            ZepConfigUpdatedEvent configUpdatedEvent = (ZepConfigUpdatedEvent) event;
            this.config = configUpdatedEvent.getConfig();
            logger.info("Configuration changed: {}", this.config);
            startEventSummaryAging();
            startEventSummaryArchiving();
            startEventArchivePurging();
            startEventTimePurging();
            this.oldConfig = config;
        } else if (event instanceof ContextRefreshedEvent) {
            this.init();
        }
    }

}
