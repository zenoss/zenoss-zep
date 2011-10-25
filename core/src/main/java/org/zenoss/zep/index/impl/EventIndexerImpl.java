/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */

package org.zenoss.zep.index.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.PluginService;
import org.zenoss.zep.ZepConstants;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventIndexHandler;
import org.zenoss.zep.dao.EventIndexQueueDao;
import org.zenoss.zep.impl.ThreadRenamingRunnable;
import org.zenoss.zep.index.EventIndexDao;
import org.zenoss.zep.index.EventIndexer;
import org.zenoss.zep.plugins.EventPostIndexContext;
import org.zenoss.zep.plugins.EventPostIndexPlugin;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class EventIndexerImpl implements EventIndexer {
    private static final Logger logger = LoggerFactory.getLogger(EventIndexerImpl.class);

    private int indexLimit = 1000;
    private long indexIntervalMilliseconds = 1000L;

    private final Object lock = new Object();
    private volatile boolean shutdown = false;
    private Future<?> indexFuture = null;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private EventIndexDao indexDao;
    private EventIndexQueueDao queueDao;
    private PluginService pluginService;

    public void setIndexIntervalMilliseconds(long indexIntervalMilliseconds) {
        this.indexIntervalMilliseconds = indexIntervalMilliseconds;
    }

    public void setIndexDao(EventIndexDao indexDao) {
        this.indexDao = indexDao;
    }
    
    public void setQueueDao(EventIndexQueueDao queueDao) {
        this.queueDao = queueDao;
    }
    
    public void setPluginService(PluginService pluginService) {
        this.pluginService = pluginService;
    }

    public void setIndexLimit(int indexLimit) {
        this.indexLimit = indexLimit;
    }

    @Override
    public synchronized void start() throws InterruptedException {
        stop();
        this.shutdown = false;
        this.indexFuture = this.executorService.submit(new ThreadRenamingRunnable(new Runnable() {
            @Override
            public void run() {
                logger.info("Indexing thread started for: {}", indexDao.getName());
                while (!shutdown) {
                    int numIndexed = 0;
                    try {
                        numIndexed = index();
                    } catch (ZepException e) {
                        logger.warn("Failed to index events", e);
                    } catch (Exception e) {
                        logger.warn("General failure indexing events", e);
                    }
                    // If we aren't shut down and we aren't processing a large backlog of events, wait to index the
                    // next batch of events after a delay.
                    if (!shutdown && numIndexed < indexLimit) {
                        synchronized (lock) {
                            try {
                                lock.wait(indexIntervalMilliseconds);
                            } catch (InterruptedException e) {
                                logger.info("Interrupted while waiting to index more events");
                            }
                        }
                    }
                }
                logger.info("Indexing thread stopped for: {}", indexDao.getName());
            }
        }, "INDEXER_" + this.indexDao.getName().toUpperCase()));
    }

    @Override
    public synchronized void stop() throws InterruptedException {
        this.shutdown = true;
        synchronized (this.lock) {
            this.lock.notifyAll();
        }
        if (this.indexFuture != null) {
            try {
                this.indexFuture.get();
            } catch (ExecutionException e) {
                logger.warn("Execution failed for index thread", e);
            } finally {
                this.indexFuture = null;
            }
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        try {
            stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            this.executorService.shutdown();
            this.executorService.awaitTermination(0L, TimeUnit.SECONDS);
        }
    }

    @Override
    public synchronized int index() throws ZepException {
        return doIndex(-1L);
    }

    @Override
    public synchronized int indexFully() throws ZepException {
        int totalIndexed = 0;
        final long now = System.currentTimeMillis();
        int numIndexed;
        do {
            numIndexed = doIndex(now);
            totalIndexed += numIndexed;
        } while (numIndexed > 0);
        
        return totalIndexed;
    }

    /**
     * When migrating events from Zenoss 3.1.x to ZEP, a detail is added to the event which is set to the initial
     * update_time value for the event. Since these events have already been processed by an earlier release of
     * Zenoss, we don't want to run post-processing on these events including triggers / fan-out.
     *
     * @param event The event.
     * @return True if post-processing should run on the event, false otherwise.
     */
    private static boolean shouldRunPostprocessing(EventSummary event) {
        boolean shouldRun = true;
        final Event occurrence = event.getOccurrence(0);
        final List<EventDetail> details = occurrence.getDetailsList();
        for (EventDetail detail : details) {
            if (ZepConstants.DETAIL_MIGRATE_UPDATE_TIME.equals(detail.getName())) {
                final List<String> values = detail.getValueList();
                if (values.size() == 1) {
                    try {
                        long migrate_update_time = Long.valueOf(values.get(0));
                        shouldRun = (migrate_update_time != event.getUpdateTime());
                    } catch (NumberFormatException nfe) {
                        logger.warn("Invalid value for detail {}: {}", detail.getName(), values);
                    }
                }
                break;
            }
        }
        return shouldRun;
    }

    private int doIndex(long throughTime) throws ZepException {
        final EventPostIndexContext context = new EventPostIndexContext() {};
        final List<EventPostIndexPlugin> plugins = this.pluginService.getPluginsByType(EventPostIndexPlugin.class);
        
        int numIndexed = queueDao.indexEvents(new EventIndexHandler() {
            @Override
            public void handle(EventSummary event) throws Exception {
                indexDao.stage(event);
                if (shouldRunPostprocessing(event)) {
                    for (EventPostIndexPlugin plugin : plugins) {
                        try {
                            plugin.processEvent(event, context);
                        } catch (Exception e) {
                            // Post-processing plug-in failures are not fatal errors.
                            logger.warn("Failed to run post-processing plug-in on event: " + event, e);
                        }
                    }
                }
            }

            @Override
            public void handleDeleted(String uuid) throws Exception {
                indexDao.stageDelete(uuid);
            }
            
            @Override
            public void handleComplete() throws Exception {
                indexDao.commit();
            }
        }, this.indexLimit, throughTime);
        
        if (numIndexed > 0) {
            logger.debug("Completed indexing {} events on {}", numIndexed, indexDao.getName());            
        }

        return numIndexed;
    }
}
