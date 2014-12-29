/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/
package org.zenoss.zep.index.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.TaskScheduler;
import org.zenoss.protobufs.zep.Zep.*;
import org.zenoss.zep.Messages;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepUtils;
import org.zenoss.zep.dao.EventBatch;
import org.zenoss.zep.dao.EventSummaryBaseDao;
import org.zenoss.zep.dao.IndexMetadata;
import org.zenoss.zep.dao.IndexMetadataDao;
import org.zenoss.zep.dao.impl.DaoUtils;
import org.zenoss.zep.events.IndexRebuildRequiredEvent;
import org.zenoss.zep.impl.ThreadRenamingRunnable;
import org.zenoss.zep.index.*;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;


public class MultiBackendEventIndexDao extends BaseEventIndexDaoImpl<MultiBackendSavedSearch>
        implements ApplicationListener<IndexRebuildRequiredEvent> {

    public static int MAX_TASK_PROCESSORS = 100;
    public static long ONE_MINUTE = TimeUnit.MINUTES.toMillis(1);

    public static enum BackendStatus { DISABLED, STANDBY, ENABLED }
    private static final Date IMMEDIATELY = new Date(0);

    private final String enabledBackendId;
    private final Map<String, EventIndexBackendConfiguration> backends;
    private final Map<String, WorkQueue> workQueues;
    private final EventSummaryBaseDao eventDao;
    private final IndexMetadataDao indexMetadataDao;
    private final IndexedDetailsConfiguration indexedDetailsConfiguration;
    private final File stateFileDir;
    private final Map<String,File> stateFiles = new HashMap<String, File>();
    private final Map<ScheduledFuture,Boolean> pendingTaskProcessors = new ConcurrentHashMap<ScheduledFuture, Boolean>();
    private final Map<String, ScheduledFuture> taskGrabbers;
    private final ConcurrentMap<String,ScheduledFuture> rebuilders = new ConcurrentHashMap<String, ScheduledFuture>();
    private ScheduledFuture taskRecycler;
    private ScheduledFuture statusLogger;
    private ScheduledFuture indexChecker;
    private volatile boolean shuttingDown = false;
    private boolean allowRebuild = true;
    private boolean allowAsync = true;

    public MultiBackendEventIndexDao(String name, EventSummaryBaseDao eventDao, IndexMetadataDao indexMetadataDao,
        IndexedDetailsConfiguration indexedDetailsConfiguration, File stateFileDir, WorkQueueBuilder queueBuilder,
        Messages messages, TaskScheduler scheduler, UUIDGenerator uuidGenerator,
        List<EventIndexBackendConfiguration> backends) {
        super(name, messages, scheduler, uuidGenerator);
        this.eventDao = eventDao;
        this.indexMetadataDao = indexMetadataDao;
        this.indexedDetailsConfiguration = indexedDetailsConfiguration;
        this.stateFileDir = stateFileDir;
        this.backends = Maps.newLinkedHashMap();
        this.workQueues = Maps.newHashMap();
        Set<String> backendIds = Sets.newHashSet();
        Map<EventIndexBackend,String> backendMap = Maps.newHashMap();
        String enabledBackendId = null;
        for (EventIndexBackendConfiguration config : backends) {
            if (config == null)
                continue;
            String backendId = config.id;
            if (!backendIds.add(backendId))
                throw new IllegalArgumentException("duplicate backend: " + backendId);
            String otherId = backendMap.put(config.backend, backendId);
            if (otherId != null)
                throw new IllegalArgumentException("same backend registered as " + otherId + " and " + backendId);
            switch(config.status) {
                case ENABLED:
                    if (enabledBackendId != null)
                        throw new IllegalArgumentException("only one backend may be enabled at a time");
                    enabledBackendId = backendId;
                    // Intentional fall through...
                case STANDBY:
                    logger.info("Configured {} backend: {}", name, config);
                    this.backends.put(backendId, config);
                    this.workQueues.put(backendId, queueBuilder.build(backendId));
                    break;
            }
        }
        this.taskGrabbers = new ConcurrentHashMap<String, ScheduledFuture>(this.backends.size());
        if (enabledBackendId == null)
            throw new IllegalArgumentException("one backend must be enabled");
        else
            this.enabledBackendId = enabledBackendId;
    }

    private void startStatusLogger() {
        Runnable statusLogger = new ThreadRenamingRunnable("ZEP_INDEXER_STATUS_LOGGER:" + getName()) {
            @Override
            public void _run() {
                try {
                    if (backends == null) return;
                    if (logger.isInfoEnabled()) {
                        logger.info("pending task processors: {}", pendingTaskProcessors.size());
                        for (EventIndexBackendConfiguration config : backends.values()) {
                            StringBuilder sb = new StringBuilder();
                            sb.append("Status of ");
                            sb.append(config);
                            sb.append(", ready: ");
                            sb.append(config.backend.isReady());
                            sb.append(", current queue size: ");
                            WorkQueue q = workQueues.get(config.id);
                            if (q == null) {
                                sb.append("n/a");
                            } else if (!q.isReady()) {
                                sb.append("unknown");
                            } else {
                                try {
                                    sb.append(q.size()); }
                                catch (RuntimeException e) {
                                    logWarn(e, "caught and ignored: %s", e.getMessage());
                                    sb.append("unknown");
                                }
                            }
                            sb.append(", index size: ");
                            if (!config.backend.isReady()) {
                                sb.append("unknown");
                            } else {
                                try {
                                    sb.append(config.backend.count());
                                } catch (ZepException e) {
                                    logWarn(e, "caught and ignored: %s", e.getMessage());
                                    sb.append("unknown");
                                } catch (RuntimeException e) {
                                    logWarn(e, "caught and ignored: %s", e.getMessage());
                                    sb.append("unknown");
                                }
                            }
                            logger.info(sb.toString());
                        }
                    }
                } catch (RuntimeException e) {
                    logWarn(e, "caught and ignored: %s", e.getMessage());
                }
            }
        };
        this.statusLogger = scheduler.scheduleAtFixedRate(statusLogger, ONE_MINUTE);
    }

    private void startTaskGrabber(final String backendId) {
        final WorkQueue q = workQueues.get(backendId);
        final EventIndexBackendConfiguration config = backends.get(backendId);
        final EventIndexBackend backend = config.backend;
        Runnable taskGrabber = new ThreadRenamingRunnable("ZEP_INDEXER_TASK_GRABBER:" + getName() + ":" + backendId) {
            @Override
            public void _run() {
                if (shuttingDown) return;
                try {
                    if (!backend.isReady()) {
                        logger.trace("waiting for backend to be ready");
                    } else if (!backend.ping()) {
                        logger.trace("backend cannot be pinged");
                    } else if (!q.isReady()) {
                        logger.trace("backend worker queue is not ready");
                    } else {
                        try {
                            if (pendingTaskProcessors.size() < MAX_TASK_PROCESSORS) {
                                List<EventIndexBackendTask> tasks = q.poll(config.batchSize, 50, TimeUnit.MILLISECONDS);
                                if (tasks != null && !tasks.isEmpty()) {
                                    logger.debug("fetched {} tasks to process", tasks.size());
                                    TaskProcessor taskProcessor = new TaskProcessor(backendId, tasks);
                                    ScheduledFuture ticket = scheduler.schedule(taskProcessor, IMMEDIATELY);
                                    taskProcessor.setSchedule(ticket);
                                    pendingTaskProcessors.put(ticket, true);
                                }
                            } else {
                                logger.trace("too many pending task processors");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                } catch (RuntimeException e) {
                    logWarn(e, "caught and ignored: %s", e.getMessage());
                }
            }
        };
        taskGrabbers.put(backendId, scheduler.scheduleWithFixedDelay(taskGrabber, 1));
    }

    private class TaskProcessor extends ThreadRenamingRunnable {
        private final String backendId;
        private final Collection<EventIndexBackendTask> tasks;
        private volatile ScheduledFuture schedule;

        public TaskProcessor(String backendId, Collection<EventIndexBackendTask> tasks) {
            super("ZEP_INDEXER_TASK_PROCESSOR:" + getName() + ":" + backendId);
            this.backendId = backendId;
            this.tasks = tasks;
        }

        public void setSchedule(ScheduledFuture schedule) {
            this.schedule = schedule;
        }

        @Override
        public void _run() {
            if (this.schedule == null) {
                logger.info("sooner than expected");
                while (this.schedule == null);
            }
            try {
                if (shuttingDown) return;
                logger.debug("Processing {} tasks", tasks.size());
                final EventIndexBackendConfiguration config = backends.get(backendId);
                final EventIndexBackend backend = config.backend;
                final WorkQueue q = workQueues.get(backendId);
                final Set<EventIndexBackendTask> flushes = Sets.newHashSet();
                final Map<String,EventIndexBackendTask> indexTasks = Maps.newHashMapWithExpectedSize(tasks.size());
                final Set<EventSummary> toIndex = Sets.newHashSetWithExpectedSize(tasks.size());

                for (EventIndexBackendTask task : tasks) {
                    switch (task.op) {
                        case FLUSH:
                            flushes.add(task);
                            break;
                        case INDEX_EVENT:
                            indexTasks.put(task.uuid, task);
                            toIndex.add(EventSummary.newBuilder().setUuid(task.uuid).setLastSeenTime(task.lastSeen).build());
                            break;
                        default:
                            logger.error("unexpected task operation: {}", task.op);
                            q.complete(task);
                    }
                }

                if (!toIndex.isEmpty()) {
                    logger.debug("looking up {} events by primary key", toIndex.size());
                    List<EventSummary> events = null;
                    try {
                        events = eventDao.findByKey(toIndex);
                    } catch (ZepException e) {
                        logWarn(e, "failed to find events for UUIDs %s", indexTasks.keySet());
                    }
                    if (events != null) {
                        if (events.size() != toIndex.size())
                            logger.info("found {} of {} events by primary key", events.size(), toIndex.size());
                        else
                            logger.debug("found {} of {} events by primary key", events.size(), toIndex.size());
                        try {
                            if (!events.isEmpty())
                                backend.index(events);
                            logger.debug("indexed {} events", events.size());
                        } catch (ZepException e) {
                            logWarn(e, "failed to process %d tasks to index events", events.size());
                        }

                        List<EventIndexBackendTask> completedTasks = Lists.newArrayListWithExpectedSize(events.size());
                        for (EventSummary event : events)
                            completedTasks.add(indexTasks.remove(event.getUuid()));
                        q.completeAll(completedTasks);

                        if (!indexTasks.isEmpty()) {
                            try {
                                if (config.honorDeletes) {
                                    logger.debug("removing {} events from the index since they weren't found by primary key in the database", indexTasks.size());
                                    backend.delete(indexTasks.keySet());
                                }
                                q.completeAll(indexTasks.values());
                            } catch (ZepException e) {
                                logWarn(e, "failed to delete %d events from index", toIndex.size());
                            }
                        }
                    }
                }

                if (!flushes.isEmpty() || !toIndex.isEmpty()) {
                    try {
                        logger.debug("flushing backend");
                        backend.flush();
                        if (!flushes.isEmpty()) {
                            q.completeAll(flushes);
                        }
                    } catch (ZepException e) {
                        logWarn(e, "failed to flush backend");
                    }
                }
            } catch (RuntimeException e) {
                logError(e, "caught and ignored: %s", e.getMessage());
            } finally {
                pendingTaskProcessors.remove(this.schedule);
            }
        }
    }

    private void startTaskRecycler() {
        Runnable taskRecycler = new ThreadRenamingRunnable("ZEP_INDEX_TASK_RECYCLER:" + getName()) {
            @Override
            public void _run() {
                try {
                    if (workQueues == null) return;
                    for (Map.Entry<String,WorkQueue> entry : workQueues.entrySet()) {
                        long count = entry.getValue().requeueOldTasks();
                        if (count > 0)
                            logger.warn("requeued {} old tasks for backend {}", count, entry.getKey());
                    }
                } catch (RuntimeException e) {
                    logWarn(e, "caught and ignored: %s", e.getMessage());
                }
            }
        };
        this.taskRecycler = scheduler.scheduleWithFixedDelay(taskRecycler, ONE_MINUTE);
    }

    public synchronized void close() throws IOException {
        logger.debug("MultiBackendEventIndexDao closing");
        shuttingDown = true;
        for (String backendId : backends.keySet()) {
            ScheduledFuture schedule = rebuilders.remove(backendId);
            if (schedule != null)
                schedule.cancel(false);
        }
        for (ScheduledFuture schedule : taskGrabbers.values()) {
            schedule.cancel(false);
        }
        for (ScheduledFuture schedule : Sets.newHashSet(pendingTaskProcessors.keySet())) {
            Boolean removed = pendingTaskProcessors.remove(schedule);
            if (removed != null && removed) {
                schedule.cancel(true);
            }
        }
        while (!pendingTaskProcessors.isEmpty()) {
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        taskRecycler.cancel(false);
        statusLogger.cancel(false);
        if (indexChecker != null)
            indexChecker.cancel(false);

        for (EventIndexBackendConfiguration b : backends.values()) {
            closeBackendSavedSearches(b.id);
            try {
                b.backend.close();
            } catch (IOException e) {
                logger.warn("exception while closing backend: " + b.id, e);
            } catch (RuntimeException e) {
                logger.warn("exception while closing backend: " + b.id, e);
            }
        }
        backends.clear();
        workQueues.clear();
        logger.debug("MultiBackendEventIndexDao close complete");
    }

    public void init() throws ZepException {
        startStatusLogger();
        startTaskRecycler();
        for (String backendId : backends.keySet()) {
            startTaskGrabber(backendId);
        }
        rescheduleStartIndexRebuildIfNeeded();
    }



    // --------------------------------------------------------------
    // Private utility methods
    // --------------------------------------------------------------

    private EventIndexBackend getEnabledBackend() {
        if (enabledBackendId == null) return null;
        final EventIndexBackendConfiguration c = backends.get(enabledBackendId);
        return (c == null) ? null : c.backend;
    }

    private void logSynchronousUpdateDueTo(Throwable t) {
        logWarn(t, "falling back to synchronous update due to error: %s", t.getMessage());
    }

    private void logWarn(Throwable t, String msg, Object... args) {
        if (logger.isDebugEnabled())
            logger.warn(String.format(msg, args), t);
        else if (logger.isWarnEnabled())
            logger.warn(String.format(msg, args));
    }

    private void logError(Throwable t, String msg, Object... args) {
        if (logger.isDebugEnabled())
            logger.error(String.format(msg, args), t);
        else if (logger.isErrorEnabled())
            logger.error(String.format(msg, args));
    }

    private static class Canceller implements Runnable {
        private final ScheduledFuture sf;
        public Canceller(ScheduledFuture sf) {this.sf = sf;}
        @Override public void run() {sf.cancel(false);}
    }

    private void scheduleCancel(ScheduledFuture sf) {
        scheduler.schedule(new Canceller(sf), IMMEDIATELY);
    }




    // --------------------------------------------------------------
    // Enabled and standby backends update methods
    // --------------------------------------------------------------

    @Override
    public void index(EventSummary event) throws ZepException {
        EventIndexBackendTask task = null;
        for (EventIndexBackendConfiguration config : backends.values()) {
            if (allowAsync && config.asyncUpdates) {
                WorkQueue q = workQueues.get(config.id);
                if (q != null && q.isReady()) {
                    if (task == null)
                        task = EventIndexBackendTask.Index(event.getUuid(), event.getLastSeenTime());
                    try {
                        q.add(task);
                        continue;
                    } catch (RuntimeException e) {
                        logSynchronousUpdateDueTo(e);
                    }
                }
            }
            config.backend.index(event);
        }
    }

    @Override
    public void indexMany(List<EventSummary> events) throws ZepException {
        if (events == null || events.isEmpty()) return;
        List<EventIndexBackendTask> tasks = null;
        for (EventIndexBackendConfiguration config : backends.values()) {
            if (allowAsync && config.asyncUpdates) {
                WorkQueue q = workQueues.get(config.id);
                if (q != null && q.isReady()) {
                    if (tasks == null) {
                        tasks = Lists.newArrayListWithExpectedSize(events.size());
                        for (EventSummary event : events)
                            tasks.add(EventIndexBackendTask.Index(event.getUuid(), event.getLastSeenTime()));
                    }
                    try {
                        q.addAll(tasks);
                        continue;
                    } catch (RuntimeException e) {
                        logSynchronousUpdateDueTo(e);
                    }
                }
            }
            config.backend.index(events);
        }
    }

    @Override
    public void delete(String uuid) throws ZepException {
        EventIndexBackendTask task = null;
        for (EventIndexBackendConfiguration config : backends.values()) {
            if (config.honorDeletes) {
                if (allowAsync && config.asyncUpdates) {
                    WorkQueue q = workQueues.get(config.id);
                    if (q != null && q.isReady()) {
                        if (task == null)
                            task = EventIndexBackendTask.Index(uuid, null);
                        try {
                            q.add(task);
                            continue;
                        } catch (RuntimeException e) {
                            logSynchronousUpdateDueTo(e);
                        }
                    }
                }
                config.backend.delete(uuid);
            }
        }
    }

    @Override
    public void delete(List<String> uuids) throws ZepException {
        if (uuids == null || uuids.isEmpty()) return;
        List<EventIndexBackendTask> tasks = null;
        for (EventIndexBackendConfiguration config : backends.values()) {
            if (config.honorDeletes) {
                if (allowAsync && config.asyncUpdates) {
                    WorkQueue q = workQueues.get(config.id);
                    if (q != null && q.isReady()) {
                        if (tasks == null) {
                            tasks = Lists.newArrayListWithExpectedSize(uuids.size());
                            for (String uuid : uuids)
                                tasks.add(EventIndexBackendTask.Index(uuid, null));
                        }
                        try {
                            q.addAll(tasks);
                            continue;
                        } catch (RuntimeException e) {
                            logSynchronousUpdateDueTo(e);
                        }
                    }
                }
                config.backend.delete(uuids);
            }
        }
    }

    @Override
    public void clear() throws ZepException {
        for (EventIndexBackendConfiguration config : backends.values()) {
            if (config.honorDeletes) {
                config.backend.clear();
            }
        }
    }

    protected void clear(String backendId) throws ZepException {
        EventIndexBackendConfiguration config = backends.get(backendId);
        if (config != null && config.honorDeletes) {
            config.backend.clear();
        }
    }

    @Override
    public void purge(Date threshold) throws ZepException {
        for (EventIndexBackendConfiguration config : backends.values()) {
            if (config.honorDeletes) {
                config.backend.purge(threshold);
            }
        }
    }

    @Override
    public void commit() throws ZepException {
        for (EventIndexBackendConfiguration config : backends.values()) {
            if (allowAsync && config.asyncUpdates) {
                WorkQueue q = workQueues.get(config.id);
                if (q != null && q.isReady()) {
                    try {
                        q.add(EventIndexBackendTask.Flush());
                        continue;
                    } catch (RuntimeException e) {
                        logSynchronousUpdateDueTo(e);
                    }
                }
            }
            config.backend.flush();
        }
    }



    // --------------------------------------------------------------
    // Enabled backend query methods
    // --------------------------------------------------------------

    @Override
    public int getNumDocs() throws ZepException {
        return (int)getEnabledBackend().count();
    }

    @Override
    public long getSize() {
        try {
            return getEnabledBackend().sizeInBytes();
        } catch (UnsupportedOperationException e) {
            return -1; //TODO: figure out the right thing to do
        }
    }

    @Override
    public EventSummary findByUuid(String uuid) throws ZepException {
        return getEnabledBackend().findByUuid(uuid);
    }

    @Override
    public EventSummaryResult list(EventSummaryRequest request) throws ZepException {
        return getEnabledBackend().list(request);
    }

    @Override
    public EventSummaryResult listUuids(EventSummaryRequest request) throws ZepException {
        return getEnabledBackend().listUuids(request);
    }

    @Override
    public EventTagSeveritiesSet getEventTagSeverities(EventFilter filter) throws ZepException {
        return getEnabledBackend().getEventTagSeverities(filter);
    }



    // --------------------------------------------------------------
    // Enabled backend saved search methods
    // --------------------------------------------------------------

    @Override
    public MultiBackendSavedSearch buildSavedSearch(String uuid, EventQuery eventQuery) throws ZepException {
        final String savedSearchId = getEnabledBackend().createSavedSearch(eventQuery);
        return new MultiBackendSavedSearch(generateUuid(), eventQuery.getTimeout(), enabledBackendId, savedSearchId, this);
    }

    @Override
    public SavedSearchProcessor<MultiBackendSavedSearch> savedSearchProcessor() {
        return new SavedSearchProcessor<MultiBackendSavedSearch>() {
            @Override
            public EventSummaryResult result(MultiBackendSavedSearch search, int offset, int limit) throws ZepException {
                return backends.get(search.backendId).backend.savedSearch(search.savedSearchId, offset, limit);
            }
        };
    }

    @Override
    public SavedSearchProcessor<MultiBackendSavedSearch> savedSearchUuidsProcessor() {
        return new SavedSearchProcessor<MultiBackendSavedSearch>() {
            @Override
            public EventSummaryResult result(MultiBackendSavedSearch search, int offset, int limit) throws ZepException {
                return backends.get(search.backendId).backend.savedSearchUuids(search.savedSearchId, offset, limit);
            }
        };
    }

    public void closeBackendSavedSearch(String backendId, String savedSearchId) {
        EventIndexBackendConfiguration config = backends.get(backendId);
        if (config == null) return;
        EventIndexBackend backend = config.backend;
        if (backend == null) return;
        backend.closeSavedSearch(savedSearchId);
    }

    private void closeBackendSavedSearches(String backendId) {
        EventIndexBackendConfiguration config = backends.get(backendId);
        if (config == null) return;
        EventIndexBackend backend = config.backend;
        if (backend == null) return;
        backend.closeSavedSearches();
    }



    // --------------------------------------------------------------
    // Index rebuilder
    // --------------------------------------------------------------

    private synchronized void startIndexRebuildIfNeeded() {
        indexChecker = null;
        if (shuttingDown || !allowRebuild) return;
        boolean anyEnabled = false;
        for (EventIndexBackendConfiguration config : backends.values()) {
            if (config.enableRebuilder) {
                anyEnabled = true;
            } else {
                logger.info("index rebuilder for " + getName() + ":" + config.id + " is disabled");
            }
        }
        if (!anyEnabled) return;

        byte[] hash;
        try {
            hash = calculateIndexVersionHash();
        } catch (ZepException e) {
            logError(e, "unable to calculate indexVersionHash: %s", e.getMessage());
            rescheduleStartIndexRebuildIfNeeded();
            return;
        }

        for (EventIndexBackendConfiguration config : backends.values()) {
            final String backendId = config.id;
            stopRebuilder(backendId);
            if (!config.enableRebuilder) continue;
            boolean resetIndexMetadata = false;
            boolean clearIndexData = false;
            boolean resetRebuildState = false;
            boolean startRebuilder = false;
            if (config.backend.isReady()) {
                final String indexMetadataKey = getName() + ":" + backendId;
                final IndexMetadata indexMetadata;
                try {
                    indexMetadata = indexMetadataDao.findIndexMetadata(indexMetadataKey);
                } catch (ZepException e) {
                    logError(e, "unable to lookup metadata for index %s: %s", indexMetadataKey, e.getMessage());
                    rescheduleStartIndexRebuildIfNeeded();
                    continue;
                }
                if (indexMetadata == null) {
                    logger.info("inconsistent state between index backend and database; clearing {}", indexMetadataKey);
                    resetIndexMetadata = resetRebuildState = clearIndexData = startRebuilder = true;
                } else if (IndexConstants.INDEX_VERSION != indexMetadata.getIndexVersion()) {
                    logger.info(String.format("index version changed: previous=%d, new=%d; clearing %s",
                            indexMetadata.getIndexVersion(), IndexConstants.INDEX_VERSION, indexMetadataKey));
                    resetIndexMetadata = resetRebuildState = clearIndexData = startRebuilder = true;
                } else if (!Arrays.equals(hash, indexMetadata.getIndexVersionHash())) {
                    logger.info("index configuration changed; clearing {}", indexMetadataKey);
                    resetIndexMetadata = resetRebuildState = clearIndexData = startRebuilder = true;
                }

                long indexed;
                try {
                    indexed = config.backend.count();
                } catch (ZepException e) {
                    logError(e, "unable to find the size of index %s: %s", indexMetadataKey, e.getMessage());
                    rescheduleStartIndexRebuildIfNeeded();
                    continue;
                }
                if (indexed == 0) {
                    logger.info("empty index detected for {} backend {}", getName(), backendId);
                    resetRebuildState = startRebuilder = true;
                }

                File file = getRebuildStateFileFor(backendId);
                IndexRebuildState state = null;
                try {
                    state = IndexRebuildState.loadState(file);
                } catch (Exception e) {
                    logWarn(e, "failed to restore index rebuild state from \"%s\": %s", file.getAbsolutePath(), e.getMessage());
                }
                if (state != null && !state.isDone()) {
                    startRebuilder = true;
                }

                if (clearIndexData) {
                    try {
                        clear(backendId);
                    } catch (ZepException e) {
                        logError(e, "failed to clean index for backend %s: %s", backendId, e.getMessage());
                    }
                }

                if (resetRebuildState) {
                    try {
                        resetRebuildState(backendId, hash, System.currentTimeMillis());
                        scheduler.schedule(new ThreadRenamingRunnable("ZEP_REINDEX_ESTIMATOR"){
                            @Override
                            protected void _run() {
                                long expected = 0;
                                try {
                                    expected = eventDao.estimateSize();
                                    estimateRebuildState(backendId, expected);
                                } catch (ZepException e) {
                                    logWarn(e, "unable to estimate expected reindex size for backend %s: %s", backendId, e.getMessage());
                                }
                            }
                        }, IMMEDIATELY);
                    } catch (IOException e) {
                        logError(e, "failed to reset index rebuild state file for backend %s: %s", config.id, e.getMessage());
                    }
                }

                if (resetIndexMetadata) {
                    try {
                        indexMetadataDao.updateIndexVersion(indexMetadataKey, IndexConstants.INDEX_VERSION, hash);
                    } catch (ZepException e) {
                        logError(e, "failed to update index metadata for %s: %s", indexMetadataKey, e.getMessage());
                    }
                }

                if (startRebuilder) {
                    startNewRebuilder(backendId);
                }
            } else {
                logger.info("Waiting for {}:{} backend to be ready", getName(), backendId);
                rescheduleStartIndexRebuildIfNeeded();
            }
        }
    }

    private synchronized void rescheduleStartIndexRebuildIfNeeded() {
        if (allowRebuild && this.indexChecker == null) {
            Runnable indexChecker = new ThreadRenamingRunnable("ZEP_INDEX_CHECKER"){
                @Override
                protected void _run() {
                    if (!shuttingDown && allowRebuild)
                        startIndexRebuildIfNeeded();
                }
            };
            Date aMinuteFromNow = new Date(System.currentTimeMillis() + ONE_MINUTE);
            this.indexChecker = scheduler.schedule(indexChecker, aMinuteFromNow);
        }

    }

    private byte[] calculateIndexVersionHash() throws ZepException {
        Map<String, EventDetailItem> unsorted = indexedDetailsConfiguration.getEventDetailItemsByName();
        StringBuilder indexConfigStr = new StringBuilder();
        for (EventDetailItem item : new TreeMap<String,EventDetailItem>(unsorted).values()) {
            // Only key and type affect the indexing behavior - ignore changes to display name
            indexConfigStr.append('|');
            indexConfigStr.append(item.getKey());
            indexConfigStr.append('|');
            indexConfigStr.append(item.getType().name());
            indexConfigStr.append('|');
        }
        if (indexConfigStr.length() == 0) {
            return null;
        }
        return DaoUtils.sha1(indexConfigStr.toString());
    }

    /** Pre-condition: backend.isReady() */
    private void startNewRebuilder(String backendId) {
        Rebuilder rebuilder = new Rebuilder(backendId);
        Date shortly = new Date(System.currentTimeMillis() + 100);
        ScheduledFuture schedule = scheduler.scheduleWithFixedDelay(rebuilder, shortly, 1);
        rebuilder.setSchedule(schedule);
        rebuilders.put(backendId, schedule);
    }

    private void stopRebuilder(String backendId) {
        ScheduledFuture schedule = rebuilders.remove(backendId);
        if (schedule != null)
            schedule.cancel(false);
    }

    private synchronized IndexRebuildState loadRebuildState(String backendId, IndexRebuildState fallback) {
        try {
            return IndexRebuildState.loadState(getRebuildStateFileFor(backendId));
        } catch (IOException e) {
            logError(e, "unable to load rebuild state file for backend %s: %s", backendId, e.getMessage());
            return fallback;
        } catch (ParseException e) {
            logError(e, "unable to load rebuild state file for backend %s: %s", backendId, e.getMessage());
            return fallback;
        }
    }

    private synchronized IndexRebuildState resetRebuildState(String backendId, byte[] hash, long throughTime)
            throws IOException {
        IndexRebuildState state = IndexRebuildState.begin(IndexConstants.INDEX_VERSION, hash, throughTime);
        state.save(getRebuildStateFileFor(backendId));
        return state;
    }

    private synchronized File getRebuildStateFileFor(String backendId) {
        File file = stateFiles.get(backendId);
        if (file == null) {
            file = new File(new File(new File(stateFileDir, getName()), backendId), ".index_state.properties");
            File parent = file.getParentFile();
            if (parent.isDirectory() || parent.mkdirs()) {
                stateFiles.put(backendId, file);
            } else {
                logger.error(String.format("failed to create directory: %s", parent.getAbsolutePath()));
            }
            stateFiles.put(backendId, file);
        }
        return file;
    }

    private class Rebuilder implements Runnable {
        private final String backendId;
        private final EventIndexBackendConfiguration config;
        private final EventIndexBackend backend;
        private final WorkQueue q;
        private ScheduledFuture schedule;

        public Rebuilder(String backendId) {
            this.backendId = backendId;
            this.config = backends.get(backendId);
            this.backend = config.backend;
            this.q = workQueues.get(backendId);
            this.schedule = null;
        }

        public void setSchedule(ScheduledFuture schedule) {
            this.schedule = schedule;
        }

        @Override
        public void run() {
            try {
                if (shuttingDown || !allowRebuild || rebuilders.get(backendId) != schedule) {
                    logger.info("shutting down. canceling rebuild for backend {}", backendId);
                    scheduleCancel(schedule);
                    return;
                }

                IndexRebuildState state = loadRebuildState(backendId, IndexRebuildState.UNKNOWN);
                if (state == null || state == IndexRebuildState.UNKNOWN || state.isDone()) {
                    if (state.isDone())
                        logger.info("index rebuild is done; shutting down rebuilder for backend {}", backendId);
                    else
                        logger.info("unknown index rebuild state; shutting down rebuilder for backend {}", backendId);
                    rebuilders.remove(backendId, scheduler);
                    scheduleCancel(schedule);
                    return;
                }

                int batchSize = config.batchSize;
                logger.debug("Fetching a batch of events to reindex for backend {}", backendId);
                boolean keysOnly = allowAsync && config.asyncUpdates && q.isReady();
                final EventBatch batch = eventDao.listBatch(state.batchParams(), state.throughTime, batchSize, keysOnly);
                logger.debug("Fetched a batch of {} events to reindex for backend {}", batch.events.size(), backendId);
                boolean queued = false;
                if (allowAsync && config.asyncUpdates) {
                    if (q.isReady()) {
                        try {
                            if (batch.events.isEmpty()) {
                                q.add(EventIndexBackendTask.Flush());
                            } else {
                                logger.debug("converting {} events into tasks", batch.events.size());
                                List<EventIndexBackendTask> tasks = Lists.newArrayListWithExpectedSize(batch.events.size());
                                for (EventSummary event : batch.events)
                                    tasks.add(EventIndexBackendTask.Index(event.getUuid(), event.getLastSeenTime()));
                                logger.debug("queueing up another {} events", batch.events.size());
                                q.addAll(tasks);
                                logger.debug("done queueing up {} events", batch.events.size());
                            }
                            queued = true;
                        } catch (RuntimeException e) {
                            logSynchronousUpdateDueTo(e);
                        }
                    }
                }


                if (queued) {
                    state = updateRebuildState(state, batch, backendId);
                    if (batch.events.isEmpty()) {
                        logger.info("all events through {} have been queued for re-indexing by backend: {}",
                                ZepUtils.formatUTC(state.throughTime), backendId);
                    } else if (state.hasEstimate()) {
                        if (logger.isDebugEnabled()) {
                            String msg = "queued %d more events for re-indexing by backend %s: %d of %d (%2.1f%%, eta: %s)";
                            msg = String.format(msg, batch.events.size(), backendId, state.indexed, state.expected, state.percent() * 100, ZepUtils.formatUTC(state.eta()));
                            logger.debug(msg);
                        }
                    } else {
                        logger.debug("queued {} more events for re-indexing by backend {}", batch.events.size(), backendId);
                    }
                } else {
                    if (batch.events.isEmpty()) {
                        backend.flush();
                        state = updateRebuildState(state, batch, backendId);
                        logger.info("done re-indexing events for backend: {}", backendId);
                    } else {
                        List<EventSummary> events = batch.events;
                        if (keysOnly) {
                            logger.debug("Refetching {} events by key", events.size());
                            events = eventDao.findByKey(events);
                        }
                        backend.index(events);
                        state = updateRebuildState(state, batch, backendId);
                        if (state.hasEstimate()) {
                            if (logger.isDebugEnabled()) {
                                String msg = "re-indexed %d more events to backend %s: %d of %d (%2.1f%%, eta: %s)";
                                msg = String.format(msg, batch.events.size(), backendId, state.indexed, state.expected, state.percent() * 100, ZepUtils.formatUTC(state.eta()));
                                logger.debug(msg);
                            }
                        } else {
                            logger.debug("re-indexed {} more events to backend {}", batch.events.size(), backendId);
                        }
                    }
                }

            } catch (ZepException e) {
                logWarn(e, "caught and ignored: %s", e.getMessage());
            } catch (RuntimeException e) {
                logWarn(e, "caught and ignored: %s", e.getMessage());
            }
        }
    }

    private synchronized IndexRebuildState updateRebuildState(IndexRebuildState state, EventBatch batch, String backendId) {
        // Note: Synchronization is OK for multi-threaded, but not for multi-processes nor multi-system.
        // We'll need to keep the state in a transactional database (perhaps Redis) if we ever want to go there.
        long indexed = batch.events.size();
        IndexRebuildState loadedState = loadRebuildState(backendId, state);
        if (isCompatible(state,loadedState)) {
            state = loadedState;
            if (indexed == 0)
                state = IndexRebuildState.end(state, 0);
            else
                state = IndexRebuildState.update(state, indexed, batch.params.nextLastSeen, batch.params.nextUuid);
            try {
                state.save(getRebuildStateFileFor(backendId));
            } catch (IOException e) {
                logWarn(e, "unable to save rebuild state file for backend %s: %s", backendId, e.getMessage());
            }
        } else {
            logger.error("index rebuild state was unexpectedly changed by another thread");
            return loadedState;
        }
        return state;
    }

    private synchronized IndexRebuildState estimateRebuildState(String backendId, long expected) {
        // Note: Synchronization is OK for multi-threaded, but not for multi-processes nor multi-system.
        // We'll need to keep the state in a transactional database (perhaps Redis) if we ever want to go there.
        IndexRebuildState state = loadRebuildState(backendId, null);
        if (state != null) {
            state = IndexRebuildState.expected(state, expected);
            try {
                state.save(getRebuildStateFileFor(backendId));
            } catch (IOException e) {
                logWarn(e, "unable to save rebuild state file for backend %s: %s", backendId, e.getMessage());
            }
        } else {
            logger.debug("index rebuild state does not yet exist, estimate was not saved");
        }
        return state;
    }

    private boolean isCompatible(IndexRebuildState state, IndexRebuildState loadedState) {
        return state.throughTime == loadedState.throughTime;
    }

    @Override
    public void onApplicationEvent(IndexRebuildRequiredEvent event) {
        rescheduleStartIndexRebuildIfNeeded();
    }

    public void disableRebuilders() {
        this.allowRebuild = false;
    }

    public void disableAsyncProcessing() {
        this.allowAsync = false;
    }
}
