/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/
package org.zenoss.zep.index.impl;

import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.scheduling.TaskScheduler;
import org.zenoss.protobufs.zep.Zep.*;
import org.zenoss.zep.Messages;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventBatch;
import org.zenoss.zep.dao.EventBatchParams;
import org.zenoss.zep.dao.EventSummaryBaseDao;
import org.zenoss.zep.index.EventIndexBackend;
import org.zenoss.zep.index.SavedSearchProcessor;
import org.zenoss.zep.index.WorkQueue;
import org.zenoss.zep.index.WorkQueueBuilder;
import org.zenoss.zep.utils.KeyValueStore;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class MultiBackendEventIndexDao extends BaseEventIndexDaoImpl<MultiBackendSavedSearch> {

    protected boolean useRedis;
    private String readerBackendId;
    private boolean disableRebuilders = false;
    private boolean enableAsyncProcessing = true;
    private final WorkQueueBuilder queueBuilder;
    private final List<EventIndexBackendConfiguration> initialBackendConfigurations;
    private final Map<String, EventIndexBackendConfiguration> backends;
    private final Map<String, WorkQueue> workQueues;
    private final Map<String, WorkerThread> workers;
    private final Map<String,RebuilderThread> rebuilders;
    private final KeyValueStore store;
    private final EventSummaryBaseDao eventDao;
    private final ReadWriteLock backendsLock = new ReentrantReadWriteLock();
    private final Lock backendsUse = backendsLock.readLock();
    private final Lock backendsModify = backendsLock.writeLock();

    public MultiBackendEventIndexDao(String name, EventSummaryBaseDao eventDao, WorkQueueBuilder queueBuilder, KeyValueStore store,
                                     Messages messages, TaskScheduler scheduler, UUIDGenerator uuidGenerator) {
        super(name, messages, scheduler, uuidGenerator);
        this.store = store;
        this.eventDao = eventDao;
        this.queueBuilder = queueBuilder;
        backends = Maps.newLinkedHashMap();
        workQueues = Maps.newConcurrentMap();
        workers = Maps.newConcurrentMap();
        rebuilders = Maps.newConcurrentMap();
        initialBackendConfigurations = Lists.newArrayList();
    }

    public final synchronized void disableAsyncProcessing() {
        this.enableAsyncProcessing = false;
        backendsUse.lock();
        try {
            for (String backendId : Lists.newArrayList(workers.keySet()))
                stopBackendWorker(backendId);
        } finally { backendsUse.unlock(); }
    }

    /** Only call this within a backendsUse.lock() block. */
    private void processTasks(String backendId, List<EventIndexBackendTask> tasks, WorkQueue q) throws ZepException {
        final EventIndexBackendConfiguration configuration = backends.get(backendId);
        if (configuration == null)
            throw new ZepException("Tried to process tasks for unknown backend: " + backendId);
        final EventIndexBackend backend = configuration.getBackend();
        if (backend == null)
            throw new ZepException("Tried to process tasks for unknown backend: " + backendId);
        logger.debug("Processing {} tasks for backend {}", tasks.size(), backendId);

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
                    logger.error("UNEXPECTED TASK OPERATION: {}", task.op);
                    q.complete(task);
            }
        }

        try {
            if (!toIndex.isEmpty()) {
                logger.debug(String.format("Looking up %d events by primary key", toIndex.size()));
                List<EventSummary> events = eventDao.findByKey(toIndex);
                if (events.size() != toIndex.size())
                    logger.info("Found {} of {} events by primary key", events.size(), toIndex.size());
                else
                    logger.debug("Found {} of {} events by primary key", events.size(), toIndex.size());
                boolean success = true;
                try {
                    backend.index(events);
                    logger.debug("Indexed {} events", events.size());
                } catch (ZepException e) {
                    success = false;
                    if (logger.isDebugEnabled())
                        logger.warn(String.format("failed to process task to index events (%d) for backend %s", events.size(), backendId), e);
                    else
                        logger.warn(String.format("failed to process task to index events (%d) for backend %s", events.size(), backendId));
                }
                if(success) {
                    List<EventIndexBackendTask> completedTasks = Lists.newArrayListWithExpectedSize(events.size());
                    for (EventSummary event : events) {
                        EventIndexBackendTask task = indexTasks.remove(event.getUuid());
                        if (task != null) // should always be true
                            completedTasks.add(task);
                    }
                    q.completeAll(completedTasks);

                    if (!indexTasks.isEmpty()) {
                        try {
                            if (configuration.isHonorDeletes()) {
                                logger.debug("Removing {} events from the index since they weren't found by primary key in the database", indexTasks.size());
                                backend.delete(indexTasks.keySet());
                            }
                            q.completeAll(indexTasks.values());
                        } catch (ZepException e) {
                            if (logger.isDebugEnabled())
                                logger.warn(String.format("failed to delete %d events from backend %s", toIndex.size(), backendId), e);
                            else
                                logger.warn(String.format("failed to delete %d events from backend %s", toIndex.size(), backendId));
                        }
                    }
                }
            }

            if (!flushes.isEmpty()) {
                try {
                    logger.debug("flushing backend");
                    backend.flush();
                    q.completeAll(flushes);
                } catch (ZepException e) {
                    if (logger.isDebugEnabled())
                        logger.warn(String.format("failed to process tasks %s for backend %s", flushes, backendId), e);
                    else
                        logger.warn(String.format("failed to process tasks %s for backend %s", flushes, backendId));

                }
            }
        } catch (ZepException e) {
            if (logger.isDebugEnabled())
                logger.warn(String.format("failed to find events for UUIDs %s for backend %s", indexTasks.keySet(), backendId), e);
            else
                logger.warn(String.format("failed to find events for UUIDs %s for backend %s", indexTasks.keySet(), backendId));
        }
    }

    /**
     * The thread will exit cleanly once its backend has no registered work queue (in workQueues).
     */
    private class WorkerThread extends Thread {
        private final String backendId;
        public WorkerThread(String backendId) {
            this.backendId = backendId;
            this.setDaemon(true);
            this.setName(MultiBackendEventIndexDao.this + " backend " + backendId + " event indexing worker thread");
        }
        @Override
        public void run() {
            logger.info("Started processing queue for {}", backendId);
            WorkQueue q = workQueues.get(backendId);
            EventIndexBackendConfiguration config = getBackendConfiguration(backendId);

            EventIndexBackend backend = config.getBackend();
            if (backend == null) {
                logger.error("Stopping worker for unknown backend: {}", backendId);
            }
            else {
                List<EventIndexBackendTask> tasks;
                while(q != null && config != null && workers.get(backendId) == this) {
                    boolean sleep_and_continue = false;

                    if (!backend.isReady()) {
                        logger.info("Waiting for backend {} to be ready", backendId);
                        sleep_and_continue = true;
                    } else if (!backend.ping()) {
                        logger.warn("Backend {} cannot be pinged", backendId);
                        sleep_and_continue = true;
                    } else if (enableAsyncProcessing && config.isAsyncUpdates() && !q.isReady()) {
                        logger.warn("Backend {}: Worker queue is not ready.", backendId);
                        sleep_and_continue = true;
                    }

                    if(sleep_and_continue) {
                        try {sleep(1000);} catch(InterruptedException e){this.interrupt();} 
                        continue;
                    }

                    try {
                        int batchSize = config.getBatchSize();
                        //logger.debug("Polling for tasks to process");
                        tasks = q.poll(batchSize, 500, TimeUnit.MILLISECONDS);
                        if (tasks == null || tasks.isEmpty()) continue;
                        backendsUse.lock();
                        try {
                            logger.debug(getName() + " fetched {} tasks to process", tasks.size());
                            processTasks(backendId, tasks, q);
                        } catch (ZepException e) {
                            logger.warn(String.format("failed to process tasks %s for backend %s", tasks, backendId), e);
                        } finally {
                            backendsUse.unlock();
                        }
                    } catch (InterruptedException e) {
                        // continue
                    } catch (RuntimeException e) {
                        logger.warn(String.format("failed to fetch tasks for backend %s", backendId), e);
                        try { sleep(1000); } catch (InterruptedException ie) { /* ignore */ }
                    } finally {
                        q = workQueues.get(backendId);
                        config = getBackendConfiguration(backendId);
                    }
                }
            }
            logger.info("Stopped processing queue for {}", backendId);
        }
    }

    /**
     * The thread will exit cleanly once its queue is no longer found in workQueues.
     */
    private class RequeueThread extends Thread {
        private final String backendId;
        private final WorkQueue q;

        public RequeueThread(String backendId, WorkQueue q) {
            this.backendId = backendId;
            this.q = q;
            this.setDaemon(true);
            this.setName(MultiBackendEventIndexDao.this + " backend " + backendId + " requeue thread");
        }

        @Override
        public void run() {
            logger.debug(getName() + " started");
            while (workQueues.get(backendId) == q) {
                long requeued = q.requeueOldTasks();
                if (requeued > 0) {
                    logger.warn(getName() + " requeued " + requeued + " old tasks");
                }
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            logger.debug(getName() + " exited");
        }

    }




    protected EventIndexBackendConfiguration getBackendConfiguration(String backendId) {
        backendsUse.lock();
        try {
            EventIndexBackendConfiguration result = backends.get(backendId);
            if (result == null) return null;
            return result.clone();
        } finally { backendsUse.unlock(); }
    }

    /**
     * Start a background thread to pull tasks off a work queue and pass them to the backend.
     *
     * Pre-condition: The backend must have an entry in workQueues (the worker thread will exit if not).
     */
    protected void startBackendWorker(String backendId) {
        backendsModify.lock();
        try {
            stopBackendWorker(backendId);
            if (!enableAsyncProcessing) return;
            WorkQueue q = queueBuilder.build(backendId);
            workQueues.put(backendId, q);
            WorkerThread worker = new WorkerThread(backendId);
            worker.start();
            if (worker.isAlive())
                workers.put(backendId, worker);
            else
                logger.error("Failed to start worker thread for event indexing backend {}", backendId);
            new RequeueThread(backendId, q).start();
        } finally { backendsModify.unlock(); }
    }

    protected void stopBackendWorker(String backendId) {
        WorkerThread worker;
        backendsModify.lock();
        try {
            worker = workers.remove(backendId);
            workQueues.remove(backendId);
        } finally { backendsModify.unlock(); }
        if (worker != null) {
            logger.info("Stopping backend worker for " + getName() + " backend " + backendId);
            try { worker.join(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    public void close() throws IOException {
        backendsModify.lock();
        try {
            for (EventIndexBackendConfiguration b : backends.values()) {
                closeBackendSavedSearches(b.getName());
                try {
                    b.getBackend().close();
                } catch (IOException e) {
                    logger.warn("exception while closing backend: " + b.getName(), e);
                }
            }
            workers.clear(); // This also causes each of the worker threads to exit cleanly.
            workQueues.clear();
        } finally { backendsModify.unlock(); }
    }

    /**
     * Prevents the use of redis for configuration rebuilder
     */
    public void setUseRedis(boolean useRedis) {
        this.useRedis = useRedis;
        if (!useRedis) {
            this.disableRebuilders();
        }
    }

    /**
     * Set the initial list of EventIndexBackendConfiguration
     */
    public void setInitialBackendConfigurations(List<EventIndexBackendConfiguration> backends) {
        initialBackendConfigurations.addAll(Collections2.filter(backends, Predicates.notNull()));
    }

    public List<EventIndexBackendConfiguration> getInitialBackendConfigurations() {
        return initialBackendConfigurations;
    }

    public void init() throws ZepException {
        setBackends(initialBackendConfigurations);
    }

    protected void setBackends(List<EventIndexBackendConfiguration> backends) throws ZepException {
        int readers = 0;
        backendsModify.lock();
        try {
            final Set<String> toDisable = enabledBackends();
            for (EventIndexBackendConfiguration newConf : backends) {
                reconfigureBackend(newConf);
                if (newConf.getStatus() == null) {
                    toDisable.remove(newConf.getName());
                } else switch(newConf.getStatus()) {
                    case READER:
                        if (++readers > 1)
                            logger.error("Multi-backend event indexer " + getName() + " configured with multiple readers! (Only one will be selected)");
                        // Intentional fall-through...
                    case WRITER:
                        toDisable.remove(newConf.getName());
                }
            }
            for (String backendId : toDisable) {
                EventIndexBackendConfiguration config = this.backends.get(backendId);
                stopRebuilder(backendId);
                stopBackendWorker(backendId);
                if (config.isWriter()) {
                    logger.info("disabled backend (" + backendId + ") for " + getName());
                    config.setStatus(BackendStatus.REGISTERED);
                }
            }
            Set<String> enabled = enabledBackends();
            if (!enabled.contains(readerBackendId)) {
                logger.warn("reader backend (" + readerBackendId + ") was disabled");
                if (enabled.isEmpty()) {
                    readerBackendId = null;
                    logger.warn("all event indexer backends for " + getName() + " have been disabled!");
                } else {
                    String newReaderId = enabled.iterator().next();
                    this.backends.get(newReaderId).setStatus(BackendStatus.READER);
                    readerBackendId = newReaderId;
                    logger.warn("auto-selected backend (" + newReaderId + ") as reader for " + getName());
                }
            }
        } finally { backendsModify.unlock(); }
    }

    private void reconfigureBackend(EventIndexBackendConfiguration input) throws ZepException {
        backendsModify.lock();
        try {
            final String backendId = input.getName();
            for (EventIndexBackendConfiguration conf : backends.values()) {
                if (conf.getBackend().equals(input.getBackend()) && !conf.getName().equals(backendId))
                    throw new ZepException("the backend (" + backendId + ") has already been registered with a different ID: " + conf.getName());
            }
            EventIndexBackendConfiguration conf = backends.get(backendId);
            if (conf == null) {
                if (input.getBackend() == null)
                    throw new ZepException("unknown backend (" + backendId + ") for " + getName());
                conf = input.clone();
                backends.put(backendId, conf);
                switch(conf.getStatus()) {
                    case READER:
                        if (backendId != null && !backendId.equals(readerBackendId)) {
                            EventIndexBackend reader = getReader();
                            if (reader != null)
                                reader.closeSavedSearches();
                            readerBackendId = backendId;
                            logger.info("selected backend (" + backendId + ") as reader for " + getName());
                        }
                        // Intentional fall-through...
                    case WRITER:
                        logger.info("enabled backend (" + backendId + ") for " + getName());
                        if (enableAsyncProcessing && conf.isAsyncUpdates()) {
                            startBackendWorker(backendId);
                        }
                        startRebuilder(backendId);
                }
            } else {
                if (input.getBackend() != null)
                    conf.setBackend(input.getBackend()); //throws ZepException if it changed.
                switch(input.getStatus()) {
                    case REGISTERED:
                        stopRebuilder(backendId);
                        stopBackendWorker(backendId);
                        if (conf.isWriter())
                            logger.info("disabled backend (" + backendId + ") for " + getName());
                        if (readerBackendId.equals(backendId) && conf.getBackend() != null)
                            conf.getBackend().closeSavedSearches();
                        break;
                    case READER:
                        if (backendId != null && !backendId.equals(readerBackendId)) {
                            EventIndexBackend reader = getReader();
                            if (reader != null)
                                reader.closeSavedSearches();
                            readerBackendId = backendId;
                            logger.info("selected backend (" + backendId + ") as reader for " + getName());
                        }
                        // Intentional fall-through...
                    case WRITER:
                        if (!conf.isWriter())
                            logger.info("enabled backend (" + backendId + ") for " + getName());
                        if (enableAsyncProcessing && input.isAsyncUpdates()) {
                            if (!workers.containsKey(backendId)) {
                                startBackendWorker(backendId);
                            }
                        }
                        if (!rebuilders.containsKey(backendId))
                            startRebuilder(backendId);
                        break;
                }
                conf.merge(input);
            }
        } finally { backendsModify.unlock(); }
    }

    protected Set<String> enabledBackends() {
        backendsUse.lock();
        try {
            Set<String> result = Sets.newLinkedHashSet();
            for (EventIndexBackendConfiguration config : backends.values())
                if (config.isWriter())
                    result.add(config.getName());
            return result;
        } finally { backendsUse.unlock(); }
    }

    public static enum BackendStatus { REGISTERED, WRITER, READER }

    /** Do not use this outside of a {@link #backendsUse} lock-block. */
    private EventIndexBackend getReader() {
        if (readerBackendId == null) return null;
        final EventIndexBackendConfiguration c = backends.get(readerBackendId);
        return (c == null) ? null : c.getBackend();
    }

    @Override
    public void index(EventSummary event) throws ZepException {
        backendsUse.lock();
        try {
            for (EventIndexBackendConfiguration config : backends.values()) {
                if (config.isWriter()) {
                    if (enableAsyncProcessing && config.isAsyncUpdates()) {
                        workQueues.get(config.getName()).add(EventIndexBackendTask.Index(event.getUuid(), event.getLastSeenTime()));
                    } else {
                        config.getBackend().index(event);
                    }
                }
            }
        } finally { backendsUse.unlock(); }
    }

    @Override
    public void indexMany(List<EventSummary> events) throws ZepException {
        if (events == null || events.isEmpty()) return;
        backendsUse.lock();
        try {
            List<EventIndexBackendTask> tasks = null;
            for (EventIndexBackendConfiguration config : backends.values()) {
                if (config.isWriter()) {
                    if (enableAsyncProcessing && config.isAsyncUpdates()) {
                        if (tasks == null) {
                            tasks = Lists.newArrayListWithExpectedSize(events.size());
                            for (EventSummary event : events)
                                tasks.add(EventIndexBackendTask.Index(event.getUuid(), event.getLastSeenTime()));
                        }
                        workQueues.get(config.getName()).addAll(tasks);
                    } else {
                        config.getBackend().index(events);
                    }
                }
            }
        } finally { backendsUse.unlock(); }
    }

    @Override
    public void delete(String uuid) throws ZepException {
        backendsUse.lock();
        try {
            for (EventIndexBackendConfiguration config : backends.values()) {
                if (config.isWriter() && config.isHonorDeletes()) {
                    if (enableAsyncProcessing && config.isAsyncUpdates()) {
                        workQueues.get(config.getName()).add(EventIndexBackendTask.Index(uuid, null));
                    } else {
                        config.getBackend().delete(uuid);
                    }
                }
            }
        } finally { backendsUse.unlock(); }
    }

    @Override
    public void delete(List<String> uuids) throws ZepException {
        if (uuids == null || uuids.isEmpty()) return;
        backendsUse.lock();
        try {
            List<EventIndexBackendTask> tasks = null;
            for (EventIndexBackendConfiguration config : backends.values()) {
                if (config.isWriter() && config.isHonorDeletes()) {
                    if (enableAsyncProcessing && config.isAsyncUpdates()) {
                        if (tasks == null) {
                            tasks = Lists.newArrayListWithExpectedSize(uuids.size());
                            for (String uuid : uuids)
                                tasks.add(EventIndexBackendTask.Index(uuid, null));
                        }
                        workQueues.get(config.getName()).addAll(tasks);
                    } else {
                        config.getBackend().delete(uuids);
                    }
                }
            }
        } finally { backendsUse.unlock(); }
    }

    @Override
    public void clear() throws ZepException {
        backendsUse.lock();
        try {
            for (EventIndexBackendConfiguration config : backends.values()) {
                if (config.isWriter() && config.isHonorDeletes()) {
                    config.getBackend().clear();
                }
            }
        } finally { backendsUse.unlock(); }
    }

    protected void clear(String backendId) throws ZepException {
        backendsUse.lock();
        try {
            EventIndexBackendConfiguration config = backends.get(backendId);
            if (config != null && config.isWriter() && config.isHonorDeletes()) {
                config.getBackend().clear();
            }
        } finally { backendsUse.unlock(); }
    }

    @Override
    public void purge(Date threshold) throws ZepException {
        backendsUse.lock();
        try {
            for (EventIndexBackendConfiguration config : backends.values()) {
                if (config.isWriter() && config.isHonorDeletes()) {
                    config.getBackend().purge(threshold);
                }
            }
        } finally { backendsUse.unlock(); }
    }

    @Override
    public void commit() throws ZepException {
        backendsUse.lock();
        try {
            for (EventIndexBackendConfiguration config : backends.values()) {
                if (config.isWriter()) {
                    if (enableAsyncProcessing && config.isAsyncUpdates()) {
                        workQueues.get(config.getName()).add(EventIndexBackendTask.Flush());
                    } else {
                        config.getBackend().flush();
                    }
                }
            }
        } finally { backendsUse.unlock(); }
    }

    @Override
    public int getNumDocs() throws ZepException {
        backendsUse.lock();
        try {
            return (int)getReader().count();
        } finally { backendsUse.unlock(); }
    }

    @Override
    public long getSize() {
        backendsUse.lock();
        try {
            return getReader().sizeInBytes();
        } catch (UnsupportedOperationException e) {
            return -1; //TODO: figure out the right thing to do
        } finally { backendsUse.unlock(); }
    }

    @Override
    public EventSummary findByUuid(String uuid) throws ZepException {
        backendsUse.lock();
        try {
            return getReader().findByUuid(uuid);
        } finally { backendsUse.unlock(); }
    }

    @Override
    public EventSummaryResult list(EventSummaryRequest request) throws ZepException {
        backendsUse.lock();
        try {
            return getReader().list(request);
        } finally { backendsUse.unlock(); }
    }

    @Override
    public EventSummaryResult listUuids(EventSummaryRequest request) throws ZepException {
        backendsUse.lock();
        try {
            return getReader().listUuids(request);
        } finally { backendsUse.unlock(); }
    }

    @Override
    public EventTagSeveritiesSet getEventTagSeverities(EventFilter filter) throws ZepException {
        backendsUse.lock();
        try {
            return getReader().getEventTagSeverities(filter);
        } finally { backendsUse.unlock(); }
    }

    @Override
    public MultiBackendSavedSearch buildSavedSearch(String uuid, EventQuery eventQuery) throws ZepException {
        backendsUse.lock();
        try {
            final String savedSearchId = getReader().createSavedSearch(eventQuery);
            return new MultiBackendSavedSearch(generateUuid(), eventQuery.getTimeout(), readerBackendId, savedSearchId, this);
        } finally { backendsUse.unlock(); }
    }

    @Override
    public SavedSearchProcessor<MultiBackendSavedSearch> savedSearchProcessor() {
        return new SavedSearchProcessor<MultiBackendSavedSearch>() {
            @Override
            public EventSummaryResult result(MultiBackendSavedSearch search, int offset, int limit) throws ZepException {
                return backends.get(search.backendId).getBackend().savedSearch(search.savedSearchId, offset, limit);
            }
        };
    }

    @Override
    public SavedSearchProcessor<MultiBackendSavedSearch> savedSearchUuidsProcessor() {
        return new SavedSearchProcessor<MultiBackendSavedSearch>() {
            @Override
            public EventSummaryResult result(MultiBackendSavedSearch search, int offset, int limit) throws ZepException {
                return backends.get(search.backendId).getBackend().savedSearchUuids(search.savedSearchId, offset, limit);
            }
        };
    }

    public void closeBackendSavedSearch(String backendId, String savedSearchId) {
        EventIndexBackendConfiguration config = backends.get(backendId);
        if (config == null) return;
        EventIndexBackend backend = config.getBackend();
        if (backend == null) return;
        backend.closeSavedSearch(savedSearchId);
    }

    private void closeBackendSavedSearches(String backendId) {
        EventIndexBackendConfiguration config = backends.get(backendId);
        if (config == null) return;
        EventIndexBackend backend = config.getBackend();
        if (backend == null) return;
        backend.closeSavedSearches();
    }

    public final synchronized void disableRebuilders() {
        this.disableRebuilders = true;
        backendsUse.lock();
        try {
            for (String backendId : Lists.newArrayList(rebuilders.keySet())) {
                stopRebuilder(backendId);
            }
        } finally { backendsUse.unlock(); }
    }

    public final synchronized void startRebuilder(String backendId) {
        stopRebuilder(backendId);
        if (disableRebuilders) return;
        RebuilderThread rebuilder = new RebuilderThread(this, backendId);
        rebuilders.put(backendId, rebuilder);
        rebuilder.start();
        if (!rebuilder.isAlive())
            logger.error("failed to start event index rebuilder thread for " + getName() + " backend " + backendId);
    }

    public final synchronized void stopRebuilder(String backendId) {
        RebuilderThread rebuilder = rebuilders.remove(backendId);
        if (rebuilder != null) {
            logger.info("shutting down event index rebuilder thread for " + getName() + " backend " + backendId);
            try { rebuilder.join(); } catch (InterruptedException e) { /* no problem */ }
        }
    }

    public final synchronized void forceRebuild(String backendId) {
        RebuilderThread rebuilder = rebuilders.get(backendId);
        if (rebuilder == null)
            logger.error("unable to force rebuild with rebuilder stopped for " + getName() + " backend " + backendId);
        else {
            rebuilder.forceRebuild = true;
        }
    }

    private static class RebuilderProgress implements Serializable {
        public final long throughTime;
        public final EventBatchParams nextBatch;
        public final boolean done;

        public static RebuilderProgress begin(long throughTime) {
            return new RebuilderProgress(throughTime, null, false);
        }

        public static RebuilderProgress done(RebuilderProgress progress) {
            return new RebuilderProgress(progress.throughTime, null, true);
        }

        public static RebuilderProgress next(RebuilderProgress progress, EventBatchParams nextBatch) {
            return new RebuilderProgress(progress.throughTime, nextBatch, false);
        }

        private RebuilderProgress(long throughTime, EventBatchParams nextBatch, boolean done) {
            this.throughTime = throughTime;
            this.nextBatch = nextBatch;
            this.done = done;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("throughTime:");
            sb.append(throughTime);
            sb.append(" ");
            if (nextBatch != null) {
                sb.append("nextLastSeen:");
                sb.append(nextBatch.nextLastSeen);
                sb.append(" ");
                if (nextBatch.nextUuid != null) {
                    sb.append("nextUuid:");
                    sb.append(nextBatch.nextUuid);
                    sb.append(" ");
                }
            }
            sb.append("done:");
            sb.append(done);
            return sb.toString();
        }

        public static RebuilderProgress parse(String s) {
            try {
                Map<String,String> pairs = new HashMap<String, String>();
                for (String pair : s.split(" ")) {
                    String[] splitPair = pair.split(":", 2);
                    if (splitPair.length == 2)
                        pairs.put(splitPair[0], splitPair[1]);
                }
                long throughTime = Long.parseLong(pairs.get("throughTime"));
                if ("true".equalsIgnoreCase(pairs.get("done")))
                    return new RebuilderProgress(throughTime, null, true);
                String lastSeenStr = pairs.get("nextLastSeen");
                if (lastSeenStr == null)
                    return new RebuilderProgress(throughTime, null, false);
                long nextLastSeen = Long.parseLong(lastSeenStr);
                String nextUuid = pairs.get("nextUuid");
                EventBatchParams nextBatch = new EventBatchParams(nextLastSeen, nextUuid);
                return new RebuilderProgress(throughTime, nextBatch, false);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Unable to parse: " + s, e);
            }
        }
    }

    private static final SimpleDateFormat UTC;
    static {
        UTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S 'UTC'");
        UTC.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    private static String formatUTC(long time) {
        synchronized (UTC) {
            return UTC.format(new Date(time));
        }
    }

    private class RebuilderThread extends Thread {

        private final String backendId;
        private final byte[] storeKey;
        private long nextStatusLog = 0;
        public transient boolean forceRebuild;

        public RebuilderThread(MultiBackendEventIndexDao dao, String backendId) {
            this.backendId = backendId;
            this.forceRebuild = false;
            this.storeKey = ("org.zenoss.zep.index.rebuilder:" + dao.getName() + ":" + backendId).getBytes();
            this.setDaemon(true);
            this.setName(dao.getName() + " backend " + backendId + " event index rebuilder thread");
        }

        private void saveRebuildProgress(RebuilderProgress progress) throws IOException {
            store.store(storeKey, progress.toString().getBytes());
        }

        private RebuilderProgress loadRebuildProgress() throws IOException {
            final byte[] data = store.load(storeKey);
            if (data == null || data.length == 0) return null;
            try {
                return RebuilderProgress.parse(new String(data));
            } catch (IllegalArgumentException e) {
                logger.error("exception parsing index rebuilder progress", e);
                return null;
            }
        }

        private void logStatus(String msg) {
            long now = System.currentTimeMillis();
            if (now > nextStatusLog) {
                long count = -1;
                EventIndexBackendConfiguration configuration = backends.get(backendId);
                if (configuration != null) {
                    EventIndexBackend backend = configuration.getBackend();
                    if (backend != null) {
                        try {
                            count = backend.count();
                        } catch (ZepException e) {
                            // ignore it
                        } catch (RuntimeException e) {
                            // ignore it
                        }
                    }
                }
                WorkQueue q = workQueues.get(backendId);
                logger.info(getName() + " " + msg +
                        " [current queue size: " +
                        (q == null ? "n/a" : q.size()) +
                        ", index size: " +
                        ((count >= 0) ? count : "unknown") +
                        "]");
                nextStatusLog = now + 60000; // no more than once a minute
            }
        }

        @Override
        public void run() {
            logger.info(getName() + " has started its run");
            EventIndexBackendConfiguration configuration = backends.get(backendId);
            if (configuration == null) {
                logger.error(getName() + " running with a missing backend configuration! Exiting!");
                return;
            }
            EventIndexBackend backend = configuration.getBackend();
            if (backend == null) {
                logger.error(getName() + " running with a missing backend! Exiting!");
                return;
            }

            while (rebuilders.get(backendId) == this) {
                try {
                    if (!backend.isReady()) {
                        logStatus("is waiting for backend to be ready");
                        sleep(1000);
                        continue;
                    }

                    if (enableAsyncProcessing && configuration.isAsyncUpdates() 
                            && (workQueues.get(backendId)==null || !workQueues.get(backendId).isReady()) ) {
                        logger.warn(getName() + " is waiting for work queue to be ready");
                        sleep(1000);
                        continue;
                    }

                    RebuilderProgress progress = loadRebuildProgress();
                    if (forceRebuild) {
                        logStatus("is starting a new rebuild");
                        saveRebuildProgress(RebuilderProgress.begin(System.currentTimeMillis()));
                        forceRebuild = false;
                        progress = loadRebuildProgress();
                    }

                    if (progress == null || progress.done) {
                        logStatus("is dormant");
                        sleep(1000);
                        continue;
                    }

                    int batchSize = configuration.getBatchSize();
                    EventBatch batch = null;
                    backendsUse.lock();
                    try {
                        if (configuration.isWriter()) {
                            List<EventSummary> events = null;
                            try {
                                batch = eventDao.listBatch(progress.nextBatch, progress.throughTime, batchSize);
                                events = batch.events;
                            } catch (RuntimeException e) {
                                logger.debug("Unable to listBatch due to exception: " + e.getMessage(), e);
                            }

                            if (enableAsyncProcessing && configuration.isAsyncUpdates()) {
                                if (events.isEmpty()) {
                                    // Finished!
                                    workQueues.get(backendId).add(EventIndexBackendTask.Flush());
                                } else {
                                    logger.debug("Converting {} events into tasks.", events.size());
                                    List<EventIndexBackendTask> tasks = Lists.newArrayListWithExpectedSize(events.size());
                                    for (EventSummary event : events)
                                        tasks.add(EventIndexBackendTask.Index(event.getUuid(), event.getLastSeenTime()));
                                    logger.debug("Queuing up another {} events.", events.size());
                                    workQueues.get(backendId).addAll(tasks);
                                    logger.debug("Done queuing up {} events.", events.size());
                                }
                                logStatus("queued events up to:" + batch);
                            } else {
                                backend.index(events);
                                logStatus("indexed events up to:" + batch);
                                if (events.isEmpty()) {
                                    // Finished!
                                    backend.flush();
                                }
                            }
                        }
                    } finally { backendsUse.unlock(); }
                    if (batch.events.isEmpty())
                        progress = RebuilderProgress.done(progress);
                    else
                        progress = RebuilderProgress.next(progress, batch.nextParams);
                    saveRebuildProgress(progress);
                } catch (InterruptedException e) {
                    // ignore it
                } catch (ZepException e) {
                    logger.warn("error while rebuilding for " + getName(), e);
                } catch (IOException e) {
                    logger.warn("error while rebuilding for " + getName(), e);
                } catch (RuntimeException e) {
                    logger.warn("error while rebuilding for " + getName(), e);
                }
            }
            logger.info(getName() + " has ended its run");
        }
    }

}
