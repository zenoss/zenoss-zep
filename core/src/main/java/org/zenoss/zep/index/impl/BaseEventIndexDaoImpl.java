/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010, 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.index.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.zenoss.protobufs.zep.Zep.*;
import org.zenoss.zep.Messages;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.impl.ThreadRenamingRunnable;
import org.zenoss.zep.index.EventIndexDao;
import org.zenoss.zep.index.SavedSearchProcessor;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public abstract class BaseEventIndexDaoImpl<SS extends SavedSearch> implements EventIndexDao {
    private final String name;
    private final Messages messages;
    private final TaskScheduler scheduler;
    private final UUIDGenerator uuidGenerator;
    private final Map<String, SS> savedSearches = new ConcurrentHashMap<String, SS>();

    protected static final Logger logger = LoggerFactory.getLogger(EventIndexDao.class);

    public BaseEventIndexDaoImpl(String name, Messages messages, TaskScheduler scheduler, UUIDGenerator uuidGenerator) {
        this.name = name;
        this.messages = messages;
        this.scheduler = scheduler;
        this.uuidGenerator = uuidGenerator;
    }

    @Override
    public final String getName() {
        return this.name;
    }

    public void purge(int duration, TimeUnit unit) throws ZepException {
        if (duration < 0) {
            throw new IllegalArgumentException("Duration must be >= 0");
        }
        final Date threshold = new Date(System.currentTimeMillis() - unit.toMillis(duration));
        logger.info("Purging events older than {}", threshold);
        purge(threshold);
        commit();
    }

    /**
     * Delete all events prior to the given threshold.
     *
     * @param threshold
     * @throws ZepException
     */
    protected abstract void purge(Date threshold) throws ZepException;

    @Override
    public void stage(EventSummary event) throws ZepException {
        index(event);
    }

    @Override
    public void stageDelete(String uuid) throws ZepException {
        delete(uuid);
    }


    @Override
    @Deprecated
    public final void commit(boolean forceOptimize) throws ZepException {
        commit();
    }

    public void shutdown() {
        deleteAllSavedSearches();
    }

    protected String generateUuid() {
        return uuidGenerator.generate().toString();
    }

    @Override
    public String createSavedSearch(EventQuery eventQuery) throws ZepException {
        if (eventQuery.getTimeout() < 1) {
            throw new ZepException("Invalid timeout: " + eventQuery.getTimeout());
        }
        final String uuid = generateUuid();
        SS search = null;
        try {
            search = buildSavedSearch(uuid, eventQuery);
            savedSearches.put(uuid, search);
            scheduleSearchTimeout(search);
        } catch (Exception e) {
            logger.warn("Exception while creating saved search", e);
            deleteSavedSearch(uuid);
            if (search != null) {
                try {
                    search.close();
                } catch (IOException ioe) {
                    logger.warn("Exception while closing saved search", e);
                }
            }
            if (e instanceof ZepException) {
                throw (ZepException) e;
            }
            throw new ZepException(e);
        }
        return uuid;
    }

    protected EventSummaryResult savedSearchInternal(String uuid, int offset, int limit,
                                                     SavedSearchProcessor<SS> processor)
            throws ZepException
    {
        final SS search = savedSearches.get(uuid);
        if (search == null) {
            throw new ZepException(messages.getMessage("saved_search_not_found", uuid));
        }
        try {
            /* Cancel the timeout for the saved search to prevent it expiring while in use */
            cancelSearchTimeout(search);
            return processor.result(search, offset, limit);
        } finally {
            scheduleSearchTimeout(search);
        }
    }

    @Override
    public EventSummaryResult savedSearch(String uuid, int offset, int limit) throws ZepException {
        return savedSearchInternal(uuid, offset, limit, savedSearchProcessor());
    }

    @Override
    public EventSummaryResult savedSearchUuids(String uuid, int offset, int limit) throws ZepException {
        return savedSearchInternal(uuid, offset, limit, savedSearchUuidsProcessor());
    }

    protected void deleteAllSavedSearches() {
        while (!savedSearches.isEmpty()) {
            for (String uuid : savedSearches.keySet()) {
                SS search = savedSearches.remove(uuid);
                if (search != null) {
                    cancelSearchTimeout(search);
                    try {
                        search.close();
                    } catch (IOException e) {
                        logger.warn("Exception while closing saved search", e);
                    }
                }
            }
        }
    }

    @Override
    public String deleteSavedSearch(String uuid) throws ZepException {
        final SS search = savedSearches.remove(uuid);
        if (search == null) {
            return null;
        }
        logger.debug("Deleting saved search: {}", uuid);
        cancelSearchTimeout(search);
        try {
            search.close();
        } catch (IOException e) {
            throw new ZepException(e);
        }
        return search.getUuid();
    }

    private void cancelSearchTimeout(final SS search) {
        logger.debug("Canceling timeout for saved search: {}", search.getUuid());
        search.setTimeoutFuture(null);
    }

    private void scheduleSearchTimeout(final SS search) {
        logger.debug("Scheduling saved search {} for expiration in {} seconds", search.getUuid(), search.getTimeout());
        Date d = new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(search.getTimeout()));
        search.setTimeoutFuture(scheduler.schedule(new ThreadRenamingRunnable(new Runnable() {
            @Override
            public void run() {
                logger.debug("Saved search timed out: {}", search.getUuid());
                savedSearches.remove(search.getUuid());
                try {
                    search.close();
                } catch (IOException e) {
                    logger.warn("Exception while closing saved search", e);
                }
            }
        }, "ZEP_SAVED_SEARCH_TIMEOUT"), d));
    }

    protected abstract SS buildSavedSearch(String uuid, EventQuery eventQuery) throws ZepException;

    protected abstract SavedSearchProcessor<SS> savedSearchProcessor();

    protected abstract SavedSearchProcessor<SS> savedSearchUuidsProcessor();


}
