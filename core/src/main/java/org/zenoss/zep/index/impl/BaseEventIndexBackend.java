/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010, 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.index.impl;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.zenoss.protobufs.zep.Zep.*;
import org.zenoss.zep.Messages;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.impl.ThreadRenamingRunnable;
import org.zenoss.zep.index.EventIndexBackend;
import org.zenoss.zep.index.SavedSearchProcessor;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


public abstract class BaseEventIndexBackend<SS extends SavedSearch> implements EventIndexBackend {
    protected final Messages messages;
    private final TaskScheduler scheduler;
    private final UUIDGenerator uuidGenerator;

    private final Map<String, SS> savedSearches = new ConcurrentHashMap<String, SS>();

    private static final Logger logger = LoggerFactory.getLogger(EventIndexBackend.class);

    protected BaseEventIndexBackend(Messages messages, TaskScheduler scheduler, UUIDGenerator uuidGenerator) {
        this.messages = messages;
        this.scheduler = scheduler;
        this.uuidGenerator = uuidGenerator;
    }

    public void close() {
        closeSavedSearches();
    }

    private static class TagSeverities {

        public static class Counter {
            public int count = 0;
            public int ackCount = 0;
        }

        public final Map<EventSeverity,Counter> counters = Maps.newEnumMap(EventSeverity.class);
        public int total = 0;

        public void updateCounts(final EventSeverity severity, final int count, boolean isAcknowledged) {
            this.total += count;
            Counter counter = counters.get(severity);
            if (counter == null) {
                counter = new Counter();
                counters.put(severity, counter);
            }
            ++counter.count;
            if (isAcknowledged)
                ++counter.ackCount;
        }

        public EventTagSeverities toEventTagSeverities(String uuid) {
            EventTagSeverities.Builder builder = EventTagSeverities.newBuilder();
            builder.setTagUuid(uuid);
            builder.setTotal(total);
            for (Map.Entry<EventSeverity, Counter> entry : counters.entrySet()) {
                Counter counter = entry.getValue();
                builder.addSeverities(EventTagSeverity
                                      .newBuilder()
                                      .setSeverity(entry.getKey())
                                      .setCount(counter.count)
                                      .setAcknowledgedCount(counter.ackCount)
                                      .build());
            }
            return builder.build();
        }
    }

    public static interface EventTagSeverityCounter {
        /**
         * Update the counts of the event tag severities.
         * @param uuid the event tag UUID
         * @param severity the severity of the event
         * @param count the number of events
         * @param acknowledged the status of the event is "acknowledged"
         */
        void update(String uuid, EventSeverity severity, int count, boolean acknowledged);
    }

    @Override
    public EventTagSeveritiesSet getEventTagSeverities(EventFilter filter) throws ZepException {
        final Map<String, TagSeverities> tagSeveritiesMap = Maps.newHashMap();
        final boolean hasTagsFilter = filter.getTagFilterCount() > 0;
        for (EventTagFilter eventTagFilter : filter.getTagFilterList()) {
            for (String eventTagUuid : eventTagFilter.getTagUuidsList()) {
                tagSeveritiesMap.put(eventTagUuid, new TagSeverities());
            }
        }
        searchEventTagSeverities(filter, new EventTagSeverityCounter() {
            @Override
            public void update(String uuid, EventSeverity severity, int count, boolean acknowledged) {
                TagSeverities severities = tagSeveritiesMap.get(uuid);
                if (severities == null && !hasTagsFilter) {
                    severities = new TagSeverities();
                    tagSeveritiesMap.put(uuid, severities);
                }
                if (severities != null)
                    severities.updateCounts(severity, count, acknowledged);
            }
        });
        EventTagSeveritiesSet.Builder builder = EventTagSeveritiesSet.newBuilder();
        for (Map.Entry<String, TagSeverities> entry : tagSeveritiesMap.entrySet()) {
            builder.addSeverities(entry.getValue().toEventTagSeverities(entry.getKey()));
        }
        return builder.build();
    }

    public final String createSavedSearch(EventQuery eventQuery) throws ZepException {
        if (eventQuery.getTimeout() < 1) {
            throw new ZepException("Invalid timeout: " + eventQuery.getTimeout());
        }
        final String uuid = uuidGenerator.generate().toString();
        SS search = null;
        try {
            search = buildSavedSearch(uuid, eventQuery);
            savedSearches.put(uuid, search);
            scheduleSearchTimeout(search);
        } catch (Exception e) {
            logger.warn("Exception creating saved search", e);
            closeSavedSearch(uuid);
            if (search != null) {
                try {
                    search.close();
                } catch (IOException ioe) {
                    logger.warn("Exception closing saved search", ioe);
                }
            }
            if (e instanceof ZepException) {
                throw (ZepException) e;
            }
            throw new ZepException(e);
        }
        return uuid;
    }

    private EventSummaryResult savedSearchInternal(String uuid, int offset, int limit,
                                                   SavedSearchProcessor<SS> processor) throws ZepException {
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

    public final EventSummaryResult savedSearch(String uuid, int offset, int limit) throws ZepException {
        return savedSearchInternal(uuid, offset, limit, savedSearchProcessor());
    }

    public final EventSummaryResult savedSearchUuids(String uuid, int offset, int limit) throws ZepException {
        return savedSearchInternal(uuid, offset, limit, savedSearchUuidsProcessor());
    }

    public final void closeSavedSearches() {
        while (!savedSearches.isEmpty()) {
            for (String uuid : savedSearches.keySet()) {
                SS search = savedSearches.remove(uuid);
                if (search != null) {
                    cancelSearchTimeout(search);
                    try {
                        search.close();
                    } catch (IOException e) {
                        logger.warn("Failed closing saved search", e);
                    }
                }
            }
        }
    }

    public final void closeSavedSearch(String uuid) {
        final SS search = savedSearches.remove(uuid);
        if (search == null)
            return;
        logger.debug("Deleting saved search: {}", uuid);
        cancelSearchTimeout(search);
        try {
            search.close();
        } catch (IOException e) {
            logger.warn("Failed closing saved search", e);
        }
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
                    logger.warn("Failed closing saved search", e);
                }
            }
        }, "ZEP_SAVED_SEARCH_TIMEOUT"), d));
    }

    /**
     * Iterate over all events matching the filter, updating the counter once per event.
     */
    protected abstract void searchEventTagSeverities(EventFilter filter, EventTagSeverityCounter counter) throws ZepException;

    /**
     * Build a SavedSearch object that can be used by the savedSearchProcessor
     * or savedSearchUuidsProcessor.
     * @param uuid a unique handle to the saved search
     * @param eventQuery specify which events and in which order
     * @return implementation-specific data structure representing the saved search state
     * @throws ZepException
     */
    protected abstract SS buildSavedSearch(String uuid, EventQuery eventQuery) throws ZepException;

    /**
     * Provides a processor for executing saved searches.
     *
     * @return A processor that can return events matching a saved search.
     */
    protected abstract SavedSearchProcessor<SS> savedSearchProcessor();

    /**
     * Provides a processor for executing saved searches.
     * The processor only returns the UUIDs of the matching events.
     *
     * @return A processor that can return UUIDs of events matching a saved search.
     */
    protected abstract SavedSearchProcessor<SS> savedSearchUuidsProcessor();

}
