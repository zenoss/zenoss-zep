/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/
package org.zenoss.zep.index;

import org.zenoss.protobufs.zep.Zep.*;
import org.zenoss.zep.ZepException;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;

public interface EventIndexBackend {

    /**
     * Checks that the backend has been correctly initialized and is ready to
     * handle requests.
     */
    boolean isReady();

    /**
     * Checks that the connection with the backend is working.
     */
    boolean ping();

    /**
     * Releases any resources held by this backend.
     *
     * This implicitly calls {@link #closeSavedSearches()}.
     *
     * @throws IOException If there were problems closing the backend.
     *                      It should be safe to just log a warning and proceed.
     */
    void close() throws IOException;

    /**
     * Returns the number of events in this index.
     *
     * @return The number of events in the index.
     * @throws ZepException If the number of events can't be retrieved.
     */
    long count() throws ZepException;

    /**
     * @return the sum of the file sizes in the index directory.
     * @throws UnsupportedOperationException unless its actually implemented.
     */
    long sizeInBytes() throws UnsupportedOperationException;
    /**
     * Add or update an event in the index.
     *
     * Changes may or may not actually be stored before flush() is called.
     *
     * @param event EventSummary to index
     * @throws ZepException If an error occurs.
     */
    void index(EventSummary event) throws ZepException;

    /**
     * Add or update many events in the index.
     *
     * Changes may or may not actually be stored before flush() is called.
     *
     * @param events EventSummary objects to index
     * @throws ZepException If an error occurs.
     */
    void index(Collection<EventSummary> events) throws ZepException;

    /**
     * Delete an event from the index.
     *
     * Changes may or may not actually be stored before flush() is called.
     *
     * @param eventUuid UUID of event to delete from index.
     * @throws ZepException If an error occurs.
     */
    void delete(String eventUuid) throws ZepException;

    /**
     * Delete many events from the index.
     *
     * Changes may or may not actually be stored before flush() is called.
     *
     * @param eventUuids UUIDs of event to delete from index.
     * @throws ZepException If an error occurs.
     */
    void delete(Collection<String> eventUuids) throws ZepException;


    /**
     * Flush changes to the index storage.
     *
     * You may not need to call this, and will likely get better write throughput without.
     *
     * @throws ZepException If an error occurs.
     */
    void flush() throws ZepException;

    /**
     * Purge records which are older than the specified time.
     *
     * Changes may or may not actually be stored before flush() is called.
     *
     * @param threshold A specified point in time.
     * @throws ZepException If an exception occurs during purging
     */
    void purge(Date threshold) throws ZepException;

    /**
     * Clear all records from the index.
     *
     * Changes may or may not actually be stored before flush() is called.
     *
     * @throws ZepException If an error occurs.
     */
    void clear() throws ZepException;

    /**
     * Retrieves event summary entries matching the specified query.
     *
     * @param request Event summary query.
     * @return The matching event summary entries.
     * @throws ZepException If an error occurs.
     */
    EventSummaryResult list(EventSummaryRequest request) throws ZepException;

    /**
     * Retrieves event summary UUIDs matching the specified query.
     *
     * @param request Event summary query.
     * @return The matching event summary entries.
     *         Only the UUIDs of each event summary will be returned.
     * @throws ZepException If an error occurs.
     */
    EventSummaryResult listUuids(EventSummaryRequest request) throws ZepException;

    /**
     * Returns the event with the matching UUID, or null if not found.
     *
     * @param uuid UUID of event to find.
     * @return The matching event, or null if not found.
     * @throws ZepException If the event database cannot be queried.
     */
    EventSummary findByUuid(String uuid) throws ZepException;

    /**
     * Returns event tag severities for the specified filter.
     *
     * If the filter specifies tags, then there will be one EventTagSeverities
     * object returned in the set for each tag. If the filter doesn't specify
     * tags, then there will be one EventTagSeverities object for each
     * element_uuid which matches the filter.
     *
     * @param filter The filter used to query for tag severities information.
     * @return The tag severities summary for each tag.
     * @throws ZepException If an error occurs.
     */
    EventTagSeveritiesSet getEventTagSeverities(EventFilter filter) throws ZepException;

    /**
     * Creates a (temporary) handle to a search.
     *
     * The handle may be passed to {@link #savedSearch(String, int, int)} or
     * {@link #savedSearchUuids(String, int, int)} to retrieve portions of the
     * result. Handles may automatically expire after some time, and their
     * search result resources be released. Nevertheless, it is good form to
     * explicitly call {@link #closeSavedSearch(String)} as soon it is no
     * longer needed.
     *
     * @param eventQuery The search query
     * @return A unique handle used to fetch the query results
     * @throws ZepException If an error occurs.
     */
    String createSavedSearch(EventQuery eventQuery) throws ZepException;

    /**
     * Execute or fetch a portion of the results of the saved search.
     *
     * @param savedSearchId A unique handle, identifying the saved search
     *                      (returned from {@link #createSavedSearch(EventQuery)}.
     * @param offset Offset within the search to return.
     * @param limit Number of results to return.
     * @return The result of the search.
     * @throws ZepException If the savedSearchId is invalid or has expired, or some other exception occurs.
     */
    EventSummaryResult savedSearch(String savedSearchId, int offset, int limit) throws ZepException;

    /**
     * Execute or fetch a portion of the results of the saved search.
     *
     * Only the UUIDs of the matching events are returned.
     *
     * @param savedSearchId A unique handle, identifying the saved search
     *                      (returned from {@link #createSavedSearch(EventQuery)}.
     * @param offset Offset within the search to return.
     * @param limit Number of results to return.
     * @return The result of the search.
     * @throws ZepException If the savedSearchId is invalid or has expired, or some other exception occurs.
     */
    EventSummaryResult savedSearchUuids(String savedSearchId, int offset, int limit) throws ZepException;

    /**
     * Invalidate the savedSearchId and release any resources being used by the query/result.
     *
     * It is not an error to close a paged result that has already expired or been invalidated.
     *
     * @param savedSearchId A unique handle, identifying the saved search
     *                      (returned from {@link #createSavedSearch(EventQuery)}.
     */
    void closeSavedSearch(String savedSearchId);

    /**
     * Invalidate all saved search IDs, and release any resources being used by their queries/results.
     */
    void closeSavedSearches();
}
