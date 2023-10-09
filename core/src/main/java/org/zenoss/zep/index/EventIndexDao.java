/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.zep.index;

import org.zenoss.protobufs.zep.Zep.EventFilter;
import org.zenoss.protobufs.zep.Zep.EventQuery;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryResult;
import org.zenoss.protobufs.zep.Zep.EventTagSeveritiesSet;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.Purgable;

import java.util.List;

/**
 * DAO for Event Index.
 */
public interface EventIndexDao extends Purgable {
    /**
     * @return String name of the index
     */
    String getName();

    /**
     * Returns the number of indexed documents in this index.
     *
     * @return The number of indexed documents in the index.
     * @throws ZepException If the number of documents can't be retrieved.
     */
    int getNumDocs() throws ZepException;

    /**
     * Add an event to the index, replaces existing event with the same UUID.
     *
     * @param event Event to index.
     * @throws org.zenoss.zep.ZepException
     *             If the event could not be indexed.
     */
    void index(EventSummary event) throws ZepException;

    /**
     * Stage an event to be indexed with a batch index. Must call commit() to
     * store changes.
     * @param event EventSummary to index
     * @throws ZepException If an error occurs.
     */
    void stage(EventSummary event) throws ZepException;

    /**
     * Stage an event to be deleted with a batch index. Must call commit() to
     * write out changes.
     *
     * @param eventUuid UUID of event to delete from index.
     * @throws ZepException If an error occurs.
     */
    void stageDelete(String eventUuid) throws ZepException;

    /**
     * Commit any staged changes to the index.
     * @throws ZepException If an error occurs.
     */
    void commit() throws ZepException;

    /**
     * Commits any staged changes to the index and optionally optimizes the index.
     *
     * @param forceOptimize If true, the index is optimized after committing the changes.
     * @throws ZepException If an error occurs.
     * @deprecated Optimizing the index is discouraged - the forceOptimize option is ignored. Use
     *             {@link #commit()} instead.
     */
    @Deprecated
    void commit(boolean forceOptimize) throws ZepException;

    /**
     * Add many events to the index, replaces existing events with the same UUID.
     *
     * @param events EventSummary to index
     * @throws org.zenoss.zep.ZepException
     *             If an event could not be indexed.
     */
    void indexMany(List<EventSummary> events) throws ZepException;

    /**
     * Retrieves event summary entries matching the specified query.
     *
     * @param request
     *            Event summary query.
     * @return The matching event summary entries.
     * @throws ZepException
     *             If an error occurs.
     */
    EventSummaryResult list(EventSummaryRequest request)
            throws ZepException;

    /**
     * Retrieves event summary UUIDs matching the specified query.
     *
     * @param request
     *            Event summary query.
     * @return The matching event summary entries. Only the UUIDs of each event
     *         summary will be returned.
     * @throws ZepException
     *             If an error occurs.
     */
    EventSummaryResult listUuids(EventSummaryRequest request)
            throws ZepException;

    /**
     * Deletes the event with the specified uuid from the index
     *
     * @param uuid
     *            UUID of the event to delete.
     * @throws ZepException
     *             If the event could not be deleted.
     */
    void delete(String uuid) throws ZepException;

    /**
     * Deletes the event with the specified UUIDs from the index.
     *
     * @param uuids
     *            UUIDs of the event to delete.
     * @throws ZepException
     *             If the event could not be deleted.
     */
    void delete(List<String> uuids) throws ZepException;

    /**
     * Returns the event with the matching UUID, or null if not found.
     *
     * @param uuid
     *            UUID of event to find.
     * @return The matching event, or null if not found.
     * @throws ZepException
     *             If the event database cannot be queried.
     */
    EventSummary findByUuid(String uuid) throws ZepException;

    /**
     * Removes all documents from the index.
     *
     * @throws ZepException
     *             If the event database cannot be queried.
     */
    void clear() throws ZepException;

    /**
     * Returns event tag severities for the specified filter. If the filter specifies
     * tags, then there will be one EventTagSeverities object returned in the
     * set for each tag. If the filter doesn't specify tags, then there
     * will be one EventTagSeverities object for each element_uuid which matches
     * the filter.
     *
     * @param filter The filter used to query for tag severities information.
     * @return The tag severities summary for each tag.
     * @throws ZepException If an error occurs.
     */
    EventTagSeveritiesSet getEventTagSeverities(EventFilter filter) throws ZepException;

    /**
     * Creates a saved search with the given event query.
     *
     * @param query Event query.
     * @return A UUID of the saved search.
     * @throws ZepException If an exception occurs creating the saved query.
     */
    String createSavedSearch(EventQuery query) throws ZepException;

    /**
     * Execute a saved search and return limit results at the specified offset.
     *
     * @param uuid UUID of the saved search (returned from {@link #createSavedSearch(EventQuery)}.
     * @param offset Offset within the search to return.
     * @param limit Number of results to return.
     * @return The result of the search.
     * @throws ZepException If an exception occurs performing the saved query.
     */
    EventSummaryResult savedSearch(String uuid, int offset, int limit) throws ZepException;

    /**
     * Executes a saved search and returns <code>limit</code> results at the specified <code>offset</code>.
     * Only the UUIDs of the matching events are returned.
     *
     * @param uuid UUID of the saved search (returned from {@link #createSavedSearch(EventQuery)}.
     * @param offset Offset within the search to return.
     * @param limit Number of results to return.
     * @return The result of the search.
     * @throws ZepException If an exception occurs performing the saved query.
     */
    EventSummaryResult savedSearchUuids(String uuid, int offset, int limit) throws ZepException;

    /**
     * Removes the saved search with the specified UUID.
     *
     * @param uuid UUID of saved search to remove.
     * @return The UUID of the saved search (if it was removed), or null if it was not found.
     * @throws ZepException If the saved search could not be deleted.
     */
    String deleteSavedSearch(String uuid) throws ZepException;

    /**
     * @return the sum of the file sizes in the index directory.
     */
    long getSize();
}
