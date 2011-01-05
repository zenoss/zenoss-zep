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

package org.zenoss.zep.index;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryResult;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.Purgable;

/**
 * DAO for Event Index.
 */
public interface EventIndexDao extends Purgable {
    /**
     * @return String name of the index
     */
    public String getName();

    /**
     * Returns the number of indexed documents in this index.
     *
     * @return The number of indexed documents in the index.
     * @throws ZepException If the number of documents can't be retrieved.
     */
    public int getNumDocs() throws ZepException;

    /**
     * Add an event to the index, replaces existing event with the same UUID.
     * 
     * @param event Event to index.
     * @throws org.zenoss.zep.ZepException
     *             If the event could not be indexed.
     */
    public void index(EventSummary event) throws ZepException;

    /**
     * Stage an event to be indexed with a batch index. Must call commit() to
     * store changes.
     * @param event EventSummary to index
     * @throws ZepException
     */
    public void stage(EventSummary event) throws ZepException;

    /**
     * Commit any staged changes to the index.
     * @throws ZepException
     */
    public void commit() throws ZepException;

    /**
     * Commits any staged changes to the index and optionally optimizes the index.
     *
     * @param forceOptimize If true, the index is optimized after committing the changes.
     * @throws ZepException If an error occurs.
     */
    public void commit(boolean forceOptimize) throws ZepException;

    /**
     * Add many events to the index, replaces existing events with the same UUID.
     * 
     * @param events EventSummary to index
     * @throws org.zenoss.zep.ZepException
     *             If an event could not be indexed.
     */
    public void indexMany(List<EventSummary> events) throws ZepException;

    /**
     * Retrieves event summary entries matching the specified query.
     * 
     * @param request
     *            Event summary query.
     * @return The matching event summary entries.
     * @throws ZepException
     *             If an error occurs.
     */
    public EventSummaryResult list(EventSummaryRequest request)
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
    public EventSummaryResult listUuids(EventSummaryRequest request)
            throws ZepException;

    /**
     * Deletes the event with the specified uuid from the index
     * 
     * @param uuid
     *            UUID of the event to delete.
     * @return The number of affected rows.
     * @throws ZepException
     *             If the event could not be deleted.
     */
    public void delete(String uuid) throws ZepException;

    /**
     * Deletes the event with the specified UUIDs from the index.
     * 
     * @param uuids
     *            UUIDs of the event to delete.
     * @return The number of affected rows.
     * @throws ZepException
     *             If the event could not be deleted.
     */
    public void delete(List<String> uuids) throws ZepException;

    /**
     * Returns the event with the matching UUID, or null if not found.
     * 
     * @param uuid
     *            UUID of event to find.
     * @return The matching event, or null if not found.
     * @throws ZepException
     *             If the event database cannot be queried.
     */
    public EventSummary findByUuid(String uuid) throws ZepException;

    /**
     * Removes all documents from the index.
     * 
     * @throws ZepException
     *             If the event database cannot be queried.
     */
    public void clear() throws ZepException;

    /**
     * Delete all events matching request
     * 
     * @param request
     *            Filter of events to delete
     * @throws ZepException
     *             If the event database cannot be queried.
     */
    public void delete(EventSummaryRequest request) throws ZepException;

    /**
     * Calculate the number of events in each severity type for each specified
     * tag.
     * 
     * @param tags
     *            The tags to calculate severities for.
     * @return A Map of UUID to a map of event severity to the number of events
     *         in that severity.
     * @throws ZepException
     *             If an error occurs.
     */
    public Map<String, Map<EventSeverity, Integer>> countSeverities(
            Set<String> tags) throws ZepException;

    /**
     * Calculates the worst severity event for each tag in the specified
     * collection.
     * 
     * @param tags
     *            The tags to determine the worst severity event for.
     * @return A map of the tag to the worst severity event for the tag. Tags
     *         which have no events are not returned in the mapping.
     * @throws ZepException
     *             If an error occurs.
     */
    public Map<String, EventSeverity> findWorstSeverity(Set<String> tags)
            throws ZepException;
}
