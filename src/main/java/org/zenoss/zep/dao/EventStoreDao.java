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

package org.zenoss.zep.dao;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryResult;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdate;
import org.zenoss.zep.EventContext;
import org.zenoss.zep.ZepException;

/**
 * DAO which provides a bridge between event database storage and indexing.
 */
public interface EventStoreDao extends Purgable {

    /**
     * Creates or updates a summary entry in the event summary table for the
     * specified event occurrence.
     * 
     * @param event
     *            The event occurrence.
     * @param eventContext
     *            The event context.
     * @throws org.zenoss.zep.ZepException
     *             If an error occurs.
     */
    public void create(Event event, EventContext eventContext)
            throws ZepException;

    /**
     * Deletes the summary entry with the specified UUID.
     * 
     * @param uuid
     *            UUID of summary entry to delete.
     * @return The number of rows affected by the query.
     * @throws ZepException
     *             If an exception occurs.
     */
    public int delete(String uuid) throws ZepException;

    /**
     * Finds the event summary entry with the specified UUID.
     * 
     * @param uuid
     *            The UUID of the event summary entry.
     * @return The event summary entry, or null if not found.
     * @throws ZepException
     *             If an error occurs.
     */
    public EventSummary findByUuid(String uuid) throws ZepException;

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
     * Retrieves event summary entries from the archive matching the specified
     * query.
     * 
     * @param request
     *            Event summary query.
     * @return The matching event summary entries.
     * @throws ZepException
     *             If an error occurs.
     */
    public EventSummaryResult listArchive(EventSummaryRequest request)
            throws ZepException;

    /**
     * Updates the event with the specified UUID.
     * 
     * @param uuid
     *            UUID of event to update.
     * @param update
     *            Changes to make to event.
     * @return The number of affected events.
     * @throws ZepException
     *             If an error occurs.
     */
    public int update(String uuid, EventSummaryUpdate update)
            throws ZepException;

    /**
     * Updates the events with the specified UUIDs.
     * 
     * @param uuids
     *            UUIDs of events to update.
     * @param update
     *            Changes to make to events.
     * @return The number of affected events.
     * @throws ZepException
     */
    public int update(List<String> uuids, EventSummaryUpdate update)
            throws ZepException;

    /**
     * Add a note to the event.
     * 
     * @param note
     *            The note to add.
     * @return The number of rows affected by the query.
     * @throws ZepException
     *             If an error occurs.
     */
    public int addNote(String uuid, EventNote note) throws ZepException;

    /**
     * Moves all events in the summary table not modified within the last aging
     * interval to the archive table. Only events with severity less than the
     * given severity are aged.
     * 
     * @param agingInverval
     *            Aging interval (in TimeUnit units).
     * @param unit
     *            Unit of time.
     * @param maxSeverity
     *            The maximum severity (exclusive) of events that will be aged.
     *            Any events with this severity or above are left in the summary
     *            table.
     * @param limit
     *            The maximum number of events to age in one batch.
     * @return The number of aged events.
     * @throws org.zenoss.zep.ZepException
     *             If an error occurs.
     */
    public int ageEvents(long agingInverval, TimeUnit unit,
            EventSeverity maxSeverity, int limit) throws ZepException;

    /**
     * Moves all events which have status {@link EventStatus#STATUS_CLOSED},
     * {@link EventStatus#STATUS_CLEARED}, or {@link EventStatus#STATUS_AGED}
     * from the summary to the archive database.
     * 
     * @param agingInverval
     *            Aging interval (in TimeUnit units).
     * @param unit
     *            Unit of time.
     * @param limit
     *            The maximum number of events to age in one batch.
     * @return The number of affected rows.
     * @throws ZepException
     *             If an error occurs.
     */
    public int archive(long agingInverval, TimeUnit unit, int limit)
            throws ZepException;

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
