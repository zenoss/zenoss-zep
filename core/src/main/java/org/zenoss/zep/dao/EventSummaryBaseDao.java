/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao;

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventDetailSet;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.plugins.EventPreCreateContext;
import org.zenoss.zep.ZepException;

import java.util.List;

/**
 * Interface representing operations supported on both event summary and event archive.
 */
public interface EventSummaryBaseDao {
    /**
     * Creates or updates a summary entry in the event summary table for the
     * specified event occurrence.
     * 
     * @param event The event occurrence.
     * @param context Additional information used to create the event.
     * @return The UUID of the created (or updated) event.
     * @throws org.zenoss.zep.ZepException If an error occurs.
     */
    public String create(Event event, EventPreCreateContext context) throws ZepException;
    
    /**
     * Returns the event summary in the archive table with the specified UUID.
     * 
     * @param uuid UUID of entry in event summary table.
     * @return The event summary entry, or null if not found.
     * @throws ZepException If an error occurs.
     */
    public EventSummary findByUuid(String uuid) throws ZepException;

    /**
     * Retrieves event summary entries with the specified UUIDs.
     * 
     * @param uuids UUIDs to find.
     * @return The matching event summary entries.
     * @throws ZepException
     *             If an error occurs.
     */
    public List<EventSummary> findByUuids(List<String> uuids) throws ZepException;
    
    /**
     * Add a note to the event.
     *
     * @param uuid The event UUID.
     * @param note The note to add.
     * @return The number of rows affected by the query.
     * @throws ZepException If an error occurs.
     */
    public int addNote(String uuid, EventNote note) throws ZepException;

    /**
     * Updates the event with the specified UUID, to add/merge/update
     * detail values given in details parameter.
     *
     * @param uuid UUID of event to update.
     * @param details list of name-value pairs of details to add/merge/update 
     *                (setting a detail to '' or null will delete it from the
     *                list of event details)
     * @return The number of affected events.
     * @throws ZepException If an error occurs.
     */
    public int updateDetails(String uuid, EventDetailSet details) throws ZepException;

    /**
     * Used to page over all events in the database (for rebuilding database index).
     * 
     * @param startingUuid The starting UUID of this batch. The first query should pass null as this parameter and
     *                     subsequent queries should use the last event summary UUID from the previous query.
     * @param maxUpdateTime The maximum update time to include.
     * @param limit The maximum number of events to return in this batch.
     * @return A list of event summaries matching the specified parameters, or an empty list if none exist.
     * @throws ZepException If an exception occurs.
     */
    public List<EventSummary> listBatch(String startingUuid, long maxUpdateTime, int limit) throws ZepException;

    /**
     * Method used to import a migrated event summary object from Zenoss 3.1.x to the new event
     * schema.
     *
     * @param eventSummary Event summary to import.
     * @throws ZepException If an exception occurs importing the event.
     */
    public void importEvent(EventSummary eventSummary) throws ZepException;
}
