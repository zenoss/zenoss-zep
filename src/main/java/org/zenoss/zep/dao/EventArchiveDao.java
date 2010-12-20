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

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;

/**
 * DAO which provides an interface to the event archive.
 */
public interface EventArchiveDao extends Partitionable, Purgable {
    /**
     * Creates an entry in the event archive table for the specified event
     * occurrence.
     * 
     * @param event
     *            Event occurrence.
     * @return The UUID of the created event summary.
     * @throws ZepException
     *             If an error occurs.
     */
    public String create(Event event) throws ZepException;

    /**
     * Deletes the entry in the event archive table with the specified UUID.
     * 
     * @param uuid
     *            UUID of entry in event archive table.
     * @return The number of rows affected by the query.
     * @throws ZepException
     *             If an error occurs.
     */
    public int delete(String uuid) throws ZepException;

    /**
     * Returns the event summary in the archive table with the specified UUID.
     * 
     * @param uuid
     *            UUID of entry in event summary table.
     * @return The event summary entry, or null if not found.
     * @throws ZepException
     *             If an error occurs.
     */
    public EventSummary findByUuid(String uuid) throws ZepException;

    /**
     * Retrieves event summary entries with the specified UUIDs.
     * 
     * @param uuids
     *            UUIDs to find.
     * @return The matching event summary entries.
     * @throws ZepException
     *             If an error occurs.
     */
    public List<EventSummary> findByUuids(List<String> uuids)
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

}
