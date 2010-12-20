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

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.zep.ZepException;

/**
 * DAO for Events.
 */
public interface EventDao extends Partitionable, Purgable {
    /**
     * Creates the event in the database.
     * 
     * @param event
     *            Event to create.
     * @return The UUID of the created event.
     * @throws ZepException
     *             If the event could not be created.
     */
    public String create(Event event) throws ZepException;

    /**
     * Deletes the event with the specified id from the database.
     * 
     * @param uuid
     *            UUID of the event to delete.
     * @return The number of affected rows.
     * @throws ZepException
     *             If the event could not be deleted.
     */
    public int delete(String uuid) throws ZepException;

    /**
     * Returns the event with the matching UUID, or null if not found.
     * 
     * @param uuid
     *            UUID of event to find.
     * @return The matching event, or null if not found.
     * @throws ZepException
     *             If the event database cannot be queried.
     */
    public Event findByUuid(String uuid) throws ZepException;
}
