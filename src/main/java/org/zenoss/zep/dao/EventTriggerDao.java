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

import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.zep.ZepException;

/**
 * DAO for Event triggers.
 */
public interface EventTriggerDao {
    /**
     * Creates the event trigger. Event trigger subscriptions on the specified
     * trigger are not created - use {@link EventTriggerSubscriptionDao} to
     * create subscriptions.
     * 
     * @param trigger
     *            The trigger to create.
     * @throws ZepException
     *             If an error occurs creating the trigger.
     */
    public void create(EventTrigger trigger) throws ZepException;

    /**
     * Deletes the trigger with the specified UUID.
     * 
     * @param uuid
     *            UUID of trigger to delete.
     * @return The number of rows affected by the query.
     * @throws ZepException
     *             If an error occurs deleting the trigger.
     */
    public int delete(String uuid) throws ZepException;

    /**
     * Finds the trigger with the specified UUID. Returns null if the trigger is
     * not found.
     * 
     * @param uuid
     *            UUID of trigger to find.
     * @return The trigger, or null if not found.
     * @throws ZepException
     *             If an error occurs looking up the trigger.
     */
    public EventTrigger findByUuid(String uuid) throws ZepException;

    /**
     * Finds all triggers in the database.
     * 
     * @return A list containing all of the triggers in the database.
     * @throws ZepException
     *             If an error occurs looking up the triggers.
     */
    public List<EventTrigger> findAll() throws ZepException;

    /**
     * Finds all enabled triggers in the database.
     *
     * @return A list containing all of the enabled triggers in the database.
     * @throws ZepException
     *             If an error occurs looking up the triggers.
     */
    public List<EventTrigger> findAllEnabled() throws ZepException;

    /**
     * Modifies the specified trigger in the database. Event trigger
     * subscriptions on the specified trigger are not modified - use
     * {@link EventTriggerSubscriptionDao} to create or modify subscriptions.
     * 
     * @param trigger
     *            The trigger to modify (UUID must be specified).
     * @return The number of affected rows from the query.
     * @throws ZepException
     *             If an error occurs modifying the trigger.
     */
    public int modify(EventTrigger trigger) throws ZepException;
}
