/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao;

import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.zep.ZepException;

import java.util.List;

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
