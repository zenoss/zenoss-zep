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

import org.zenoss.zep.ZepException;

import java.util.List;

/**
 * DAO for the Event signal spool
 */
public interface EventSignalSpoolDao {
    /**
     * Creates the signal spool item.
     * 
     * @param spool
     *            EventSignalSpool to create.
     * @return The UUID of the created spool.
     * @throws ZepException
     *             If an error occurs creating signal spool.
     */
    public String create(EventSignalSpool spool) throws ZepException;

    /**
     * Deletes the signal with the specified UUID.
     * 
     * @param uuid
     *            UUID of this signal spool item to delete.
     * @return The number of rows affected by the query.
     * @throws ZepException
     *             If an error occurs deleting the signal spool item.
     */
    public int delete(String uuid) throws ZepException;

    /**
     * Deletes the signal with the specified trigger and subscription UUIDs.
     * 
     * @param triggerUuid
     *            trigger UUID of this signal spool item to delete.
     * @param eventSummaryUuid
     *            event summary UUID of this signal spool item to delete.
     * @return The number of rows affected by the query.
     * @throws ZepException
     *             If an error occurs deleting the signal spool item.
     */
    public int delete(String triggerUuid, String eventSummaryUuid)
            throws ZepException;

    /**
     * Deletes all spooled items with the specified trigger UUID.
     * 
     * @param triggerUuid
     *            Trigger UUID.
     * @return The number of affected rows.
     * @throws ZepException
     *             If an error occurs.
     */
    public int deleteByTriggerUuid(String triggerUuid) throws ZepException;

    /**
     * Finds the signal with the specified UUID. Returns null if the signal is
     * not found.
     * 
     * @param uuid
     *            UUID of signal spool item to find.
     * @return The signal spool item, or null if not found.
     * @throws ZepException
     *             If an error occurs looking up the signal spool item.
     */
    public EventSignalSpool findByUuid(String uuid) throws ZepException;

    /**
     * Modifies the flushTime for the specified signal spool item in the
     * database.
     * 
     * @param uuid
     *            UUID of signal spool item to update.
     * @param newFlushTime
     *            long timestamp of next time to signal subscribers of this
     *            spool.
     * @return The number of affected rows from the query.
     * @throws ZepException
     *             If an error occurs modifying the signal spool item.
     */
    public int updateFlushTime(String uuid, long newFlushTime)
            throws ZepException;

    /**
     * Finds the signal spool created for a specific subscription/event summary pair.
     * 
     * @param subscriptionUuid
     *            UUID of matching EventTriggerSubscription
     * @param eventSummaryUuid
     *            UUID of matching EventSummary
     * @return The signal spool item, or null if not found.
     * @throws ZepException
     *             If an error occurs looking up the signal spool item.
     */
    public EventSignalSpool findBySubscriptionAndEventSummaryUuids(
            String subscriptionUuid, String eventSummaryUuid) throws ZepException;

    /**
     * Finds all signal spools due to be signaled.
     * 
     * @return A list of spools due to be signaled.
     * @throws ZepException
     *             If an error occurs looking up the signal spool item.
     */
    public List<EventSignalSpool> findAllDue() throws ZepException;

    /**
     * Returns all spools for the given event summary UUID.
     *
     * @param eventSummaryUuid Event summary UUID.
     * @return A list of all spools for the event summary UUID.
     * @throws ZepException If an error occurs.
     */
    public List<EventSignalSpool> findAllByEventSummaryUuid(String eventSummaryUuid) throws ZepException;

    /**
     * Returns the next flush time in the spool.
     *
     * @return The next flush time in the spool. If there are no spooled items, returns 0.
     * @throws ZepException If an error occurs.
     */
    public long getNextFlushTime() throws ZepException;
}
