/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao;

import org.zenoss.zep.ZepException;

import java.util.Collection;
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
     * Deletes the signals with the specified UUIDs.
     *
     * @param uuids UUIDs of the signal spool items to delete.
     * @return The number of rows affected by the query.
     * @throws ZepException If an error occurs deleting the signal spool items.
     */
    public int delete(List<String> uuids) throws ZepException;

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
     * Deletes all spooled items with the specified event summary UUID.
     * 
     * @param eventSummaryUuid Event summary UUID.
     * @return The number of affected rows.
     * @throws ZepException If an error occurs deleting the signal spool items.
     */
    public int deleteByEventSummaryUuid(String eventSummaryUuid) throws ZepException;

    /**
     * Deletes all spooled items with the specified event summary UUIDs.
     *
     * @param eventSummaryUuids Event summary UUIDs.
     * @return The number of affected rows.
     * @throws ZepException If an error occurs deleting the signal spool items.
     */
    public int deleteByEventSummaryUuids(Collection<String> eventSummaryUuids) throws ZepException;

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
     * Updates the EventSignalSpool entry in the database.
     * 
     * @param spool Event signal spool.
     * @return The number of affected rows.
     * @throws ZepException If an error occurs updating the signal spool.
     */
    public int update(EventSignalSpool spool) throws ZepException;

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
     * Returns all spools for the given event summary UUIDs.
     *
     * @param eventSummaryUuids Event summary UUIDs.
     * @return A list of all spools for the event summary UUIDs.
     * @throws ZepException If an error occurs.
     */
    public List<EventSignalSpool> findAllByEventSummaryUuids(Collection<String> eventSummaryUuids) throws ZepException;

    /**
     * Returns the next flush time in the spool.
     *
     * @return The next flush time in the spool. If there are no spooled items, returns 0.
     * @throws ZepException If an error occurs.
     */
    public long getNextFlushTime() throws ZepException;
}
