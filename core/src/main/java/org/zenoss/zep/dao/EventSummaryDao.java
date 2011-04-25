/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao;

import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.zep.ZepException;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * DAO which provides an interface to the event summary table.
 */
public interface EventSummaryDao extends EventSummaryBaseDao {

    /**
     * Creates a clear event with the specified clear classes.
     * 
     * @param event
     *            Event occurrence.
     * @param clearClasses
     *            Clear classes.
     * @return The UUID of the created clear event, or null if this clear
     *         event didn't clear any existing events and was dropped.
     * @throws ZepException If an exception occurred.
     */
    public String createClearEvent(Event event, Set<String> clearClasses)
            throws ZepException;

    /**
     * Updates event summaries recorded with null device UUIDs, after
     * receiving a ModelChange event for the device's addition - sets the
     * device UUID for all matching events summaries with null UUID, and updates
     * the summary update time.
     * 
     * @param type The model type.
     * @param id The ID of the element which has been identified.
     * @param uuid The UUID of the element.
     * @param parentUuid The UUID of the element's parent (For components, this will be the
     *                   device UUID.
     * @return The number of rows affected by the update.
     * @throws ZepException
     *             If an exception occurred.
     */
    public int reidentify(ModelElementType type, String id, String uuid, String parentUuid) throws ZepException;

    /**
     * De-identifies a previously identified UUID on an event. This can occur
     * if a device has been removed from Zenoss.
     *
     * @param uuid The previous UUID of the device.
     * @return The number of affected rows.
     * @throws ZepException If an exception occurred.
     */
    public int deidentify(String uuid) throws ZepException;

    /**
     * Ages events from the summary database which are older than the specified
     * interval and whose severity is less than the specified maximum severity.
     * 
     * @param agingInterval
     *            Aging duration.
     * @param unit
     *            Aging unit.
     * @param maxSeverity
     *            The maximum severity of events to age. Only events with a
     *            severity less than this severity are aged from the database.
     * @param limit
     *            The maximum number of events to age.
     * @return The number of aged events.
     * @throws ZepException
     *             If an error occurs.
     */
    public int ageEvents(long agingInterval, TimeUnit unit,
            EventSeverity maxSeverity, int limit) throws ZepException;

    /**
     * Reopens events with event status of {@link EventStatus#STATUS_AGED},
     * {@link EventStatus#STATUS_CLOSED} or {@link EventStatus#STATUS_CLEARED}.
     * 
     * @param uuids
     *            The event UUIDs to return to {@link EventStatus#STATUS_NEW}.
     * @param userUuid
     *            The UUID of the user who acknowledged the event.
     * @param userName
     *            The name of the user who acknowledged the event.
     * @return The number of reopened events.
     * @throws ZepException
     *             If an error occurs.
     */
    public int reopen(List<String> uuids, String userUuid, String userName) throws ZepException;

    /**
     * Acknowledges the events with event status of
     * {@link EventStatus#STATUS_NEW} or {@link EventStatus#STATUS_SUPPRESSED}
     * with the specified UUIDs. The suppressed by event UUID column is cleared
     * if it is set.
     * 
     * @param uuids
     *            UUIDs of events to acknowledge.
     * @param userUuid
     *            The UUID of the user who acknowledged the event.
     * @param userName
     *            The name of the user who acknowledged the event.
     * @return The number of acknowledged events.
     * @throws ZepException
     *             If an error occurs.
     */
    public int acknowledge(List<String> uuids, String userUuid, String userName)
            throws ZepException;

    /**
     * Suppresses the events with event status of {@link EventStatus#STATUS_NEW}.
     * 
     * @param uuids
     *            UUIDs of events to suppress.
     * @return The number of suppressed events.
     * @throws ZepException
     *             If an error occurs.
     */
    public int suppress(List<String> uuids) throws ZepException;

    /**
     * Closes the events with the specified UUIDs.
     * 
     * @param uuids
     *            UUIDs of events to close.
     * @param userUuid
     *            The UUID of the user who acknowledged the event.
     * @param userName
     *            The name of the user who acknowledged the event.
     * @return The number of closed events.
     * @throws ZepException
     *             If an error occurs.
     */
    public int close(List<String> uuids, String userUuid, String userName) throws ZepException;

    /**
     * Archives events with the specified UUIDs.
     *
     * @param uuids UUIDs of events to move to the archive.
     * @return The number of archived events.
     * @throws ZepException If an error occurs.
     */
    public int archive(List<String> uuids) throws ZepException;

    /**
     * Moves all events with last seen time before the duration and a closed
     * status ({@link EventStatus#STATUS_CLOSED},
     * {@link EventStatus#STATUS_CLEARED}, {@link EventStatus#STATUS_AGED}) to
     * the event archive.
     * 
     * @param duration
     *            Archive duration.
     * @param unit
     *            Archive time unit.
     * @param limit
     *            The maximum number of events to archive.
     * @return The number of archived events.
     * @throws ZepException
     *             If an error occurs.
     */
    public int archive(long duration, TimeUnit unit, int limit)
            throws ZepException;
}
