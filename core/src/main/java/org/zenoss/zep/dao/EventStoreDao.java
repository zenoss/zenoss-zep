/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao;

import org.zenoss.protobufs.zep.Zep.EventDetailSet;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdate;
import org.zenoss.zep.ZepException;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * DAO which provides a bridge between event database storage and indexing.
 */
public interface EventStoreDao extends Purgable {
    /**
     * Finds the event summary entry with the specified UUID.
     * 
     * @param uuid
     *            The UUID of the event summary entry.
     * @return The event summary entry, or null if not found.
     * @throws ZepException
     *             If an error occurs.
     */
    EventSummary findByUuid(String uuid) throws ZepException;

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
    int update(String uuid, EventSummaryUpdate update)
            throws ZepException;

    /**
     * Updates the events with the specified UUIDs.
     *
     * @param uuids
     *            UUIDs of events to update.
     * @param update
     *            Changes to make to events.
     * @return The number of affected events.
     * @throws ZepException If an error occurs.
     */
    int update(List<String> uuids, EventSummaryUpdate update)
            throws ZepException;

    /**
     * Add a note to the event.
     *
     * @param uuid The event UUID.
     * @param note
     *            The note to add.
     * @return The number of rows affected by the query.
     * @throws ZepException
     *             If an error occurs.
     */
    int addNote(String uuid, EventNote note) throws ZepException;

    /**
     * Updates the event with the specified UUID, to add/merge/update
     * detail values given in details parameter.
     *
     * @param uuid
     *            UUID of event to update.
     * @param details
     *            list of name-value pairs of details to add/merge/update
     *            (setting a detail to '' or null will delete it from the
     *            list of event details)
     * @return The number of affected events.
     * @throws ZepException
     *             If an error occurs.
     */
    int updateDetails(String uuid, EventDetailSet details)
            throws ZepException;

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
    int ageEvents(long agingInverval, TimeUnit unit,
            EventSeverity maxSeverity, int limit, boolean inclusiveSeverity) throws ZepException;

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
    int archive(long agingInverval, TimeUnit unit, int limit)
            throws ZepException;
}
