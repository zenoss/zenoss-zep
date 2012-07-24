/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.plugins;

import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdate;
import org.zenoss.zep.ZepException;

import java.util.List;

/**
 * Plug-in which can be invoked after an event is updated.
 */
public abstract class EventUpdatePlugin extends EventPlugin  {

    /**
     * Called when events are modified.
     *
     * @param uuids The UUIDs of the events that have been updated.
     * @param update Protobuf data containing the new state of the events.
     * @param context Context passed to plugins.
     */
    public abstract void onStatusUpdate(List<String> uuids, EventSummaryUpdate update, EventUpdateContext context)
            throws ZepException;

    /**
     * Called when a note is added to an event.
     *
     * @param uuid The UUID of the event that a note has been added to.
     * @param note The note.
     * @param context Context passed to plugins.
     */
    public abstract void onNoteAdd(String uuid, EventNote note, EventUpdateContext context) throws ZepException;
}
