/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.plugins;

import org.zenoss.protobufs.zep.Zep.EventSummaryUpdate;

import java.util.List;

/**
 * Plug-in which can be invoked after an event is updated.
 */
public abstract class EventUpdatePlugin extends EventPlugin  {

    /**
     * Handle event updates.
     * @param uuids The UUIDs of the events that have been updated
     * @param update an event summary update protobufs message
     */
    public abstract void onStatusUpdate(List<String> uuids, EventSummaryUpdate update);

}
