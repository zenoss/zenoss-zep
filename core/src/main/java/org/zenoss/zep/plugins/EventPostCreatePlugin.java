/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.plugins;

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;

/**
 * Plug-in which can be invoked after an event is persisted.
 */
public abstract class EventPostCreatePlugin extends EventPlugin {
    /**
     * Processes the event.
     *
     * @param eventOccurrence The event occurrence.
     * @param event The event summary (Can be null if the event was dropped).
     * @param context Context passed to EventPostCreatePlugin.
     * @throws ZepException If an exception occurs processing the event.
     */
    public abstract void processEvent(Event eventOccurrence, EventSummary event, EventPostCreateContext context)
            throws ZepException;
}
