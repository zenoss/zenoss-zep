/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
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
