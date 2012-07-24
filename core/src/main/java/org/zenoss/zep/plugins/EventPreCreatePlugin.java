/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.plugins;

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.zep.ZepException;

/**
 * Pre-processing plug-ins operate on an event before it is persisted, and can
 * change properties of the event.
 */
public abstract class EventPreCreatePlugin extends EventPlugin {
    /**
     * Processes the event. If no processing takes place, this method can return
     * null.
     * 
     * @param event
     *            The event to process.
     * @param ctx
     *            Context information available to the plug-in.
     * @return The processed event, or null if the event is not modified.
     * @throws org.zenoss.zep.ZepException
     *             If an exception occurred processing the event.
     */
    public abstract Event processEvent(Event event, EventPreCreateContext ctx) throws ZepException;
}
