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
package org.zenoss.zep;

import org.zenoss.protobufs.zep.Zep.Event;

/**
 * Pre-processing plug-ins operate on an event before it is persisted, and can
 * mutate properties of the event.
 */
public interface EventPreProcessingPlugin extends EventPlugin {
    /**
     * Processes the event. If no processing takes place, this method can return
     * null.
     * 
     * @param event
     *            The event to process.
     * @param ctx
     *            Context information available to the plug-in.
     * @return The processed event, or null if the event is not modified.
     * @throws ZepException
     *             If an exception occurred processing the event.
     */
    public Event processEvent(Event event, EventContext ctx)
            throws ZepException;
}
