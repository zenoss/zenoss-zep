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

import java.util.Set;

import org.zenoss.protobufs.zep.Zep.EventStatus;

/**
 * Context information available to event plug-ins.
 */
public interface EventContext {

    /**
     * Returns the status of the event.
     * 
     * @return The status of the event.
     */
    public EventStatus getStatus();

    /**
     * Sets the status of the event. This allows events to be dropped or moved
     * directly to history by transforms.
     * 
     * @param status
     *            The new status for the event.
     */
    public void setStatus(EventStatus status);

    /**
     * Returns the set of clear classes for the event (set in clear events).
     * 
     * @return The set of clear classes for the event.
     */
    public Set<String> getClearClasses();

}
