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

import org.zenoss.protobufs.zep.Zep.EventSummary;

/**
 * Defines a post-processing plug-in which operates on an event after it has
 * been pre-processed and persisted.
 */
public interface EventPostProcessingPlugin extends EventPlugin {
    /**
     * Processes the event.
     * 
     * @param event
     *            The event to process.
     * @throws ZepException
     *             If an exception occurs processing the event.
     */
    public void processEvent(EventSummary event) throws ZepException;
}
