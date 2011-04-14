/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
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
