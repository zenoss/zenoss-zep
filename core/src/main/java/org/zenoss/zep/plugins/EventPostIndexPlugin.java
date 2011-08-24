/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.plugins;

import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;

/**
 * Defines a post-index plug-in which operates on an eventSummary after it has
 * been persisted and indexed.
 */
public abstract class EventPostIndexPlugin extends EventPlugin {
    /**
     * Processes the eventSummary.
     * 
     * @param eventSummary The eventSummary to process.
     * @param context Context passed to EventPostIndexPlugin.
     * @throws ZepException If an exception occurs processing the eventSummary.
     */
    public abstract void processEvent(EventSummary eventSummary, EventPostIndexContext context) throws ZepException;
}
