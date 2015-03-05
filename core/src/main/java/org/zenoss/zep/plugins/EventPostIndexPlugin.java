/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.plugins;

import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;

import java.util.*;

/**
 * Defines a post-index plug-in which operates on an eventSummary after it has
 * been persisted and indexed.
 */
public abstract class EventPostIndexPlugin extends EventPlugin {
    /**
     * Prepare to process the eventSummaries. This method gives the plugin a chance to do some batch-oriented
     * optimizations before processEvent is called for each individual event.
     *
     * @param eventSummaries The eventSummaries that will be passed to {@link #processEvent(EventSummary, EventPostIndexContext)}.
     * @param context Context passed to EventPostIndexPlugin.
     * @throws ZepException If an exception occurs.
     */
    public void preProcessEvents(Collection<EventSummary> eventSummaries, EventPostIndexContext context) throws ZepException {}

    /**
     * Processes the eventSummary.
     * 
     * @param eventSummary The eventSummary to process.
     * @param context Context passed to EventPostIndexPlugin.
     * @throws ZepException If an exception occurs processing the eventSummary.
     */
    public abstract void processEvent(EventSummary eventSummary, EventPostIndexContext context) throws ZepException;

    /**
     * Called when the post index batch operation is about to begin. This
     * method should be used to initialize any short term state for the
     * span of the current batch.
     *
     * @param context Context for the post-index plug-in.
     * @throws Exception If an exception occurs.
     */
    public void startBatch(EventPostIndexContext context) throws Exception {
        // Default implementation does nothing
    }

    /**
     * Called when the post index batch operation has completed. This should
     * perform any final operations for the plug-in and clean up any shared
     * state initialized in {@link #startBatch(EventPostIndexContext)}.
     *
     * @param context Context for the post-index plug-in.
     * @throws Exception If an exception occurs.
     */
    public void endBatch(EventPostIndexContext context) throws Exception {
        // Default implementation does nothing
    }
}
