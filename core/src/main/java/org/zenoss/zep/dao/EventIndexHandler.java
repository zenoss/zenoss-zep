/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao;

import org.zenoss.protobufs.zep.Zep.EventSummary;

import java.util.*;

/**
 * Event index handler interface.
 */
public interface EventIndexHandler {

    /**
     * Handler called (usually in batches) for each event before it is passed to {@link #handle(EventSummary)}.
     * @param events The events to be handled.
     * @throws Exception If an exception occurs.
     */
    void prepareToHandle(Collection<EventSummary> events) throws Exception;

    /**
     * Handler called for each event summary to be indexed.
     * 
     * @param event The event to be indexed.
     * @throws Exception If an exception occurs.
     */
    void handle(EventSummary event) throws Exception;

    /**
     * Callback method for when an event summary is not found.
     * 
     * @param uuid The UUID of the deleted event summary.
     * @throws Exception If an exception occurs.
     */
    void handleDeleted(String uuid) throws Exception;

    /**
     * Called when indexing is complete if rows were found in 
     * query.
     * 
     * @throws Exception If an exception occurs.
     */
    void handleComplete() throws Exception;
}
