/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep;

import org.zenoss.protobufs.zep.Zep.Event;

/**
 * Interface for publishing events to the raw event queue.
 */
public interface EventPublisher {
    /**
     * Publishes the event to the raw queue.
     *
     * @param event Event to publish.
     * @throws ZepException If an exception occurs.
     */
    void publishEvent(Event event) throws ZepException;
}
