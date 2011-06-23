/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
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
    public void publishEvent(Event event) throws ZepException;
}
