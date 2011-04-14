/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep;

import org.zenoss.protobufs.zep.Zep.RawEvent;

/**
 * Interface for publishing events to the raw event queue.
 */
public interface EventPublisher {
    /**
     * Publishes the event to the raw queue.
     *
     * @param rawEvent Raw event to publish.
     * @throws ZepException If an exception occurs.
     */
    public void publishRawEvent(RawEvent rawEvent) throws ZepException;
}
