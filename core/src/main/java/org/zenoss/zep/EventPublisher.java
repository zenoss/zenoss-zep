/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2011, Zenoss Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * For complete information please visit: http://www.zenoss.com/oss/
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
