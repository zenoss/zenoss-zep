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
 * Interface used to publish events to the fan-out exchange.
 */
public interface EventPublisher {
    /**
     * Adds the specified event to be published.
     * 
     * @param event
     *            The event to be published.
     * @throws ZepException If an exception occurs.
     */
    public void addEvent(EventSummary event) throws ZepException;

    /**
     * Publishes any events to the fan-out exchange.
     * 
     * @throws ZepException If an exception occurs.
     */
    public void publish() throws ZepException;
}
