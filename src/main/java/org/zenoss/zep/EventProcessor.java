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

import org.zenoss.protobufs.zep.Zep.ZepRawEvent;

/**
 * Service for processing incoming events from the raw event queue, processing
 * them (identifying the event class, transforming them, persisting them), and
 * perform any post-processing on the event including alerting or publishing
 * events.
 */
public interface EventProcessor {
    /**
     * Processes the event.
     * 
     * @param event
     *            The raw event.
     * @throws ZepException
     *             If an error occurs processing the event.
     */
    public void processEvent(ZepRawEvent event) throws ZepException;
}
