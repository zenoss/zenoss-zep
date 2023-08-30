/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
    void processEvent(ZepRawEvent event) throws ZepException;
}
