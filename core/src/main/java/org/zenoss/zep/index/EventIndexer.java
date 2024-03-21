/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.index;

import org.zenoss.zep.ZepException;
import org.zenoss.protobufs.zep.Zep.ZepConfig;

/**
 * Interface to the background event indexing process.
 */
public interface EventIndexer {
    /**
     * Performs an index of events in the event summary and archive.
     *
     * @return The number of indexed events.
     * @throws ZepException If an exception occurs.
     */
    int index() throws ZepException;

    /**
     * Performs a full index through the current timestamp.
     *
     * @return The number of indexed events.
     * @throws ZepException If an exception occurs.
     */
    int indexFully() throws ZepException;

    /**
     * Starts the event indexer (starts the background indexing thread and starts processing the index
     * queue).
     *
     * @throws InterruptedException If the indexer is running and is interrupted while stopping.
     */
    void start(ZepConfig config) throws InterruptedException;

    /**
     * Stops the event indexer (stops the background indexing thread).
     *
     * @throws InterruptedException If the indexer is running and is interrupted while stopping.
     */
    void stop() throws InterruptedException;

    /**
     * Shuts down the event indexer (any further calls to start() will fail).
     *
     * @throws InterruptedException If the indexer is interrupted while stopping.
     */
    void shutdown() throws InterruptedException;
}
