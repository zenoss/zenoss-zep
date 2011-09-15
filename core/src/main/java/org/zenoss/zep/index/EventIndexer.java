/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */

package org.zenoss.zep.index;

import org.zenoss.zep.ZepException;

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
    public int index() throws ZepException;

    /**
     * Performs a full index through the current timestamp.
     *
     * @return The number of indexed events.
     * @throws ZepException If an exception occurs.
     */
    public int indexFully() throws ZepException;

    /**
     * Starts the event indexer (starts the background indexing thread and starts processing the index
     * queue).
     *
     * @throws InterruptedException If the indexer is running and is interrupted while stopping.
     */
    public void start() throws InterruptedException;

    /**
     * Stops the event indexer (stops the background indexing thread).
     *
     * @throws InterruptedException If the indexer is running and is interrupted while stopping.
     */
    public void stop() throws InterruptedException;

    /**
     * Shuts down the event indexer (any further calls to start() will fail).
     *
     * @throws InterruptedException If the indexer is interrupted while stopping.
     */
    public void shutdown() throws InterruptedException;
}
