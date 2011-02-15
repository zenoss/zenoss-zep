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

package org.zenoss.zep.index;

import org.zenoss.zep.ZepException;

/**
 * Interface to the background event indexing process.
 */
public interface EventIndexer {
    /**
     * Initializes the event indexer. This will rebuild the index if it has been
     * deleted.
     *
     * @throws ZepException
     *             If an exception occurs.
     */
    public void init() throws ZepException;

    /**
     * Shuts down the event indexer. This will commit the indexes and update the
     * last commit time in the database for the index.
     *
     * @throws ZepException If an exception occurs.
     */
    public void shutdown() throws ZepException;

    /**
     * Performs an index if either the summary or archive is out of date.
     * 
     * @throws ZepException
     *             If an exception occurs.
     */
    public void index() throws ZepException;

    /**
     * Performs an index and optionally forces the index update.
     * 
     * @param force
     *            If true, the index update is forced regardless of dirty flags
     *            on summary or archive.
     * @throws ZepException
     *             If an exception occurs.
     */
    public void index(boolean force) throws ZepException;

    /**
     * Marks the event summary as being modified so the next index operation
     * will index the event summary.
     */
    public void markSummaryDirty();

    /**
     * Marks the event archive as being modified so the next index operation
     * will index the event archive.
     */
    public void markArchiveDirty();
}
