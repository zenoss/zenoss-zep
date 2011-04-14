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
     * Initializes the event indexer. This will rebuild the index if it has been
     * deleted.
     *
     * @throws ZepException
     *             If an exception occurs.
     */
    public void init() throws ZepException;

    /**
     * Performs an index if either the summary or archive is out of date.
     *
     * @param throughTime The timestamp through which events will be indexed.
     * @return The number of indexed events.
     * @throws ZepException If an exception occurs.
     */
    public int index(long throughTime) throws ZepException;
}
