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
     * Performs an index if either the summary or archive is out of date.
     *
     * @param throughTime The timestamp through which events will be indexed.
     * @return The number of indexed events.
     * @throws ZepException If an exception occurs.
     */
    public int index(long throughTime) throws ZepException;
}
