/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao;

import org.zenoss.zep.ZepException;

/**
 * DAO used by the event indexer to determine which events to index.
 */
public interface EventIndexQueueDao {
    /**
     * Indexes events. The handler is invoked for each event summary to be indexed.
     * 
     * @param handler Handler which performs the actual indexing.
     * @param limit The maximum number of events to be indexed.
     * @return The number of processed events.
     * @throws ZepException If an exception occurs.
     */
    public int indexEvents(EventIndexHandler handler, int limit) throws ZepException;
    
    /**
     * Indexes events. The handler is invoked for each event summary to be indexed.
     * 
     * @param handler Handler which performs the actual indexing.
     * @param limit The maximum number of events to be indexed.
     * @param maxUpdateTime The maximum update time to process (inclusive).
     * @return The number of processed events.
     * @throws ZepException If an exception occurs.
     */
    public int indexEvents(EventIndexHandler handler, int limit, long maxUpdateTime) throws ZepException;

    public long getQueueLength();

}
