/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao;

import org.zenoss.zep.ZepException;

import java.util.List;

/**
 * DAO used by the event indexer to determine which events to index.
 */
public interface EventIndexQueueDao {
    /**
     * Indexes events. The handler is invoked for each event summary to be indexed.
     * 
     * @param handler Handler which performs the actual indexing.
     * @param limit The maximum number of events to be indexed.
     * @return The internal queue identifiers processed.
     * @throws ZepException If an exception occurs.
     */
    public List<Long> indexEvents(EventIndexHandler handler, int limit) throws ZepException;
    
    /**
     * Indexes events. The handler is invoked for each event summary to be indexed.
     * 
     * @param handler Handler which performs the actual indexing.
     * @param limit The maximum number of events to be indexed.
     * @param maxUpdateTime The maximum update time to process (inclusive).
     * @return The internal queue identifiers processed.
     * @throws ZepException If an exception occurs.
     */
    public List<Long> indexEvents(EventIndexHandler handler, int limit, long maxUpdateTime) throws ZepException;

    public long getQueueLength();

    /**
     * Removes items from the queue with the specified identifiers.
     *
     * @param queueIds The list of queue identifiers to delete.
     * @return The number of affected rows by the query.
     * @throws ZepException If an exception occurs.
     */
    public int deleteIndexQueueIds(List<Long> queueIds) throws ZepException;

    /**
     * time stamp of the last indexed entry
     * @return time stamp, 0 if nothing has been indexed
     */
    public long getLastIndexTime();

}
