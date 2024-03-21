/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao;

import org.zenoss.zep.ZepException;

/**
 * Interface implemented by tables which support partitioning.
 */
public interface Partitionable extends Purgable {
    /**
     * Initializes partitions in the specified table.
     * 
     * @throws ZepException
     *             If an exception occurs initializing partitions in the table.
     */
    void initializePartitions() throws ZepException;

    /**
     * Returns the partition interval in milliseconds. This is used in order to
     * schedule new partitions to be created at the appropriate time.
     * 
     * @return The partition interval in milliseconds.
     */
    long getPartitionIntervalInMs();
}
