/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao;

import java.util.concurrent.TimeUnit;

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
    public void initializePartitions() throws ZepException;

    /**
     * Returns the partition interval in milliseconds. This is used in order to
     * schedule new partitions to be created at the appropriate time.
     * 
     * @return The partition interval in milliseconds.
     */
    public long getPartitionIntervalInMs();
}
