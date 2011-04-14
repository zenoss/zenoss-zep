/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao;

import java.util.concurrent.TimeUnit;

import org.zenoss.zep.ZepException;

/**
 * Interface implemented by tables which support partitioning.
 */
public interface Partitionable {
    /**
     * Initializes partitions in the specified table.
     * 
     * @throws ZepException
     *             If an exception occurs initializing partitions in the table.
     */
    public void initializePartitions() throws ZepException;

    /**
     * Purges partitions from the table which are older than the specified time.
     * 
     * @param duration
     *            Duration of time.
     * @param unit
     *            Time unit.
     * @return The number of dropped partitions.
     * @throws ZepException
     *             If an exception occurs purging the table.
     */
    public int dropPartitionsOlderThan(int duration, TimeUnit unit)
            throws ZepException;

    /**
     * Returns the partition interval in milliseconds. This is used in order to
     * schedule new partitions to be created at the appropriate time.
     * 
     * @return The partition interval in milliseconds.
     */
    public long getPartitionIntervalInMs();
}
