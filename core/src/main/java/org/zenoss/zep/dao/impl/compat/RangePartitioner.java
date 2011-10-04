/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl.compat;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Range partitioner interface.
 */
public interface RangePartitioner {
    /**
     * Returns true if the table supports partitioning, false otherwise.
     *
     * @return True if the table supports partitioning, false otherwise.
     */
    public boolean hasPartitioning();

    /**
     * Returns a list of configured partitions.
     *
     * @return List of configured partitions.
     */
    public List<Partition> listPartitions();

    /**
     * Creates the specified number of past and future partitions.
     *
     * @param pastPartitions Number of past partitions to create.
     * @param futurePartitions Number of future partitions to create.
     * @return The number of created partitions.
     */
    public int createPartitions(int pastPartitions, int futurePartitions);

    /**
     * Removes all partitions for the table.
     */
    public void removeAllPartitions();

    /**
     * Drops partitions older than the specified time.
     *
     * @param duration The amount of time.
     * @param unit The unit of time.
     * @return The number of dropped partitions.
     */
    public int dropPartitionsOlderThan(int duration, TimeUnit unit);
}
