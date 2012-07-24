/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
