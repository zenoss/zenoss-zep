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
package org.zenoss.zep.dao.impl;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PartitionConfig {
    private final Properties partitionConfig;

    public PartitionConfig(Properties partitioningProperties) {
        this.partitionConfig = partitioningProperties;
    }

    public PartitionTableConfig getConfig(String tableName) {
        int duration = Integer.valueOf(this.partitionConfig
                .getProperty(tableName + ".duration"));
        TimeUnit unit = TimeUnit.valueOf(this.partitionConfig
                .getProperty(tableName + ".unit"));
        int initialPastPartitions = Integer.valueOf(this.partitionConfig
                .getProperty(tableName + ".initial_past_partitions"));
        int futurePartitions = Integer.valueOf(this.partitionConfig
                .getProperty(tableName + ".future_partitions"));
        return new PartitionTableConfig(tableName, duration, unit,
                initialPastPartitions, futurePartitions);
    }
}
