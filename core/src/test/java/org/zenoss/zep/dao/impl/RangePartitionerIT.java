/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.utils.dao.Partition;
import org.zenoss.utils.dao.RangePartitioner;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.DatabaseType;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class RangePartitionerIT extends AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    public DatabaseCompatibility databaseCompatibility;

    @Autowired
    public DataSource dataSource;

    @Before
    public void createSampleTable() {
        if (databaseCompatibility.getDatabaseType() == DatabaseType.MYSQL) {
            this.simpleJdbcTemplate
                    .update("CREATE TABLE `range_partition` (`col_ts` BIGINT NOT NULL, `message` VARCHAR(256) NOT NULL)");
        } else if (databaseCompatibility.getDatabaseType() == DatabaseType.POSTGRESQL) {
            this.simpleJdbcTemplate
                    .update("CREATE TABLE range_partition (col_ts timestamp without time zone, message VARCHAR(256) NOT NULL)");
        }
    }

    @After
    public void dropSampleTable() {
        this.simpleJdbcTemplate.update("DROP TABLE range_partition");
    }

    @Test
    public void testRangePartitioner() {
        String dbname = "zenoss_zep_test";
        RangePartitioner partitioner = databaseCompatibility.getRangePartitioner(
                this.dataSource, dbname, "range_partition", "col_ts",
                1, TimeUnit.DAYS);
        assertEquals(0, partitioner.listPartitions().size());
        /*
         * Initialize partitions with 5 previous days partitions and 10 future
         * partitions.
         */
        int numBefore = 0, numAfter = 0;
        partitioner.createPartitions(5, 10);
        Timestamp time = new Timestamp(System.currentTimeMillis());
        List<Partition> partitions = partitioner.listPartitions();
        assertEquals(15, partitions.size());
        for (Partition partition : partitions) {
            if (partition.getRangeLessThan().before(time)) {
                numBefore++;
            } else {
                numAfter++;
            }
        }
        assertEquals(5, numBefore);
        assertEquals(10, numAfter);

        // Try to create partitions which overlap existing ranges and verify it doesn't create any.
        assertEquals(0, partitioner.createPartitions(10, 10));
        assertEquals(0, partitioner.createPartitions(0, 5));

        // Create two additional ones in the future and verify it only creates those two
        assertEquals(2, partitioner.createPartitions(5, 12));
        assertEquals(17, partitioner.listPartitions().size());

        // Test pruning partitions older than 3 days ago
        numBefore = 0;
        numAfter = 0;
        partitioner.pruneAndCreatePartitions(3, TimeUnit.DAYS, 0, 0);
        partitions = partitioner.listPartitions();
        assertEquals(15, partitions.size());
        for (Partition partition : partitions) {
            if (partition.getRangeLessThan().before(time)) {
                numBefore++;
            } else {
                numAfter++;
            }
        }
        assertEquals(3, numBefore);
        assertEquals(12, numAfter);

        // Test pruning partitions again, expecting same results
        numBefore = 0;
        numAfter = 0;
        partitioner.pruneAndCreatePartitions(3, TimeUnit.DAYS, 0, 0);
        partitions = partitioner.listPartitions();
        assertEquals(15, partitions.size());
        for (Partition partition : partitions) {
            if (partition.getRangeLessThan().before(time)) {
                numBefore++;
            } else {
                numAfter++;
            }
        }
        assertEquals(3, numBefore);
        assertEquals(12, numAfter);

        // Test dropping partitions
        partitioner.removeAllPartitions();
        assertEquals(0, partitioner.listPartitions().size());
    }
}
