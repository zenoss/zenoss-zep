/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class RangePartitionerIT extends
        AbstractTransactionalJUnit4SpringContextTests {

    @Before
    public void createSampleTable() {
        this.simpleJdbcTemplate
                .update("CREATE TABLE `range_partition` (`col_ts` BIGINT NOT NULL, `message` VARCHAR(256) NOT NULL)");
    }

    @After
    public void dropSampleTable() {
        this.simpleJdbcTemplate.update("DROP TABLE range_partition");
    }

    @Test
    public void testRangePartitioner() {
        String dbname = this.simpleJdbcTemplate.queryForObject(
                "SELECT DATABASE()", String.class);
        RangePartitioner partitioner = new RangePartitioner(
                this.simpleJdbcTemplate, dbname, "range_partition", "col_ts",
                1, TimeUnit.DAYS);
        assertTrue(partitioner.hasPartitioning());
        assertEquals(0, partitioner.listPartitions().size());
        /*
         * Initialize partitions with 5 previous days partitions and 10 future
         * partitions.
         */
        int numBefore = 0, numAfter = 0;
        partitioner.createPartitions(5, 10);
        long time = System.currentTimeMillis();
        List<Partition> partitions = partitioner.listPartitions();
        assertEquals(15, partitions.size());
        for (Partition partition : partitions) {
            if (partition.getRangeLessThan() < time) {
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
        partitioner.dropPartitionsOlderThan(3, TimeUnit.DAYS);
        partitions = partitioner.listPartitions();
        assertEquals(15, partitions.size());
        for (Partition partition : partitions) {
            if (partition.getRangeLessThan() <= time) {
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
