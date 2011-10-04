/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl.compat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcOperations;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Class used to simplify creation of range partitions on integer columns. This
 * class does not currently support ranges on date columns (with custom
 * expressions), sub-partitioning, or reorganizing partitions (merging /
 * splitting).
 */
public class RangePartitionerMySQL implements RangePartitioner {

    private static final Logger logger = LoggerFactory
            .getLogger(RangePartitionerMySQL.class);

    private final SimpleJdbcOperations template;
    private final String databaseName;
    private final String tableName;
    private final String columnName;
    private final int durationInMillis;

    /**
     * Creates a range partitioner helper class which creates partitions of the
     * specified range on the table.
     * 
     * @param ds
     *            DataSource.
     * @param databaseName
     *            Database name.
     * @param tableName
     *            Table name.
     * @param columnName
     *            Column name where range partitioning should be performed.
     * @param duration
     *            Duration of each range partition.
     * @param unit
     *            Unit of duration.
     */
    public RangePartitionerMySQL(DataSource ds, String databaseName,
                                 String tableName, String columnName, long duration, TimeUnit unit) {
        if (ds == null || databaseName == null || tableName == null || unit == null) {
            throw new NullPointerException();
        }
        if (duration <= 0) {
            throw new IllegalArgumentException("Duration <= 0");
        }
        this.template = new SimpleJdbcTemplate(ds);
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columnName = columnName;
        long millis = unit.toMillis(duration);
        if (millis > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Overflow");
        }
        this.durationInMillis = (int) millis;
    }

    /**
     * Returns true if the database supports partitioning, false otherwise.
     * 
     * @return True if the database supports partitioning, false otherwise.
     */
    @TransactionalReadOnly
    public boolean hasPartitioning() {
        boolean hasPartitioning = false;
        List<Map<String, String>> list = this.template.query(
                "SHOW VARIABLES LIKE '%partition%'",
                new RowMapper<Map<String, String>>() {
                    @Override
                    public Map<String, String> mapRow(ResultSet rs, int rowNum)
                            throws SQLException {
                        return Collections.singletonMap(
                                rs.getString("VARIABLE_NAME"),
                                rs.getString("VALUE"));
                    }
                });
        for (Map<String, String> map : list) {
            Map.Entry<String, String> first = map.entrySet().iterator().next();
            String key = first.getKey();
            String val = first.getValue().toLowerCase();
            if ("have_partition_engine".equals(key)
                    || "have_partitioning".equals(key)) {
                hasPartitioning = "yes".equals(val);
                break;
            }
        }
        return hasPartitioning;
    }

    /**
     * Returns a list of all partitions found on the table. If there are no
     * partitions defined, this returns an empty list. All partitions are
     * returned in sorted order with the first partition having the lowest range
     * value.
     * 
     * @return A list of all partitions found on the table.
     */
    @TransactionalReadOnly
    public List<Partition> listPartitions() {
        List<Partition> partitions = new ArrayList<Partition>();
        List<Map<String, Object>> fields = this.template
                .queryForList(
                        "SELECT * FROM information_schema.partitions WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
                        this.databaseName, this.tableName);
        for (Map<String, Object> map : fields) {
            PartitionMySQL partition = PartitionMySQL.fromResultSetFields(map);
            if (partition.getPartitionName() == null
                    && partition.getSubpartitionName() == null) {
                continue;
            }
            if (!"RANGE".equals(partition.getPartitionMethod())) {
                logger.warn("Table {} is not partitioned by RANGE",
                        this.tableName);
            } else {
                partitions.add(partition);
            }
        }
        Collections.sort(partitions, new Comparator<Partition>() {
            @Override
            public int compare(Partition p1, Partition p2) {
                Long p1Range = p1.getRangeLessThan();
                Long p2Range = p2.getRangeLessThan();
                if (p1Range != null && p2Range != null) {
                    return p1Range.compareTo(p2Range);
                }
                return 0;
            }
        });
        return partitions;
    }

    /**
     * Calculate which partition timestamps should be created assuming the
     * specified number of past partitions and future partitions. This method
     * ensures there are no overlapping partitions by not returning any
     * timestamps which are less than the current maximum partition.
     * 
     * @param pastPartitions
     *            The number of partitions in the past to create.
     * @param futurePartitions
     *            The number of future partitions to create.
     * @param currentMaxPartition
     *            The current maximum partition in the table (used to ensure no
     *            partitions spanning existing ranges are created).
     * @return A list of all partition timestamps which should be created in the
     *         table.
     */
    private List<Long> calculatePartitionTimestamps(int pastPartitions,
            int futurePartitions, long currentMaxPartition) {
        if (pastPartitions < 0 || futurePartitions < 0) {
            throw new IllegalArgumentException(
                    "Past or future partitions cannot be negative.");
        }
        final int totalPartitions = pastPartitions + futurePartitions;
        if (totalPartitions == 0) {
            throw new IllegalArgumentException(
                    "Partitions to create must be > 0");
        }
        final List<Long> partitions = new ArrayList<Long>(totalPartitions);
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        /* Create on boundary of interval */
        long initialTime = cal.getTimeInMillis() + this.durationInMillis;
        initialTime -= initialTime % this.durationInMillis;
        cal.setTimeInMillis(initialTime);
        for (int i = 0; i < pastPartitions; i++) {
            cal.add(Calendar.MILLISECOND, -this.durationInMillis);
        }
        for (int i = 0; i < totalPartitions; i++) {
            final long millis = cal.getTimeInMillis();
            if (millis > currentMaxPartition) {
                partitions.add(millis);
            }
            cal.add(Calendar.MILLISECOND, this.durationInMillis);
        }
        return partitions;
    }

    /**
     * Creates the specified number of past and future partitions for the table.
     * No new partitions are created within existing ranges (no splitting of
     * existing partitions).
     * 
     * @param pastPartitions
     *            The number of past partitions to create in the table.
     * @param futurePartitions
     *            The number of future partitions to create in the table.
     * @return The number of created partitions.
     */
    @TransactionalRollbackAllExceptions
    public int createPartitions(int pastPartitions, int futurePartitions) {
        final StringBuilder sb = new StringBuilder();
        final List<Partition> partitions = listPartitions();
        long lastPartitionTimestamp = Long.MIN_VALUE;
        if (!partitions.isEmpty()) {
            lastPartitionTimestamp = partitions.get(partitions.size() - 1).getRangeLessThan();
        }
        final SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd_HHmmss");
        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        final List<Long> partitionTimestamps = calculatePartitionTimestamps(
                pastPartitions, futurePartitions, lastPartitionTimestamp);
        if (partitions.isEmpty()) {
            sb.append(String.format("ALTER TABLE %s PARTITION BY RANGE(%s) (",
                    this.tableName, this.columnName));
            for (Iterator<Long> it = partitionTimestamps.iterator(); it
                    .hasNext();) {
                final long partitionTimestamp = it.next();
                final String partitionName = "p"
                        + fmt.format(new Date(partitionTimestamp));
                logger.info("Adding partition {} to table {}", partitionName,
                        this.tableName);
                sb.append(String.format("PARTITION %s VALUES LESS THAN(%s)",
                        partitionName, partitionTimestamp));
                if (it.hasNext()) {
                    sb.append(',');
                }
            }
            sb.append(')');
            this.template.update(sb.toString());
        } else {
            for (Long partitionTimestamp : partitionTimestamps) {
                final String partitionName = "p"
                        + fmt.format(new Date(partitionTimestamp));
                logger.info("Adding partition {} to table {}", partitionName,
                        this.tableName);
                this.template
                        .update(String
                                .format("ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES LESS THAN(%s))",
                                        this.tableName, partitionName,
                                        partitionTimestamp));
            }
        }
        return partitionTimestamps.size();
    }

    /**
     * Removes all partitions on the specified table.
     */
    @TransactionalRollbackAllExceptions
    public void removeAllPartitions() {
        this.template.update(String.format(
                "ALTER TABLE %s REMOVE PARTITIONING", this.tableName));
    }

    /**
     * Prunes all partitions which are older than the specified amount of time.
     * 
     * @param duration
     *            The duration of time.
     * @param unit
     *            The unit of time.
     * @return The number of pruned partitions, or zero if no partitions were
     *         pruned.
     */
    @TransactionalRollbackAllExceptions
    public int dropPartitionsOlderThan(int duration, TimeUnit unit) {
        if (duration < 0) {
            throw new IllegalArgumentException("Duration must be >= 0");
        }
        final long millis = unit.toMillis(duration);
        final long pruneTimestamp = System.currentTimeMillis() - millis;
        int numPruned = 0;
        List<Partition> partitions = listPartitions();
        StringBuilder partitionsToRemove = new StringBuilder();
        for (Partition partition : partitions) {
            long rangeLessThan = partition.getRangeLessThan();
            if (rangeLessThan > pruneTimestamp) {
                break;
            }
            if (partitionsToRemove.length() > 0) {
                partitionsToRemove.append(',');
            }
            partitionsToRemove.append(partition.getPartitionName());
            ++numPruned;
            logger.info("Pruning table {} partition {}: prune timestamp {}",
                    new Object[] { this.tableName,
                            partition.getPartitionName(), pruneTimestamp });
        }
        if (numPruned > 0) {
            this.template.update(String.format(
                    "ALTER TABLE %s DROP PARTITION %s", this.tableName,
                    partitionsToRemove.toString()));
        }
        return numPruned;
    }
}
