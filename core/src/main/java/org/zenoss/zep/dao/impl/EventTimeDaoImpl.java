/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.zenoss.protobufs.zep.Zep;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventTimeDao;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.RangePartitioner;
import org.zenoss.zep.dao.impl.compat.TypeConverter;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.zenoss.zep.dao.impl.EventConstants.*;

/**
 * NOTE: This table is backed by MyISAM storage instead of InnoDB storage, so normal transaction
 * handling won't work here. Ignore the @Transactional* annotations on the methods as they are
 * ignored.
 */
public class EventTimeDaoImpl implements EventTimeDao {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(EventTimeDaoImpl.class);

    private final SimpleJdbcTemplate template;
    private final PartitionTableConfig partitionTableConfig;
    private final DatabaseCompatibility databaseCompatibility;
    private final RangePartitioner partitioner;
    private final TypeConverter<String> uuidConverter;

    public EventTimeDaoImpl(DataSource dataSource, String databaseName,
                            PartitionConfig partitionConfig, DatabaseCompatibility databaseCompatibility) {
        this.template = new SimpleJdbcTemplate(dataSource);
        this.partitionTableConfig = partitionConfig.getConfig(TABLE_EVENT_TIME);
        this.databaseCompatibility = databaseCompatibility;
        this.partitioner = databaseCompatibility.getRangePartitioner(dataSource, databaseName,
                TABLE_EVENT_TIME, COLUMN_PROCESSED,
                partitionTableConfig.getPartitionDuration(),
                partitionTableConfig.getPartitionUnit());
        this.uuidConverter = databaseCompatibility.getUUIDConverter();
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void purge(int duration, TimeUnit unit) throws ZepException {
        dropPartitionsOlderThan(duration, unit);
        initializePartitions();
    }


    @Override
    @TransactionalRollbackAllExceptions
    public void initializePartitions() throws ZepException {
        this.partitioner.createPartitions(
                this.partitionTableConfig.getInitialPastPartitions(),
                this.partitionTableConfig.getFuturePartitions());
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int dropPartitionsOlderThan(int duration, TimeUnit unit)
            throws ZepException {
        return this.partitioner.dropPartitionsOlderThan(duration, unit);
    }

    @Override
    public long getPartitionIntervalInMs() {
        return this.partitionTableConfig.getPartitionUnit().toMillis(
                this.partitionTableConfig.getPartitionDuration());
    }

    @Override
    @TransactionalReadOnly
    public List<Zep.EventTime> findProcessedSince(Date startDate, int limit) {
        long timestamp = startDate.getTime();
        final Map<String, Object> params = Collections.singletonMap("since",
                databaseCompatibility.getTimestampConverter().toDatabaseType(timestamp));

        String sql = "SELECT * from %s where %s >= :since order by %s asc limit %s";
        sql = String.format(sql, TABLE_EVENT_TIME, COLUMN_PROCESSED, COLUMN_PROCESSED, limit);

        return template.query(sql, new EventTimeRowMapper(), params);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void save(Zep.EventTime eventTime) {
        Map<String, Object> fields = new LinkedHashMap<String, Object>();
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        fields.put(COLUMN_PROCESSED, timestampConverter.toDatabaseType(eventTime.getProcessedTime()));
        fields.put(COLUMN_CREATED, timestampConverter.toDatabaseType(eventTime.getCreatedTime()));
        fields.put(COLUMN_FIRST_SEEN, timestampConverter.toDatabaseType(eventTime.getFirstSeenTime()));
        fields.put(COLUMN_SUMMARY_UUID, uuidConverter.toDatabaseType(eventTime.getSummaryUuid()));

        String insert = DaoUtils.createNamedInsert(TABLE_EVENT_TIME, fields.keySet());
        template.update(insert, fields);
    }

    private class EventTimeRowMapper implements RowMapper<Zep.EventTime> {
        @Override
        public Zep.EventTime mapRow(ResultSet rs, int rowNum) throws SQLException {
            Zep.EventTime.Builder builder = Zep.EventTime.newBuilder();
            TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
            builder.setCreatedTime(timestampConverter.fromDatabaseType(rs.getObject(COLUMN_CREATED)));
            builder.setProcessedTime(timestampConverter.fromDatabaseType(rs.getObject(COLUMN_PROCESSED)));
            builder.setFirstSeenTime(timestampConverter.fromDatabaseType(rs.getObject(COLUMN_FIRST_SEEN)));
            String summaryUuid = uuidConverter.fromDatabaseType(rs.getObject(COLUMN_SUMMARY_UUID));
            builder.setSummaryUuid(summaryUuid);
            return builder.build();
        }
    }

}
