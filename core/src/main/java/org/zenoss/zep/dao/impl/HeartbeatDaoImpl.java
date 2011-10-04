/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.zenoss.protobufs.zep.Zep.DaemonHeartbeat;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.HeartbeatDao;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.DatabaseType;
import org.zenoss.zep.dao.impl.compat.TypeConverter;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link org.zenoss.zep.dao.HeartbeatDao}.
 */
public class HeartbeatDaoImpl implements HeartbeatDao {
    private static final String COLUMN_MONITOR = "monitor";
    private static final String COLUMN_DAEMON = "daemon";
    private static final String COLUMN_TIMEOUT_SECONDS = "timeout_seconds";
    private static final String COLUMN_LAST_TIME = "last_time";

    private final DataSource ds;
    private final SimpleJdbcTemplate template;
    private DatabaseCompatibility databaseCompatibility;
    private SimpleJdbcCall postgresqlUpsertCall = null;

    public HeartbeatDaoImpl(DataSource ds) {
        this.ds = ds;
        this.template = new SimpleJdbcTemplate(ds);
    }

    public void setDatabaseCompatibility(DatabaseCompatibility databaseCompatibility) {
        this.databaseCompatibility = databaseCompatibility;
        if (databaseCompatibility.getDatabaseType() == DatabaseType.POSTGRESQL) {
            this.postgresqlUpsertCall = new SimpleJdbcCall(ds).withFunctionName("daemon_heartbeat_upsert")
                    .declareParameters(new SqlParameter("p_monitor", Types.VARCHAR),
                            new SqlParameter("p_daemon", Types.VARCHAR),
                            new SqlParameter("p_timeout_seconds", Types.INTEGER),
                            new SqlParameter("p_last_time", Types.TIMESTAMP));
        }
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void createHeartbeat(DaemonHeartbeat heartbeat) throws ZepException {
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        final long now = System.currentTimeMillis();
        final Map<String,Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_MONITOR, heartbeat.getMonitor());
        fields.put(COLUMN_DAEMON, heartbeat.getDaemon());
        fields.put(COLUMN_TIMEOUT_SECONDS, heartbeat.getTimeoutSeconds());
        fields.put("_now", timestampConverter.toDatabaseType(now));

        if (databaseCompatibility.getDatabaseType() == DatabaseType.MYSQL) {
            final String sql = "INSERT INTO daemon_heartbeat (monitor, daemon, timeout_seconds, last_time)" +
                " VALUES(:monitor, :daemon, :timeout_seconds, :_now)" +
                " ON DUPLICATE KEY UPDATE timeout_seconds=VALUES(timeout_seconds), last_time=VALUES(last_time)";
            this.template.update(sql, fields);
        }
        else if (databaseCompatibility.getDatabaseType() == DatabaseType.POSTGRESQL) {
            this.postgresqlUpsertCall.execute(fields.get(COLUMN_MONITOR), fields.get(COLUMN_DAEMON),
                    fields.get(COLUMN_TIMEOUT_SECONDS), fields.get("_now"));
        }
        else {
            throw new IllegalStateException("Unsupported database type: " + databaseCompatibility.getDatabaseType());
        }
    }

    @Override
    @TransactionalReadOnly
    public List<DaemonHeartbeat> findAll() throws ZepException {
        final String sql = "SELECT * FROM daemon_heartbeat";
        return this.template.query(sql, MAPPER);
    }

    @Override
    @TransactionalReadOnly
    public List<DaemonHeartbeat> findByMonitor(String monitor) throws ZepException {
        final Map<String,String> fields = Collections.singletonMap(COLUMN_MONITOR, monitor);
        final String sql = "SELECT * FROM daemon_heartbeat WHERE monitor=:monitor";
        return this.template.query(sql, MAPPER, fields);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int deleteAll() throws ZepException {
        final String sql = "DELETE FROM daemon_heartbeat";
        return this.template.update(sql);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int deleteByMonitor(String monitor) throws ZepException {
        final Map<String,String> fields = Collections.singletonMap(COLUMN_MONITOR, monitor);
        final String sql = "DELETE FROM daemon_heartbeat WHERE monitor=:monitor";
        return this.template.update(sql, fields);
    }

    private final RowMapper<DaemonHeartbeat> MAPPER = new RowMapper<DaemonHeartbeat>()
    {
        @Override
        public DaemonHeartbeat mapRow(ResultSet rs, int rowNum) throws SQLException {
            TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
            DaemonHeartbeat.Builder hb = DaemonHeartbeat.newBuilder();
            hb.setMonitor(rs.getString(COLUMN_MONITOR));
            hb.setDaemon(rs.getString(COLUMN_DAEMON));
            hb.setTimeoutSeconds(rs.getInt(COLUMN_TIMEOUT_SECONDS));
            hb.setLastTime(timestampConverter.fromDatabaseType(rs.getObject((COLUMN_LAST_TIME))));
            return hb.build();
        }
    };
}
