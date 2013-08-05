/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcOperations;
import org.zenoss.protobufs.zep.Zep.DaemonHeartbeat;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.HeartbeatDao;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.NestedTransactionService;
import org.zenoss.zep.dao.impl.compat.TypeConverter;
import org.zenoss.zep.dao.impl.SimpleJdbcTemplateProxy;

import java.lang.reflect.Proxy;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    private final SimpleJdbcOperations template;
    private DatabaseCompatibility databaseCompatibility;
    private NestedTransactionService nestedTransactionService;

    public HeartbeatDaoImpl(DataSource ds) {
    	this.template = (SimpleJdbcOperations) Proxy.newProxyInstance(SimpleJdbcOperations.class.getClassLoader(), 
    			new Class[] {SimpleJdbcOperations.class}, new SimpleJdbcTemplateProxy(ds));
    }

    public void setDatabaseCompatibility(DatabaseCompatibility databaseCompatibility) {
        this.databaseCompatibility = databaseCompatibility;
    }

    public void setNestedTransactionService(NestedTransactionService nestedTransactionService) {
        this.nestedTransactionService = nestedTransactionService;
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
        fields.put(COLUMN_LAST_TIME, timestampConverter.toDatabaseType(now));

        String insertSql = "INSERT INTO daemon_heartbeat (monitor, daemon, timeout_seconds, last_time)" +
                        " VALUES(:monitor, :daemon, :timeout_seconds, :last_time)";
        String updateSql = "UPDATE daemon_heartbeat SET timeout_seconds=:timeout_seconds, last_time=:last_time" +
                " WHERE monitor=:monitor AND daemon=:daemon";
        // In most cases, we insert and then retry as an update, however in these cases we will mostly be doing updates
        // so we try an update first before the insert/update.
        DaoUtils.updateOrInsert(nestedTransactionService, template, insertSql, updateSql, fields);
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

    @Override
    @TransactionalRollbackAllExceptions
    public int deleteByMonitorAndDaemon(String monitor, String daemon) throws ZepException {
        final Map<String, String> fields = new HashMap<String, String>(2);
        fields.put(COLUMN_MONITOR, monitor);
        fields.put(COLUMN_DAEMON, daemon);
        final String sql = "DELETE FROM daemon_heartbeat WHERE monitor=:monitor AND daemon=:daemon";
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
            hb.setLastTime(timestampConverter.fromDatabaseType(rs, COLUMN_LAST_TIME));
            return hb.build();
        }
    };
}
