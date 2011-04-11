/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2011, Zenoss Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.dao.impl;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.zep.Zep.DaemonHeartbeat;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.HeartbeatDao;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Implementation of {@link org.zenoss.zep.dao.HeartbeatDao}.
 */
public class HeartbeatDaoImpl implements HeartbeatDao {
    private static final String COLUMN_MONITOR = "monitor";
    private static final String COLUMN_DAEMON = "daemon";
    private static final String COLUMN_TIMEOUT_SECONDS = "timeout_seconds";
    private static final String COLUMN_LAST_TIME = "last_time";

    private static final Calendar GMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

    private final SimpleJdbcTemplate template;

    public HeartbeatDaoImpl(DataSource ds) {
        this.template = new SimpleJdbcTemplate(ds);
    }

    @Override
    @Transactional
    public void createHeartbeat(DaemonHeartbeat heartbeat) throws ZepException {
        final long now = System.currentTimeMillis();
        final String sql = "INSERT INTO daemon_heartbeat (monitor, daemon, timeout_seconds, last_time)" +
                " VALUES(:monitor, :daemon, :timeout_seconds, :_now)" +
                " ON DUPLICATE KEY UPDATE timeout_seconds=VALUES(timeout_seconds), last_time=VALUES(last_time)";
        final Map<String,Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_MONITOR, heartbeat.getMonitor());
        fields.put(COLUMN_DAEMON, heartbeat.getDaemon());
        fields.put(COLUMN_TIMEOUT_SECONDS, heartbeat.getTimeoutSeconds());
        fields.put("_now", now);

        this.template.update(sql, fields);
    }

    @Override
    @Transactional(readOnly=true)
    public List<DaemonHeartbeat> findAll() throws ZepException {
        final String sql = "SELECT * FROM daemon_heartbeat";
        return this.template.query(sql, MAPPER);
    }

    private static final RowMapper<DaemonHeartbeat> MAPPER = new RowMapper<DaemonHeartbeat>()
    {
        @Override
        public DaemonHeartbeat mapRow(ResultSet rs, int rowNum) throws SQLException {
            DaemonHeartbeat.Builder hb = DaemonHeartbeat.newBuilder();
            hb.setMonitor(rs.getString(COLUMN_MONITOR));
            hb.setDaemon(rs.getString(COLUMN_DAEMON));
            hb.setTimeoutSeconds(rs.getInt(COLUMN_TIMEOUT_SECONDS));
            hb.setLastTime(rs.getLong(COLUMN_LAST_TIME));
            return hb.build();
        }
    };
}
