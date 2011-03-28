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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSignalSpool;
import org.zenoss.zep.dao.EventSignalSpoolDao;

public class EventSignalSpoolDaoImpl implements EventSignalSpoolDao {

    public static final String TABLE_EVENT_TRIGGER_SIGNAL_SPOOL = "event_trigger_signal_spool";
    public static final String COLUMN_UUID = "uuid";
    public static final String COLUMN_EVENT_TRIGGER_SUBSCRIPTION_UUID = "event_trigger_subscription_uuid";
    public static final String COLUMN_EVENT_SUMMARY_UUID = "event_summary_uuid";
    public static final String COLUMN_FLUSH_TIME = "flush_time";
    public static final String COLUMN_CREATED = "created";
    public static final String COLUMN_EVENT_COUNT = "event_count";

    private static final class EventSignalSpoolMapper implements
            RowMapper<EventSignalSpool> {

        @Override
        public EventSignalSpool mapRow(ResultSet rs, int rowNum)
                throws SQLException {
            EventSignalSpool spool = new EventSignalSpool();
            spool.setUuid(DaoUtils.uuidFromBytes(rs.getBytes(COLUMN_UUID)));
            spool.setCreated(rs.getLong(COLUMN_CREATED));
            spool.setEventCount(rs.getInt(COLUMN_EVENT_COUNT));
            spool.setEventSummaryUuid(DaoUtils.uuidFromBytes(rs
                    .getBytes(COLUMN_EVENT_SUMMARY_UUID)));
            spool.setSubscriptionUuid(DaoUtils.uuidFromBytes(rs
                    .getBytes(COLUMN_EVENT_TRIGGER_SUBSCRIPTION_UUID)));
            spool.setFlushTime(rs.getLong(COLUMN_FLUSH_TIME));
            return spool;
        }
    }

    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory
            .getLogger(EventSignalSpoolDaoImpl.class);

    private final SimpleJdbcTemplate template;

    public EventSignalSpoolDaoImpl(DataSource dataSource) {
        this.template = new SimpleJdbcTemplate(dataSource);
    }

    private static Map<String, Object> spoolToFields(EventSignalSpool spool) {
        final Map<String, Object> fields = new LinkedHashMap<String, Object>();

        fields.put(COLUMN_CREATED, spool.getCreated());
        fields.put(COLUMN_EVENT_SUMMARY_UUID,
                DaoUtils.uuidToBytes(spool.getEventSummaryUuid()));
        fields.put(COLUMN_EVENT_TRIGGER_SUBSCRIPTION_UUID,
                DaoUtils.uuidToBytes(spool.getSubscriptionUuid()));
        fields.put(COLUMN_FLUSH_TIME, spool.getFlushTime());
        fields.put(COLUMN_EVENT_COUNT, spool.getEventCount());

        return fields;
    }

    @Override
    @Transactional
    public String create(EventSignalSpool spool) throws ZepException {
        final Map<String, Object> fields = spoolToFields(spool);
        String uuid = spool.getUuid();
        if (uuid == null) {
            uuid = UUID.randomUUID().toString();
        }
        fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
        try {
            final StringBuilder names = new StringBuilder();
            final StringBuilder values = new StringBuilder();
            for (String key : fields.keySet()) {
                if (names.length() > 0) {
                    names.append(',');
                    values.append(',');
                }
                names.append(key);
                values.append(':').append(key);
            }
            final String sql = String
                    .format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s = %s + 1",
                            TABLE_EVENT_TRIGGER_SIGNAL_SPOOL, names, values,
                            COLUMN_EVENT_COUNT, COLUMN_EVENT_COUNT);
            this.template.update(sql, fields);
            spool.setUuid(uuid);
            return uuid;
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }

    }

    @Override
    @Transactional
    public int delete(String uuid) throws ZepException {
        try {
            final String sql = String.format("DELETE FROM %s WHERE %s=?",
                    TABLE_EVENT_TRIGGER_SIGNAL_SPOOL, COLUMN_UUID);
            return this.template.update(sql, DaoUtils.uuidToBytes(uuid));
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @Transactional
    public int delete(String triggerUuid, String summaryUuid)
            throws ZepException {
        try {
            final byte[] triggerUuidBytes = DaoUtils.uuidToBytes(triggerUuid);
            final byte[] eventSummaryUuidBytes = DaoUtils
                    .uuidToBytes(summaryUuid);

            final String sql = String
                    .format("DELETE FROM %s WHERE %s=? AND %s IN (SELECT %s FROM %s WHERE %s=?)",
                            TABLE_EVENT_TRIGGER_SIGNAL_SPOOL,
                            COLUMN_EVENT_SUMMARY_UUID,
                            COLUMN_EVENT_TRIGGER_SUBSCRIPTION_UUID,
                            EventTriggerSubscriptionDaoImpl.COLUMN_UUID,
                            EventTriggerSubscriptionDaoImpl.TABLE_EVENT_TRIGGER_SUBSCRIPTION,
                            EventTriggerSubscriptionDaoImpl.COLUMN_EVENT_TRIGGER_UUID);
            return this.template.update(sql, eventSummaryUuidBytes,
                    triggerUuidBytes);
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @Transactional
    public int deleteByTriggerUuid(String triggerUuid) throws ZepException {
        try {
            byte[] triggerUuidBytes = DaoUtils.uuidToBytes(triggerUuid);
            final String sql = String
                    .format("DELETE FROM %s WHERE %s IN (SELECT %s FROM %s WHERE %s=?)",
                            TABLE_EVENT_TRIGGER_SIGNAL_SPOOL,
                            COLUMN_EVENT_TRIGGER_SUBSCRIPTION_UUID,
                            EventTriggerSubscriptionDaoImpl.COLUMN_UUID,
                            EventTriggerSubscriptionDaoImpl.TABLE_EVENT_TRIGGER_SUBSCRIPTION,
                            EventTriggerSubscriptionDaoImpl.COLUMN_EVENT_TRIGGER_UUID);
            return this.template.update(sql, triggerUuidBytes);
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public EventSignalSpool findByUuid(String uuid) throws ZepException {
        try {
            byte[] uuidBytes = DaoUtils.uuidToBytes(uuid);
            final String sql = String.format("SELECT * FROM %s WHERE %s=?",
                    TABLE_EVENT_TRIGGER_SIGNAL_SPOOL, COLUMN_UUID);
            List<EventSignalSpool> spools = this.template.query(sql,
                    new EventSignalSpoolMapper(), uuidBytes);
            EventSignalSpool spool = null;
            if (!spools.isEmpty()) {
                spool = spools.get(0);
            }
            return spool;
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @Transactional
    public int updateFlushTime(String uuid, long newFlushTime)
            throws ZepException {
        try {
            final String sql = String.format("UPDATE %s SET %s=? WHERE %s=?",
                    TABLE_EVENT_TRIGGER_SIGNAL_SPOOL, COLUMN_FLUSH_TIME,
                    COLUMN_UUID);
            return template.update(sql, newFlushTime,
                    DaoUtils.uuidToBytes(uuid));
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public EventSignalSpool findByTriggerAndEventSummaryUuids(
            String triggerUuid, String summaryUuid) throws ZepException {
        try {
            final byte[] triggerUuidBytes = DaoUtils.uuidToBytes(triggerUuid);
            final byte[] summaryUuidBytes = DaoUtils.uuidToBytes(summaryUuid);

            final String sql = String
                    .format("SELECT * FROM %s WHERE %s=? AND %s IN (SELECT %s FROM %s WHERE %s=?)",
                            TABLE_EVENT_TRIGGER_SIGNAL_SPOOL,
                            COLUMN_EVENT_SUMMARY_UUID,
                            COLUMN_EVENT_TRIGGER_SUBSCRIPTION_UUID,
                            EventTriggerSubscriptionDaoImpl.COLUMN_UUID,
                            EventTriggerSubscriptionDaoImpl.TABLE_EVENT_TRIGGER_SUBSCRIPTION,
                            EventTriggerSubscriptionDaoImpl.COLUMN_EVENT_TRIGGER_UUID);
            final List<EventSignalSpool> spools = this.template.query(sql,
                    new EventSignalSpoolMapper(), summaryUuidBytes,
                    triggerUuidBytes);
            return (!spools.isEmpty()) ? spools.get(0) : null;
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<EventSignalSpool> findAllDue() throws ZepException {
        try {
            final String sql = String.format("SELECT * FROM %s WHERE %s <= ?",
                    TABLE_EVENT_TRIGGER_SIGNAL_SPOOL, COLUMN_FLUSH_TIME);
            return this.template.query(sql, new EventSignalSpoolMapper(),
                    System.currentTimeMillis());
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public long getNextFlushTime() throws ZepException {
        try {
            final String sql = "SELECT MIN(flush_time) FROM event_trigger_signal_spool";
            return this.template.queryForLong(sql);
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }
}
