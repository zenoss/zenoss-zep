/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventSignalSpool;
import org.zenoss.zep.dao.EventSignalSpoolDao;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
    @TransactionalRollbackAllExceptions
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
    @TransactionalRollbackAllExceptions
    public int delete(String uuid) throws ZepException {
        try {
            final Map<String,byte[]> fields = Collections.singletonMap(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
            final String sql = "DELETE FROM event_trigger_signal_spool WHERE uuid=:uuid";
            return this.template.update(sql, fields);
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @TransactionalRollbackAllExceptions
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
    @TransactionalRollbackAllExceptions
    public int deleteByTriggerUuid(String triggerUuid) throws ZepException {
        try {
            final Map<String, byte[]> fields = Collections.singletonMap(
                    EventTriggerSubscriptionDaoImpl.COLUMN_EVENT_TRIGGER_UUID, DaoUtils.uuidToBytes(triggerUuid));
            final String sql = "DELETE FROM event_trigger_signal_spool WHERE event_trigger_subscription_uuid IN " +
                    "(SELECT uuid FROM event_trigger_subscription WHERE event_trigger_uuid=:event_trigger_uuid)";
            return this.template.update(sql, fields);
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @TransactionalReadOnly
    public EventSignalSpool findByUuid(String uuid) throws ZepException {
        try {
            Map<String,byte[]> fields = Collections.singletonMap(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
            final String sql = "SELECT * FROM event_trigger_signal_spool WHERE uuid=:uuid";
            List<EventSignalSpool> spools = this.template.query(sql, new EventSignalSpoolMapper(), fields);
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
    @TransactionalRollbackAllExceptions
    public int updateFlushTime(String uuid, long newFlushTime)
            throws ZepException {
        try {
            Map<String,Object> fields = new HashMap<String,Object>();
            fields.put(COLUMN_FLUSH_TIME, newFlushTime);
            fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
            final String sql = "UPDATE event_trigger_signal_spool SET flush_time=:flush_time WHERE uuid=:uuid";
            return template.update(sql, fields);
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @TransactionalReadOnly
    public EventSignalSpool findBySubscriptionAndEventSummaryUuids(
            String subscriptionUuid, String summaryUuid) throws ZepException {
        try {
            final Map<String,byte[]> fields = new HashMap<String,byte[]>();
            fields.put(COLUMN_EVENT_TRIGGER_SUBSCRIPTION_UUID, DaoUtils.uuidToBytes(subscriptionUuid));
            fields.put(COLUMN_EVENT_SUMMARY_UUID, DaoUtils.uuidToBytes(summaryUuid));

            final String sql = "SELECT * FROM event_trigger_signal_spool WHERE " +
                    "event_trigger_subscription_uuid=:event_trigger_subscription_uuid AND " +
                    "event_summary_uuid=:event_summary_uuid";
            final List<EventSignalSpool> spools = this.template.query(sql,
                    new EventSignalSpoolMapper(), fields);
            return (!spools.isEmpty()) ? spools.get(0) : null;
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @TransactionalReadOnly
    public List<EventSignalSpool> findAllDue() throws ZepException {
        try {
            Map<String,Long> fields = Collections.singletonMap(COLUMN_FLUSH_TIME, System.currentTimeMillis());
            final String sql = "SELECT * FROM event_trigger_signal_spool WHERE flush_time <= :flush_time";
            return this.template.query(sql, new EventSignalSpoolMapper(), fields);
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @TransactionalReadOnly
    public List<EventSignalSpool> findAllByEventSummaryUuid(String eventSummaryUuid) throws ZepException {
        final String sql = "SELECT * FROM event_trigger_signal_spool WHERE event_summary_uuid=:event_summary_uuid";
        final Map<String,byte[]> fields =
                Collections.singletonMap(COLUMN_EVENT_SUMMARY_UUID, DaoUtils.uuidToBytes(eventSummaryUuid));
        try {
            return this.template.query(sql, new EventSignalSpoolMapper(), fields);
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @TransactionalReadOnly
    public long getNextFlushTime() throws ZepException {
        try {
            final String sql = "SELECT MIN(flush_time) FROM event_trigger_signal_spool";
            return this.template.queryForLong(sql);
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }
}
