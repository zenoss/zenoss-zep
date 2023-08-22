/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcOperations;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventSignalSpool;
import org.zenoss.zep.dao.EventSignalSpoolDao;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.NestedTransactionService;
import org.zenoss.zep.dao.impl.compat.TypeConverter;
import org.zenoss.zep.dao.impl.compat.TypeConverterUtils;

import java.lang.reflect.Proxy;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EventSignalSpoolDaoImpl implements EventSignalSpoolDao {

    public static final String COLUMN_UUID = "uuid";
    public static final String COLUMN_EVENT_TRIGGER_SUBSCRIPTION_UUID = "event_trigger_subscription_uuid";
    public static final String COLUMN_EVENT_SUMMARY_UUID = "event_summary_uuid";
    public static final String COLUMN_FLUSH_TIME = "flush_time";
    public static final String COLUMN_CREATED = "created";
    public static final String COLUMN_EVENT_COUNT = "event_count";
    public static final String COLUMN_SENT_SIGNAL = "sent_signal";

    private class EventSignalSpoolMapper implements RowMapper<EventSignalSpool> {
        @Override
        public EventSignalSpool mapRow(ResultSet rs, int rowNum)
                throws SQLException {
            TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
            EventSignalSpool spool = new EventSignalSpool();
            spool.setUuid(uuidConverter.fromDatabaseType(rs, COLUMN_UUID));
            spool.setCreated(timestampConverter.fromDatabaseType(rs, COLUMN_CREATED));
            spool.setEventCount(rs.getInt(COLUMN_EVENT_COUNT));
            spool.setEventSummaryUuid(uuidConverter.fromDatabaseType(rs, COLUMN_EVENT_SUMMARY_UUID));
            spool.setSubscriptionUuid(uuidConverter.fromDatabaseType(rs, COLUMN_EVENT_TRIGGER_SUBSCRIPTION_UUID));
            spool.setFlushTime(rs.getLong(COLUMN_FLUSH_TIME));
            spool.setSentSignal(rs.getBoolean(COLUMN_SENT_SIGNAL));
            return spool;
        }
    }

    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory.getLogger(EventSignalSpoolDaoImpl.class);

    private final SimpleJdbcOperations template;
    private UUIDGenerator uuidGenerator;
    private DatabaseCompatibility databaseCompatibility;
    private TypeConverter<String> uuidConverter;
    private NestedTransactionService nestedTransactionService;

    public EventSignalSpoolDaoImpl(DataSource dataSource) {
        this.template = (SimpleJdbcOperations) Proxy.newProxyInstance(SimpleJdbcOperations.class.getClassLoader(),
                new Class<?>[] {SimpleJdbcOperations.class}, new SimpleJdbcTemplateProxy(dataSource));
    }

    public void setUuidGenerator(UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    public void setDatabaseCompatibility(DatabaseCompatibility databaseCompatibility) {
        this.databaseCompatibility = databaseCompatibility;
        this.uuidConverter = databaseCompatibility.getUUIDConverter();
    }

    public void setNestedTransactionService(NestedTransactionService nestedTransactionService) {
        this.nestedTransactionService = nestedTransactionService;
    }

    private Map<String, Object> spoolToFields(EventSignalSpool spool) {
        final Map<String, Object> fields = new LinkedHashMap<String, Object>();

        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        fields.put(COLUMN_CREATED, timestampConverter.toDatabaseType(spool.getCreated()));
        fields.put(COLUMN_EVENT_SUMMARY_UUID, uuidConverter.toDatabaseType(spool.getEventSummaryUuid()));
        fields.put(COLUMN_EVENT_TRIGGER_SUBSCRIPTION_UUID, uuidConverter.toDatabaseType(spool.getSubscriptionUuid()));
        fields.put(COLUMN_FLUSH_TIME, spool.getFlushTime());
        fields.put(COLUMN_EVENT_COUNT, spool.getEventCount());
        fields.put(COLUMN_SENT_SIGNAL, spool.isSentSignal());
        return fields;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public String create(EventSignalSpool spool) throws ZepException {
        final Map<String, Object> fields = spoolToFields(spool);
        String uuid = spool.getUuid();
        if (uuid == null) {
            uuid = uuidGenerator.generate().toString();
        }
        fields.put(COLUMN_UUID, uuidConverter.toDatabaseType(uuid));
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

        final String insertSql = String.format("INSERT INTO event_trigger_signal_spool (%s) VALUES(%s)", names,
                    values);
        final String updateSql = "UPDATE event_trigger_signal_spool SET event_count = event_count + 1" +
                    " WHERE uuid=:uuid";
        DaoUtils.insertOrUpdate(nestedTransactionService, template, insertSql, updateSql, fields);
        spool.setUuid(uuid);
        return uuid;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int delete(String uuid) throws ZepException {
        return delete(Collections.singletonList(uuid));
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int delete(List<String> uuids) throws ZepException {
        final Map<String,List<Object>> fields = Collections.singletonMap("_uuids",
                TypeConverterUtils.batchToDatabaseType(uuidConverter, uuids));
        final String sql = "DELETE FROM event_trigger_signal_spool WHERE uuid IN (:_uuids)";
        return this.template.update(sql, fields);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int delete(String triggerUuid, String summaryUuid) throws ZepException {
        Map<String,Object> fields = new HashMap<String, Object>(2);
        fields.put(COLUMN_EVENT_SUMMARY_UUID, uuidConverter.toDatabaseType(summaryUuid));
        fields.put(EventTriggerSubscriptionDaoImpl.COLUMN_EVENT_TRIGGER_UUID,
                uuidConverter.toDatabaseType(triggerUuid));

        final String sql = "DELETE FROM event_trigger_signal_spool WHERE event_summary_uuid=:event_summary_uuid " +
                "AND event_trigger_subscription_uuid IN " +
                "(SELECT uuid FROM event_trigger_subscription WHERE event_trigger_uuid=:event_trigger_uuid)";
        return this.template.update(sql, fields);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int deleteByEventSummaryUuid(String eventSummaryUuid) throws ZepException {
        return deleteByEventSummaryUuids(Collections.singletonList(eventSummaryUuid));
    }

    @Override
    public int deleteByEventSummaryUuids(Collection<String> eventSummaryUuids) throws ZepException {
        if (eventSummaryUuids.isEmpty()) {
            return 0;
        }
        final Map<String,List<Object>> fields = Collections.singletonMap("_uuids",
                TypeConverterUtils.batchToDatabaseType(uuidConverter, eventSummaryUuids));
        final String sql = "DELETE FROM event_trigger_signal_spool WHERE event_summary_uuid IN (:_uuids)";
        return this.template.update(sql, fields);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int deleteByTriggerUuid(String triggerUuid) throws ZepException {
        final Map<String, Object> fields = Collections.singletonMap(
                EventTriggerSubscriptionDaoImpl.COLUMN_EVENT_TRIGGER_UUID, uuidConverter.toDatabaseType(triggerUuid));
        final String sql = "DELETE FROM event_trigger_signal_spool WHERE event_trigger_subscription_uuid IN " +
                "(SELECT uuid FROM event_trigger_subscription WHERE event_trigger_uuid=:event_trigger_uuid)";
        return this.template.update(sql, fields);
    }

    @Override
    @TransactionalReadOnly
    public EventSignalSpool findByUuid(String uuid) throws ZepException {
        Map<String,Object> fields = Collections.singletonMap(COLUMN_UUID, uuidConverter.toDatabaseType(uuid));
        final String sql = "SELECT * FROM event_trigger_signal_spool WHERE uuid=:uuid";
        List<EventSignalSpool> spools = this.template.query(sql, new EventSignalSpoolMapper(), fields);
        EventSignalSpool spool = null;
        if (!spools.isEmpty()) {
            spool = spools.get(0);
        }
        return spool;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int update(EventSignalSpool spool) throws ZepException {
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put(COLUMN_UUID, uuidConverter.toDatabaseType(spool.getUuid()));
        fields.put(COLUMN_FLUSH_TIME, spool.getFlushTime());
        fields.put(COLUMN_EVENT_COUNT, spool.getEventCount());
        fields.put(COLUMN_SENT_SIGNAL, spool.isSentSignal());
        final String sql = "UPDATE event_trigger_signal_spool SET flush_time=:flush_time,event_count=:event_count,"
                + "sent_signal=:sent_signal WHERE uuid=:uuid";
        return this.template.update(sql, fields);
    }

    @Override
    @TransactionalReadOnly
    public EventSignalSpool findBySubscriptionAndEventSummaryUuids(
            String subscriptionUuid, String summaryUuid) throws ZepException {
        final Map<String,Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_EVENT_TRIGGER_SUBSCRIPTION_UUID, uuidConverter.toDatabaseType(subscriptionUuid));
        fields.put(COLUMN_EVENT_SUMMARY_UUID, uuidConverter.toDatabaseType(summaryUuid));

        final String sql = "SELECT * FROM event_trigger_signal_spool WHERE " +
                "event_trigger_subscription_uuid=:event_trigger_subscription_uuid AND " +
                "event_summary_uuid=:event_summary_uuid";
        final List<EventSignalSpool> spools = this.template.query(sql,
                new EventSignalSpoolMapper(), fields);
        return (!spools.isEmpty()) ? spools.get(0) : null;
    }

    @Override
    @TransactionalReadOnly
    public List<EventSignalSpool> findAllDue() throws ZepException {
        Map<String,Long> fields = Collections.singletonMap(COLUMN_FLUSH_TIME,System.currentTimeMillis());
        final String sql = "SELECT * FROM event_trigger_signal_spool WHERE flush_time <= :flush_time";
        return this.template.query(sql, new EventSignalSpoolMapper(), fields);
    }

    @Override
    @TransactionalReadOnly
    public List<EventSignalSpool> findAllByEventSummaryUuid(String eventSummaryUuid) throws ZepException {
        return findAllByEventSummaryUuids(Collections.singletonList(eventSummaryUuid));
    }

    @Override
    public List<EventSignalSpool> findAllByEventSummaryUuids(Collection<String> eventSummaryUuids) throws ZepException {
        if (eventSummaryUuids.isEmpty()) {
            return Collections.emptyList();
        }
        final String sql = "SELECT * FROM event_trigger_signal_spool WHERE event_summary_uuid IN (:_uuids)";
        final Map<String,List<Object>> fields = Collections.singletonMap("_uuids",
                TypeConverterUtils.batchToDatabaseType(uuidConverter, eventSummaryUuids));
        return this.template.query(sql, new EventSignalSpoolMapper(), fields);
    }

    @Override
    @TransactionalReadOnly
    public long getNextFlushTime() throws ZepException {
        final String sql = "SELECT MIN(flush_time) FROM event_trigger_signal_spool";
        return this.template.queryForLong(sql);
    }
}
