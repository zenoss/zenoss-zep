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
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcOperations;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventTriggerSubscriptionDao;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.NestedTransactionService;
import org.zenoss.zep.dao.impl.compat.TypeConverter;
import org.zenoss.zep.dao.impl.SimpleJdbcTemplateProxy;

import java.lang.reflect.Proxy;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EventTriggerSubscriptionDaoImpl implements EventTriggerSubscriptionDao {

    public static final String TABLE_EVENT_TRIGGER_SUBSCRIPTION = "event_trigger_subscription";
    public static final String COLUMN_UUID = "uuid";
    public static final String COLUMN_EVENT_TRIGGER_UUID = "event_trigger_uuid";
    public static final String COLUMN_SUBSCRIBER_UUID = "subscriber_uuid";
    public static final String COLUMN_DELAY_SECONDS = "delay_seconds";
    public static final String COLUMN_REPEAT_SECONDS = "repeat_seconds";
    public static final String COLUMN_SEND_INITIAL_OCCURRENCE = "send_initial_occurrence";

    private class EventTriggerSubscriptionMapper implements RowMapper<EventTriggerSubscription> {

        @Override
        public EventTriggerSubscription mapRow(ResultSet rs, int rowNum) throws SQLException {
            EventTriggerSubscription.Builder evtTriggerSub = EventTriggerSubscription.newBuilder();

            evtTriggerSub.setUuid(uuidConverter.fromDatabaseType(rs, COLUMN_UUID));
            evtTriggerSub.setDelaySeconds(rs.getInt(COLUMN_DELAY_SECONDS));
            evtTriggerSub.setRepeatSeconds(rs.getInt(COLUMN_REPEAT_SECONDS));
            evtTriggerSub.setTriggerUuid(uuidConverter.fromDatabaseType(rs, COLUMN_EVENT_TRIGGER_UUID));
            evtTriggerSub.setSubscriberUuid(uuidConverter.fromDatabaseType(rs, COLUMN_SUBSCRIBER_UUID));
            evtTriggerSub.setSendInitialOccurrence(rs.getBoolean(COLUMN_SEND_INITIAL_OCCURRENCE));
            return evtTriggerSub.build();
        }
    }

    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory.getLogger(EventTriggerSubscriptionDaoImpl.class);

    private final SimpleJdbcOperations template;
    private final SimpleJdbcInsert insert;
    private UUIDGenerator uuidGenerator;
    private TypeConverter<String> uuidConverter;
    private NestedTransactionService nestedTransactionService;

    public EventTriggerSubscriptionDaoImpl(DataSource dataSource) {
    	this.template = (SimpleJdbcOperations) Proxy.newProxyInstance(SimpleJdbcOperations.class.getClassLoader(), 
    			new Class<?>[] {SimpleJdbcOperations.class}, new SimpleJdbcTemplateProxy(dataSource));
        this.insert = new SimpleJdbcInsert(dataSource).withTableName(TABLE_EVENT_TRIGGER_SUBSCRIPTION);
    }

    public void setUuidGenerator(UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    public void setDatabaseCompatibility(DatabaseCompatibility databaseCompatibility) {
        this.uuidConverter = databaseCompatibility.getUUIDConverter();
    }

    public void setNestedTransactionService(NestedTransactionService nestedTransactionService) {
        this.nestedTransactionService = nestedTransactionService;
    }

    private Map<String, Object> subscriptionToFields(EventTriggerSubscription evtTriggerSub) {
        final Map<String, Object> fields = new LinkedHashMap<String, Object>();

        fields.put(COLUMN_EVENT_TRIGGER_UUID, uuidConverter.toDatabaseType(evtTriggerSub.getTriggerUuid()));
        fields.put(COLUMN_SUBSCRIBER_UUID, uuidConverter.toDatabaseType(evtTriggerSub.getSubscriberUuid()));
        fields.put(COLUMN_DELAY_SECONDS, evtTriggerSub.getDelaySeconds());
        fields.put(COLUMN_REPEAT_SECONDS, evtTriggerSub.getRepeatSeconds());
        fields.put(COLUMN_SEND_INITIAL_OCCURRENCE, evtTriggerSub.getSendInitialOccurrence());
        return fields;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public String create(EventTriggerSubscription evtTriggerSubscription) throws ZepException {
        if (evtTriggerSubscription.getDelaySeconds() < 0 || evtTriggerSubscription.getRepeatSeconds() < 0) {
            throw new ZepException("Delay seconds or repeat seconds cannot be negative");
        }
        final Map<String, Object> fields = subscriptionToFields(evtTriggerSubscription);
        String uuid = evtTriggerSubscription.getUuid();
        if (uuid == null || uuid.isEmpty()) {
            uuid = this.uuidGenerator.generate().toString();
        }
        fields.put(COLUMN_UUID, uuidConverter.toDatabaseType(uuid));
        this.insert.execute(fields);
        return uuid;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int delete(String uuid) throws ZepException {
        final String sql = "DELETE FROM event_trigger_subscription WHERE uuid=?";
        return this.template.update(sql, uuidConverter.toDatabaseType(uuid));
    }

    @Override
    @TransactionalReadOnly
    public List<EventTriggerSubscription> findAll() throws ZepException {
        final String sql = "SELECT * FROM event_trigger_subscription";
        return this.template.query(sql, new EventTriggerSubscriptionMapper());
    }

    @Override
    @TransactionalReadOnly
    public EventTriggerSubscription findByUuid(String uuid) throws ZepException {
        final String sql = "SELECT * FROM event_trigger_subscription WHERE uuid=?";
        List<EventTriggerSubscription> subs = this.template.query(sql, new EventTriggerSubscriptionMapper(),
                uuidConverter.toDatabaseType(uuid));
        return (subs.size() > 0) ? subs.get(0) : null;
    }

    @Override
    @TransactionalReadOnly
    public List<EventTriggerSubscription> findBySubscriberUuid(String subscriberUuid) throws ZepException {
        final String sql = "SELECT * FROM event_trigger_subscription WHERE subscriber_uuid=?";
        return this.template.query(sql, new EventTriggerSubscriptionMapper(),
                uuidConverter.toDatabaseType(subscriberUuid));
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int updateSubscriptions(String subscriberUuid, List<EventTriggerSubscription> subscriptions)
            throws ZepException {
        final Object subscriberUuidBytes = uuidConverter.toDatabaseType(subscriberUuid);
        int numRows = 0;
        if (subscriptions.isEmpty()) {
            String sql = "DELETE FROM event_trigger_subscription WHERE subscriber_uuid=?";
            numRows += this.template.update(sql, subscriberUuidBytes);
        } else {
            List<Map<String, Object>> subscriptionFields = new ArrayList<Map<String, Object>>(subscriptions.size());
            List<Object> eventTriggerUuids = new ArrayList<Object>(subscriptions.size());
            for (EventTriggerSubscription eventTriggerSubscription : subscriptions) {
                if (!subscriberUuid.equals(eventTriggerSubscription.getSubscriberUuid())) {
                    throw new ZepException("Subscriber id mismatch in subscriptions update");
                }
                eventTriggerUuids.add(uuidConverter.toDatabaseType(eventTriggerSubscription.getTriggerUuid()));
                Map<String, Object> fields = subscriptionToFields(eventTriggerSubscription);
                String uuid = eventTriggerSubscription.getUuid();
                if (uuid == null || uuid.isEmpty()) {
                    uuid = this.uuidGenerator.generate().toString();
                }
                fields.put(COLUMN_UUID, uuidConverter.toDatabaseType(uuid));
                subscriptionFields.add(fields);
            }

            Map<String,Object> deleteFields = new HashMap<String, Object>(2);
            deleteFields.put(COLUMN_SUBSCRIBER_UUID, subscriberUuidBytes);
            deleteFields.put("_event_trigger_uuids", eventTriggerUuids);
            String deleteSql = "DELETE FROM event_trigger_subscription WHERE subscriber_uuid=:subscriber_uuid" +
                    " AND event_trigger_uuid NOT IN (:_event_trigger_uuids)";
            numRows += this.template.update(deleteSql, deleteFields);
            
            final String insertSql = "INSERT INTO event_trigger_subscription (uuid, event_trigger_uuid, " +
                    "subscriber_uuid, delay_seconds, repeat_seconds, send_initial_occurrence)" +
                    " VALUES(:uuid,:event_trigger_uuid,:subscriber_uuid,:delay_seconds,:repeat_seconds," +
                    ":send_initial_occurrence)";
            final String updateSql = "UPDATE event_trigger_subscription SET delay_seconds=:delay_seconds, " +
                    "repeat_seconds=:repeat_seconds, send_initial_occurrence=:send_initial_occurrence" +
                    " WHERE event_trigger_uuid=:event_trigger_uuid AND subscriber_uuid=:subscriber_uuid";
            for (final Map<String, Object> fields : subscriptionFields) {
                numRows += DaoUtils.insertOrUpdate(this.nestedTransactionService, template, insertSql, updateSql,
                        fields);
            }
        }
        return numRows;
    }
}
