/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */

package org.zenoss.zep.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventTriggerSubscriptionDao;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EventTriggerSubscriptionDaoImpl implements
        EventTriggerSubscriptionDao {

    public static final String TABLE_EVENT_TRIGGER_SUBSCRIPTION = "event_trigger_subscription";
    public static final String COLUMN_UUID = "uuid";
    public static final String COLUMN_EVENT_TRIGGER_UUID = "event_trigger_uuid";
    public static final String COLUMN_SUBSCRIBER_UUID = "subscriber_uuid";
    public static final String COLUMN_DELAY_SECONDS = "delay_seconds";
    public static final String COLUMN_REPEAT_SECONDS = "repeat_seconds";
    public static final String COLUMN_SEND_INITIAL_OCCURRENCE = "send_initial_occurrence";

    private static final class EventTriggerSubscriptionMapper implements
            RowMapper<EventTriggerSubscription> {

        @Override
        public EventTriggerSubscription mapRow(ResultSet rs, int rowNum)
                throws SQLException {
            EventTriggerSubscription.Builder evtTriggerSub = EventTriggerSubscription
                    .newBuilder();

            evtTriggerSub.setUuid(DaoUtils.uuidFromBytes(rs
                    .getBytes(COLUMN_UUID)));
            evtTriggerSub.setDelaySeconds(rs.getInt(COLUMN_DELAY_SECONDS));
            evtTriggerSub.setRepeatSeconds(rs.getInt(COLUMN_REPEAT_SECONDS));
            evtTriggerSub.setTriggerUuid(DaoUtils.uuidFromBytes(rs
                    .getBytes(COLUMN_EVENT_TRIGGER_UUID)));
            evtTriggerSub.setSubscriberUuid(DaoUtils.uuidFromBytes(rs
                    .getBytes(COLUMN_SUBSCRIBER_UUID)));
            evtTriggerSub.setSendInitialOccurrence(rs.getBoolean(COLUMN_SEND_INITIAL_OCCURRENCE));
            return evtTriggerSub.build();
        }
    }

    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory.getLogger(EventTriggerSubscriptionDaoImpl.class);

    private final SimpleJdbcTemplate template;
    private final SimpleJdbcInsert insert;
    private UUIDGenerator uuidGenerator;

    public EventTriggerSubscriptionDaoImpl(DataSource dataSource) {
        this.template = new SimpleJdbcTemplate(dataSource);
        this.insert = new SimpleJdbcInsert(dataSource)
                .withTableName(TABLE_EVENT_TRIGGER_SUBSCRIPTION);
    }

    public void setUuidGenerator(UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    private static Map<String, Object> subscriptionToFields(
            EventTriggerSubscription evtTriggerSub) {
        final Map<String, Object> fields = new LinkedHashMap<String, Object>();

        fields.put(COLUMN_EVENT_TRIGGER_UUID,
                DaoUtils.uuidToBytes(evtTriggerSub.getTriggerUuid()));
        fields.put(COLUMN_SUBSCRIBER_UUID,
                DaoUtils.uuidToBytes(evtTriggerSub.getSubscriberUuid()));
        fields.put(COLUMN_DELAY_SECONDS, evtTriggerSub.getDelaySeconds());
        fields.put(COLUMN_REPEAT_SECONDS, evtTriggerSub.getRepeatSeconds());
        fields.put(COLUMN_SEND_INITIAL_OCCURRENCE, evtTriggerSub.getSendInitialOccurrence());
        return fields;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public String create(EventTriggerSubscription evtTriggerSubscription)
            throws ZepException {
        if (evtTriggerSubscription.getDelaySeconds() < 0
                || evtTriggerSubscription.getRepeatSeconds() < 0) {
            throw new ZepException(
                    "Delay seconds or repeat seconds cannot be negative");
        }
        final Map<String, Object> fields = subscriptionToFields(evtTriggerSubscription);
        String uuid = evtTriggerSubscription.getUuid();
        if (uuid == null || uuid.isEmpty()) {
            uuid = this.uuidGenerator.generate().toString();
        }
        fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
        try {
            this.insert.execute(fields);
            return uuid;
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int delete(String uuid) throws ZepException {
        try {
            return this.template.update(String.format(
                    "DELETE FROM %s WHERE %s=?",
                    TABLE_EVENT_TRIGGER_SUBSCRIPTION, COLUMN_UUID), DaoUtils
                    .uuidToBytes(uuid));
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @TransactionalReadOnly
    public List<EventTriggerSubscription> findAll() throws ZepException {
        try {
            final String sql = String.format("SELECT * FROM %s",
                    TABLE_EVENT_TRIGGER_SUBSCRIPTION);
            return this.template.query(sql,
                    new EventTriggerSubscriptionMapper());
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @TransactionalReadOnly
    public EventTriggerSubscription findByUuid(String uuid) throws ZepException {
        try {
            List<EventTriggerSubscription> subs = this.template.query(String
                    .format("SELECT * FROM %s WHERE %s=?",
                            TABLE_EVENT_TRIGGER_SUBSCRIPTION, COLUMN_UUID),
                    new EventTriggerSubscriptionMapper(), DaoUtils
                            .uuidToBytes(uuid));
            return (subs.size() > 0) ? subs.get(0) : null;
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @TransactionalReadOnly
    public List<EventTriggerSubscription> findBySubscriberUuid(
            String subscriberUuid) throws ZepException {
        try {
            return this.template.query(String.format(
                    "SELECT * FROM %s WHERE %s=?",
                    TABLE_EVENT_TRIGGER_SUBSCRIPTION, COLUMN_SUBSCRIBER_UUID),
                    new EventTriggerSubscriptionMapper(), DaoUtils
                            .uuidToBytes(subscriberUuid));
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int updateSubscriptions(String subscriberUuid,
            List<EventTriggerSubscription> subscriptions) throws ZepException {
        final byte[] subscriberUuidBytes = DaoUtils.uuidToBytes(subscriberUuid);
        int numRows = 0;
        if (subscriptions.isEmpty()) {
            String sql = String.format("DELETE FROM %s WHERE %s=?",
                    TABLE_EVENT_TRIGGER_SUBSCRIPTION, COLUMN_SUBSCRIBER_UUID);
            numRows += this.template.update(sql, subscriberUuidBytes);
        } else {
            List<Map<String, Object>> subscriptionFields = new ArrayList<Map<String, Object>>(
                    subscriptions.size());
            List<byte[]> eventTriggerUuids = new ArrayList<byte[]>(
                    subscriptions.size());
            for (EventTriggerSubscription eventTriggerSubscription : subscriptions) {
                if (!subscriberUuid.equals(eventTriggerSubscription
                        .getSubscriberUuid())) {
                    throw new ZepException(
                            "Subscriber id mismatch in subscriptions update");
                }
                eventTriggerUuids
                        .add(DaoUtils.uuidToBytes(eventTriggerSubscription
                                .getTriggerUuid()));
                Map<String, Object> fields = subscriptionToFields(eventTriggerSubscription);
                String uuid = eventTriggerSubscription.getUuid();
                if (uuid == null || uuid.isEmpty()) {
                    uuid = this.uuidGenerator.generate().toString();
                }
                fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
                subscriptionFields.add(fields);
            }
            numRows += this.template.update(String.format(
                    "DELETE FROM %s WHERE %s=? AND %s NOT IN(?)",
                    TABLE_EVENT_TRIGGER_SUBSCRIPTION, COLUMN_SUBSCRIBER_UUID,
                    COLUMN_EVENT_TRIGGER_UUID), subscriberUuidBytes,
                    eventTriggerUuids);
            String sql = String.format("INSERT INTO %s (%s, %s, %s, %s, %s, %s) "
                    + "VALUES(:%s, :%s, :%s, :%s, :%s, :%s)"
                    + "ON DUPLICATE KEY UPDATE %s=VALUES(%s), %s=VALUES(%s), %s=VALUES(%s)",
                    TABLE_EVENT_TRIGGER_SUBSCRIPTION, COLUMN_UUID,
                    COLUMN_EVENT_TRIGGER_UUID, COLUMN_SUBSCRIBER_UUID,
                    COLUMN_DELAY_SECONDS, COLUMN_REPEAT_SECONDS, COLUMN_SEND_INITIAL_OCCURRENCE,
                    COLUMN_UUID,
                    COLUMN_EVENT_TRIGGER_UUID, COLUMN_SUBSCRIBER_UUID,
                    COLUMN_DELAY_SECONDS, COLUMN_REPEAT_SECONDS, COLUMN_SEND_INITIAL_OCCURRENCE,
                    COLUMN_DELAY_SECONDS, COLUMN_DELAY_SECONDS,
                    COLUMN_REPEAT_SECONDS, COLUMN_REPEAT_SECONDS,
                    COLUMN_SEND_INITIAL_OCCURRENCE, COLUMN_SEND_INITIAL_OCCURRENCE);
            for (Map<String, Object> fields : subscriptionFields) {
                numRows += this.template.update(sql, fields);
            }
        }
        return numRows;
    }
}
