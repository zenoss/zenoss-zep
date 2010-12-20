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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.protobufs.zep.Zep.Rule;
import org.zenoss.protobufs.zep.Zep.RuleType;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSignalSpoolDao;
import org.zenoss.zep.dao.EventTriggerDao;

public class EventTriggerDaoImpl implements EventTriggerDao {
    private static final String TABLE_EVENT_TRIGGER = "event_trigger";
    private static final String COLUMN_UUID = "uuid";
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_ENABLED = "enabled";
    private static final String COLUMN_RULE_API_VERSION = "rule_api_version";
    private static final String COLUMN_RULE_TYPE_ID = "rule_type_id";
    private static final String COLUMN_RULE_SOURCE = "rule_source";

    private static final Logger logger = LoggerFactory
            .getLogger(EventTriggerDaoImpl.class);

    private final SimpleJdbcTemplate template;

    private EventSignalSpoolDao eventSignalSpoolDao;

    public EventTriggerDaoImpl(DataSource dataSource) {
        this.template = new SimpleJdbcTemplate(dataSource);
    }

    public void setEventSignalSpoolDao(EventSignalSpoolDao eventSignalSpoolDao) {
        this.eventSignalSpoolDao = eventSignalSpoolDao;
    }

    @Override
    @Transactional
    public void create(EventTrigger trigger) throws ZepException {
        final Map<String, Object> fields = triggerToFields(trigger);
        try {
            insert("event_trigger", fields);
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    private void insert(String tableName, Map<String, Object> fields) {
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
        this.template.update(String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName, names, values), fields);
    }

    @Override
    @Transactional
    public int delete(String uuidStr) throws ZepException {
        return this.template.update("DELETE FROM event_trigger WHERE uuid=?",
                DaoUtils.uuidToBytes(uuidStr));
    }

    @Override
    @Transactional(readOnly = true)
    public EventTrigger findByUuid(String uuidStr) throws ZepException {
        final byte[] uuidBytes = DaoUtils.uuidToBytes(uuidStr);
        String sql = "SELECT event_trigger.*,sub.uuid AS event_sub_uuid,sub.subscriber_uuid,sub.delay_seconds,sub.repeat_seconds "
                + "FROM event_trigger "
                + "LEFT JOIN event_trigger_subscription AS sub ON event_trigger.uuid = sub.event_trigger_uuid "
                + "WHERE event_trigger.uuid=?";
        List<EventTrigger> triggers = this.template.getJdbcOperations().query(
                sql, new EventTriggerExtractor(), uuidBytes);
        EventTrigger trigger = null;
        if (!triggers.isEmpty()) {
            trigger = triggers.get(0);
        }
        return trigger;
    }

    @Override
    @Transactional(readOnly = true)
    public List<EventTrigger> findAll() throws ZepException {
        String sql = "SELECT event_trigger.*,sub.uuid AS event_sub_uuid,sub.subscriber_uuid,sub.delay_seconds,sub.repeat_seconds "
                + "FROM event_trigger "
                + "LEFT JOIN event_trigger_subscription AS sub ON event_trigger.uuid = sub.event_trigger_uuid";
        try {
            return this.template.getJdbcOperations().query(sql,
                    new EventTriggerExtractor());
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @Transactional
    public int modify(EventTrigger trigger) throws ZepException {
        final Map<String, Object> fields = triggerToFields(trigger);
        final StringBuilder fieldsSql = new StringBuilder();
        for (String key : fields.keySet()) {
            if (!COLUMN_UUID.equals(key)) {
                if (fieldsSql.length() > 0) {
                    fieldsSql.append(",");
                }
                fieldsSql.append(key).append("=:").append(key);
            }
        }
        if (fieldsSql.length() == 0) {
            logger.warn("No fields to modify in trigger: {}", trigger);
            return 0;
        }
        String sql = String.format("UPDATE %s SET %s WHERE %s=:%s",
                TABLE_EVENT_TRIGGER, fieldsSql.toString(), COLUMN_UUID,
                COLUMN_UUID);
        int numRows = template.update(sql, fields);

        /* If trigger is now disabled, remove any spooled signals */
        if (trigger.hasEnabled() && !trigger.getEnabled()) {
            this.eventSignalSpoolDao.deleteByTriggerUuid(trigger.getUuid());
        }
        return numRows;
    }

    private static Map<String, Object> triggerToFields(EventTrigger trigger) {
        final Map<String, Object> fields = new LinkedHashMap<String, Object>();
        fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(trigger.getUuid()));
        fields.put(COLUMN_NAME, trigger.hasName() ? trigger.getName() : null);
        fields.put(COLUMN_ENABLED, trigger.getEnabled());

        Rule rule = trigger.getRule();
        fields.put(COLUMN_RULE_API_VERSION, rule.getApiVersion());
        fields.put(COLUMN_RULE_SOURCE, rule.getSource());
        fields.put(COLUMN_RULE_TYPE_ID, rule.getType().getNumber());
        return fields;
    }

    private static final class EventTriggerExtractor implements
            ResultSetExtractor<List<EventTrigger>> {

        @Override
        public List<EventTrigger> extractData(ResultSet rs)
                throws SQLException, DataAccessException {
            Map<String, EventTrigger.Builder> triggersByUuid = new HashMap<String, EventTrigger.Builder>();

            while (rs.next()) {
                String uuid = DaoUtils.uuidFromBytes(rs.getBytes(COLUMN_UUID));
                EventTrigger.Builder triggerBuilder = triggersByUuid.get(uuid);
                if (triggerBuilder == null) {
                    triggerBuilder = EventTrigger.newBuilder();
                    triggerBuilder.setUuid(uuid);
                    String name = rs.getString(COLUMN_NAME);
                    if (name != null) {
                        triggerBuilder.setName(name);
                    }
                    triggerBuilder.setEnabled(rs.getBoolean(COLUMN_ENABLED));
                    Rule.Builder ruleBuilder = Rule.newBuilder();
                    ruleBuilder.setApiVersion(rs
                            .getInt(COLUMN_RULE_API_VERSION));
                    ruleBuilder.setType(RuleType.valueOf(rs
                            .getInt(COLUMN_RULE_TYPE_ID)));
                    ruleBuilder.setSource(rs.getString(COLUMN_RULE_SOURCE));
                    triggerBuilder.setRule(ruleBuilder.build());
                    triggersByUuid.put(uuid, triggerBuilder);
                }

                /* Add subscriptions (LEFT JOINED table) */
                byte[] subUuid = rs.getBytes("event_sub_uuid");
                if (subUuid != null) {
                    EventTriggerSubscription.Builder subBuilder = EventTriggerSubscription
                            .newBuilder();
                    subBuilder.setUuid(DaoUtils.uuidFromBytes(subUuid));
                    subBuilder
                            .setSubscriberUuid(DaoUtils.uuidFromBytes(rs
                                    .getBytes(EventTriggerSubscriptionDaoImpl.COLUMN_SUBSCRIBER_UUID)));
                    subBuilder
                            .setDelaySeconds(rs
                                    .getInt(EventTriggerSubscriptionDaoImpl.COLUMN_DELAY_SECONDS));
                    subBuilder
                            .setRepeatSeconds(rs
                                    .getInt(EventTriggerSubscriptionDaoImpl.COLUMN_REPEAT_SECONDS));
                    subBuilder.setTriggerUuid(uuid);
                    triggerBuilder.addSubscriptions(subBuilder.build());
                }
            }

            List<EventTrigger> triggers = new ArrayList<EventTrigger>(
                    triggersByUuid.size());
            for (EventTrigger.Builder eventTriggerBuilder : triggersByUuid
                    .values()) {
                triggers.add(eventTriggerBuilder.build());
            }
            return triggers;
        }
    }
}
