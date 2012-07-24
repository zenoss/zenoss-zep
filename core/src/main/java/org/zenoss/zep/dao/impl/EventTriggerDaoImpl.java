/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.protobufs.zep.Zep.Rule;
import org.zenoss.protobufs.zep.Zep.RuleType;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventSignalSpoolDao;
import org.zenoss.zep.dao.EventTriggerDao;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.TypeConverter;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EventTriggerDaoImpl implements EventTriggerDao {
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
    private TypeConverter<String> uuidConverter;

    public EventTriggerDaoImpl(DataSource dataSource) {
        this.template = new SimpleJdbcTemplate(dataSource);
    }

    public void setEventSignalSpoolDao(EventSignalSpoolDao eventSignalSpoolDao) {
        this.eventSignalSpoolDao = eventSignalSpoolDao;
    }

    public void setDatabaseCompatibility(DatabaseCompatibility databaseCompatibility) {
        this.uuidConverter = databaseCompatibility.getUUIDConverter();
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void create(EventTrigger trigger) throws ZepException {
        final Map<String, Object> fields = triggerToFields(trigger);
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
        this.template.update(String.format("INSERT INTO event_trigger (%s) VALUES (%s)", names, values), fields);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int delete(String uuidStr) throws ZepException {
        final Map<String,Object> fields = Collections.singletonMap(COLUMN_UUID, uuidConverter.toDatabaseType(uuidStr));
        return this.template.update("DELETE FROM event_trigger WHERE uuid=:uuid", fields);
    }

    @Override
    @TransactionalReadOnly
    public EventTrigger findByUuid(String uuidStr) throws ZepException {
        final Object uuid = uuidConverter.toDatabaseType(uuidStr);
        final Map<String,Object> fields = Collections.singletonMap(COLUMN_UUID, uuid);
        String sql = "SELECT event_trigger.*,sub.uuid AS event_sub_uuid,sub.subscriber_uuid,sub.delay_seconds,"
                + "sub.repeat_seconds,sub.send_initial_occurrence FROM event_trigger "
                + "LEFT JOIN event_trigger_subscription AS sub ON event_trigger.uuid = sub.event_trigger_uuid "
                + "WHERE event_trigger.uuid=:uuid";
        List<EventTrigger> triggers = this.template.getNamedParameterJdbcOperations().query(
                sql, fields, new EventTriggerExtractor());
        EventTrigger trigger = null;
        if (!triggers.isEmpty()) {
            trigger = triggers.get(0);
        }
        return trigger;
    }

    @Override
    @TransactionalReadOnly
    public List<EventTrigger> findAll() throws ZepException {
        String sql = "SELECT event_trigger.*,sub.uuid AS event_sub_uuid,sub.subscriber_uuid,sub.delay_seconds,"
                + "sub.repeat_seconds,sub.send_initial_occurrence FROM event_trigger "
                + "LEFT JOIN event_trigger_subscription AS sub ON event_trigger.uuid = sub.event_trigger_uuid";
        return this.template.getJdbcOperations().query(sql, new EventTriggerExtractor());
    }

    @Override
    @TransactionalReadOnly
    public List<EventTrigger> findAllEnabled() throws ZepException {
        String sql = "SELECT event_trigger.*,sub.uuid AS event_sub_uuid,sub.subscriber_uuid,sub.delay_seconds,"
                + "sub.repeat_seconds,sub.send_initial_occurrence FROM event_trigger "
                + "LEFT JOIN event_trigger_subscription AS sub ON event_trigger.uuid = sub.event_trigger_uuid "
                + "WHERE event_trigger.enabled <> ?";
        return this.template.getJdbcOperations().query(sql, new EventTriggerExtractor(), Boolean.FALSE);
    }

    @Override
    @TransactionalRollbackAllExceptions
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
        String sql = String.format("UPDATE event_trigger SET %s WHERE uuid=:uuid", fieldsSql.toString());
        int numRows = template.update(sql, fields);

        /* If trigger is now disabled, remove any spooled signals */
        if (trigger.hasEnabled() && !trigger.getEnabled()) {
            this.eventSignalSpoolDao.deleteByTriggerUuid(trigger.getUuid());
        }
        return numRows;
    }

    private Map<String, Object> triggerToFields(EventTrigger trigger) {
        final Map<String, Object> fields = new LinkedHashMap<String, Object>();
        fields.put(COLUMN_UUID, uuidConverter.toDatabaseType(trigger.getUuid()));
        fields.put(COLUMN_NAME, trigger.hasName() ? trigger.getName() : null);
        fields.put(COLUMN_ENABLED, trigger.getEnabled());

        Rule rule = trigger.getRule();
        fields.put(COLUMN_RULE_API_VERSION, rule.getApiVersion());
        fields.put(COLUMN_RULE_SOURCE, rule.getSource());
        fields.put(COLUMN_RULE_TYPE_ID, rule.getType().getNumber());
        return fields;
    }

    private class EventTriggerExtractor implements ResultSetExtractor<List<EventTrigger>> {

        @Override
        public List<EventTrigger> extractData(ResultSet rs)
                throws SQLException, DataAccessException {
            Map<String, EventTrigger.Builder> triggersByUuid = new HashMap<String, EventTrigger.Builder>();

            while (rs.next()) {
                String uuid = uuidConverter.fromDatabaseType(rs, COLUMN_UUID);
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
                    ruleBuilder.setApiVersion(rs.getInt(COLUMN_RULE_API_VERSION));
                    ruleBuilder.setType(RuleType.valueOf(rs.getInt(COLUMN_RULE_TYPE_ID)));
                    ruleBuilder.setSource(rs.getString(COLUMN_RULE_SOURCE));
                    triggerBuilder.setRule(ruleBuilder.build());
                    triggersByUuid.put(uuid, triggerBuilder);
                }

                /* Add subscriptions (LEFT JOINED table) */
                String subUuid = uuidConverter.fromDatabaseType(rs, "event_sub_uuid");
                if (subUuid != null) {
                    EventTriggerSubscription.Builder subBuilder = EventTriggerSubscription.newBuilder();
                    subBuilder.setUuid(subUuid);
                    subBuilder.setSubscriberUuid(uuidConverter.fromDatabaseType(rs,
                            EventTriggerSubscriptionDaoImpl.COLUMN_SUBSCRIBER_UUID));
                    subBuilder.setDelaySeconds(rs.getInt(EventTriggerSubscriptionDaoImpl.COLUMN_DELAY_SECONDS));
                    subBuilder.setRepeatSeconds(rs.getInt(EventTriggerSubscriptionDaoImpl.COLUMN_REPEAT_SECONDS));
                    subBuilder.setTriggerUuid(uuid);
                    subBuilder.setSendInitialOccurrence(rs.getBoolean(
                            EventTriggerSubscriptionDaoImpl.COLUMN_SEND_INITIAL_OCCURRENCE));
                    triggerBuilder.addSubscriptions(subBuilder.build());
                }
            }

            List<EventTrigger> triggers = new ArrayList<EventTrigger>(triggersByUuid.size());
            for (EventTrigger.Builder eventTriggerBuilder : triggersByUuid.values()) {
                triggers.add(eventTriggerBuilder.build());
            }
            return triggers;
        }
    }
}
