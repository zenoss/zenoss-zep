/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;


import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventDetailItem.EventDetailType;
import org.zenoss.zep.ZepConstants;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventDetailsConfigDao;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.DatabaseType;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventDetailsConfigDaoImpl implements EventDetailsConfigDao {

    private final DataSource ds;
    private final SimpleJdbcTemplate template;
    private SimpleJdbcCall postgresqlCreateCall;
    private DatabaseCompatibility databaseCompatibility;
    private static final String COLUMN_DETAIL_ITEM_NAME = "detail_item_name";
    private static final String COLUMN_PROTO_JSON = "proto_json";

    public EventDetailsConfigDaoImpl(DataSource ds) {
        this.ds = ds;
        this.template = new SimpleJdbcTemplate(ds);
    }

    public void setDatabaseCompatibility(DatabaseCompatibility databaseCompatibility) {
        this.databaseCompatibility = databaseCompatibility;
        if (this.databaseCompatibility.getDatabaseType() == DatabaseType.POSTGRESQL) {
            this.postgresqlCreateCall = new SimpleJdbcCall(this.ds).withFunctionName("event_detail_index_config_upsert")
                    .declareParameters(new SqlParameter("p_detail_item_name", Types.VARCHAR),
                            new SqlParameter("p_proto_json", Types.VARCHAR));
        }
    }

    private void createDetailItem(String key, EventDetailType type, String name) throws ZepException {
        EventDetailItem item = EventDetailItem.newBuilder().setKey(key).setType(type).setName(name).build();
        create(item);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void init() throws ZepException {
        // Create default EventDetailItem objects in the database.
        createDetailItem(ZepConstants.DETAIL_DEVICE_PRIORITY, EventDetailType.INTEGER, "Priority");
        createDetailItem(ZepConstants.DETAIL_DEVICE_PRODUCTION_STATE, EventDetailType.INTEGER, "Production State");
        createDetailItem(ZepConstants.DETAIL_DEVICE_IP_ADDRESS, EventDetailType.IP_ADDRESS, "IP Address");
        createDetailItem(ZepConstants.DETAIL_DEVICE_LOCATION, EventDetailType.PATH, "Location");
        createDetailItem(ZepConstants.DETAIL_DEVICE_GROUPS, EventDetailType.PATH, "Groups");
        createDetailItem(ZepConstants.DETAIL_DEVICE_SYSTEMS, EventDetailType.PATH, "Systems");
        createDetailItem(ZepConstants.DETAIL_DEVICE_CLASS, EventDetailType.PATH, "Device Class");
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void create(EventDetailItem item) throws ZepException {
        final Map<String,Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_DETAIL_ITEM_NAME, item.getKey());
        fields.put(COLUMN_PROTO_JSON, DaoUtils.protobufToJson(item));
        DatabaseType dbType = this.databaseCompatibility.getDatabaseType();
        if (dbType == DatabaseType.MYSQL) {
            final String sql = "INSERT INTO event_detail_index_config (detail_item_name, proto_json) " +
                    "VALUES (:detail_item_name, :proto_json) ON DUPLICATE KEY UPDATE proto_json = VALUES(proto_json)";
            this.template.update(sql, fields);
        }
        else if (dbType == DatabaseType.POSTGRESQL) {
            this.postgresqlCreateCall.execute(fields.get(COLUMN_DETAIL_ITEM_NAME), fields.get(COLUMN_PROTO_JSON));
        }
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int delete(String eventDetailName) throws ZepException {
        final Map<String,String> fields = Collections.singletonMap(COLUMN_DETAIL_ITEM_NAME, eventDetailName);
        final String sql = "DELETE FROM event_detail_index_config WHERE detail_item_name = :detail_item_name";
        return this.template.update(sql, fields);
    }

    @Override
    @TransactionalReadOnly
    public EventDetailItem findByName(String eventDetailName) throws ZepException {
        final Map<String,String> fields = Collections.singletonMap(COLUMN_DETAIL_ITEM_NAME, eventDetailName);
        final String sql = "SELECT proto_json FROM event_detail_index_config WHERE detail_item_name=:detail_item_name";
        final List<EventDetailItem> items = this.template.query(sql, new RowMapper<EventDetailItem>() {
            @Override
            public EventDetailItem mapRow(ResultSet rs, int rowNum) throws SQLException {
                return DaoUtils.protobufFromJson(rs.getString(COLUMN_PROTO_JSON), EventDetailItem.getDefaultInstance());
            }
        }, fields);
        return (items.isEmpty()) ? null : items.get(0);
    }

    @Override
    @TransactionalReadOnly
    public Map<String, EventDetailItem> getEventDetailItemsByName() throws ZepException {
        final String sql = "SELECT proto_json FROM event_detail_index_config";
        final Map<String, EventDetailItem> itemsByName = new HashMap<String, EventDetailItem>();
        this.template.query(sql, new RowMapper<Object>() {
            @Override
            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                EventDetailItem item = DaoUtils.protobufFromJson(rs.getString(COLUMN_PROTO_JSON),
                        EventDetailItem.getDefaultInstance());
                itemsByName.put(item.getKey(), item);
                return null;
            }
        });
        return itemsByName;
    }
}
