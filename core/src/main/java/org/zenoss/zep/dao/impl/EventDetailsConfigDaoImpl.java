/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;


import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcOperations;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventDetailItem.EventDetailType;
import org.zenoss.zep.ZepConstants;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventDetailsConfigDao;
import org.zenoss.zep.dao.impl.compat.NestedTransactionService;

import java.lang.reflect.Proxy;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventDetailsConfigDaoImpl implements EventDetailsConfigDao {

    private final SimpleJdbcOperations template;
    private NestedTransactionService nestedTransactionService;
    private static final String COLUMN_DETAIL_ITEM_NAME = "detail_item_name";
    private static final String COLUMN_PROTO_JSON = "proto_json";

    public EventDetailsConfigDaoImpl(DataSource ds) {
        this.template = (SimpleJdbcOperations) Proxy.newProxyInstance(SimpleJdbcOperations.class.getClassLoader(), 
                new Class<?>[] {SimpleJdbcOperations.class}, new SimpleJdbcTemplateProxy(ds));
    }

    public void setNestedTransactionService(NestedTransactionService nestedTransactionService) {
        this.nestedTransactionService = nestedTransactionService;
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
        final Map<String,Object> fields = new HashMap<String,Object>(2);
        fields.put(COLUMN_DETAIL_ITEM_NAME, item.getKey());
        fields.put(COLUMN_PROTO_JSON, DaoUtils.protobufToJson(item));

        final String insertSql = "INSERT INTO event_detail_index_config (detail_item_name, proto_json) " +
                    "VALUES (:detail_item_name, :proto_json)";
        final String updateSql = "UPDATE event_detail_index_config SET proto_json=:proto_json" +
                " WHERE detail_item_name=:detail_item_name";
        DaoUtils.updateOrInsert(nestedTransactionService, template, insertSql, updateSql, fields);
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
