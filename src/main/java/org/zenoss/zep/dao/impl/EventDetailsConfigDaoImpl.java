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
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventDetailItem.EventDetailType;
import org.zenoss.zep.ZepConstants;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventDetailsConfigDao;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventDetailsConfigDaoImpl implements EventDetailsConfigDao {

    private final SimpleJdbcTemplate template;
    private static final String COLUMN_DETAIL_ITEM_NAME = "detail_item_name";
    private static final String COLUMN_PROTO_JSON = "proto_json";
    private volatile Map<String,EventDetailItem> initialItems = null;

    public EventDetailsConfigDaoImpl(DataSource ds) {
        this.template = new SimpleJdbcTemplate(ds);
    }

    private void createDetailItem(String key, EventDetailType type, String name) throws ZepException {
        EventDetailItem item = EventDetailItem.newBuilder().setKey(key).setType(type).setName(name).build();
        create(item);
    }

    public void init() throws ZepException {
        // Create default EventDetailItem objects in the database.
        createDetailItem(ZepConstants.DETAIL_DEVICE_PRIORITY, EventDetailType.INTEGER, "Priority");
        createDetailItem(ZepConstants.DETAIL_DEVICE_PRODUCTION_STATE, EventDetailType.INTEGER, "Production State");
        this.initialItems = getEventDetailItemsByName();
    }

    @Override
    @Transactional
    public void create(EventDetailItem item) throws ZepException {
        final Map<String,Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_DETAIL_ITEM_NAME, item.getKey());
        fields.put(COLUMN_PROTO_JSON, DaoUtils.protobufToJson(item));
        final String sql = "INSERT INTO event_detail_index_config (detail_item_name, proto_json) " +
                "VALUES (:detail_item_name, :proto_json) ON DUPLICATE KEY UPDATE proto_json = VALUES(proto_json)";
        this.template.update(sql, fields);
    }

    @Override
    @Transactional
    public int delete(String eventDetailName) throws ZepException {
        final Map<String,String> fields = Collections.singletonMap(COLUMN_DETAIL_ITEM_NAME, eventDetailName);
        final String sql = "DELETE FROM event_detail_index_config WHERE detail_item_name = :detail_item_name";
        return this.template.update(sql, fields);
    }

    @Override
    @Transactional(readOnly = true)
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
    @Transactional(readOnly = true)
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

    @Override
    public Map<String, EventDetailItem> getInitialEventDetailItemsByName() throws ZepException {
        return Collections.unmodifiableMap(this.initialItems);
    }
}
