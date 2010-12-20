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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventDao;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.zenoss.zep.dao.impl.EventConstants.*;

public class EventDaoImpl implements EventDao {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory
            .getLogger(EventDaoImpl.class);

    private final SimpleJdbcTemplate template;
    private EventDaoHelper eventDaoHelper;
    private final RangePartitioner partitioner;
    private final PartitionTableConfig partitionTableConfig;

    public EventDaoImpl(DataSource dataSource, String databaseName,
            PartitionConfig partitionConfig) {
        this.template = new SimpleJdbcTemplate(dataSource);
        this.partitionTableConfig = partitionConfig.getConfig(TABLE_EVENT);
        this.partitioner = new RangePartitioner(template, databaseName,
                TABLE_EVENT, COLUMN_CREATED,
                partitionTableConfig.getPartitionDuration(),
                partitionTableConfig.getPartitionUnit());
    }

    public void setEventDaoHelper(EventDaoHelper eventDaoHelper) {
        this.eventDaoHelper = eventDaoHelper;
    }

    @Override
    @Transactional
    public String create(Event event) throws ZepException {
        Map<String, Object> fields = eventDaoHelper.insert(event);
        return DaoUtils.uuidFromBytes((byte[]) fields.get(COLUMN_UUID));
    }

    @Override
    @Transactional
    public int delete(String uuid) throws ZepException {
        return this.template.update("DELETE FROM event WHERE uuid=?",
                DaoUtils.uuidToBytes(uuid));
    }

    @Override
    @Transactional(readOnly = true)
    public Event findByUuid(String uuid) throws ZepException {
        List<Event> events = template.query("SELECT * FROM event WHERE uuid=?",
                new EventRowMapper(this.eventDaoHelper),
                DaoUtils.uuidToBytes(uuid));
        return (events.size() > 0) ? events.get(0) : null;
    }

    @Override
    public void purge(int duration, TimeUnit unit) throws ZepException {
        dropPartitionsOlderThan(duration, unit);
        initializePartitions();
    }

    private static class EventRowMapper implements RowMapper<Event> {
        private final EventDaoHelper helper;
        private Set<String> fields = null;

        public EventRowMapper(EventDaoHelper helper) {
            this.helper = helper;
        }

        @Override
        public Event mapRow(ResultSet rs, int rowNum) throws SQLException {
            if (fields == null) {
                fields = DaoUtils.getFieldsInResultSet(rs.getMetaData());
            }
            return this.helper.eventMapper(rs, false, fields);
        }
    }

    @Override
    public void initializePartitions() throws ZepException {
        this.partitioner.createPartitions(
                this.partitionTableConfig.getInitialPastPartitions(),
                this.partitionTableConfig.getFuturePartitions());
    }

    @Override
    public int dropPartitionsOlderThan(int duration, TimeUnit unit)
            throws ZepException {
        return this.partitioner.dropPartitionsOlderThan(duration, unit);
    }

    @Override
    public long getPartitionIntervalInMs() {
        return this.partitionTableConfig.getPartitionUnit().toMillis(
                this.partitionTableConfig.getPartitionDuration());
    }

}
