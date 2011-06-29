/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventIndexHandler;
import org.zenoss.zep.dao.EventIndexQueueDao;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of EventIndexQueueDao.
 */
public class EventIndexQueueDaoImpl implements EventIndexQueueDao {
    
    private final SimpleJdbcTemplate template;
    private final String queueTableName;
    private final String tableName;
    private final EventSummaryRowMapper rowMapper;

    private final boolean isArchive;
    
    public EventIndexQueueDaoImpl(DataSource ds, boolean isArchive, EventDaoHelper daoHelper) {
        this.template = new SimpleJdbcTemplate(ds);
        this.isArchive = isArchive;
        if (isArchive) {
            this.tableName = EventConstants.TABLE_EVENT_ARCHIVE;
        }
        else {
            this.tableName = EventConstants.TABLE_EVENT_SUMMARY;
        }
        this.queueTableName = this.tableName + "_index_queue";
        this.rowMapper = new EventSummaryRowMapper(daoHelper);
    }
    
    @Override
    @TransactionalRollbackAllExceptions
    public int indexEvents(final EventIndexHandler handler, final int limit) throws ZepException {
        return indexEvents(handler, limit, -1L);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int indexEvents(final EventIndexHandler handler, final int limit,
                           final long maxUpdateTime) throws ZepException {
        final Map<String,Object> selectFields = new HashMap<String,Object>();
        selectFields.put("_limit", limit);
        
        final String sql;

        // Used for partition pruning
        final String queryJoinLastSeen = (this.isArchive) ? "AND iq.last_seen=es.last_seen " : "";

        if (maxUpdateTime > 0L) {
            selectFields.put("_max_update_time", maxUpdateTime);
            sql = "SELECT iq.id AS iq_id, iq.uuid AS iq_uuid, iq.update_time AS iq_update_time," + 
                "es.* FROM " + this.queueTableName + " AS iq " + 
                "LEFT JOIN " + this.tableName + " es ON iq.uuid=es.uuid " + queryJoinLastSeen +
                "WHERE iq.update_time <= :_max_update_time " +
                "ORDER BY iq_update_time LIMIT :_limit";
        }
        else {
            sql = "SELECT iq.id AS iq_id, iq.uuid AS iq_uuid, iq.update_time AS iq_update_time," + 
                "es.* FROM " + this.queueTableName + " AS iq " + 
                "LEFT JOIN " + this.tableName + " es ON iq.uuid=es.uuid " + queryJoinLastSeen +
                "ORDER BY iq_update_time LIMIT :_limit";
        }

        final int[] numIndexed = new int[1];
        final List<Integer> indexQueueIds = this.template.query(sql, new RowMapper<Integer>() {
            @Override
            public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
                final byte[] uuid = rs.getBytes("uuid");
                if (uuid != null) {
                    EventSummary summary = rowMapper.mapRow(rs, rowNum);
                    try {
                        handler.handle(summary);
                    } catch (Exception e) {
                        throw new SQLException(e.getLocalizedMessage(), e);
                    }
                }
                else {
                    try {
                        handler.handleDeleted(DaoUtils.uuidFromBytes(rs.getBytes("iq_uuid")));
                    } catch (Exception e) {
                        throw new SQLException(e.getLocalizedMessage(), e);
                    }
                }
                ++numIndexed[0];
                return rs.getInt("iq_id");
            }
        }, selectFields);
        
        if (!indexQueueIds.isEmpty()) {
            try {
                handler.handleComplete();
            } catch (Exception e) {
                throw new ZepException(e.getLocalizedMessage(), e);
            }
            
            final String deleteSql = "DELETE FROM " + this.queueTableName + " WHERE id IN (:_iq_ids)";
            final Map<String,List<Integer>> deleteFields = Collections.singletonMap("_iq_ids", indexQueueIds);
            this.template.update(deleteSql, deleteFields);
        }
        return numIndexed[0];
    }
}
