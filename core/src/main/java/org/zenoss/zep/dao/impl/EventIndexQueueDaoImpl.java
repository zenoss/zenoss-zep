/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventIndexHandler;
import org.zenoss.zep.dao.EventIndexQueueDao;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.TypeConverter;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of EventIndexQueueDao.
 */
public class EventIndexQueueDaoImpl implements EventIndexQueueDao {
    
    private final SimpleJdbcTemplate template;
    private final String queueTableName;
    private final String tableName;
    private final EventSummaryRowMapper rowMapper;

    private final TypeConverter<String> uuidConverter;
    private final TypeConverter<Long> timestampConverter;

    private final boolean isArchive;
    
    public EventIndexQueueDaoImpl(DataSource ds, boolean isArchive, EventDaoHelper daoHelper,
                                  DatabaseCompatibility databaseCompatibility) {
        this.template = new SimpleJdbcTemplate(ds);
        this.isArchive = isArchive;
        if (isArchive) {
            this.tableName = EventConstants.TABLE_EVENT_ARCHIVE;
        }
        else {
            this.tableName = EventConstants.TABLE_EVENT_SUMMARY;
        }
        this.queueTableName = this.tableName + "_index_queue";
        this.uuidConverter = databaseCompatibility.getUUIDConverter();
        this.timestampConverter = databaseCompatibility.getTimestampConverter();
        this.rowMapper = new EventSummaryRowMapper(daoHelper, databaseCompatibility);
    }
    
    @Override
    @TransactionalRollbackAllExceptions
    public List<Long> indexEvents(final EventIndexHandler handler, final int limit) throws ZepException {
        return indexEvents(handler, limit, -1L);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public List<Long> indexEvents(final EventIndexHandler handler, final int limit,
                                  final long maxUpdateTime) throws ZepException {
        final Map<String,Object> selectFields = new HashMap<String,Object>();
        selectFields.put("_limit", limit);
        
        final String sql;

        // Used for partition pruning
        final String queryJoinLastSeen = (this.isArchive) ? "AND iq.last_seen=es.last_seen " : "";

        if (maxUpdateTime > 0L) {
            selectFields.put("_max_update_time", timestampConverter.toDatabaseType(maxUpdateTime));
            sql = "SELECT iq.id AS iq_id, iq.uuid AS iq_uuid, iq.update_time AS iq_update_time," + 
                "es.* FROM " + this.queueTableName + " AS iq " + 
                "LEFT JOIN " + this.tableName + " es ON iq.uuid=es.uuid " + queryJoinLastSeen +
                "WHERE iq.update_time <= :_max_update_time " +
                "ORDER BY iq_id LIMIT :_limit";
        }
        else {
            sql = "SELECT iq.id AS iq_id, iq.uuid AS iq_uuid, iq.update_time AS iq_update_time," + 
                "es.* FROM " + this.queueTableName + " AS iq " + 
                "LEFT JOIN " + this.tableName + " es ON iq.uuid=es.uuid " + queryJoinLastSeen +
                "ORDER BY iq_id LIMIT :_limit";
        }

        final Set<String> eventUuids = new HashSet<String>();
        final List<Long> indexQueueIds = this.template.query(sql, new RowMapper<Long>() {
            @Override
            public Long mapRow(ResultSet rs, int rowNum) throws SQLException {
                final long iqId = rs.getLong("iq_id");
                final String iqUuid = uuidConverter.fromDatabaseType(rs, "iq_uuid");
                // Don't process the same event multiple times.
                if (eventUuids.add(iqUuid)) {
                    final Object uuid = rs.getObject("uuid");
                    if (uuid != null) {
                        EventSummary summary = rowMapper.mapRow(rs, rowNum);
                        try {
                            handler.handle(summary);
                        } catch (Exception e) {
                            throw new RuntimeException(e.getLocalizedMessage(), e);
                        }
                    }
                    else {
                        try {
                            handler.handleDeleted(iqUuid);
                        } catch (Exception e) {
                            throw new RuntimeException(e.getLocalizedMessage(), e);
                        }
                    }
                }
                return iqId;
            }
        }, selectFields);
        
        if (!indexQueueIds.isEmpty()) {
            try {
                handler.handleComplete();
            } catch (Exception e) {
                throw new ZepException(e.getLocalizedMessage(), e);
            }
        }
        return indexQueueIds;
    }

    @Override
    @Transactional(readOnly = true)
    public long getQueueLength() {
        String sql = String.format("SELECT COUNT(*) FROM %s", queueTableName);
        return template.queryForLong(sql);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int deleteIndexQueueIds(List<Long> queueIds) throws ZepException {
        if (queueIds.isEmpty()) {
            return 0;
        }
        final String deleteSql = "DELETE FROM " + this.queueTableName + " WHERE id IN (:_iq_ids)";
        final Map<String,List<Long>> deleteFields = Collections.singletonMap("_iq_ids", queueIds);
        return this.template.update(deleteSql, deleteFields);
    }
}
