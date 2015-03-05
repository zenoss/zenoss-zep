/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import com.codahale.metrics.annotation.Timed;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcOperations;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventIndexHandler;
import org.zenoss.zep.dao.EventIndexQueueDao;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.TypeConverter;
import org.zenoss.zep.events.EventIndexQueueSizeEvent;

import java.lang.reflect.Proxy;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Gauge;

import javax.annotation.Resource;

/**
 * Implementation of EventIndexQueueDao.
 */
public class EventIndexQueueDaoImpl implements EventIndexQueueDao, ApplicationEventPublisherAware {
    
    private final SimpleJdbcOperations template;
    private final String queueTableName;
    private final String tableName;
    private final EventSummaryRowMapper rowMapper;
    private ApplicationEventPublisher applicationEventPublisher;

    private final TypeConverter<String> uuidConverter;
    private final TypeConverter<Long> timestampConverter;

    private final boolean isArchive;

    private MetricRegistry metrics;
    private int lastQueueSize = -1;

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    public EventIndexQueueDaoImpl(DataSource ds, boolean isArchive, EventDaoHelper daoHelper,
                                  DatabaseCompatibility databaseCompatibility) {
        this.template = (SimpleJdbcOperations) Proxy.newProxyInstance(SimpleJdbcOperations.class.getClassLoader(),
                new Class<?>[] {SimpleJdbcOperations.class}, new SimpleJdbcTemplateProxy(ds));
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

    @Resource(name="metrics")
    public void setBean( MetricRegistry metrics ) {
        this.metrics = metrics;
        String metricName = "";
        if(this.isArchive) {
            metricName = MetricRegistry.name(this.getClass().getCanonicalName(), "archiveIndexQueueSize");
            }
        else {
            metricName = MetricRegistry.name(this.getClass().getCanonicalName(), "summaryIndexQueueSize");
            }
        this.metrics.register(metricName, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return lastQueueSize;
                }
            });
        }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
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
        final List<EventSummary> indexed = new ArrayList<EventSummary>();
        final List<String> deleted = new ArrayList<String>();
        final List<Long> indexQueueIds;
        try {
            indexQueueIds = metrics.timer(getClass().getName() + ".indexEventsQuery").time(new Callable<List<Long>>() {
                @Override
                public List<Long> call() throws Exception {
                    return template.query(sql, new RowMapper<Long>() {
                        @Override
                        public Long mapRow(ResultSet rs, int rowNum) throws SQLException {
                            final long iqId = rs.getLong("iq_id");
                            final String iqUuid = uuidConverter.fromDatabaseType(rs, "iq_uuid");
                            // Don't process the same event multiple times.
                            if (eventUuids.add(iqUuid)) {
                                final Object uuid = rs.getObject("uuid");
                                if (uuid != null) {
                                    indexed.add(rowMapper.mapRow(rs, rowNum));
                                }
                                else {
                                    deleted.add(iqUuid);
                                }
                            }
                            return iqId;
                        }
                    }, selectFields);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            metrics.timer(getClass().getName() + ".indexEvents-handle").time(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    for (final EventSummary summary : indexed) {
                        metrics.timer(getClass().getName() + ".indexEvents-handleOne").time(new Callable<Object>() {
                            @Override
                            public Object call() throws Exception {
                                handler.handle(summary);
                                return null;
                            }
                        });
                    }
                    return null;
                }
            });
            metrics.timer(getClass().getName() + ".indexEvents-handleDelete").time(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    for (final String iqUuid : deleted) {
                        metrics.timer(getClass().getName() + ".indexEvents-handleDeletedOne").time(new Callable<Object>() {
                            @Override
                            public Object call() throws Exception {
                                handler.handleDeleted(iqUuid);
                                return null;
                            }
                        });
                    }
                    return null;
                }
            });

            if (!indexQueueIds.isEmpty()) {
                metrics.timer(getClass().getName() + ".indexEvents-handleComplete").time(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        handler.handleComplete();
                        return null;
                    }
                });
            }

            // publish current size of event_*_index_queue table
            metrics.timer(getClass().getName() + ".indexEvents-count").time(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    lastQueueSize = template.queryForInt("SELECT COUNT(1) FROM " + queueTableName);
                    applicationEventPublisher.publishEvent(
                            new EventIndexQueueSizeEvent(this, tableName, lastQueueSize, limit));
                    return null;
                }
            });
        } catch (Exception e) {
            throw new ZepException(e.getLocalizedMessage(), e);
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
