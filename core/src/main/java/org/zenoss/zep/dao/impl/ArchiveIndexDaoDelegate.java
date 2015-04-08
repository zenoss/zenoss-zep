package org.zenoss.zep.dao.impl;

import com.google.common.collect.Lists;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcOperations;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.IndexQueueID;
import org.zenoss.zep.dao.impl.EventIndexQueueDaoImpl.PollEvents;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.TypeConverter;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ArchiveIndexDaoDelegate implements IndexDaoDelegate {
    private final SimpleJdbcOperations template;
    private final String queueTableName;
    private final String tableName;
    private final EventSummaryRowMapper rowMapper;
    private final TypeConverter<String> uuidConverter;
    private final TypeConverter<Long> timestampConverter;


    public ArchiveIndexDaoDelegate(DataSource ds, EventDaoHelper daoHelper,
                                   DatabaseCompatibility databaseCompatibility) {
        this.template = (SimpleJdbcOperations) Proxy.newProxyInstance(SimpleJdbcOperations.class.getClassLoader(),
                new Class<?>[]{SimpleJdbcOperations.class}, new SimpleJdbcTemplateProxy(ds));
        this.tableName = EventConstants.TABLE_EVENT_ARCHIVE;
        this.queueTableName = EventConstants.TABLE_EVENT_ARCHIVE + "_index_queue";
        this.uuidConverter = databaseCompatibility.getUUIDConverter();
        this.timestampConverter = databaseCompatibility.getTimestampConverter();
        this.rowMapper = new EventSummaryRowMapper(daoHelper, databaseCompatibility);
    }

    @Override
    public PollEvents pollEvents(int limit, long maxUpdateTime) {
        return new PollArchiveIndexEvents(limit, maxUpdateTime);
    }

    @Override
    @Transactional(readOnly = true)
    public long getQueueLength() {
        String sql = String.format("SELECT COUNT(*) FROM %s", queueTableName);
        return template.queryForLong(sql);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void deleteIndexQueueIds(List<IndexQueueID> queueIds) throws ZepException {
        if (queueIds.isEmpty()) {
            return;
        }
        List<Long> ids = Lists.newArrayListWithCapacity(queueIds.size());
        for (IndexQueueID id : queueIds) {
            ids.add((Long) id.id);
        }
        final String deleteSql = "DELETE FROM " + this.queueTableName + " WHERE id IN (:_iq_ids)";
        final Map<String, List<Long>> deleteFields = Collections.singletonMap("_iq_ids", ids);
        this.template.update(deleteSql, deleteFields);
    }


    @Override
    public String getQueueName() {
        return queueTableName;
    }


    private class PollArchiveIndexEvents implements PollEvents {
        private int limit;
        private long maxUpdateTime;
        private List<EventSummary> indexed;
        private Set<String> deleted;
        private List<Long> indexQueueIds;

        public PollArchiveIndexEvents(int limit, long maxUpdateTime) {
            this.limit = limit;
            this.maxUpdateTime = maxUpdateTime;
        }

        public List<EventSummary> getIndexed() {
            return indexed;
        }

        public Set<String> getDeleted() {
            return deleted;
        }

        public List<IndexQueueID> getIndexQueueIds() {
            ArrayList<IndexQueueID> result = Lists.newArrayListWithCapacity(indexQueueIds.size());
            for (Long id : indexQueueIds) {
                result.add(new IndexQueueID(id));
            }
            return result;
        }

        public PollArchiveIndexEvents invoke() {
            final Map<String, Object> selectFields = new HashMap<String, Object>();
            selectFields.put("_limit", limit);

            final String sql;

            // Used for partition pruning
            final String queryJoinLastSeen = "AND iq.last_seen=es.last_seen ";

            if (maxUpdateTime > 0L) {
                selectFields.put("_max_update_time", timestampConverter.toDatabaseType(maxUpdateTime));
                sql = "SELECT iq.id AS iq_id, iq.uuid AS iq_uuid, iq.update_time AS iq_update_time," +
                        "es.* FROM " + queueTableName + " AS iq " +
                        "LEFT JOIN " + tableName + " es ON iq.uuid=es.uuid " + queryJoinLastSeen +
                        "WHERE iq.update_time <= :_max_update_time " +
                        "ORDER BY iq_id LIMIT :_limit";
            } else {
                sql = "SELECT iq.id AS iq_id, iq.uuid AS iq_uuid, iq.update_time AS iq_update_time," +
                        "es.* FROM " + queueTableName + " AS iq " +
                        "LEFT JOIN " + tableName + " es ON iq.uuid=es.uuid " + queryJoinLastSeen +
                        "ORDER BY iq_id LIMIT :_limit";
            }

            final Set<String> eventUuids = new HashSet<String>();
            indexed = new ArrayList<EventSummary>();
            deleted = new HashSet<String>();
            indexQueueIds = template.query(sql, new RowMapper<Long>() {
                @Override
                public Long mapRow(ResultSet rs, int rowNum) throws SQLException {
                    final long iqId = rs.getLong("iq_id");
                    final String iqUuid = uuidConverter.fromDatabaseType(rs, "iq_uuid");
                    // Don't process the same event multiple times.
                    if (eventUuids.add(iqUuid)) {
                        final Object uuid = rs.getObject("uuid");
                        if (uuid != null) {
                            indexed.add(rowMapper.mapRow(rs, rowNum));
                        } else {
                            deleted.add(iqUuid);
                        }
                    }
                    return iqId;
                }
            }, selectFields);
            return this;
        }
    }
}
