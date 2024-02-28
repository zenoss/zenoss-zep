/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, 2014 all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import com.codahale.metrics.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventDetailSet;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.utils.dao.RangePartitioner;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepConstants;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventBatch;
import org.zenoss.zep.dao.EventBatchParams;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.TypeConverter;
import org.zenoss.zep.dao.impl.compat.TypeConverterUtils;
import org.zenoss.zep.plugins.EventPreCreateContext;

import java.lang.reflect.Proxy;
import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.zenoss.zep.dao.impl.EventConstants.*;

public class EventArchiveDaoImpl implements EventArchiveDao {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(EventArchiveDaoImpl.class);

    @Autowired
    private MetricRegistry metricRegistry;

    private final NamedParameterJdbcOperations template;

    private EventDaoHelper eventDaoHelper;

    private UUIDGenerator uuidGenerator;

    private final DatabaseCompatibility databaseCompatibility;

    private final TypeConverter<String> uuidConverter;

    private final PartitionTableConfig partitionTableConfig;

    private final RangePartitioner partitioner;

    public EventArchiveDaoImpl(DataSource dataSource, PartitionConfig partitionConfig,
                               DatabaseCompatibility databaseCompatibility) {
    	this.template = (NamedParameterJdbcOperations) Proxy.newProxyInstance(NamedParameterJdbcOperations.class.getClassLoader(),
    			new Class<?>[] {NamedParameterJdbcOperations.class}, new JdbcTemplateProxy(dataSource));
        this.partitionTableConfig = partitionConfig
                .getConfig(TABLE_EVENT_ARCHIVE);
        this.databaseCompatibility = databaseCompatibility;
        this.uuidConverter = databaseCompatibility.getUUIDConverter();
        this.partitioner = databaseCompatibility.getRangePartitioner(dataSource,
                TABLE_EVENT_ARCHIVE, COLUMN_LAST_SEEN,
                partitionTableConfig.getPartitionDuration(),
                partitionTableConfig.getPartitionUnit());
    }

    public void setEventDaoHelper(EventDaoHelper eventDaoHelper) {
        this.eventDaoHelper = eventDaoHelper;
    }

    public void setUuidGenerator(UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    public void setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public String create(Event event, EventPreCreateContext context) throws ZepException {
        try (Timer.Context ignored = metricRegistry.timer("EventArchive.create").time()){
            if (!ZepConstants.CLOSED_STATUSES.contains(event.getStatus())) {
                throw new ZepException("Invalid status for event in event archive: " + event.getStatus());
            }

            TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
            Map<String, Object> occurrenceFields = eventDaoHelper.createOccurrenceFields(event);
            Map<String, Object> fields = new HashMap<String,Object>(occurrenceFields);
            final long created = event.getCreatedTime();
            final long firstSeen = (event.hasFirstSeenTime()) ? event.getFirstSeenTime() : created;
            long updateTime = System.currentTimeMillis();
            final String uuid = this.uuidGenerator.generate().toString();
            fields.put(COLUMN_UUID, uuidConverter.toDatabaseType(uuid));
            fields.put(COLUMN_STATUS_ID, event.getStatus().getNumber());
            fields.put(COLUMN_FIRST_SEEN, timestampConverter.toDatabaseType(firstSeen));
            fields.put(COLUMN_STATUS_CHANGE, timestampConverter.toDatabaseType(created));
            fields.put(COLUMN_LAST_SEEN, timestampConverter.toDatabaseType(created));
            fields.put(COLUMN_EVENT_COUNT, event.getCount());
            fields.put(COLUMN_UPDATE_TIME, timestampConverter.toDatabaseType(updateTime));

            this.template.update(DaoUtils.createNamedInsert(TABLE_EVENT_ARCHIVE, fields.keySet()), fields);
            return uuid;
        }
    }

    @Override
    @TransactionalReadOnly
    public EventSummary findByUuid(String uuid) throws ZepException {
        try (Timer.Context ignored = metricRegistry.timer("EventArchive.findByUuid").time()) {
            final Map<String,Object> fields = Collections.singletonMap(COLUMN_UUID, uuidConverter.toDatabaseType(uuid));
            List<EventSummary> summaries = this.template.query("SELECT * FROM event_archive WHERE uuid=:uuid", fields,
                    new EventArchiveRowMapper(this.eventDaoHelper, databaseCompatibility));
            return (summaries.size() > 0) ? summaries.get(0) : null;
        }
    }

    @Override
    @TransactionalReadOnly
    @Deprecated
    /** @deprecated use {@link #findByKey(Collection) instead}. */
    public List<EventSummary> findByUuids(List<String> uuids)
            throws ZepException {
        try (Timer.Context ignored = metricRegistry.timer("EventArchive.findByUuids").time()) {
            Map<String, List<Object>> fields = Collections.singletonMap("uuids",
                    TypeConverterUtils.batchToDatabaseType(uuidConverter, uuids));
            return this.template.query(
                    "SELECT * FROM event_archive WHERE uuid IN(:uuids)", fields,
                    new EventArchiveRowMapper(this.eventDaoHelper, databaseCompatibility));
        }
    }

    @Override
    @TransactionalReadOnly
    public List<EventSummary> findByKey(Collection<EventSummary> toLookup) throws ZepException {
        try (Timer.Context ignored = metricRegistry.timer("EventArchive.findByKey").time()) {
            Map<String, Object> fields = new HashMap<>(toLookup.size() * 2);
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT * FROM event_archive WHERE (");
            int i = 0;
            for (EventSummary event : toLookup) {
                if (i++ > 0)
                    sql.append(" OR ");
                sql.append(String.format("(uuid = :uuid_%d AND last_seen = :last_seen_%d)", i, i));
                fields.put(String.format("uuid_%d", i), uuidConverter.toDatabaseType(event.getUuid()));
                fields.put(String.format("last_seen_%d", i), event.getLastSeenTime());
            }
            sql.append(")");
            return this.template.query(sql.toString(), fields,
                    new EventArchiveRowMapper(this.eventDaoHelper, databaseCompatibility));
        }
    }

    @Override
    @TransactionalReadOnly
    public EventBatch listBatch(EventBatchParams batchParams, long maxUpdateTime, int limit) throws ZepException {
        try (Timer.Context ignored = metricRegistry.timer("EventArchive.listBatch").time()) {
            return this.eventDaoHelper.listBatch(this.template, TABLE_EVENT_ARCHIVE, this.partitioner, batchParams, maxUpdateTime, limit,
                    new EventArchiveRowMapper(eventDaoHelper, databaseCompatibility));
        }
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void initializePartitions() throws ZepException {
        try (Timer.Context ignored = metricRegistry.timer("EventArchive.initializePartitions").time()) {
            this.partitioner.createPartitions(
                    this.partitionTableConfig.getInitialPastPartitions(),
                    this.partitionTableConfig.getFuturePartitions());
        }
    }

    @Override
    public long getPartitionIntervalInMs() {
        try (Timer.Context ignored = metricRegistry.timer("EventArchive.getPartitionIntervalInMs").time()) {
            return this.partitionTableConfig.getPartitionUnit().toMillis(
                    this.partitionTableConfig.getPartitionDuration());
        }
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int addNote(String uuid, EventNote note) throws ZepException {
        try (Timer.Context ignored = metricRegistry.timer("EventArchive.addNote").time()) {
            return this.eventDaoHelper.addNote(TABLE_EVENT_ARCHIVE, uuid, note, template);
        }
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int updateDetails(String uuid, EventDetailSet details)
            throws ZepException {
        try (Timer.Context ignored = metricRegistry.timer("EventArchive.updateDetails").time()) {
            return this.eventDaoHelper.updateDetails(TABLE_EVENT_ARCHIVE, uuid, details.getDetailsList(), template);
        }
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void purge(int duration, TimeUnit unit) throws ZepException {
        try (Timer.Context ignored = metricRegistry.timer("EventArchive.purge").time()) {
            this.partitioner.pruneAndCreatePartitions(duration,
                    unit,
                    this.partitionTableConfig.getInitialPastPartitions(),
                    this.partitionTableConfig.getFuturePartitions());
        }
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void importEvent(EventSummary eventSummary) throws ZepException {
        try (Timer.Context ignored = metricRegistry.timer("EventArchive.importEvent").time()) {
            if (!ZepConstants.CLOSED_STATUSES.contains(eventSummary.getStatus())) {
                throw new ZepException("Invalid status for event in event archive: " + eventSummary.getStatus());
            }

            final long updateTime = System.currentTimeMillis();
            final EventSummary.Builder summaryBuilder = EventSummary.newBuilder(eventSummary);
            final Event.Builder eventBuilder = summaryBuilder.getOccurrenceBuilder(0);
            summaryBuilder.setUpdateTime(updateTime);
            EventDaoHelper.addMigrateUpdateTimeDetail(eventBuilder, updateTime);

            final EventSummary summary = summaryBuilder.build();
            final Map<String,Object> fields = this.eventDaoHelper.createImportedSummaryFields(summary);
            this.template.update(DaoUtils.createNamedInsert(TABLE_EVENT_ARCHIVE, fields.keySet()), fields);
        }
    }
}
