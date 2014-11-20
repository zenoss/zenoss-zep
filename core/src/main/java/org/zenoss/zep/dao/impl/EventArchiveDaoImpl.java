/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, 2014 all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.SimpleJdbcOperations;
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
    private static Logger logger = LoggerFactory.getLogger(EventArchiveDaoImpl.class);

    private final SimpleJdbcOperations template;

    private EventDaoHelper eventDaoHelper;

    private UUIDGenerator uuidGenerator;

    private final DatabaseCompatibility databaseCompatibility;

    private final TypeConverter<String> uuidConverter;

    private final PartitionTableConfig partitionTableConfig;

    private final RangePartitioner partitioner;

    public EventArchiveDaoImpl(DataSource dataSource, PartitionConfig partitionConfig,
                               DatabaseCompatibility databaseCompatibility) {
    	this.template = (SimpleJdbcOperations) Proxy.newProxyInstance(SimpleJdbcOperations.class.getClassLoader(), 
    			new Class<?>[] {SimpleJdbcOperations.class}, new SimpleJdbcTemplateProxy(dataSource));
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

    @Override
    @TransactionalRollbackAllExceptions
    public String create(Event event, EventPreCreateContext context) throws ZepException {
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

    @Override
    @TransactionalReadOnly
    public EventSummary findByUuid(String uuid) throws ZepException {
        final Map<String,Object> fields = Collections.singletonMap(COLUMN_UUID, uuidConverter.toDatabaseType(uuid));
        List<EventSummary> summaries = this.template.query("SELECT * FROM event_archive WHERE uuid=:uuid",
                new EventArchiveRowMapper(this.eventDaoHelper, databaseCompatibility), fields);
        return (summaries.size() > 0) ? summaries.get(0) : null;
    }

    @Override
    @TransactionalReadOnly
    @Deprecated
    /** @deprecated use {@link #findByKey(Collection) instead}. */
    public List<EventSummary> findByUuids(List<String> uuids)
            throws ZepException {
        Map<String, List<Object>> fields = Collections.singletonMap("uuids",
                TypeConverterUtils.batchToDatabaseType(uuidConverter, uuids));
        return this.template.query(
                "SELECT * FROM event_archive WHERE uuid IN(:uuids)",
                new EventArchiveRowMapper(this.eventDaoHelper, databaseCompatibility), fields);
    }

    @Override
    @TransactionalReadOnly
    public List<EventSummary> findByKey(Collection<EventSummary> toLookup) throws ZepException {
        ArrayList<Object> fields = new ArrayList<Object>(toLookup.size() * 2);
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM event_archive WHERE (");
        int i = 0;
        for (EventSummary event : toLookup) {
            if (i++ > 0)
                sql.append(" OR ");
            sql.append("(uuid = ? AND last_seen = ?)");
            fields.add(uuidConverter.toDatabaseType(event.getUuid()));
            fields.add(event.getLastSeenTime());
        }
        sql.append(")");
        return this.template.query(sql.toString(),
                new EventArchiveRowMapper(this.eventDaoHelper, databaseCompatibility), fields.toArray());
    }

    @Override
    @TransactionalReadOnly
    public EventBatch listBatch(EventBatchParams batchParams, long maxUpdateTime, int limit) throws ZepException {
        return this.eventDaoHelper.listBatch(this.template, TABLE_EVENT_ARCHIVE, this.partitioner, batchParams, maxUpdateTime, limit,
                new EventArchiveRowMapper(eventDaoHelper, databaseCompatibility));
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void initializePartitions() throws ZepException {
        this.partitioner.createPartitions(
                this.partitionTableConfig.getInitialPastPartitions(),
                this.partitionTableConfig.getFuturePartitions());
    }

    @Override
    public long getPartitionIntervalInMs() {
        return this.partitionTableConfig.getPartitionUnit().toMillis(
                this.partitionTableConfig.getPartitionDuration());
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int addNote(String uuid, EventNote note) throws ZepException {
        return this.eventDaoHelper.addNote(TABLE_EVENT_ARCHIVE, uuid, note, template);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int updateDetails(String uuid, EventDetailSet details)
            throws ZepException {
        return this.eventDaoHelper.updateDetails(TABLE_EVENT_ARCHIVE, uuid, details.getDetailsList(), template);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void purge(int duration, TimeUnit unit) throws ZepException {
        this.partitioner.pruneAndCreatePartitions(duration,
                unit,
                this.partitionTableConfig.getInitialPastPartitions(),
                this.partitionTableConfig.getFuturePartitions());
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void importEvent(EventSummary eventSummary) throws ZepException {
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
