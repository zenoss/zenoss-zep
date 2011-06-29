/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventDetailSet;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepConstants;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventArchiveDao;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.zenoss.zep.dao.impl.EventConstants.*;

public class EventArchiveDaoImpl implements EventArchiveDao {

    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory.getLogger(EventArchiveDaoImpl.class);

    private final SimpleJdbcTemplate template;

    private EventDaoHelper eventDaoHelper;

    private final PartitionTableConfig partitionTableConfig;

    private final RangePartitioner partitioner;

    public EventArchiveDaoImpl(DataSource dataSource, String databaseName,
            PartitionConfig partitionConfig) {
        this.template = new SimpleJdbcTemplate(dataSource);
        this.partitionTableConfig = partitionConfig
                .getConfig(TABLE_EVENT_ARCHIVE);
        this.partitioner = new RangePartitioner(template, databaseName,
                TABLE_EVENT_ARCHIVE, COLUMN_LAST_SEEN,
                partitionTableConfig.getPartitionDuration(),
                partitionTableConfig.getPartitionUnit());
    }

    public void setEventDaoHelper(EventDaoHelper eventDaoHelper) {
        this.eventDaoHelper = eventDaoHelper;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public String create(Event event, EventStatus eventStatus) throws ZepException {
        if (!ZepConstants.CLOSED_STATUSES.contains(eventStatus)) {
            throw new ZepException("Invalid status for event in event archive: " + eventStatus);
        }

        Map<String, Object> occurrenceFields = eventDaoHelper.createOccurrenceFields(event);
        Map<String, Object> fields = new HashMap<String,Object>(occurrenceFields);
        final long created = event.getCreatedTime();
        long updateTime = System.currentTimeMillis();
        final String uuid = UUID.randomUUID().toString();
        final int eventCount = 1;
        fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
        fields.put(COLUMN_STATUS_ID, eventStatus.getNumber());
        fields.put(COLUMN_FIRST_SEEN, created);
        fields.put(COLUMN_STATUS_CHANGE, created);
        fields.put(COLUMN_LAST_SEEN, created);
        fields.put(COLUMN_EVENT_COUNT, eventCount);
        fields.put(COLUMN_UPDATE_TIME, updateTime);

        this.template.update(DaoUtils.createNamedInsert(TABLE_EVENT_ARCHIVE, fields.keySet()), fields);
        return uuid;
    }

    @Override
    @TransactionalReadOnly
    public EventSummary findByUuid(String uuid) throws ZepException {
        final Map<String,byte[]> fields = Collections.singletonMap(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
        List<EventSummary> summaries = this.template.query("SELECT * FROM event_archive WHERE uuid=:uuid",
                new EventSummaryRowMapper(this.eventDaoHelper), fields);
        return (summaries.size() > 0) ? summaries.get(0) : null;
    }

    @Override
    @TransactionalReadOnly
    public List<EventSummary> findByUuids(List<String> uuids)
            throws ZepException {
        Map<String, List<byte[]>> fields = Collections.singletonMap("uuids",
                DaoUtils.uuidsToBytes(uuids));
        return this.template.query(
                "SELECT * FROM event_archive WHERE uuid IN(:uuids)",
                new EventSummaryRowMapper(this.eventDaoHelper), fields);
    }

    @Override
    @TransactionalReadOnly
    public List<EventSummary> listBatch(String startingUuid, long maxUpdateTime, int limit) throws ZepException {
        return this.eventDaoHelper.listBatch(this.template, TABLE_EVENT_ARCHIVE, startingUuid, maxUpdateTime, limit);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int dropPartitionsOlderThan(int duration, TimeUnit unit)
            throws ZepException {
        return this.partitioner.dropPartitionsOlderThan(duration, unit);
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
        return EventDaoHelper.addNote(TABLE_EVENT_ARCHIVE, uuid, note, template);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int updateDetails(String uuid, EventDetailSet details)
            throws ZepException {
        return EventDaoHelper.updateDetails(TABLE_EVENT_ARCHIVE, uuid, details.getDetailsList(), template);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void purge(int duration, TimeUnit unit) throws ZepException {
        dropPartitionsOlderThan(duration, unit);
        initializePartitions();
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
