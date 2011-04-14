/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import static org.zenoss.zep.dao.impl.EventConstants.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventDetailSet;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventArchiveDao;

public class EventArchiveDaoImpl implements EventArchiveDao {

    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory
            .getLogger(EventArchiveDaoImpl.class);

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
    @Transactional
    public String create(Event event) throws ZepException {
        Map<String, Object> occurrenceFields = eventDaoHelper.createOccurrenceFields(event);
        Map<String, Object> fields = new HashMap<String,Object>(occurrenceFields);
        long created = (Long) fields.remove(COLUMN_CREATED);
        long updateTime = System.currentTimeMillis();
        final String uuid = UUID.randomUUID().toString();
        final EventStatus status = EventStatus.STATUS_CLOSED;
        final int eventCount = 1;
        fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
        fields.put(COLUMN_STATUS_ID, status.getNumber());
        fields.put(COLUMN_FIRST_SEEN, created);
        fields.put(COLUMN_STATUS_CHANGE, created);
        fields.put(COLUMN_LAST_SEEN, created);
        fields.put(COLUMN_EVENT_COUNT, eventCount);
        fields.put(COLUMN_UPDATE_TIME, updateTime);
        fields.put(COLUMN_INDEXED, 0);
        if (event.getSeverity() != EventSeverity.SEVERITY_CLEAR) {
            fields.put(COLUMN_CLEAR_FINGERPRINT_HASH,
                    EventDaoUtils.createClearHash(event));
        }

        this.template.update(DaoUtils.createNamedInsert(TABLE_EVENT_ARCHIVE, fields.keySet()), fields);

        /* Create occurrence */
        occurrenceFields.put(COLUMN_SUMMARY_UUID, fields.get(COLUMN_UUID));
        this.eventDaoHelper.insert(occurrenceFields);
        return uuid;
    }

    @Override
    @Transactional
    public int delete(String uuid) throws ZepException {
        final Map<String,byte[]> fields = Collections.singletonMap(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
        return this.template.update("DELETE FROM event_archive WHERE uuid=:uuid", fields);
    }

    @Override
    @Transactional(readOnly = true)
    public EventSummary findByUuid(String uuid) throws ZepException {
        final Map<String,byte[]> fields = Collections.singletonMap(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
        List<EventSummary> summaries = this.template.query("SELECT * FROM event_archive WHERE uuid=:uuid",
                new EventSummaryRowMapper(this.eventDaoHelper), fields);
        return (summaries.size() > 0) ? summaries.get(0) : null;
    }

    @Override
    @Transactional(readOnly = true)
    public List<EventSummary> findByUuids(List<String> uuids)
            throws ZepException {
        Map<String, List<byte[]>> fields = Collections.singletonMap("uuids",
                DaoUtils.uuidsToBytes(uuids));
        return this.template.query(
                "SELECT * FROM event_archive WHERE uuid IN(:uuids)",
                new EventSummaryRowMapper(this.eventDaoHelper), fields);
    }

    @Override
    @Transactional
    public int dropPartitionsOlderThan(int duration, TimeUnit unit)
            throws ZepException {
        return this.partitioner.dropPartitionsOlderThan(duration, unit);
    }

    @Override
    @Transactional
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
    @Transactional
    public int addNote(String uuid, EventNote note) throws ZepException {
        return EventDaoHelper
                .addNote(TABLE_EVENT_ARCHIVE, uuid, note, template);
    }

    @Override
    @Transactional
    public int updateDetails(String uuid, EventDetailSet details)
            throws ZepException {
        return EventDaoHelper.updateDetails(TABLE_EVENT_ARCHIVE, uuid, details.getDetailsList(), template);
    }

    @Override
    @Transactional
    public void purge(int duration, TimeUnit unit) throws ZepException {
        dropPartitionsOlderThan(duration, unit);
        initializePartitions();
    }
}
