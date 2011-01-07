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

import static org.zenoss.zep.dao.impl.EventConstants.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.modelevents.Modelevents.ModelEvent;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSummaryDao;

public class EventSummaryDaoImpl implements EventSummaryDao {

    private static Logger logger = LoggerFactory
            .getLogger(EventSummaryDaoImpl.class);

    private final SimpleJdbcTemplate template;

    private EventDaoHelper eventDaoHelper;

    public EventSummaryDaoImpl(DataSource dataSource) {
        this.template = new SimpleJdbcTemplate(dataSource);
    }

    public void setEventDaoHelper(EventDaoHelper eventDaoHelper) {
        this.eventDaoHelper = eventDaoHelper;
    }

    @Override
    @Transactional
    public String create(Event event, EventStatus status) throws ZepException {
        Map<String, Object> fields = eventDaoHelper.insert(event);
        long created = (Long) fields.remove(COLUMN_CREATED);
        fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(UUID.randomUUID()));
        fields.put(COLUMN_STATUS_ID, status.getNumber());
        fields.put(COLUMN_FIRST_SEEN, created);
        fields.put(COLUMN_STATUS_CHANGE, created);
        fields.put(COLUMN_LAST_SEEN, created);
        fields.put(COLUMN_EVENT_COUNT, 1);
        fields.put(COLUMN_UPDATE_TIME, System.currentTimeMillis());
        if (event.getSeverity() != EventSeverity.SEVERITY_CLEAR) {
            fields.put(COLUMN_CLEAR_FINGERPRINT_HASH,
                    EventDaoUtils.createClearHash(event));
        }

        StringBuilder names = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (String columnName : fields.keySet()) {
            if (names.length() > 0) {
                names.append(',');
                values.append(',');
            }
            names.append(columnName);
            values.append(':').append(columnName);
        }

        fields.put("_acknowledged", EventStatus.STATUS_ACKNOWLEDGED.getNumber());
        fields.put("_new", EventStatus.STATUS_NEW.getNumber());
        fields.put("_suppressed", EventStatus.STATUS_SUPPRESSED.getNumber());
        fields.put("_cleared", EventStatus.STATUS_CLEARED.getNumber());

        String sql = "INSERT INTO event_summary (" + names.toString()
                + ") VALUES("
                + values.toString()
                + ") ON DUPLICATE KEY UPDATE "
                // Increment count
                + "event_count=event_count+1,"
                // Latest severity
                + "severity_id=IF(:last_seen>=last_seen,VALUES(severity_id),severity_id),"
                // Latest summary
                + "summary=IF(:last_seen>=last_seen,VALUES(summary),summary),"
                // Latest message
                + "message=IF(:last_seen>=last_seen,VALUES(message),message),"
                // Latest details
                + "details_json=IF(:last_seen>=last_seen,VALUES(details_json),details_json),"
                // Latest tags
                + "tags_json=IF(:last_seen>=last_seen,VALUES(tags_json),tags_json),"
                // Latest clear_fingerprint_hash
                + "clear_fingerprint_hash=IF(:last_seen>=last_seen,VALUES(clear_fingerprint_hash),clear_fingerprint_hash),"
                // Always use latest update_time for indexing
                + "update_time=:update_time,"
                // Update status_id unless currently ACKNOWLEDGED and new status is NEW/SUPPRESSED 
                + "status_id=IF((@old_status_id := status_id) IS NOT NULL AND :last_seen>=last_seen,IF(status_id=:_acknowledged AND :status_id IN (:_new,:_suppressed),status_id,:status_id),status_id),"
                // Update status_change_time if event status changed
                + "status_change=IF(@old_status_id <> status_id,:last_seen,status_change),"
                // Latest acknowledged_by_user_uuid
                + "acknowledged_by_user_uuid=IF(status_id=:_acknowledged,acknowledged_by_user_uuid,NULL),"
                // Latest acknowledged_by_user_name
                + "acknowledged_by_user_name=IF(status_id=:_acknowledged,acknowledged_by_user_name,NULL),"
                // Latest cleared_by_event_uuid
                + "cleared_by_event_uuid=IF(status_id=:_cleared,cleared_by_event_uuid,NULL),"
                // Use latest last_seen (events may come out of order)
                + "last_seen=IF(:last_seen>last_seen,:last_seen,last_seen),"
                // Use earliest first_seen (events may come out of order)
                + "first_seen=IF(:first_seen<first_seen,:first_seen,first_seen)";
        this.template.update(sql, fields);
        return this.template.queryForObject(
                "SELECT uuid FROM event_summary WHERE fingerprint_hash=?",
                new RowMapper<String>() {
                    @Override
                    public String mapRow(ResultSet rs, int rowNum)
                            throws SQLException {
                        return DaoUtils.uuidFromBytes(rs.getBytes(COLUMN_UUID));
                    }
                }, fields.get(COLUMN_FINGERPRINT_HASH));
    }

    @Override
    @Transactional
    public String createClearEvent(Event event, Set<String> clearClasses)
            throws ZepException {
        String uuid = create(event, EventStatus.STATUS_CLOSED);
        byte[] uuidBytes = DaoUtils.uuidToBytes(uuid);

        // Need to retrieve UUID and last_seen from created event
        String sql = String.format("SELECT %s FROM %s WHERE %s=?",
                COLUMN_LAST_SEEN, TABLE_EVENT_SUMMARY, COLUMN_UUID);
        long lastSeen = this.template.queryForObject(sql,
                new RowMapper<Long>() {
                    @Override
                    public Long mapRow(ResultSet rs, int rowNum)
                            throws SQLException {
                        return rs.getLong(COLUMN_LAST_SEEN);
                    }
                }, uuidBytes);

        final List<byte[]> clearHashes = EventDaoUtils.createClearHashes(event,
                clearClasses);
        List<Object[]> fields = new ArrayList<Object[]>(clearClasses.size());
        for (byte[] clearHash : clearHashes) {
            fields.add(new Object[] { uuidBytes, clearHash, lastSeen });
        }
        this.template.update(String.format("DELETE FROM %s WHERE %s=?",
                TABLE_CLEAR_EVENTS, COLUMN_EVENT_SUMMARY_UUID), uuidBytes);
        sql = String.format("INSERT INTO %s (%s,%s,%s) VALUES(?,?,?)",
                TABLE_CLEAR_EVENTS, COLUMN_EVENT_SUMMARY_UUID,
                COLUMN_CLEAR_FINGERPRINT_HASH, COLUMN_LAST_SEEN);
        this.template.batchUpdate(sql, fields);
        return uuid;
    }

    @Override
    @Transactional
    public int clearEvents() throws ZepException {
        long updateTime = System.currentTimeMillis();

        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("_cleared", EventStatus.STATUS_CLEARED.getNumber());
        fields.put("_closed_status_ids", CLOSED_STATUS_IDS);
        fields.put(COLUMN_STATUS_CHANGE, updateTime);
        fields.put(COLUMN_UPDATE_TIME, updateTime);
        fields.put(COLUMN_ACKNOWLEDGED_BY_USER_UUID, null);
        fields.put(COLUMN_ACKNOWLEDGED_BY_USER_NAME, null);

        String updateSql = "UPDATE event_summary es, clear_events ce SET es.status_id=:_cleared, "
                + "es.status_change=:status_change, es.update_time=:update_time, "
                + "es.cleared_by_event_uuid=ce.event_summary_uuid, "
                + "acknowledged_by_user_uuid=:acknowledged_by_user_uuid, "
                + "acknowledged_by_user_name=:acknowledged_by_user_name "
                + "WHERE es.clear_fingerprint_hash = ce.clear_fingerprint_hash AND "
                + "es.last_seen <= ce.last_seen AND "
                + "es.status_id NOT IN (:_closed_status_ids)";
        return this.template.update(updateSql, fields);
    }

    @Override
    @Transactional
    public int reidentify(ModelEvent event) throws ZepException {
        String deviceId = event.getDevice().getId(); 
        String deviceUuidStr = event.getDevice().getUuid();
        long updateTime = System.currentTimeMillis();

        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put(COLUMN_ELEMENT_UUID, DaoUtils.uuidToBytes(deviceUuidStr));
        fields.put(COLUMN_ELEMENT_TYPE_ID, ModelElementType.DEVICE.getNumber());
        fields.put(COLUMN_ELEMENT_IDENTIFIER, deviceId);
        fields.put(COLUMN_UPDATE_TIME, updateTime);

        String updateSql = "UPDATE event_summary "
                + "SET element_uuid=:element_uuid, "
                +       "update_time=:update_time "
                + "WHERE element_type_id=:element_type_id and "
                +       "element_identifier=:element_identifier and "
                +       "element_uuid IS NULL";
        return this.template.update(updateSql, fields);
    }

    @Override
    @Transactional
    public int delete(String uuid) throws ZepException {
        return this.template.update("DELETE FROM event_summary WHERE uuid=?",
                DaoUtils.uuidToBytes(uuid));
    }

    @Override
    @Transactional(readOnly = true)
    public EventSummary findByUuid(String uuid) throws ZepException {
        List<EventSummary> summaries = this.template.query(
                "SELECT * FROM event_summary WHERE uuid=?",
                new EventSummaryRowMapper(this.eventDaoHelper),
                DaoUtils.uuidToBytes(uuid));
        return (summaries.size() > 0) ? summaries.get(0) : null;
    }

    @Override
    @Transactional(readOnly = true)
    public EventSummary findByFingerprint(String fingerprint)
            throws ZepException {
        try {
            fingerprint = DaoUtils.truncateStringToUtf8(fingerprint,
                    MAX_FINGERPRINT);
            return findByFingerprintHash(DaoUtils.sha1(fingerprint));
        } catch (DataAccessException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }

    private EventSummary findByFingerprintHash(byte[] fingerprintHash)
            throws ZepException {
        List<EventSummary> summaries = this.template
                .query("SELECT * FROM event_summary WHERE fingerprint_hash=?",
                        new EventSummaryRowMapper(this.eventDaoHelper),
                        fingerprintHash);
        return (summaries.size() > 0) ? summaries.get(0) : null;
    }

    @Override
    @Transactional(readOnly = true)
    public List<EventSummary> findByUuids(final List<String> uuids)
            throws ZepException {
        if (uuids.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, List<byte[]>> fields = Collections.singletonMap("uuids",
                DaoUtils.uuidsToBytes(uuids));
        return this.template.query(
                "SELECT * FROM event_summary WHERE uuid IN(:uuids)",
                new EventSummaryRowMapper(this.eventDaoHelper), fields);
    }

    private static final List<Integer> CLOSED_STATUS_IDS = Arrays.asList(
            EventStatus.STATUS_AGED.getNumber(),
            EventStatus.STATUS_CLEARED.getNumber(),
            EventStatus.STATUS_CLOSED.getNumber());

    @Override
    @Transactional
    public int ageEvents(long agingInterval, TimeUnit unit,
            EventSeverity maxSeverity, int limit) throws ZepException {
        long agingIntervalMs = unit.toMillis(agingInterval);
        if (agingIntervalMs < 0 || agingIntervalMs == Long.MAX_VALUE) {
            throw new ZepException("Invalid aging interval: " + agingIntervalMs);
        }
        if (limit <= 0) {
            throw new ZepException("Limit can't be negative: " + limit);
        }
        List<Integer> severityIds = eventDaoHelper
                .getSeverityIdsLessThan(maxSeverity);
        if (severityIds.isEmpty()) {
            logger.debug("Not aging events - max severity specified");
            return 0;
        }
        long now = System.currentTimeMillis();
        long ageTs = now - agingIntervalMs;

        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put(COLUMN_LAST_SEEN, ageTs);
        fields.put("_severity_ids", severityIds);
        fields.put("_closed_status_ids", CLOSED_STATUS_IDS);
        fields.put(COLUMN_STATUS_ID, EventStatus.STATUS_AGED.getNumber());
        fields.put(COLUMN_STATUS_CHANGE, now);
        fields.put(COLUMN_UPDATE_TIME, now);
        fields.put(COLUMN_ACKNOWLEDGED_BY_USER_UUID, null);
        fields.put(COLUMN_ACKNOWLEDGED_BY_USER_NAME, null);

        String updateSql = "UPDATE event_summary SET status_id=:status_id, "
                + "status_change=:status_change, update_time=:update_time, "
                + "acknowledged_by_user_uuid=:acknowledged_by_user_uuid, "
                + "acknowledged_by_user_name=:acknowledged_by_user_name "
                + "WHERE last_seen < :last_seen AND "
                + "severity_id IN (:_severity_ids) AND "
                + "status_id NOT IN (:_closed_status_ids) LIMIT " + limit;
        return this.template.update(updateSql, fields);
    }

    @Override
    @Transactional
    public int addNote(String uuid, EventNote note) throws ZepException {
        return EventDaoHelper
                .addNote(TABLE_EVENT_SUMMARY, uuid, note, template);
    }

    private int update(List<String> uuids, EventStatus status,
            String userUuid, String userName) throws ZepException {
        byte[] ackUuidBytes = null;
        if (userUuid != null) {
            ackUuidBytes = DaoUtils.uuidToBytes(userUuid);
        }
        String truncatedUserName = null;
        if (userName != null) {
            truncatedUserName = DaoUtils.truncateStringToUtf8(userName, MAX_ACKNOWLEDGED_BY_USER_NAME);
        }

        Object[] fields = new Object[6];
        fields[0] = status.getNumber();
        fields[1] = fields[2] = System.currentTimeMillis();
        fields[3] = ackUuidBytes;
        fields[4] = truncatedUserName;

        String updateSql = "UPDATE event_summary SET status_id=?, "
                + "status_change=?, update_time=?, acknowledged_by_user_uuid=?, "
                + "acknowledged_by_user_name=? WHERE uuid = ?";
        List<Object[]> batchFields = new ArrayList<Object[]>(uuids.size());
        for (String uuid : uuids) {
            Object[] newFields = fields.clone();
            newFields[5] = DaoUtils.uuidToBytes(uuid);
            batchFields.add(newFields);
        }
        int numUpdated = 0;
        int[] updateCounts = this.template.batchUpdate(updateSql, batchFields);
        for (int updateCount : updateCounts) {
            numUpdated += updateCount;
        }
        return numUpdated;
    }

    @Override
    @Transactional
    public int acknowledge(List<String> uuids, String userUuid, String userName)
            throws ZepException {
        return update(uuids, EventStatus.STATUS_ACKNOWLEDGED, userUuid, userName);
    }

    @Override
    @Transactional
    public int archive(long duration, TimeUnit unit, int limit)
            throws ZepException {
        final long updateTime = System.currentTimeMillis();
        final long lastSeen = updateTime - unit.toMillis(duration);
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put(COLUMN_UPDATE_TIME, updateTime);
        fields.put("_last_seen", lastSeen);
        fields.put("_closed_status_ids", CLOSED_STATUS_IDS);

        StringBuilder columnSql = new StringBuilder();
        final Set<String> columnNames = getSummaryColumnNames();

        for (Iterator<String> it = columnNames.iterator(); it.hasNext();) {
            String columnName = it.next();
            if (fields.containsKey(columnName)) {
                columnSql.append(':').append(columnName);
            } else {
                columnSql.append(columnName);
            }
            if (it.hasNext()) {
                columnSql.append(',');
            }
        }

        final String sql = "SELECT uuid FROM event_summary WHERE status_id IN (:_closed_status_ids) AND "
                + "last_seen < :_last_seen LIMIT " + limit;
        final List<byte[]> uuids = this.template.query(sql,
                new RowMapper<byte[]>() {
                    @Override
                    public byte[] mapRow(ResultSet rs, int rowNum)
                            throws SQLException {
                        return rs.getBytes(COLUMN_UUID);
                    }
                }, fields);
        fields.put("_uuids", uuids);

        /* Archive event */
        int updated = 0;
        if (!uuids.isEmpty()) {
            String insertSql = String
                    .format("INSERT INTO %1$s (%2$s) SELECT %3$s FROM %4$s WHERE uuid IN (:_uuids)",
                            TABLE_EVENT_ARCHIVE,
                            StringUtils
                                    .collectionToCommaDelimitedString(columnNames),
                            columnSql, TABLE_EVENT_SUMMARY);

            updated = this.template.update(insertSql, fields);
            this.template
                    .update("DELETE FROM event_summary WHERE uuid IN (:_uuids)",
                            fields);
        }
        return updated;
    }

    @Override
    @Transactional
    public int close(List<String> uuids) throws ZepException {
        return update(uuids, EventStatus.STATUS_CLOSED, null, null);
    }

    @Override
    @Transactional
    public int reopen(List<String> uuids) throws ZepException {
        return update(uuids, EventStatus.STATUS_NEW, null, null);
    }

    @Override
    @Transactional
    public int suppress(List<String> uuids) throws ZepException {
        return update(uuids, EventStatus.STATUS_SUPPRESSED, null, null);
    }
}
