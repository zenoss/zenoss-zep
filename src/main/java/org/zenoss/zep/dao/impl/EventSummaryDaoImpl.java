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
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.zenoss.protobufs.JsonFormat;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSummaryDao;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.zenoss.zep.dao.impl.EventConstants.*;

public class EventSummaryDaoImpl implements EventSummaryDao {

    private static Logger logger = LoggerFactory
            .getLogger(EventSummaryDaoImpl.class);

    private final SimpleJdbcTemplate template;

    private final PreparedStatementCreatorFactory psFactory;

    private EventDaoHelper eventDaoHelper;

    public EventSummaryDaoImpl(DataSource dataSource) {
        this.template = new SimpleJdbcTemplate(dataSource);
        this.psFactory = new PreparedStatementCreatorFactory(
                "SELECT * FROM event_summary WHERE fingerprint_hash=? FOR UPDATE");
        this.psFactory.addParameter(new SqlParameter(Types.BINARY));
        this.psFactory.setUpdatableResults(true);
    }

    public void setEventDaoHelper(EventDaoHelper eventDaoHelper) {
        this.eventDaoHelper = eventDaoHelper;
    }

    @Override
    @Transactional
    public String create(Event event, EventStatus status) throws ZepException {
        final Map<String, Object> fields = eventDaoHelper.insert(event);
        fields.remove(COLUMN_UUID);
        final long created = (Long) fields.remove(COLUMN_CREATED);
        final long updateTime = System.currentTimeMillis();
        final int eventCount = 1;

        fields.put(COLUMN_STATUS_ID, status.getNumber());
        fields.put(COLUMN_UPDATE_TIME, updateTime);
        fields.put(COLUMN_FIRST_SEEN, created);
        fields.put(COLUMN_STATUS_CHANGE, created);
        fields.put(COLUMN_LAST_SEEN, created);
        fields.put(COLUMN_EVENT_COUNT, eventCount);
        if (event.getSeverity() != EventSeverity.SEVERITY_CLEAR) {
            fields.put(COLUMN_CLEAR_FINGERPRINT_HASH, EventDaoUtils.createClearHash(event));
        }

        String uuid;
        final PreparedStatementCreator creator = this.psFactory.newPreparedStatementCreator(
                Collections.singletonList(fields.get(COLUMN_FINGERPRINT_HASH)));
        final RowUpdaterMapper mapper = new RowUpdaterMapper(event, fields);
        final List<String> existing = this.template.getJdbcOperations().query(creator, mapper);
        if (existing.isEmpty()) {
            try {
                final String createdUuid = UUID.randomUUID().toString();
                fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(createdUuid));
                this.template.update(DaoUtils.createNamedInsert(TABLE_EVENT_SUMMARY, fields.keySet()), fields);
                uuid = createdUuid;
            } catch (DuplicateKeyException e) {
                logger.warn("Duplicate key exception", e);
                uuid = this.template.getJdbcOperations().query(creator, mapper).get(0);
            }
        }
        else {
            uuid = existing.get(0);
        }
        return uuid;
    }

    private static class RowUpdaterMapper implements RowMapper<String>
    {
        private final Event occurrence;
        private final Map<String,Object> fields;

        public RowUpdaterMapper(Event newOccurrence, Map<String,Object> fields) {
            this.occurrence = newOccurrence;
            this.fields = fields;
        }

        private void updateColumn(ResultSet rs, String columnName) throws SQLException {
            Object val = this.fields.get(columnName);
            if (val == null) {
                rs.updateNull(columnName);
            }
            else if (val instanceof Integer) {
                rs.updateInt(columnName, (Integer) val);
            }
            else if (val instanceof Long) {
                rs.updateLong(columnName, (Long) val);
            }
            else if (val instanceof byte[]) {
                rs.updateBytes(columnName, (byte[]) val);
            }
            else if (val instanceof String) {
                rs.updateString(columnName, (String) val);
            }
            else {
                throw new SQLException("Unsupported data type: " + val.getClass().getName());
            }
        }

        private String updateDetailsJson(String oldDetailsJson, String newDetailsJson) throws SQLException {
            if (oldDetailsJson == null) {
                return newDetailsJson;
            }
            if (newDetailsJson == null) {
                return oldDetailsJson;
            }

            List<EventDetail> oldDetails;
            try {
                oldDetails = JsonFormat.mergeAllDelimitedFrom(oldDetailsJson, EventDetail.getDefaultInstance());
            } catch (IOException e) {
                throw new SQLException(e.getLocalizedMessage(), e);
            }

            List<EventDetail> newDetails;
            try {
                newDetails = JsonFormat.mergeAllDelimitedFrom(newDetailsJson, EventDetail.getDefaultInstance());
            } catch (IOException e) {
                throw new SQLException(e.getLocalizedMessage(), e);
            }

            Map<String,EventDetail> detailsMap = new LinkedHashMap<String,EventDetail>(oldDetails.size() + newDetails.size());
            for (EventDetail oldDetail : oldDetails) {
                detailsMap.put(oldDetail.getName(), oldDetail);
            }
            for (EventDetail newDetail : newDetails) {
                detailsMap.put(newDetail.getName(), newDetail);
            }
            try {
                return JsonFormat.writeAllDelimitedAsString(detailsMap.values());
            } catch (IOException e) {
                throw new SQLException(e.getLocalizedMessage(), e);
            }
        }

        @Override
        public String mapRow(ResultSet rs, int rowNum) throws SQLException {
            // Always increment count
            rs.updateInt(COLUMN_EVENT_COUNT, rs.getInt(COLUMN_EVENT_COUNT) + 1);
            
            // Always modify update_time (for indexing)
            rs.updateLong(COLUMN_UPDATE_TIME, (Long)fields.get(COLUMN_UPDATE_TIME));
            
            final long previousLastSeen = rs.getLong(COLUMN_LAST_SEEN);
            if (this.occurrence.getCreatedTime() >= previousLastSeen) {
                updateColumn(rs, COLUMN_LAST_SEEN);
                updateColumn(rs, COLUMN_SEVERITY_ID);
                updateColumn(rs, COLUMN_MESSAGE);
                updateColumn(rs, COLUMN_SUMMARY);
                updateColumn(rs, COLUMN_TAGS_JSON);
                updateColumn(rs, COLUMN_CLEAR_FINGERPRINT_HASH);
                updateColumn(rs, COLUMN_EVENT_GROUP_ID);
                updateColumn(rs, COLUMN_EVENT_CLASS_ID);
                updateColumn(rs, COLUMN_EVENT_CLASS_KEY_ID);
                updateColumn(rs, COLUMN_EVENT_CLASS_MAPPING_UUID);
                updateColumn(rs, COLUMN_EVENT_KEY_ID);
                updateColumn(rs, COLUMN_ELEMENT_UUID);
                updateColumn(rs, COLUMN_ELEMENT_TYPE_ID);
                updateColumn(rs, COLUMN_ELEMENT_IDENTIFIER);
                updateColumn(rs, COLUMN_ELEMENT_SUB_UUID);
                updateColumn(rs, COLUMN_ELEMENT_SUB_TYPE_ID);
                updateColumn(rs, COLUMN_ELEMENT_SUB_IDENTIFIER);
                updateColumn(rs, COLUMN_MONITOR_ID);
                updateColumn(rs, COLUMN_AGENT_ID);
                updateColumn(rs, COLUMN_SYSLOG_FACILITY);
                updateColumn(rs, COLUMN_SYSLOG_PRIORITY);
                updateColumn(rs, COLUMN_NT_EVENT_CODE);

                // Update status except for ACKNOWLEDGED -> {NEW|SUPPRESSED}
                // Stays in ACKNOWLEDGED in these cases
                boolean updateStatus = true;
                EventStatus oldStatus = EventStatus.valueOf(rs.getInt(COLUMN_STATUS_ID));
                EventStatus newStatus = EventStatus.valueOf((Integer) this.fields.get(COLUMN_STATUS_ID));
                switch (oldStatus) {
                    case STATUS_ACKNOWLEDGED:
                        switch (newStatus) {
                            case STATUS_NEW:
                            case STATUS_SUPPRESSED:
                                updateStatus = false;
                                break;
                        }
                        break;
                }
                if (updateStatus && oldStatus != newStatus) {
                    updateColumn(rs, COLUMN_STATUS_ID);
                    this.fields.put(COLUMN_STATUS_CHANGE, this.occurrence.getCreatedTime());
                    updateColumn(rs, COLUMN_STATUS_CHANGE);
                }

                // Merge event details
                String oldDetailsJson = rs.getString(COLUMN_DETAILS_JSON);
                String newDetailsJson = (String) this.fields.get(COLUMN_DETAILS_JSON);
                if (newDetailsJson != null) {
                    this.fields.put(COLUMN_DETAILS_JSON, updateDetailsJson(oldDetailsJson, newDetailsJson));
                    updateColumn(rs, COLUMN_DETAILS_JSON);
                }
            }
            else {
                // Merge event details - order swapped b/c of out of order event
                String oldDetailsJson = (String) this.fields.get(COLUMN_DETAILS_JSON);
                String newDetailsJson = rs.getString(COLUMN_DETAILS_JSON);
                if (oldDetailsJson != null) {
                    this.fields.put(COLUMN_DETAILS_JSON, updateDetailsJson(oldDetailsJson, newDetailsJson));
                    updateColumn(rs, COLUMN_DETAILS_JSON);
                }
            }

            final long previousFirstSeen = rs.getLong(COLUMN_FIRST_SEEN);
            if (this.occurrence.getCreatedTime() < previousFirstSeen) {
                updateColumn(rs, COLUMN_FIRST_SEEN);
            }

            rs.updateRow();
            return DaoUtils.uuidFromBytes(rs.getBytes(COLUMN_UUID));
        }
    }

    @Override
    @Transactional
    public String createClearEvent(Event event, Set<String> clearClasses)
            throws ZepException {
        final String uuid = create(event, EventStatus.STATUS_CLOSED);
        final byte[] uuidBytes = DaoUtils.uuidToBytes(uuid);

        // Need to retrieve last_seen from created event
        long lastSeen = this.template.queryForLong("SELECT last_seen FROM event_summary WHERE uuid=?", uuidBytes);

        final List<byte[]> clearHashes = EventDaoUtils.createClearHashes(event, clearClasses);
        final List<Object[]> fields = new ArrayList<Object[]>(clearHashes.size());
        for (byte[] clearHash : clearHashes) {
            fields.add(new Object[] { uuidBytes, clearHash, lastSeen });
        }
        this.template.update("DELETE FROM clear_events WHERE event_summary_uuid=?", uuidBytes);
        this.template.batchUpdate("INSERT INTO clear_events (event_summary_uuid,clear_fingerprint_hash,last_seen) " +
                "VALUES(?,?,?) ON DUPLICATE KEY UPDATE " +
                "event_summary_uuid=IF(VALUES(last_seen) > last_seen, VALUES(event_summary_uuid), event_summary_uuid)," +
                "last_seen=IF(VALUES(last_seen) > last_seen, VALUES(last_seen), last_seen)", fields);
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
    public int reidentify(ModelElementType type, String id, String uuid, String parentUuid) throws ZepException {
        long updateTime = System.currentTimeMillis();

        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("_uuid", DaoUtils.uuidToBytes(uuid));
        fields.put("_uuid_str", uuid);
        fields.put("_type_id", type.getNumber());
        fields.put("_id", id);
        fields.put(COLUMN_UPDATE_TIME, updateTime);

        int numRows = 0;
        String updateSql = "UPDATE event_summary SET element_uuid=:_uuid, update_time=:update_time "
                + "WHERE element_uuid IS NULL AND element_type_id=:_type_id AND element_identifier=:_id";
        numRows += this.template.update(updateSql, fields);

        if (parentUuid != null) {
            fields.put("_parent_uuid", DaoUtils.uuidToBytes(parentUuid));
            updateSql = "UPDATE event_summary es INNER JOIN event_class ON es.event_class_id = event_class.id " +
                    "LEFT JOIN event_key ON es.event_key_id = event_key.id " +
                    "SET element_sub_uuid=:_uuid, update_time=:update_time, " +
                    "clear_fingerprint_hash = UNHEX(SHA1(CONCAT_WS('|',:_uuid_str,event_class.name,IFNULL(event_key.name,'')))) " +
                    "WHERE es.element_uuid=:_parent_uuid AND es.element_sub_uuid IS NULL AND " +
                    "es.element_sub_type_id=:_type_id AND es.element_sub_identifier=:_id";
            numRows += this.template.update(updateSql, fields);
        }
        return numRows;
    }

    @Override
    @Transactional
    public int deidentify(String uuid) throws ZepException {
        long updateTime = System.currentTimeMillis();

        Map<String,Object> fields = new HashMap<String,Object>();
        fields.put("_uuid", DaoUtils.uuidToBytes(uuid));
        fields.put(COLUMN_UPDATE_TIME, updateTime);

        int numRows = 0;
        String updateSql = "UPDATE event_summary SET element_uuid=NULL, update_time=:update_time "
                + "WHERE element_uuid=:_uuid";
        numRows += this.template.update(updateSql, fields);

        updateSql = "UPDATE event_summary es INNER JOIN event_class ON es.event_class_id = event_class.id " +
                "LEFT JOIN event_key ON es.event_key_id = event_key.id " +
                "SET element_sub_uuid=NULL, update_time=:update_time, " +
                "clear_fingerprint_hash=UNHEX(SHA1(CONCAT_WS('|',element_identifier,IFNULL(element_sub_identifier,''),event_class.name,IFNULL(event_key.name,'')))) " +
                "WHERE element_sub_uuid=:_uuid";
        numRows += this.template.update(updateSql, fields);
        return numRows;
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
