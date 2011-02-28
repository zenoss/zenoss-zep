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
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.JsonFormat;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventDetail.EventDetailMergeBehavior;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepUtils;
import org.zenoss.zep.dao.DaoCache;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.zenoss.zep.dao.impl.EventConstants.*;

public class EventDaoHelper {

    private SimpleJdbcInsert insert;

    private DaoCache daoCache;

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(EventDaoHelper.class);

    public EventDaoHelper() {
    }

    public void setDataSource(DataSource dataSource) {
        this.insert = new SimpleJdbcInsert(dataSource).withTableName(TABLE_EVENT);
    }

    public void setDaoCache(DaoCache daoCache) {
        this.daoCache = daoCache;
    }

    public Map<String, Object> createOccurrenceFields(Event event) throws ZepException {
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(event.getUuid()));
        String fingerprint = DaoUtils.truncateStringToUtf8(event.getFingerprint(), MAX_FINGERPRINT);
        fields.put(COLUMN_FINGERPRINT_HASH, DaoUtils.sha1(fingerprint));
        fields.put(COLUMN_FINGERPRINT, fingerprint);

        Integer eventGroupId = null;
        if (event.hasEventGroup()) {
            String eventGroup = DaoUtils.truncateStringToUtf8(event.getEventGroup(), MAX_EVENT_GROUP);
            eventGroupId = daoCache.getEventGroupId(eventGroup);
        }
        fields.put(COLUMN_EVENT_GROUP_ID, eventGroupId);

        String eventClass = DaoUtils.truncateStringToUtf8(event.getEventClass(), MAX_EVENT_CLASS);
        fields.put(COLUMN_EVENT_CLASS_ID, daoCache.getEventClassId(eventClass));

        Integer eventClassKeyId = null;
        if (event.hasEventClassKey()) {
            String eventClassKey = DaoUtils.truncateStringToUtf8(event.getEventClassKey(), MAX_EVENT_CLASS_KEY);
            eventClassKeyId = daoCache.getEventClassKeyId(eventClassKey);
        }
        fields.put(COLUMN_EVENT_CLASS_KEY_ID, eventClassKeyId);

        Integer eventKeyId = null;
        if (event.hasEventKey()) {
            String eventKey = DaoUtils.truncateStringToUtf8(event.getEventKey(), MAX_EVENT_KEY);
            eventKeyId = daoCache.getEventKeyId(eventKey);
        }
        fields.put(COLUMN_EVENT_KEY_ID, eventKeyId);

        byte[] eventClassMappingUuid = null;
        if (!event.getEventClassMappingUuid().isEmpty()) {
            eventClassMappingUuid = DaoUtils.uuidToBytes(event.getEventClassMappingUuid());
        }
        fields.put(COLUMN_EVENT_CLASS_MAPPING_UUID, eventClassMappingUuid);

        fields.put(COLUMN_SEVERITY_ID, event.getSeverity().getNumber());

        if (event.hasActor()) {
            populateEventActorFields(event.getActor(), fields);
        }

        fields.put(COLUMN_CREATED, event.getCreatedTime());

        Integer monitorId = null;
        if (event.hasMonitor()) {
            monitorId = daoCache.getMonitorId(DaoUtils.truncateStringToUtf8(event.getMonitor(), MAX_MONITOR));
        }
        fields.put(COLUMN_MONITOR_ID, monitorId);

        Integer agentId = null;
        if (event.hasAgent()) {
            agentId = daoCache.getAgentId(DaoUtils.truncateStringToUtf8(event.getAgent(), MAX_AGENT));
        }
        fields.put(COLUMN_AGENT_ID, agentId);

        Integer syslogFacility = null;
        if (event.hasSyslogFacility()) {
            syslogFacility = event.getSyslogFacility();
        }
        fields.put(COLUMN_SYSLOG_FACILITY, syslogFacility);

        Integer syslogPriority = null;
        if (event.hasSyslogPriority()) {
            syslogPriority = EventDaoUtils.syslogPriorityToInt(event.getSyslogPriority());
        }
        fields.put(COLUMN_SYSLOG_PRIORITY, syslogPriority);

        Integer ntEventCode = null;
        if (event.hasNtEventCode()) {
            ntEventCode = event.getNtEventCode();
        }
        fields.put(COLUMN_NT_EVENT_CODE, ntEventCode);

        fields.put(COLUMN_SUMMARY, DaoUtils.truncateStringToUtf8(event.getSummary(), MAX_SUMMARY));
        fields.put(COLUMN_MESSAGE, DaoUtils.truncateStringToUtf8(event.getMessage(), MAX_MESSAGE));

        String detailsJson = null;
        if (event.getDetailsCount() > 0) {
            try {
                detailsJson = JsonFormat.writeAllDelimitedAsString(mergeDuplicateDetails(event.getDetailsList()));
            } catch (IOException e) {
                throw new ZepException(e.getLocalizedMessage(), e);
            }
        }
        fields.put(COLUMN_DETAILS_JSON, detailsJson);

        String tagsJson = null;
        if (event.getTagsCount() > 0) {
            List<EventTag> tags = buildTags(event);
            try {
                tagsJson = JsonFormat.writeAllDelimitedAsString(tags);
            } catch (IOException e) {
                throw new ZepException(e.getLocalizedMessage(), e);
            }
        }
        fields.put(COLUMN_TAGS_JSON, tagsJson);

        return fields;
    }

    @Transactional
    public void insert(Map<String,Object> occurrenceFields) throws ZepException {
        try {
            this.insert.execute(occurrenceFields);
        } catch (DataAccessException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Removes duplicate tags from the event.
     * 
     * @param event
     *            Original event.
     * @return New event with duplicate tags removed.
     */
    private static List<EventTag> buildTags(Event event) {
        final List<EventTag> tags = new ArrayList<EventTag>(event.getTagsCount());
        Set<String> uuids = new HashSet<String>(event.getTagsCount());
        for (EventTag tag : event.getTagsList()) {
            if (uuids.add(tag.getUuid())) {
                tags.add(tag);
            }
        }
        return tags;
    }

    /**
     * Collapses duplicates in a list of event details.
     *
     * @param details Event details (may contain duplicate names).
     * @return A collection of event details where duplicates are removed and values are appended.
     */
    private static Collection<EventDetail> mergeDuplicateDetails(List<EventDetail> details) {
        Map<String, EventDetail> names = new LinkedHashMap<String, EventDetail>(details.size());
        for (EventDetail detail : details) {
            EventDetail existing = names.get(detail.getName());
            if (existing == null) {
                names.put(detail.getName(), detail);
            } else {
                // Append detail values to existing
                EventDetail merged = EventDetail.newBuilder(existing).addAllValue(detail.getValueList()).build();
                names.put(detail.getName(), merged);
            }
        }
        return names.values();
    }

    /**
     * Merges the old and new event detail lists. Uses the EventDetailMergeBehavior setting
     * to determine how details with the same name in both lists should be handled.
     *
     * @param oldDetails Old event details.
     * @param newDetails New event details.
     * @return A merged collections of event details.
     */
    static Collection<EventDetail> mergeDetails(List<EventDetail> oldDetails, List<EventDetail> newDetails) {
        Map<String, EventDetail> detailsMap = new LinkedHashMap<String, EventDetail>(oldDetails.size() + newDetails.size());
        for (EventDetail detail : oldDetails) {
            detailsMap.put(detail.getName(), detail);
        }
        for (EventDetail newDetail : newDetails) {
            final EventDetailMergeBehavior mergeBehavior = newDetail.getMergeBehavior();
            if (mergeBehavior == EventDetailMergeBehavior.REPLACE) {
                // If a new detail specifies an empty value, it is removed
                if (newDetail.getValueCount() == 0) {
                    detailsMap.remove(newDetail.getName());
                }
                else {
                    detailsMap.put(newDetail.getName(), newDetail);
                }
            }
            else {
                final EventDetail existing = detailsMap.get(newDetail.getName());
                if (existing == null) {
                    detailsMap.put(newDetail.getName(), newDetail);
                }
                else if (mergeBehavior == EventDetailMergeBehavior.APPEND) {
                    final EventDetail.Builder merged = EventDetail.newBuilder(existing);
                    merged.addAllValue(newDetail.getValueList());
                    detailsMap.put(newDetail.getName(), merged.build());
                }
                else if (mergeBehavior == EventDetailMergeBehavior.UNIQUE) {
                    final EventDetail.Builder merged = EventDetail.newBuilder(existing);
                    final Set<String> newValues = new LinkedHashSet<String>(newDetail.getValueList());
                    newValues.removeAll(existing.getValueList());
                    merged.addAllValue(newValues);
                    detailsMap.put(newDetail.getName(), merged.build());
                }
                else {
                    logger.warn("Unsupported merge behavior: {}", mergeBehavior);
                }
            }
        }
        return detailsMap.values();
    }

    private void populateEventActorFields(EventActor actor,
            Map<String, Object> fields) {
        if (!actor.getElementUuid().isEmpty()) {
            fields.put(COLUMN_ELEMENT_UUID, DaoUtils.uuidToBytes(actor.getElementUuid()));
        }
        if (actor.hasElementTypeId()) {
            fields.put(COLUMN_ELEMENT_TYPE_ID, actor.getElementTypeId().getNumber());
        }
        if (actor.hasElementIdentifier()) {
            fields.put(COLUMN_ELEMENT_IDENTIFIER, DaoUtils.truncateStringToUtf8(actor.getElementIdentifier(), MAX_ELEMENT_IDENTIFIER));
        }
        if (!actor.getElementSubUuid().isEmpty()) {
            fields.put(COLUMN_ELEMENT_SUB_UUID, DaoUtils.uuidToBytes(actor.getElementSubUuid()));
        }
        if (actor.hasElementSubTypeId()) {
            fields.put(COLUMN_ELEMENT_SUB_TYPE_ID, actor.getElementSubTypeId().getNumber());
        }
        if (actor.hasElementSubIdentifier()) {
            fields.put(COLUMN_ELEMENT_SUB_IDENTIFIER, DaoUtils.truncateStringToUtf8(actor.getElementSubIdentifier(), MAX_ELEMENT_SUB_IDENTIFIER));
        }
    }

    public Event eventMapper(ResultSet rs, boolean isSummary)
            throws SQLException {
        Event.Builder eventBuilder = Event.newBuilder();

        if (isSummary) {
            eventBuilder.setCreatedTime(rs.getLong(COLUMN_LAST_SEEN));
        } else {
            eventBuilder.setCreatedTime(rs.getLong(COLUMN_CREATED));
            eventBuilder.setUuid(DaoUtils.uuidFromBytes(rs.getBytes(COLUMN_UUID)));
        }

        eventBuilder.setFingerprint(rs.getString(COLUMN_FINGERPRINT));

        int eventGroupId = rs.getInt(COLUMN_EVENT_GROUP_ID);
        if (!rs.wasNull()) {
            eventBuilder.setEventGroup(daoCache.getEventGroupFromId(eventGroupId));
        }

        int eventClassId = rs.getInt(COLUMN_EVENT_CLASS_ID);
        if (!rs.wasNull()) {
            eventBuilder.setEventClass(daoCache.getEventClassFromId(eventClassId));
        }

        int eventClassKeyId = rs.getInt(COLUMN_EVENT_CLASS_KEY_ID);
        if (!rs.wasNull()) {
            eventBuilder.setEventClassKey(daoCache.getEventClassKeyFromId(eventClassKeyId));
        }

        int eventKeyId = rs.getInt(COLUMN_EVENT_KEY_ID);
        if (!rs.wasNull()) {
            eventBuilder.setEventKey(daoCache.getEventKeyFromId(eventKeyId));
        }

        byte[] classMappingUuid = rs.getBytes(COLUMN_EVENT_CLASS_MAPPING_UUID);
        if (classMappingUuid != null) {
            eventBuilder.setEventClassMappingUuid(DaoUtils.uuidFromBytes(classMappingUuid));
        }

        eventBuilder.setSeverity(EventSeverity.valueOf(rs.getInt(COLUMN_SEVERITY_ID)));

        eventBuilder.setActor(deserializeEventActor(rs));

        int monitorId = rs.getInt(COLUMN_MONITOR_ID);
        if (!rs.wasNull()) {
            eventBuilder.setMonitor(daoCache.getMonitorFromId(monitorId));
        }

        int agentId = rs.getInt(COLUMN_AGENT_ID);
        if (!rs.wasNull()) {
            eventBuilder.setAgent(daoCache.getAgentFromId(agentId));
        }

        int syslogFacility = rs.getInt(COLUMN_SYSLOG_FACILITY);
        if (!rs.wasNull()) {
            eventBuilder.setSyslogFacility(syslogFacility);
        }

        int syslogPriority = rs.getInt(COLUMN_SYSLOG_PRIORITY);
        if (!rs.wasNull()) {
            eventBuilder.setSyslogPriority(EventDaoUtils.syslogPriorityFromInt(syslogPriority));
        }

        int ntEventCode = rs.getInt(COLUMN_NT_EVENT_CODE);
        if (!rs.wasNull()) {
            eventBuilder.setNtEventCode(ntEventCode);
        }

        eventBuilder.setSummary(rs.getString(COLUMN_SUMMARY));

        eventBuilder.setMessage(rs.getString(COLUMN_MESSAGE));

        String detailsJson = rs.getString(COLUMN_DETAILS_JSON);
        if (detailsJson != null && !detailsJson.isEmpty()) {
            try {
                List<EventDetail> details = JsonFormat.mergeAllDelimitedFrom(detailsJson, EventDetail.getDefaultInstance());
                eventBuilder.addAllDetails(details);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }

        String tagsJson = rs.getString(COLUMN_TAGS_JSON);
        if (tagsJson != null && !tagsJson.isEmpty()) {
            try {
                List<EventTag> tags = JsonFormat.mergeAllDelimitedFrom(tagsJson, EventTag.getDefaultInstance());
                eventBuilder.addAllTags(tags);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }

        return eventBuilder.build();
    }

    private EventActor deserializeEventActor(ResultSet rs)
            throws SQLException {
        EventActor.Builder actorBuilder = EventActor.newBuilder();
        byte[] elementUuid = rs.getBytes(COLUMN_ELEMENT_UUID);
        if (elementUuid != null) {
            actorBuilder.setElementUuid(DaoUtils.uuidFromBytes(elementUuid));
        }

        int elementTypeId = rs.getInt(COLUMN_ELEMENT_TYPE_ID);
        if (!rs.wasNull()) {
            actorBuilder.setElementTypeId(ModelElementType.valueOf(elementTypeId));
        }

        String elementIdentifier = rs.getString(COLUMN_ELEMENT_IDENTIFIER);
        if (elementIdentifier != null) {
            actorBuilder.setElementIdentifier(elementIdentifier);
        }

        byte[] subUuid = rs.getBytes(COLUMN_ELEMENT_SUB_UUID);
        if (subUuid != null) {
            actorBuilder.setElementSubUuid(DaoUtils.uuidFromBytes(subUuid));
        }

        int subTypeId = rs.getInt(COLUMN_ELEMENT_SUB_TYPE_ID);
        if (!rs.wasNull()) {
            actorBuilder.setElementSubTypeId(ModelElementType.valueOf(subTypeId));
        }

        String subIdentifier = rs.getString(COLUMN_ELEMENT_SUB_IDENTIFIER);
        if (subIdentifier != null) {
            actorBuilder.setElementSubIdentifier(subIdentifier);
        }
        return actorBuilder.build();
    }

    public static int addNote(String tableName, String uuid, EventNote note, SimpleJdbcTemplate template)
            throws ZepException {
        EventNote.Builder builder = EventNote.newBuilder(note);
        if (builder.getUuid().isEmpty()) {
            builder.setUuid(UUID.randomUUID().toString());
        }
        builder.setCreatedTime(System.currentTimeMillis());
        try {
            // Notes are expected to be returned in reverse order
            Map<String,Object> fields = new HashMap<String,Object>();
            fields.put(COLUMN_UPDATE_TIME, System.currentTimeMillis());
            fields.put(COLUMN_INDEXED, 0);
            fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
            fields.put(COLUMN_NOTES_JSON, JsonFormat.writeAsString(builder.build()));
            final String sql = "UPDATE " + tableName + " SET update_time=:update_time,indexed=:indexed," +
                    "notes_json=CONCAT_WS(',\n',:notes_json,notes_json) WHERE uuid=:uuid";
            return template.update(sql, fields);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    public static int updateDetails(String tableName, String uuid, List<EventDetail> details, SimpleJdbcTemplate template)
            throws ZepException {
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(uuid));
        final String selectSql = "SELECT details_json FROM " + tableName + " WHERE uuid = :uuid FOR UPDATE";

        final List<EventDetail> existingDetailList;
        try {
            String currentDetailsJson = template.queryForObject(selectSql, String.class, fields);
            if (currentDetailsJson == null) {
                existingDetailList = Collections.emptyList();
            }
            else {
                existingDetailList = JsonFormat.mergeAllDelimitedFrom(currentDetailsJson, EventDetail.getDefaultInstance());
            }
        } catch (IncorrectResultSizeDataAccessException irsdae) {
            logger.debug("unexpected results size data access exception retrieving event summary", irsdae);
            return 0;
        } catch (IOException e) {
            throw new ZepException(e);
        }
        
        // update details with new values
        Collection<EventDetail> listWithUpdates = EventDaoHelper.mergeDetails(existingDetailList, details);
        // serialize updated details to json
        try {
            final String updatedDetailsJson = JsonFormat.writeAllDelimitedAsString(listWithUpdates);
            // update current event_summary record
            fields.put(COLUMN_DETAILS_JSON, updatedDetailsJson);
            fields.put(COLUMN_UPDATE_TIME, System.currentTimeMillis());
            fields.put(COLUMN_INDEXED, 0);
            String updateSql = "UPDATE " + tableName + " SET details_json=:details_json, "
                    + "update_time=:update_time,indexed=:indexed WHERE uuid = :uuid";

            return template.update(updateSql, fields);
        } catch (IOException ioe) {
            throw new ZepException(ioe);
        }
    }

    public static List<Integer> getSeverityIdsLessThan(EventSeverity severity) {
        final List<Integer> severityIds = new ArrayList<Integer>(ZepUtils.ORDERED_SEVERITIES.size() - 1);
        for (EventSeverity orderedSeverity : ZepUtils.ORDERED_SEVERITIES) {
            if (orderedSeverity == severity) {
                break;
            }
            severityIds.add(orderedSeverity.getNumber());
        }
        return severityIds;
    }
}
