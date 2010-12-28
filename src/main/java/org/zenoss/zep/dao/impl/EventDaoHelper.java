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
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.JsonFormat;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetail;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.zenoss.zep.dao.impl.EventConstants.*;

public class EventDaoHelper {

    private SimpleJdbcInsert insert;

    private DaoCache daoCache;

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory
            .getLogger(EventDaoHelper.class);

    public EventDaoHelper() {
    }

    public void setDataSource(DataSource dataSource) {
        this.insert = new SimpleJdbcInsert(dataSource).withTableName("event");
    }

    public void setDaoCache(DaoCache daoCache) {
        this.daoCache = daoCache;
    }

    @Transactional
    public Map<String, Object> insert(Event event) throws ZepException {
        try {
            Map<String, Object> fields = new HashMap<String, Object>();
            fields.put(COLUMN_UUID, DaoUtils.uuidToBytes(event.getUuid()));
            String fingerprint = DaoUtils.truncateStringToUtf8(
                    event.getFingerprint(), MAX_FINGERPRINT);
            fields.put(COLUMN_FINGERPRINT_HASH, DaoUtils.sha1(fingerprint));
            fields.put(COLUMN_FINGERPRINT, fingerprint);
            Integer eventGroupId = null;
            if (event.hasEventGroup()) {
                eventGroupId = daoCache.getEventGroupId(DaoUtils
                        .truncateStringToUtf8(event.getEventGroup(),
                                MAX_EVENT_GROUP));
            }
            fields.put(COLUMN_EVENT_GROUP_ID, eventGroupId);
            fields.put(COLUMN_EVENT_CLASS_ID, daoCache.getEventClassId(DaoUtils
                    .truncateStringToUtf8(event.getEventClass(),
                            MAX_EVENT_CLASS)));
            if (event.hasEventClassKey()) {
                fields.put(COLUMN_EVENT_CLASS_KEY_ID, daoCache
                        .getEventClassKeyId(DaoUtils.truncateStringToUtf8(
                                event.getEventClassKey(), MAX_EVENT_CLASS_KEY)));
            }
            if (event.hasEventKey()) {
                fields.put(COLUMN_EVENT_KEY_ID, daoCache.getEventKeyId(DaoUtils
                        .truncateStringToUtf8(event.getEventKey(),
                                MAX_EVENT_KEY)));
            }
            if (!event.getEventClassMappingUuid().isEmpty()) {
                fields.put(COLUMN_EVENT_CLASS_MAPPING_UUID,
                        DaoUtils.uuidToBytes(event.getEventClassMappingUuid()));
            }
            fields.put(COLUMN_SEVERITY_ID, event.getSeverity().getNumber());
            if (event.hasActor()) {
                populateEventActorFields(event.getActor(), fields);
            }
            fields.put(COLUMN_CREATED, event.getCreatedTime());
            if (event.hasMonitor()) {
                fields.put(COLUMN_MONITOR_ID, daoCache.getMonitorId(DaoUtils
                        .truncateStringToUtf8(event.getMonitor(), MAX_MONITOR)));
            }
            if (event.hasAgent()) {
                fields.put(
                        COLUMN_AGENT_ID,
                        daoCache.getAgentId(DaoUtils.truncateStringToUtf8(
                                event.getAgent(), MAX_AGENT)));
            }
            if (event.hasSyslogFacility()) {
                fields.put(COLUMN_SYSLOG_FACILITY, event.getSyslogFacility());
            }
            if (event.hasSyslogPriority()) {
                fields.put(COLUMN_SYSLOG_PRIORITY, EventDaoUtils
                        .syslogPriorityToInt(event.getSyslogPriority()));
            }
            if (event.hasNtEventCode()) {
                fields.put(COLUMN_NT_EVENT_CODE, event.getNtEventCode());
            }
            if (event.hasSummary()) {
                fields.put(COLUMN_SUMMARY, DaoUtils.truncateStringToUtf8(
                        event.getSummary(), MAX_SUMMARY));
            }
            if (event.hasMessage()) {
                fields.put(COLUMN_MESSAGE, DaoUtils.truncateStringToUtf8(
                        event.getMessage(), MAX_MESSAGE));
            }
            if (event.getDetailsCount() > 0) {
                fields.put(COLUMN_DETAILS_JSON, JsonFormat
                        .writeAllDelimitedAsString(mergeDuplicateDetails(event
                                .getDetailsList())));
            }
            List<EventTag> tags = buildTags(event);
            if (!tags.isEmpty()) {
                fields.put(COLUMN_TAGS_JSON,
                        JsonFormat.writeAllDelimitedAsString(tags));
            }

            insert.execute(fields);
            return fields;
        } catch (DataAccessException e) {
            throw new ZepException(e);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    private static String getTagType(ModelElementType type) {
        return "zenoss."
                + (type != null ? type.name().toLowerCase() : "unknown");
    }

    /**
     * Removes duplicate tags from the event and adds the element_uuid and
     * element_sub_uuid to the tags list if they are specified.
     * 
     * @param event
     *            Original event.
     * @return New event with duplicate tags removed and tags added for
     *         element_uuid and element_sub_uuid.
     */
    private static List<EventTag> buildTags(Event event) {
        final List<EventTag> tags = new ArrayList<EventTag>(
                event.getTagsCount());
        Set<String> uuids = new HashSet<String>();
        for (EventTag tag : event.getTagsList()) {
            if (uuids.add(tag.getUuid())) {
                tags.add(tag);
            }
        }
        return tags;
    }

    private static Collection<EventDetail> mergeDuplicateDetails(
            List<EventDetail> details) {
        Map<String, EventDetail> names = new LinkedHashMap<String, EventDetail>();
        for (EventDetail detail : details) {
            EventDetail existing = names.get(detail.getName());
            if (existing == null) {
                names.put(detail.getName(), detail);
            } else {
                // Merge details with existing
                EventDetail merged = EventDetail.newBuilder(existing)
                        .addAllValue(detail.getValueList()).build();
                names.put(detail.getName(), merged);
            }
        }
        return names.values();
    }

    private void populateEventActorFields(EventActor actor,
            Map<String, Object> fields) {
        if (!actor.getElementUuid().isEmpty()) {
            fields.put(COLUMN_ELEMENT_UUID,
                    DaoUtils.uuidToBytes(actor.getElementUuid()));
        }
        if (actor.hasElementTypeId()) {
            fields.put(COLUMN_ELEMENT_TYPE_ID, actor.getElementTypeId()
                    .getNumber());
        }
        if (actor.hasElementIdentifier()) {
            fields.put(COLUMN_ELEMENT_IDENTIFIER, DaoUtils
                    .truncateStringToUtf8(actor.getElementIdentifier(),
                            MAX_ELEMENT_IDENTIFIER));
        }
        if (!actor.getElementSubUuid().isEmpty()) {
            fields.put(COLUMN_ELEMENT_SUB_UUID,
                    DaoUtils.uuidToBytes(actor.getElementSubUuid()));
        }
        if (actor.hasElementSubTypeId()) {
            fields.put(COLUMN_ELEMENT_SUB_TYPE_ID, actor.getElementSubTypeId()
                    .getNumber());
        }
        if (actor.hasElementSubIdentifier()) {
            fields.put(COLUMN_ELEMENT_SUB_IDENTIFIER, DaoUtils
                    .truncateStringToUtf8(actor.getElementSubIdentifier(),
                            MAX_ELEMENT_SUB_IDENTIFIER));
        }
    }

    public Event eventMapper(ResultSet rs, boolean isSummary, Set<String> fields)
            throws SQLException {
        Event.Builder eventBuilder = Event.newBuilder();

        if (isSummary) {
            if (fields.contains(COLUMN_LAST_SEEN)) {
                eventBuilder.setCreatedTime(rs.getLong(COLUMN_LAST_SEEN));
            }
        } else {
            if (fields.contains(COLUMN_CREATED)) {
                eventBuilder.setCreatedTime(rs.getLong(COLUMN_CREATED));
            }
            if (fields.contains(COLUMN_UUID)) {
                eventBuilder.setUuid(DaoUtils.uuidFromBytes(rs
                        .getBytes(COLUMN_UUID)));
            }
        }

        if (fields.contains(COLUMN_FINGERPRINT)) {
            eventBuilder.setFingerprint(rs.getString(COLUMN_FINGERPRINT));
        }

        if (fields.contains(COLUMN_EVENT_GROUP_ID)) {
            int eventGroupId = rs.getInt(COLUMN_EVENT_GROUP_ID);
            if (!rs.wasNull()) {
                eventBuilder.setEventGroup(daoCache
                        .getEventGroupFromId(eventGroupId));
            }
        }

        if (fields.contains(COLUMN_EVENT_CLASS_ID)) {
            int eventClassId = rs.getInt(COLUMN_EVENT_CLASS_ID);
            if (!rs.wasNull()) {
                eventBuilder.setEventClass(daoCache
                        .getEventClassFromId(eventClassId));
            }
        }

        if (fields.contains(COLUMN_EVENT_CLASS_KEY_ID)) {
            int eventClassKeyId = rs.getInt(COLUMN_EVENT_CLASS_KEY_ID);
            if (!rs.wasNull()) {
                eventBuilder.setEventClassKey(daoCache
                        .getEventClassKeyFromId(eventClassKeyId));
            }
        }

        if (fields.contains(COLUMN_EVENT_KEY_ID)) {
            int eventKeyId = rs.getInt(COLUMN_EVENT_KEY_ID);
            if (!rs.wasNull()) {
                eventBuilder
                        .setEventKey(daoCache.getEventKeyFromId(eventKeyId));
            }
        }

        if (fields.contains(COLUMN_EVENT_CLASS_MAPPING_UUID)) {
            byte[] classMappingUuid = rs
                    .getBytes(COLUMN_EVENT_CLASS_MAPPING_UUID);
            if (classMappingUuid != null) {
                eventBuilder.setEventClassMappingUuid(DaoUtils
                        .uuidFromBytes(classMappingUuid));
            }
        }

        if (fields.contains(COLUMN_SEVERITY_ID)) {
            eventBuilder.setSeverity(EventSeverity.valueOf(rs
                    .getInt(COLUMN_SEVERITY_ID)));
        }

        EventActor actor = deserializeEventActor(rs, fields);
        if (actor != null) {
            eventBuilder.setActor(actor);
        }

        if (fields.contains(COLUMN_MONITOR_ID)) {
            int monitorId = rs.getInt(COLUMN_MONITOR_ID);
            if (!rs.wasNull()) {
                eventBuilder.setMonitor(daoCache.getMonitorFromId(monitorId));
            }
        }

        if (fields.contains(COLUMN_AGENT_ID)) {
            int agentId = rs.getInt(COLUMN_AGENT_ID);
            if (!rs.wasNull()) {
                eventBuilder.setAgent(daoCache.getAgentFromId(agentId));
            }
        }

        if (fields.contains(COLUMN_SYSLOG_FACILITY)) {
            int syslogFacility = rs.getInt(COLUMN_SYSLOG_FACILITY);
            if (!rs.wasNull()) {
                eventBuilder.setSyslogFacility(syslogFacility);
            }
        }

        if (fields.contains(COLUMN_SYSLOG_PRIORITY)) {
            int syslogPriority = rs.getInt(COLUMN_SYSLOG_PRIORITY);
            if (!rs.wasNull()) {
                eventBuilder.setSyslogPriority(EventDaoUtils
                        .syslogPriorityFromInt(syslogPriority));
            }
        }

        if (fields.contains(COLUMN_NT_EVENT_CODE)) {
            int ntEventCode = rs.getInt(COLUMN_NT_EVENT_CODE);
            if (!rs.wasNull()) {
                eventBuilder.setNtEventCode(ntEventCode);
            }
        }

        if (fields.contains(COLUMN_SUMMARY)) {
            eventBuilder.setSummary(rs.getString(COLUMN_SUMMARY));
        }

        if (fields.contains(COLUMN_MESSAGE)) {
            eventBuilder.setMessage(rs.getString(COLUMN_MESSAGE));
        }

        if (fields.contains(COLUMN_DETAILS_JSON)) {
            String json = rs.getString(COLUMN_DETAILS_JSON);
            if (json != null && !json.isEmpty()) {
                try {
                    List<EventDetail> details = JsonFormat
                            .mergeAllDelimitedFrom(json,
                                    EventDetail.getDefaultInstance());
                    eventBuilder.addAllDetails(details);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }
        }

        if (fields.contains(COLUMN_TAGS_JSON)) {
            String json = rs.getString(COLUMN_TAGS_JSON);
            if (json != null && !json.isEmpty()) {
                try {
                    List<EventTag> tags = JsonFormat.mergeAllDelimitedFrom(
                            json, EventTag.getDefaultInstance());
                    eventBuilder.addAllTags(tags);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }
        }

        return eventBuilder.build();
    }

    private EventActor deserializeEventActor(ResultSet rs, Set<String> fields)
            throws SQLException {
        byte[] elementUuid = null;
        ModelElementType elementType = null;
        String elementIdentifier = null;
        byte[] subUuid = null;
        ModelElementType subType = null;
        String subIdentifier = null;

        if (fields.contains(COLUMN_ELEMENT_UUID)) {
            elementUuid = rs.getBytes(COLUMN_ELEMENT_UUID);
        }
        if (fields.contains(COLUMN_ELEMENT_TYPE_ID)) {
            int elementTypeId = rs.getInt(COLUMN_ELEMENT_TYPE_ID);
            if (!rs.wasNull()) {
                elementType = ModelElementType.valueOf(elementTypeId);
            }
        }
        if (fields.contains(COLUMN_ELEMENT_IDENTIFIER)) {
            elementIdentifier = rs.getString(COLUMN_ELEMENT_IDENTIFIER);
        }

        if (fields.contains(COLUMN_ELEMENT_SUB_UUID)) {
            subUuid = rs.getBytes(COLUMN_ELEMENT_SUB_UUID);
        }
        if (fields.contains(COLUMN_ELEMENT_SUB_TYPE_ID)) {
            int subTypeId = rs.getInt(COLUMN_ELEMENT_SUB_TYPE_ID);
            if (!rs.wasNull()) {
                subType = ModelElementType.valueOf(subTypeId);
            }
        }
        if (fields.contains(COLUMN_ELEMENT_SUB_IDENTIFIER)) {
            subIdentifier = rs.getString(COLUMN_ELEMENT_SUB_IDENTIFIER);
        }

        final EventActor actor;
        if (elementUuid == null && elementType == null
                && elementIdentifier == null && subUuid == null
                && subType == null && subIdentifier == null) {
            actor = null;
        } else {
            EventActor.Builder actorBuilder = EventActor.newBuilder();
            if (elementUuid != null) {
                actorBuilder
                        .setElementUuid(DaoUtils.uuidFromBytes(elementUuid));
            }
            if (elementType != null) {
                actorBuilder.setElementTypeId(elementType);
            }
            if (elementIdentifier != null) {
                actorBuilder.setElementIdentifier(elementIdentifier);
            }

            if (subUuid != null) {
                actorBuilder.setElementSubUuid(DaoUtils.uuidFromBytes(subUuid));
            }
            if (subType != null) {
                actorBuilder.setElementSubTypeId(subType);
            }
            if (subIdentifier != null) {
                actorBuilder.setElementSubIdentifier(subIdentifier);
            }
            actor = actorBuilder.build();
        }
        return actor;
    }

    public static int addNote(String tableName, String uuid, EventNote note,
            SimpleJdbcTemplate template) throws ZepException {
        EventNote.Builder builder = EventNote.newBuilder(note);
        if (builder.getUuid().isEmpty()) {
            builder.setUuid(UUID.randomUUID().toString());
        }
        builder.setCreatedTime(System.currentTimeMillis());
        try {
            // Notes are expected to be returned in reverse order
            final String sql = String
                    .format("UPDATE %s SET notes_json=CONCAT_WS(',\n',?,notes_json) WHERE uuid=?",
                            tableName);
            return template.update(sql,
                    JsonFormat.writeAsString(builder.build()),
                    DaoUtils.uuidToBytes(uuid));
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    public List<Integer> getSeverityIdsLessThan(Zep.EventSeverity severity) {
        final List<Integer> severityIds = new ArrayList<Integer>(
                ZepUtils.ORDERED_SEVERITIES.size() - 1);
        for (Zep.EventSeverity orderedSeverity : ZepUtils.ORDERED_SEVERITIES) {
            if (orderedSeverity == severity) {
                break;
            }
            severityIds.add(orderedSeverity.getNumber());
        }
        return severityIds;
    }
}
