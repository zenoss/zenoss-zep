/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, 2014 all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.simple.SimpleJdbcOperations;
import org.zenoss.protobufs.JsonFormat;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventDetail.EventDetailMergeBehavior;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.protobufs.zep.Zep.EventTag.Builder;
import org.zenoss.protobufs.zep.Zep.SyslogPriority;
import org.zenoss.utils.dao.Partition;
import org.zenoss.utils.dao.RangePartitioner;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepConfigService;
import org.zenoss.zep.ZepConstants;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepUtils;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.dao.DaoCache;
import org.zenoss.zep.dao.EventBatch;
import org.zenoss.zep.dao.EventBatchParams;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.TypeConverter;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.zenoss.zep.dao.impl.EventConstants.*;

public class EventDaoHelper {

    private DaoCache daoCache;
    private UUIDGenerator uuidGenerator;
    private DatabaseCompatibility databaseCompatibility;
    private TypeConverter<String> uuidConverter;
    private ZepConfigService zepConfigService;


    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(EventDaoHelper.class);

    public EventDaoHelper() {
    }
    
    public void setZepConfigService(ZepConfigService zepConfigService) throws ZepException {
        this.zepConfigService = zepConfigService;
    }

    public void setDaoCache(DaoCache daoCache) {
        this.daoCache = daoCache;
    }


    public void setUuidGenerator(UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    public void setDatabaseCompatibility(DatabaseCompatibility databaseCompatibility) {
        this.databaseCompatibility = databaseCompatibility;
        this.uuidConverter = databaseCompatibility.getUUIDConverter();
    }

    private boolean isValidDetailsSize(String str, long eventMaxSizeBytes) throws ZepException {
        return str.length() * 2 < eventMaxSizeBytes;
    }

    private List<EventDetail> removeUnimportantDetails(List<EventDetail> details) {
        List<EventDetail> cleanDetails = new ArrayList<EventDetail>();
        for (EventDetail detail : details) {
            String name = detail.getName();
            if (name.startsWith("zenoss.") || name.startsWith("impact.") || name.startsWith("__meta__.impact.")) {
                cleanDetails.add(detail);
            }
        }
        return cleanDetails;
    }

    public Map<String, Object> createOccurrenceFields(Event event) throws ZepException {
        Map<String, Object> fields = new HashMap<String, Object>();

        String fingerprint = DaoUtils.truncateStringToUtf8(event.getFingerprint(), MAX_FINGERPRINT);
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

        Object eventClassMappingUuid = null;
        if (!event.getEventClassMappingUuid().isEmpty()) {
            eventClassMappingUuid = uuidConverter.toDatabaseType(event.getEventClassMappingUuid());
        }
        fields.put(COLUMN_EVENT_CLASS_MAPPING_UUID, eventClassMappingUuid);

        fields.put(COLUMN_SEVERITY_ID, event.getSeverity().getNumber());

        if (event.hasActor()) {
            populateEventActorFields(event.getActor(), fields);
        }

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
            syslogPriority = event.getSyslogPriority().getNumber();
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

                // if the detailsJson string is too big, filter out non-important
                // details and try again.
                long eventMaxSizeBytes = zepConfigService.getConfig().getEventMaxSizeBytes();
                if (!isValidDetailsSize(detailsJson, eventMaxSizeBytes)) {
                    detailsJson = JsonFormat.writeAllDelimitedAsString(
                            mergeDuplicateDetails(removeUnimportantDetails(event.getDetailsList())));

                    if (!isValidDetailsSize(detailsJson, eventMaxSizeBytes)) {
                        // TODO: What to do when we can't reduce the size enough?
                        logger.warn("Could not reduce event size below event_max_size_bytes setting: " +
                                zepConfigService.getConfig().getEventMaxSizeBytes() + " Event: " + event);
                    }
                }


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

    /**
     * Removes duplicate tags from the event.
     *
     * @param event
     *            Original event.
     * @return New event with duplicate tags removed.
     */
    public static List<EventTag> buildTags(Event event) {
        final Map<String,EventTag.Builder> tagTypesMap = new TreeMap<String,Builder>();
        final Set<String> uuids = new HashSet<String>();
        for (EventTag tag : event.getTagsList()) {
            EventTag.Builder tagBuilder = tagTypesMap.get(tag.getType());
            if (tagBuilder == null) {
                tagBuilder = EventTag.newBuilder();
                tagBuilder.setType(tag.getType());
                tagTypesMap.put(tag.getType(), tagBuilder);
            }
            for (String tagUuid : tag.getUuidList()) {
                if (uuids.add(tagUuid)) {
                    tagBuilder.addUuid(tagUuid);
                }
            }
        }
        final List<EventTag> tags = new ArrayList<EventTag>(tagTypesMap.size());
        for (EventTag.Builder tagBuilder : tagTypesMap.values()) {
            if (tagBuilder.getUuidCount() > 0) {
                tags.add(tagBuilder.build());
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
     * @return A JSON string of the details aftering merging.
     * @throws org.zenoss.zep.ZepException X
     */
    public String mergeDetailsToJson(List<EventDetail> oldDetails, List<EventDetail> newDetails) throws ZepException {

        Map<String, EventDetail> detailsMap = mergeDetails(oldDetails, newDetails);

        try {
            String results = JsonFormat.writeAllDelimitedAsString(detailsMap.values());
            long eventMaxSizeBytes = zepConfigService.getConfig().getEventMaxSizeBytes();
            if (!isValidDetailsSize(results, eventMaxSizeBytes)) {
                final String newDetailsJson = JsonFormat.writeAllDelimitedAsString(newDetails);
                if (isValidDetailsSize(newDetailsJson, eventMaxSizeBytes)) {
                    // new details are a valid size, truncate the old and use the new
                    results = newDetailsJson;
                    logger.warn("Truncating old details because details are not a valid size: " + oldDetails);
                }
                else {
                    // If the entire set of new details is not small enough,
                    // truncate all non-zenoss and non-impact details.
                    final String originalResults = results;
                    final List<EventDetail> newZenossDetails = removeUnimportantDetails(newDetails);
                    results = JsonFormat.writeAllDelimitedAsString(newZenossDetails);
                    logger.warn("Truncating old details because details are not a valid size. " +
                            "New non-Zenoss details have also been truncated due to size. " +
                            "ORIGINAL DATA: " + originalResults);
                }
            }

            return results;
        } catch (IOException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }

    public static Map<String, EventDetail> mergeDetails(List<EventDetail> oldDetails, List<EventDetail> newDetails) {

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
        return detailsMap;
    }

    private void populateEventActorFields(EventActor actor, Map<String, Object> fields) {
        if (!actor.getElementUuid().isEmpty()) {
            fields.put(COLUMN_ELEMENT_UUID, uuidConverter.toDatabaseType(actor.getElementUuid()));
        }
        if (actor.hasElementTypeId()) {
            fields.put(COLUMN_ELEMENT_TYPE_ID, actor.getElementTypeId().getNumber());
        }

        if (actor.hasElementIdentifier()) {
            final String elementId = DaoUtils.truncateStringToUtf8(actor.getElementIdentifier(),
                    MAX_ELEMENT_IDENTIFIER);
            fields.put(COLUMN_ELEMENT_IDENTIFIER, elementId);
        }

        if (actor.hasElementTitle()) {
            final String elementTitle = DaoUtils.truncateStringToUtf8(actor.getElementTitle(), MAX_ELEMENT_TITLE);
            fields.put(COLUMN_ELEMENT_TITLE, elementTitle);
        }

        if (!actor.getElementSubUuid().isEmpty()) {
            fields.put(COLUMN_ELEMENT_SUB_UUID, uuidConverter.toDatabaseType(actor.getElementSubUuid()));
        }
        if (actor.hasElementSubTypeId()) {
            fields.put(COLUMN_ELEMENT_SUB_TYPE_ID, actor.getElementSubTypeId().getNumber());
        }

        if (actor.hasElementSubIdentifier()) {
            final String elementSubId = DaoUtils.truncateStringToUtf8(actor.getElementSubIdentifier(),
                    MAX_ELEMENT_SUB_IDENTIFIER);
            fields.put(COLUMN_ELEMENT_SUB_IDENTIFIER, elementSubId);
        }

        if (actor.hasElementSubTitle()) {
            final String elementSubTitle = DaoUtils.truncateStringToUtf8(actor.getElementSubTitle(),
                    MAX_ELEMENT_SUB_TITLE);
            fields.put(COLUMN_ELEMENT_SUB_TITLE, elementSubTitle);
        }
    }

    public Event eventMapper(ResultSet rs, final boolean isArchive) throws SQLException {
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        Event.Builder eventBuilder = Event.newBuilder();

        eventBuilder.setCreatedTime(timestampConverter.fromDatabaseType(rs, COLUMN_LAST_SEEN));
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

        String eventClassMappingUuid = uuidConverter.fromDatabaseType(rs, COLUMN_EVENT_CLASS_MAPPING_UUID);
        if (eventClassMappingUuid != null) {
            eventBuilder.setEventClassMappingUuid(eventClassMappingUuid);
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
            eventBuilder.setSyslogPriority(SyslogPriority.valueOf(syslogPriority));
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
        if (isArchive == true) {
            EventDetail archiveDetail = EventDetail.newBuilder().setName("is_archive").addValue("true").build();
            eventBuilder.addDetails(archiveDetail);
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
        final EventActor.Builder actorBuilder = EventActor.newBuilder();
        String elementUuid = uuidConverter.fromDatabaseType(rs, COLUMN_ELEMENT_UUID);
        if (elementUuid != null) {
            actorBuilder.setElementUuid(elementUuid);
        }

        final int elementTypeId = rs.getInt(COLUMN_ELEMENT_TYPE_ID);
        if (!rs.wasNull()) {
            actorBuilder.setElementTypeId(ModelElementType.valueOf(elementTypeId));
        }

        final String elementIdentifier = rs.getString(COLUMN_ELEMENT_IDENTIFIER);
        if (elementIdentifier != null) {
            actorBuilder.setElementIdentifier(elementIdentifier);
        }

        final String elementTitle = rs.getString(COLUMN_ELEMENT_TITLE);
        if (elementTitle != null) {
            actorBuilder.setElementTitle(elementTitle);
        }
        // titleOrId
        else if (elementIdentifier != null) {
            actorBuilder.setElementTitle(elementIdentifier);
        }

        String subUuid = uuidConverter.fromDatabaseType(rs, COLUMN_ELEMENT_SUB_UUID);
        if (subUuid != null) {
            actorBuilder.setElementSubUuid(subUuid);
        }

        final int subTypeId = rs.getInt(COLUMN_ELEMENT_SUB_TYPE_ID);
        if (!rs.wasNull()) {
            actorBuilder.setElementSubTypeId(ModelElementType.valueOf(subTypeId));
        }

        final String subIdentifier = rs.getString(COLUMN_ELEMENT_SUB_IDENTIFIER);
        if (subIdentifier != null) {
            actorBuilder.setElementSubIdentifier(subIdentifier);
        }

        final String subTitle = rs.getString(COLUMN_ELEMENT_SUB_TITLE);
        if (subTitle != null) {
            actorBuilder.setElementSubTitle(subTitle);
        }
        // titleOrId
        else if (subIdentifier != null) {
            actorBuilder.setElementSubTitle(subIdentifier);
        }
        return actorBuilder.build();
    }

    public int addNote(String tableName, String uuid, EventNote note, SimpleJdbcOperations template)
            throws ZepException {
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        EventNote.Builder builder = EventNote.newBuilder(note);
        if (builder.getUuid().isEmpty()) {
            builder.setUuid(this.uuidGenerator.generate().toString());
        }
        builder.setCreatedTime(System.currentTimeMillis());
        try {
            Map<String,Object> fields = new HashMap<String,Object>();
            fields.put(COLUMN_UPDATE_TIME, timestampConverter.toDatabaseType(System.currentTimeMillis()));
            fields.put(COLUMN_UUID, uuidConverter.toDatabaseType(uuid));

            // Get the current notes (if any)
            final String querySql = "SELECT notes_json FROM " + tableName + " WHERE uuid=:uuid FOR UPDATE";
            final String currentNoteJson;
            try {
                currentNoteJson = template.queryForObject(querySql, String.class, fields);
            } catch (EmptyResultDataAccessException e) {
                // If the event doesn't exist, we return 0 as the number of affected rows
                return 0;
            }

            // Prepend the new note
            final StringBuilder newNoteJson = new StringBuilder(JsonFormat.writeAsString(builder.build()));
            if (currentNoteJson != null) {
                newNoteJson.append(",\n").append(currentNoteJson);
            }
            fields.put(COLUMN_NOTES_JSON, newNoteJson.toString());
            try {
                final String updateSql = "UPDATE " + tableName + " SET update_time=:update_time,notes_json=:notes_json" +
                                         " WHERE uuid=:uuid";
                return template.update(updateSql, fields);
            } catch (DataIntegrityViolationException e) {
                int sizeOfNotes = newNoteJson.length();
                logger.warn(" Truncating the NotesJson field value to half to avoid DataIntegrityExceptions");
                final StringBuilder updatedNoteJson;
                updatedNoteJson = newNoteJson.delete(0,newNoteJson.indexOf(",\n",sizeOfNotes/2)+1);
                fields.put(COLUMN_NOTES_JSON, updatedNoteJson.toString());
                final String updateSql = "UPDATE " + tableName + " SET update_time=:update_time,notes_json=:notes_json" +
                            " WHERE uuid=:uuid";
                return template.update(updateSql, fields);

            }
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    public int updateDetails(String tableName, String uuid, List<EventDetail> details, SimpleJdbcOperations template)
            throws ZepException {
        Map<String, Object> fields = new HashMap<String, Object>();

        fields.put(COLUMN_UUID, uuidConverter.toDatabaseType(uuid));
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
        final String updatedDetailsJson = mergeDetailsToJson(existingDetailList, details);
        fields.put(COLUMN_DETAILS_JSON, updatedDetailsJson);

        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        fields.put(COLUMN_UPDATE_TIME, timestampConverter.toDatabaseType(System.currentTimeMillis()));

        String updateSql = "UPDATE " + tableName + " SET details_json=:details_json, "
                + "update_time=:update_time WHERE uuid = :uuid";

        return template.update(updateSql, fields);
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

    @TransactionalReadOnly
    private List<EventSummary> listBatch(SimpleJdbcOperations template, String tableName,
                                        String startingUuid, long maxUpdateTime, int limit, EventSummaryRowMapper esrm)
            throws ZepException {
        final String sql;
        final Map<String,Object> fields = new HashMap<String,Object>();
        fields.put("_max_update_time", databaseCompatibility.getTimestampConverter().toDatabaseType(maxUpdateTime));
        fields.put("_limit", limit);
        if (startingUuid == null) {
            sql = "SELECT * FROM " + tableName + " WHERE update_time <= :_max_update_time ORDER BY uuid LIMIT :_limit";
        }
        else {
            fields.put("_starting_uuid", uuidConverter.toDatabaseType(startingUuid));
            sql = "SELECT * FROM " + tableName + " WHERE uuid > :_starting_uuid AND update_time <= :_max_update_time " +
                    "ORDER BY uuid LIMIT :_limit";
        }
        return template.query(sql, esrm, fields);
    }

    @TransactionalReadOnly
    public EventBatch listBatch(SimpleJdbcOperations template, String tableName, RangePartitioner partitioner,
                                        EventBatchParams batchParams, long maxUpdateTime, int limit,
                                        EventSummaryRowMapper esrm)
            throws ZepException {
        if (partitioner == null) {
            List<EventSummary> events = listBatch(template, tableName, batchParams == null ? null : batchParams.nextUuid, maxUpdateTime, limit, esrm);
            if (events.isEmpty()) {
                return new EventBatch(events, Long.MIN_VALUE, null);
            } else {
                return new EventBatch(events, Long.MIN_VALUE, Iterables.getLast(events).getUuid());
            }
        } else {
            final Object maxUpdateTimeObject = databaseCompatibility.getTimestampConverter().toDatabaseType(maxUpdateTime);
            List<EventSummary> events = new ArrayList<EventSummary>(limit);
            long nextLastSeen = (batchParams == null) ? Long.MAX_VALUE : batchParams.nextLastSeen;
            String nextUuid = (batchParams == null) ? null : batchParams.nextUuid;
            for (Partition p : Lists.reverse(partitioner.listPartitions())) {
                if (p.getRangeMinimum() != null) {
                    long partitionMin = p.getRangeMinimum().getTime();
                    if (partitionMin > nextLastSeen)
                        continue;
                    else if (partitionMin < nextLastSeen)
                        nextLastSeen = partitionMin;
                    events.addAll(listBatchInPartition(template, tableName, p, nextUuid, maxUpdateTimeObject, limit - events.size(), esrm));
                    if (events.size() >= limit) {
                        nextUuid = Iterables.getLast(events).getUuid();
                        break;
                    } else {
                        nextUuid = null;
                    }
                } else {
                    nextLastSeen = Long.MIN_VALUE;
                    events.addAll(listBatchInPartition(template, tableName, p, nextUuid, maxUpdateTimeObject, limit - events.size(), esrm));
                    if (events.size() >= limit) {
                        nextUuid = Iterables.getLast(events).getUuid();
                        break;
                    } else {
                        nextUuid = null;
                    }
                }
            }
            return new EventBatch(events, nextLastSeen, nextUuid);
        }
    }

    @TransactionalReadOnly
    private List<EventSummary> listBatchInPartition(SimpleJdbcOperations template, String tableName,
                                                   Partition partition, String nextUuid, Object maxUpdateTime,
                                                   int limit, EventSummaryRowMapper esrm)
        throws ZepException {
        final Map<String,Object> fields = new HashMap<String,Object>();
        final StringBuffer sql = new StringBuffer();
        sql.append("SELECT * FROM ");
        sql.append(tableName);
        sql.append(" WHERE update_time <= :_max_update_time");
        fields.put("_max_update_time", maxUpdateTime);

        if (partition.getRangeMinimum() != null) {
            sql.append(" AND last_seen >= :_range_min");
            fields.put("_range_min", partition.getRangeMinimum().getTime());
        }

        if (partition.getRangeLessThan() != null) {
            sql.append(" AND last_seen <= :_range_max");
            fields.put("_range_max", partition.getRangeLessThan().getTime());
        }

        if (nextUuid != null) {
            sql.append(" AND uuid > :_starting_uuid");
            fields.put("_starting_uuid", uuidConverter.toDatabaseType(nextUuid));
        }

        sql.append(" ORDER BY uuid LIMIT :_limit");
        fields.put("_limit", limit);
        return template.query(sql.toString(), esrm, fields);
    }



    /**
     * Adds the {@link ZepConstants#DETAIL_MIGRATE_UPDATE_TIME} detail to the event occurrence.
     *
     * @param eventBuilder Event builder.
     * @param updateTime   Update time to use as value for event detail.
     */
    public static void addMigrateUpdateTimeDetail(Event.Builder eventBuilder, long updateTime) {
        // Add migrate_update_time detail
        final int detailsCount = eventBuilder.getDetailsCount();
        for (int i = 0; i < detailsCount; i++) {
            EventDetail detail = eventBuilder.getDetails(i);
            // Clear out existing detail if it exists
            if (ZepConstants.DETAIL_MIGRATE_UPDATE_TIME.equals(detail.getName())) {
                eventBuilder.getDetailsBuilder(i).clearValue();
            }
        }
        eventBuilder.addDetails(EventDetail.newBuilder().setName(ZepConstants.DETAIL_MIGRATE_UPDATE_TIME)
                .addValue(Long.toString(updateTime)));
    }

    private static String collectionToJsonDelimited(List<? extends Message> messages) throws ZepException {
        if (messages.isEmpty()) {
            return null;
        }
        final StringBuilder sb = new StringBuilder();
        try {
            for (Iterator<? extends Message> it = messages.iterator(); it.hasNext(); ) {
                sb.append(JsonFormat.writeAsString(it.next()));
                if (it.hasNext()) {
                    sb.append(",\n");
                }
            }
        } catch (IOException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
        return sb.toString();
    }

    /**
     * Creates a map of column names to values suitable for inserting into the event_summary and event_archive tables.
     *
     * @param summary Event summary to be created.
     * @return Map of field names to values.
     * @throws ZepException If the summary can't be serialized.
     */
    public Map<String,Object> createImportedSummaryFields(EventSummary summary) throws ZepException {
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        final Map<String, Object> fields = createOccurrenceFields(summary.getOccurrence(0));
        fields.put(COLUMN_UUID, uuidConverter.toDatabaseType(summary.getUuid()));
        fields.put(COLUMN_STATUS_ID, summary.getStatus().getNumber());
        fields.put(COLUMN_UPDATE_TIME, timestampConverter.toDatabaseType(summary.getUpdateTime()));
        fields.put(COLUMN_FIRST_SEEN, timestampConverter.toDatabaseType(summary.getFirstSeenTime()));
        fields.put(COLUMN_STATUS_CHANGE, timestampConverter.toDatabaseType(summary.getStatusChangeTime()));
        fields.put(COLUMN_LAST_SEEN, timestampConverter.toDatabaseType(summary.getLastSeenTime()));
        fields.put(COLUMN_EVENT_COUNT, summary.getCount());
        if (summary.hasCurrentUserUuid()) {
            fields.put(COLUMN_CURRENT_USER_UUID, uuidConverter.toDatabaseType(summary.getCurrentUserUuid()));
        }
        if (summary.hasCurrentUserName()) {
            fields.put(COLUMN_CURRENT_USER_NAME, summary.getCurrentUserName());
        }
        if (summary.hasClearedByEventUuid()) {
            fields.put(COLUMN_CLEARED_BY_EVENT_UUID, uuidConverter.toDatabaseType(summary.getClearedByEventUuid()));
        }
        fields.put(COLUMN_NOTES_JSON, EventDaoHelper.collectionToJsonDelimited(summary.getNotesList()));
        fields.put(COLUMN_AUDIT_JSON, EventDaoHelper.collectionToJsonDelimited(summary.getAuditLogList()));
        return fields;
    }

}
