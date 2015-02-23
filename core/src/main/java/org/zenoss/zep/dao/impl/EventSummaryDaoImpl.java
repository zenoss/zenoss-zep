/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2012, 2014 all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcOperations;
import org.springframework.jdbc.support.MetaDataAccessException;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.StringUtils;
import org.zenoss.protobufs.JsonFormat;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.*;
import org.zenoss.zep.Counters;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepConstants;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventBatch;
import org.zenoss.zep.dao.EventBatchParams;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.dao.impl.compat.*;
import org.zenoss.zep.plugins.EventPreCreateContext;

import javax.sql.DataSource;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.zenoss.zep.dao.impl.EventConstants.*;

public class EventSummaryDaoImpl implements EventSummaryDao {

    private static final Logger logger = LoggerFactory.getLogger(EventSummaryDaoImpl.class);

    @Autowired
    protected MetricRegistry metricRegistry;

    private final ConcurrentMap<String,List<Event>> deduping = new ConcurrentHashMap<String, List<Event>>();

    private final DataSource dataSource;

    private final SimpleJdbcOperations template;

    private final SimpleJdbcInsert insert;

    private volatile List<String> archiveColumnNames;

    private EventDaoHelper eventDaoHelper;

    private UUIDGenerator uuidGenerator;

    private DatabaseCompatibility databaseCompatibility;

    private TypeConverter<String> uuidConverter;

    private NestedTransactionService nestedTransactionService;

    private RowMapper<EventSummary.Builder> eventDedupMapper;

    private Counters counters;

    public EventSummaryDaoImpl(DataSource dataSource) throws MetaDataAccessException {
        this.dataSource = dataSource;
        this.template = (SimpleJdbcOperations) Proxy.newProxyInstance(SimpleJdbcOperations.class.getClassLoader(), 
        		new Class<?>[] {SimpleJdbcOperations.class}, new SimpleJdbcTemplateProxy(dataSource));
        this.insert = new SimpleJdbcInsert(dataSource).withTableName(TABLE_EVENT_SUMMARY);
    }

    public void setEventDaoHelper(EventDaoHelper eventDaoHelper) {
        this.eventDaoHelper = eventDaoHelper;
    }

    public void setUuidGenerator(UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    public void setDatabaseCompatibility(final DatabaseCompatibility databaseCompatibility) {
        this.databaseCompatibility = databaseCompatibility;
        this.uuidConverter = databaseCompatibility.getUUIDConverter();

        // When we perform de-duping of events, we select a subset of just the fields we care about to determine
        // the de-duping behavior (depending on the timestamps on the event, we may perform merging or update either
        // the first_seen or last_seen dates appropriately). This mapper converts the subset of fields to an
        // EventSummaryOrBuilder object which has convenient accessor methods to retrieve the fields by name.
        this.eventDedupMapper = new RowMapper<EventSummary.Builder>() {
            @Override
            public EventSummary.Builder mapRow(ResultSet rs, int rowNum) throws SQLException {
                final TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
                final EventSummary.Builder oldSummaryBuilder = EventSummary.newBuilder();
                oldSummaryBuilder.setCount(rs.getInt(COLUMN_EVENT_COUNT));
                oldSummaryBuilder.setFirstSeenTime(timestampConverter.fromDatabaseType(rs, COLUMN_FIRST_SEEN));
                oldSummaryBuilder.setLastSeenTime(timestampConverter.fromDatabaseType(rs, COLUMN_LAST_SEEN));
                oldSummaryBuilder.setStatus(EventStatus.valueOf(rs.getInt(COLUMN_STATUS_ID)));
                oldSummaryBuilder.setStatusChangeTime(timestampConverter.fromDatabaseType(rs, COLUMN_STATUS_CHANGE));
                oldSummaryBuilder.setUuid(uuidConverter.fromDatabaseType(rs, COLUMN_UUID));

                final Event.Builder occurrenceBuilder = oldSummaryBuilder.addOccurrenceBuilder(0);
                final String detailsJson = rs.getString(COLUMN_DETAILS_JSON);
                if (detailsJson != null) {
                    try {
                        occurrenceBuilder.addAllDetails(JsonFormat.mergeAllDelimitedFrom(detailsJson,
                                EventDetail.getDefaultInstance()));
                    } catch (IOException e) {
                        throw new SQLException(e.getLocalizedMessage(), e);
                    }
                }
                return oldSummaryBuilder;
            }
        };
    }

    public void setNestedTransactionService(NestedTransactionService nestedTransactionService) {
        this.nestedTransactionService = nestedTransactionService;
    }

    public void setCounters(final Counters counters) {
        this.counters = counters;
    }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public String create(Event event, final EventPreCreateContext context) throws ZepException {

        /*
         * Clear events are dropped if they don't clear any corresponding events.
         */
        final List<String> clearedEventUuids;
        final boolean createClearHash;
        if (event.getSeverity() == EventSeverity.SEVERITY_CLEAR) {
            clearedEventUuids = clearEvents(event, context);
            if (clearedEventUuids.isEmpty()) {
                logger.debug("Clear event didn't clear any events, dropping: {}", event);
                return null;
            }
            // Clear events always get created in CLOSED status
            if (event.getStatus() != EventStatus.STATUS_CLOSED) {
                event = Event.newBuilder(event).setStatus(EventStatus.STATUS_CLOSED).build();
            }
            createClearHash = false;
        }
        else {
            createClearHash = true;
            clearedEventUuids = Collections.emptyList();
        }

        /*
         * Closed events have a unique fingerprint_hash in summary to allow multiple rows
         * but only allow one active event (where the de-duplication occurs).
         */
        final String fingerprint = DaoUtils.truncateStringToUtf8(event.getFingerprint(), MAX_FINGERPRINT);
        final byte[] fingerprintHash;
        final String uuid;
        if (ZepConstants.CLOSED_STATUSES.contains(event.getStatus())) {
            fingerprintHash = DaoUtils.sha1(fingerprint + '|' + System.currentTimeMillis());
            uuid = saveEventByFingerprint(fingerprintHash, Collections.singleton(event), context, createClearHash);
        }
        else {
            fingerprintHash = DaoUtils.sha1(fingerprint);
            final String hashAsString = new String(fingerprintHash).intern();
            final Event finalEvent = event;
            try {
                metricRegistry.timer(getClass().getName() + ".queueDedup").time(new Callable() {
                    @Override
                    public Object call() throws Exception {
                        boolean queued = false;
                        while (!queued) {
                            List<Event> events = deduping.get(hashAsString);
                            if (events == null) {
                                deduping.putIfAbsent(hashAsString, Collections.EMPTY_LIST);
                                continue;
                            }
                            List<Event> newEvents = Lists.newArrayList(events);
                            newEvents.add(finalEvent);
                            queued = deduping.replace(hashAsString, events, newEvents);
                        }
                        return null;
                    }
                });
            } catch (Exception e) {
                throw new ZepException(e);
            }

            try {
                uuid = metricRegistry.timer(getClass().getName() + ".dedupSync").time(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        synchronized (hashAsString) {
                            List<Event> events = deduping.remove(hashAsString);
                            if (events == null)
                                events = Collections.EMPTY_LIST;
                            return saveEventByFingerprint(fingerprintHash, events, context, createClearHash);
                        }
                    }
                });
            } catch (Exception e) {
                throw new ZepException(e);
            }
        }

        try {
            metricRegistry.timer(getClass().getName() + ".dedupClearEvents").time(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    // Mark cleared events as cleared by this event
                    if (!clearedEventUuids.isEmpty()) {
                        final EventSummaryUpdateFields updateFields = new EventSummaryUpdateFields();
                        updateFields.setClearedByEventUuid(uuid);
                        update(clearedEventUuids, EventStatus.STATUS_CLEARED, updateFields, ZepConstants.OPEN_STATUSES);
                    }
                    return null;  //To change body of implemented methods use File | Settings | File Templates.
                }
            });
        } catch (Exception e) {
            throw new ZepException(e);
        }
        return uuid;
    }

    private Map<String,Object> getInsertFields(EventSummaryOrBuilder summary, EventPreCreateContext context,
                                               boolean createClearHash)
            throws ZepException
    {
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        Event event = summary.getOccurrence(0);
        final Map<String, Object> fields = eventDaoHelper.createOccurrenceFields(event);
        fields.put(COLUMN_STATUS_ID, summary.getStatus().getNumber());
        fields.put(COLUMN_CLOSED_STATUS, ZepConstants.CLOSED_STATUSES.contains(summary.getStatus()));
        fields.put(COLUMN_UPDATE_TIME, timestampConverter.toDatabaseType(summary.getUpdateTime()));
        fields.put(COLUMN_FIRST_SEEN, timestampConverter.toDatabaseType(summary.getFirstSeenTime()));
        fields.put(COLUMN_STATUS_CHANGE, timestampConverter.toDatabaseType(summary.getStatusChangeTime()));
        fields.put(COLUMN_LAST_SEEN, timestampConverter.toDatabaseType(summary.getLastSeenTime()));
        fields.put(COLUMN_EVENT_COUNT, summary.getCount());

        final String createdUuid = this.uuidGenerator.generate().toString();
        fields.put(COLUMN_UUID, uuidConverter.toDatabaseType(createdUuid));
        if (createClearHash) {
            fields.put(COLUMN_CLEAR_FINGERPRINT_HASH, EventDaoUtils.createClearHash(event,
                    context.getClearFingerprintGenerator()));
        }

        return fields;
    }

    @Timed
    public String saveEventByFingerprint(final byte[] fingerprintHash, final Collection<Event> events,
                                          EventPreCreateContext context, boolean createClearHash)
            throws ZepException
    {
        final List<EventSummary.Builder> oldSummaryList = template.getJdbcOperations().query(
                "SELECT event_count,first_seen,last_seen,details_json,status_id,status_change,uuid" +
                " FROM event_summary WHERE fingerprint_hash=? FOR UPDATE",
                new RowMapperResultSetExtractor<EventSummary.Builder>(eventDedupMapper, 1),
                fingerprintHash);
        final EventSummary.Builder summary;
        final boolean deduping;
        if (!oldSummaryList.isEmpty() && !events.isEmpty()) {
            deduping = true;
            summary = oldSummaryList.get(0);
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
                @Override
                public void afterCommit() {
                    counters.addToDedupedEventCount(events.size());
                }
            });
        } else {
            summary = EventSummary.newBuilder();
            summary.setCount(0);
            summary.addOccurrenceBuilder(0);
            deduping = false;
        }

        boolean isNewer = false;
        for (Event event : events) {
            isNewer = merge(summary, event) || isNewer;
        }
        summary.setUpdateTime(System.currentTimeMillis());

        if (!events.isEmpty()) {
            if (deduping) {
                final Map<String,Object> fields = getUpdateFields(summary,isNewer, context, createClearHash);
                final StringBuilder updateSql = new StringBuilder("UPDATE event_summary SET ");
                int i = 0;
                for (String fieldName : fields.keySet()) {
                    if (++i > 1) updateSql.append(',');
                    updateSql.append(fieldName).append("=:").append(fieldName);
                }
                updateSql.append(" WHERE fingerprint_hash=:fingerprint_hash");
                fields.put("fingerprint_hash", fingerprintHash);
                template.update(updateSql.toString(), fields);
                final String indexSql = "INSERT INTO event_summary_index_queue (uuid, update_time) SELECT uuid, " +
                        String.valueOf(System.currentTimeMillis()) +
                        " FROM event_summary WHERE fingerprint_hash=:fingerprint_hash";
                template.update(indexSql, fields);
            } else {
                summary.setUuid(this.uuidGenerator.generate().toString());
                final Map<String,Object> fields = getInsertFields(summary, context, createClearHash);
                fields.put(COLUMN_FINGERPRINT_HASH, fingerprintHash);
                insert.execute(fields);
                indexSignal(summary.getUuid(), System.currentTimeMillis());
            }
        }
        return summary.getUuid();

    }

    private boolean merge(EventSummary.Builder merged, Event occurrence)
            throws ZepException
    {
        boolean isNewer = false;
        merged.setCount(merged.getCount() + occurrence.getCount());
        if (!merged.hasLastSeenTime() || occurrence.getCreatedTime() >= merged.getLastSeenTime()) {
            isNewer = true;
            merged.setLastSeenTime(occurrence.getCreatedTime());
            Event.Builder ob = merged.getOccurrenceBuilder(0);
            EventActor.Builder ab = ob.getActorBuilder();
            ob.setEventGroup(occurrence.getEventGroup());
            ob.setEventClass(occurrence.getEventClass());
            ob.setEventClassKey(occurrence.getEventClassKey());
            ob.setEventClassMappingUuid(occurrence.getEventClassMappingUuid());
            ob.setEventKey(occurrence.getEventKey());
            ob.setSeverity(occurrence.getSeverity());
            ob.setMonitor(occurrence.getMonitor());
            ob.setAgent(occurrence.getAgent());
            ob.setSyslogFacility(occurrence.getSyslogFacility());
            ob.setSyslogPriority(occurrence.getSyslogPriority());
            ob.setNtEventCode(occurrence.getNtEventCode());
            ob.setSummary(occurrence.getSummary());
            ob.setMessage(occurrence.getMessage());
            ob.clearTags();
            ob.addAllTags(occurrence.getTagsList());
            if (occurrence.getActor() != null) {
                ab.setElementUuid(occurrence.getActor().getElementUuid());
                ab.setElementTypeId(occurrence.getActor().getElementTypeId());
                ab.setElementIdentifier(occurrence.getActor().getElementIdentifier());
                ab.setElementTitle(occurrence.getActor().getElementTitle());
                ab.setElementSubUuid(occurrence.getActor().getElementSubUuid());
                ab.setElementSubTypeId(occurrence.getActor().getElementSubTypeId());
                ab.setElementSubIdentifier(occurrence.getActor().getElementSubIdentifier());
                ab.setElementSubTitle(occurrence.getActor().getElementSubTitle());
            }

            // Update status except for ACKNOWLEDGED -> {NEW|SUPPRESSED}
            // Stays in ACKNOWLEDGED in these cases
            boolean updateStatus = true;
            EventStatus oldStatus = merged.hasStatus() ? merged.getStatus() : null;
            EventStatus newStatus = occurrence.getStatus();
            if (oldStatus == EventStatus.STATUS_ACKNOWLEDGED) {
                    switch (newStatus) {
                        case STATUS_NEW:
                        case STATUS_SUPPRESSED:
                            updateStatus = false;
                            break;
                    }
            }
            if (updateStatus && oldStatus != newStatus) {
                merged.setStatus(occurrence.getStatus());
                merged.setStatusChangeTime(occurrence.getCreatedTime());
            }
            if (!merged.hasStatusChangeTime()) {
                merged.setStatusChangeTime(occurrence.getCreatedTime());
            }

            // Merge event details
            List<EventDetail> newDetails = occurrence.getDetailsList();
            if (!newDetails.isEmpty()) {
                List<EventDetail> oldDetails = ob.getDetailsList();
                if (oldDetails.isEmpty()) {
                    ob.addAllDetails(newDetails);
                } else {
                    ob.clearDetails();
                    ob.addAllDetails(eventDaoHelper.mergeDetails(oldDetails, newDetails).values());
                }
            }
        } else {
            // This is the case where the event that we're processing is OLDER
            // than the last seen time on the summary.

            // Merge event details - order swapped b/c of out of order event
            List<EventDetail> oldDetails = occurrence.getDetailsList();
            if (!oldDetails.isEmpty()) {
                Event.Builder ob = merged.getOccurrenceBuilder(0);
                List<EventDetail> newDetails = ob.getDetailsList();
                if (newDetails.isEmpty()) {
                    ob.addAllDetails(oldDetails);
                } else {
                    ob.clearDetails();
                    ob.addAllDetails(eventDaoHelper.mergeDetails(oldDetails, newDetails).values());
                }
            }
        }

        long firstSeen = occurrence.hasFirstSeenTime() ? occurrence.getFirstSeenTime() : occurrence.getCreatedTime();
        if (!merged.hasFirstSeenTime() || firstSeen < merged.getFirstSeenTime()) {
            merged.setFirstSeenTime(firstSeen);
        }
        return isNewer;
    }

    /**
     * When an event is de-duped, if the event occurrence has a created time greater than or equal to the current
     * last_seen for the event summary, these fields from the event summary row are overwritten by values from the new
     * event occurrence. Special handling is performed when de-duping for event status and event details.
     */
    private static final List<String> UPDATE_FIELD_NAMES = Arrays.asList(COLUMN_EVENT_GROUP_ID,
            COLUMN_EVENT_CLASS_ID, COLUMN_EVENT_CLASS_KEY_ID, COLUMN_EVENT_CLASS_MAPPING_UUID, COLUMN_EVENT_KEY_ID,
            COLUMN_SEVERITY_ID, COLUMN_ELEMENT_UUID, COLUMN_ELEMENT_TYPE_ID, COLUMN_ELEMENT_IDENTIFIER,
            COLUMN_ELEMENT_TITLE, COLUMN_ELEMENT_SUB_UUID, COLUMN_ELEMENT_SUB_TYPE_ID, COLUMN_ELEMENT_SUB_IDENTIFIER,
            COLUMN_ELEMENT_SUB_TITLE, COLUMN_LAST_SEEN, COLUMN_MONITOR_ID, COLUMN_AGENT_ID, COLUMN_SYSLOG_FACILITY,
            COLUMN_SYSLOG_PRIORITY, COLUMN_NT_EVENT_CODE, COLUMN_CLEAR_FINGERPRINT_HASH, COLUMN_SUMMARY, COLUMN_MESSAGE,
            COLUMN_TAGS_JSON);

    private Map<String,Object> getUpdateFields(EventSummaryOrBuilder summary, boolean isNewer,
                                               EventPreCreateContext context, boolean createClearHash)
            throws ZepException
    {
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        Map<String,Object> fields = new HashMap<String, Object>();
        Map<String,Object> insertFields = getInsertFields(summary, context, createClearHash);
        if (isNewer) {
            for (String fieldName : UPDATE_FIELD_NAMES) {
                fields.put(fieldName, insertFields.get(fieldName));
            }
            fields.put(COLUMN_STATUS_ID, insertFields.get(COLUMN_STATUS_ID));
            fields.put(COLUMN_STATUS_ID, insertFields.get(COLUMN_CLOSED_STATUS));
            fields.put(COLUMN_STATUS_CHANGE, timestampConverter.toDatabaseType(summary.getStatusChangeTime()));
        }
        fields.put(COLUMN_EVENT_COUNT, summary.getCount());
        fields.put(COLUMN_UPDATE_TIME, timestampConverter.toDatabaseType(summary.getUpdateTime()));
        fields.put(COLUMN_DETAILS_JSON, insertFields.get(COLUMN_DETAILS_JSON));
        fields.put(COLUMN_FIRST_SEEN, timestampConverter.toDatabaseType(summary.getFirstSeenTime()));
        return fields;
    }

    @Timed
    public List<String> clearEvents(Event event, EventPreCreateContext context)
            throws ZepException {
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        final List<byte[]> clearHashes = EventDaoUtils.createClearHashes(event, context);
        if (clearHashes.isEmpty()) {
            logger.debug("Clear event didn't contain any clear hashes: {}, {}", event, context);
            return Collections.emptyList();
        }
        final long lastSeen = event.getCreatedTime();
        
        Map<String,Object> fields = new HashMap<String,Object>(2);
        fields.put("_clear_created_time", timestampConverter.toDatabaseType(lastSeen));
        fields.put("_clear_hashes", clearHashes);

        long updateTime = System.currentTimeMillis();
        String indexSql = "INSERT INTO event_summary_index_queue (uuid, update_time) " 
                + "SELECT uuid, " + String.valueOf(updateTime) + " FROM event_summary " +
                "WHERE last_seen <= :_clear_created_time " +
                "AND clear_fingerprint_hash IN (:_clear_hashes) " +
                "AND closed_status = FALSE ";
        this.template.update(indexSql, fields); 

        /* Find events that this clear event would clear. */
        final String sql = "SELECT uuid FROM event_summary " + 
                "WHERE last_seen <= :_clear_created_time " +
                "AND clear_fingerprint_hash IN (:_clear_hashes) " +
                "AND closed_status = FALSE " +
                "FOR UPDATE";

        final List<String> results = this.template.query(sql, new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet rs, int rowNum) throws SQLException {
                return uuidConverter.fromDatabaseType(rs, COLUMN_UUID);
            }
        }, fields);
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
            @Override
            public void afterCommit() {
                counters.addToClearedEventCount(results.size());
            }
        });
        return results;
    }

    /**
     * When re-identifying or de-identifying events, we recalculate the clear_fingerprint_hash for the event to either
     * include (re-identify) or exclude (de-identify) the UUID of the sub_element. This mapper retrieves a subset of
     * fields for the event in order to recalculate the clear_fingerprint_hash.
     */
    private static class IdentifyMapper implements RowMapper<Map<String,Object>>
    {
        private final Map<String,Object> fields;
        private final String elementSubUuid;

        public IdentifyMapper(Map<String,Object> fields, String elementSubUuid) {
            this.fields = Collections.unmodifiableMap(fields);
            this.elementSubUuid = elementSubUuid;
        }

        @Override
        public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
            Map<String,Object> updateFields = new HashMap<String, Object>(fields);
            Event.Builder event = Event.newBuilder();
            EventActor.Builder actor = event.getActorBuilder();
            actor.setElementIdentifier(rs.getString(COLUMN_ELEMENT_IDENTIFIER));
            String elementSubIdentifier = rs.getString(COLUMN_ELEMENT_SUB_IDENTIFIER);
            if (elementSubIdentifier != null) {
                actor.setElementSubIdentifier(elementSubIdentifier);
            }
            if (this.elementSubUuid != null) {
                actor.setElementSubUuid(this.elementSubUuid);
            }
            event.setEventClass(rs.getString("event_class_name"));
            String eventKey = rs.getString("event_key_name");
            if (eventKey != null) {
                event.setEventKey(eventKey);
            }

            updateFields.put(COLUMN_UUID, rs.getObject(COLUMN_UUID));
            updateFields.put(COLUMN_CLEAR_FINGERPRINT_HASH, EventDaoUtils.createClearHash(event.build()));
            return updateFields;
        }
    }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public int reidentify(ModelElementType type, String id, String uuid, String title, String parentUuid)
            throws ZepException {
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        long updateTime = System.currentTimeMillis();

        final Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("_uuid", uuidConverter.toDatabaseType(uuid));
        fields.put("_uuid_str", uuid);
        fields.put("_type_id", type.getNumber());
        fields.put("_id", id);
        fields.put("_title", DaoUtils.truncateStringToUtf8(title, EventConstants.MAX_ELEMENT_TITLE));
        fields.put(COLUMN_UPDATE_TIME, timestampConverter.toDatabaseType(updateTime));

        String indexSql = "INSERT INTO event_summary_index_queue (uuid, update_time) " 
                + "SELECT uuid, " + String.valueOf(updateTime) + " FROM event_summary " 
                + "WHERE element_uuid IS NULL AND element_type_id=:_type_id AND element_identifier=:_id"; 
        this.template.update(indexSql, fields); 

        int numRows = 0;
        String updateSql = "UPDATE event_summary SET element_uuid=:_uuid, element_title=:_title," +
                " update_time=:update_time WHERE element_uuid IS NULL AND element_type_id=:_type_id" +
                " AND element_identifier=:_id";
        numRows += this.template.update(updateSql, fields);

        if (parentUuid != null) {
            fields.put("_parent_uuid", uuidConverter.toDatabaseType(parentUuid));
            indexSql = "INSERT INTO event_summary_index_queue (uuid, update_time) " +
                    "SELECT es.uuid, " + String.valueOf(updateTime) + " " +
                    "FROM event_summary es INNER JOIN event_class ON es.event_class_id = event_class.id " +
                    "LEFT JOIN event_key ON es.event_key_id = event_key.id " +
                    "WHERE es.element_uuid=:_parent_uuid AND es.element_sub_uuid IS NULL AND " +
                    "es.element_sub_type_id=:_type_id AND es.element_sub_identifier=:_id";
            this.template.update(indexSql, fields);

            String selectSql = "SELECT uuid,element_identifier,element_sub_identifier," +
                    "event_class.name AS event_class_name,event_key.name AS event_key_name FROM event_summary es" +
                    " INNER JOIN event_class ON es.event_class_id = event_class.id" +
                    " LEFT JOIN event_key on es.event_key_id = event_key.id" +
                    " WHERE es.element_uuid=:_parent_uuid AND es.element_sub_uuid IS NULL" +
                    " AND es.element_sub_type_id=:_type_id AND es.element_sub_identifier=:_id FOR UPDATE";
            // MySQL locks all joined rows, PostgreSQL requires you to specify the rows from each table to lock
            if (this.databaseCompatibility.getDatabaseType() == DatabaseType.POSTGRESQL) {
                selectSql += " OF es";
            }
            List<Map<String,Object>> updateFields = this.template.query(selectSql, new IdentifyMapper(fields, uuid),
                    fields);

            String updateSubElementSql = "UPDATE event_summary SET element_sub_uuid=:_uuid, " +
                    "element_sub_title=:_title, update_time=:update_time, " +
                    "clear_fingerprint_hash=:clear_fingerprint_hash WHERE uuid=:uuid";
            int[] updated = this.template.batchUpdate(updateSubElementSql,
                    updateFields.toArray(new Map[updateFields.size()]));
            for (int updatedRows : updated) {
                numRows += updatedRows;
            }
        }
        return numRows;
    }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public int deidentify(String uuid) throws ZepException {
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        long updateTime = System.currentTimeMillis();

        final Map<String,Object> fields = new HashMap<String,Object>(2);
        fields.put("_uuid", uuidConverter.toDatabaseType(uuid));
        fields.put(COLUMN_UPDATE_TIME, timestampConverter.toDatabaseType(updateTime));

        String indexSql = "INSERT INTO event_summary_index_queue (uuid, update_time) " 
                + "SELECT uuid, " + String.valueOf(updateTime) + " FROM event_summary " 
                + "WHERE element_uuid=:_uuid"; 
        this.template.update(indexSql, fields);

        int numRows = 0;
        String updateElementSql = "UPDATE event_summary SET element_uuid=NULL, update_time=:update_time" +
                " WHERE element_uuid=:_uuid";
        numRows += this.template.update(updateElementSql, fields);

        indexSql = "INSERT INTO event_summary_index_queue (uuid, update_time) " 
                + "SELECT uuid, " + String.valueOf(updateTime) + " FROM event_summary " 
                + "WHERE element_sub_uuid=:_uuid"; 
        this.template.update(indexSql, fields);

        String selectSql = "SELECT uuid,element_identifier,element_sub_identifier," +
                "event_class.name AS event_class_name,event_key.name AS event_key_name FROM event_summary es" +
                " INNER JOIN event_class ON es.event_class_id = event_class.id" +
                " LEFT JOIN event_key on es.event_key_id = event_key.id WHERE element_sub_uuid=:_uuid FOR UPDATE";
        // MySQL locks all joined rows, PostgreSQL requires you to specify the rows from each table to lock
        if (this.databaseCompatibility.getDatabaseType() == DatabaseType.POSTGRESQL) {
            selectSql += " OF es";
        }
        List<Map<String,Object>> updateFields = this.template.query(selectSql, new IdentifyMapper(fields, null),
                fields);

        String updateSubElementSql = "UPDATE event_summary SET element_sub_uuid=NULL, update_time=:update_time, " +
                "clear_fingerprint_hash=:clear_fingerprint_hash WHERE uuid=:uuid";
        int[] updated = this.template.batchUpdate(updateSubElementSql,
                updateFields.toArray(new Map[updateFields.size()]));
        for (int updatedRows : updated) {
            numRows += updatedRows;
        }
        return numRows;
    }

    @Override
    @TransactionalReadOnly
    @Timed
    public EventSummary findByUuid(String uuid) throws ZepException {
        final Map<String,Object> fields = Collections.singletonMap(COLUMN_UUID, uuidConverter.toDatabaseType(uuid));
        List<EventSummary> summaries = this.template.query("SELECT * FROM event_summary WHERE uuid=:uuid",
                new EventSummaryRowMapper(this.eventDaoHelper, this.databaseCompatibility), fields);
        return (summaries.size() > 0) ? summaries.get(0) : null;
    }

    @Override
    @Deprecated
    @Timed
    /** @deprecated use {@link #findByKey(Collection) instead}. */
    public List<EventSummary> findByUuids(final List<String> uuids) throws ZepException {
        return findByUuids((Collection)uuids);
    }

    @TransactionalReadOnly
    private List<EventSummary> findByUuids(final Collection<String> uuids)
            throws ZepException {
        if (uuids.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, List<Object>> fields = Collections.singletonMap("uuids",
                TypeConverterUtils.batchToDatabaseType(uuidConverter, uuids));
        return this.template.query("SELECT * FROM event_summary WHERE uuid IN(:uuids)",
                new EventSummaryRowMapper(this.eventDaoHelper, this.databaseCompatibility), fields);
    }

    @Override
    @TransactionalReadOnly
    @Timed
    /**
     * This implementation only makes use of the UUID field to lookup the events.
     */
    public List<EventSummary> findByKey(final Collection<EventSummary> toLookup) throws ZepException {
        if (toLookup == null || toLookup.isEmpty())
            return Collections.emptyList();
        Set<String> uuids = Sets.newHashSetWithExpectedSize(toLookup.size());
        for (EventSummary event : toLookup) uuids.add(event.getUuid());
        return findByUuids(uuids);
    }

    @Override
    @TransactionalReadOnly
    @Timed
    public EventBatch listBatch(EventBatchParams batchParams, long maxUpdateTime, int limit) throws ZepException {
        return this.eventDaoHelper.listBatch(this.template, TABLE_EVENT_SUMMARY, null, batchParams, maxUpdateTime, limit,
                new EventSummaryRowMapper(eventDaoHelper, databaseCompatibility));
    }

    private static final EnumSet<EventStatus> AUDIT_LOG_STATUSES = EnumSet.of(
            EventStatus.STATUS_NEW, EventStatus.STATUS_ACKNOWLEDGED, EventStatus.STATUS_CLOSED,
            EventStatus.STATUS_CLEARED);

    private static List<Integer> getSeverityIds(EventSeverity maxSeverity, boolean inclusiveSeverity) {
        List<Integer> severityIds = EventDaoHelper.getSeverityIdsLessThan(maxSeverity);
        if (inclusiveSeverity) {
            severityIds.add(maxSeverity.getNumber());
        }
        return severityIds;
    }

    @Override
    @Timed
    public long getAgeEligibleEventCount(long duration, TimeUnit unit, EventSeverity maxSeverity,
                                         boolean inclusiveSeverity) {
        List<Integer> severityIds = getSeverityIds(maxSeverity, inclusiveSeverity);
        // Aging disabled.
        if (severityIds.isEmpty()) {
            return 0;
        }
        String sql = "SELECT count(*) FROM event_summary WHERE closed_status = FALSE AND " +
                "last_seen < :_last_seen AND severity_id IN (:_severity_ids)";
        Map <String, Object> fields = createSharedFields(duration, unit);
        fields.put("_severity_ids", severityIds);
        return template.queryForInt(sql, fields);
    }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public int ageEvents(long agingInterval, TimeUnit unit,
                         EventSeverity maxSeverity, int limit, boolean inclusiveSeverity) throws ZepException {
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        long agingIntervalMs = unit.toMillis(agingInterval);
        if (agingIntervalMs < 0 || agingIntervalMs == Long.MAX_VALUE) {
            throw new ZepException("Invalid aging interval: " + agingIntervalMs);
        }
        if (limit <= 0) {
            throw new ZepException("Limit can't be negative: " + limit);
        }
        List<Integer> severityIds = getSeverityIds(maxSeverity, inclusiveSeverity);
        if (severityIds.isEmpty()) {
            logger.debug("Not aging events - min severity specified");
            return 0;
        }
        long now = System.currentTimeMillis();
        long ageTs = now - agingIntervalMs;

        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put(COLUMN_STATUS_ID, EventStatus.STATUS_AGED.getNumber());
        fields.put(COLUMN_CLOSED_STATUS, ZepConstants.CLOSED_STATUSES.contains(EventStatus.STATUS_AGED));
        fields.put(COLUMN_STATUS_CHANGE, timestampConverter.toDatabaseType(now));
        fields.put(COLUMN_UPDATE_TIME, timestampConverter.toDatabaseType(now));
        fields.put(COLUMN_LAST_SEEN, timestampConverter.toDatabaseType(ageTs));
        fields.put("_severity_ids", severityIds);
        fields.put("_limit", limit);

        final String updateSql;
        if (databaseCompatibility.getDatabaseType() == DatabaseType.MYSQL) {
            String indexSql = "INSERT INTO event_summary_index_queue (uuid, update_time) " +
                    "SELECT uuid, " + String.valueOf(now) + " " +
                    "FROM event_summary " + 
                    " WHERE last_seen < :last_seen AND" +
                    " severity_id IN (:_severity_ids) AND" +
                    " closed_status = FALSE LIMIT :_limit";
            this.template.update(indexSql, fields);

            // Use UPDATE ... LIMIT
            updateSql = "UPDATE event_summary SET" +
                    " status_id=:status_id,status_change=:status_change,update_time=:update_time" +
                    ",closed_status=:closed_status" +
                    " WHERE last_seen < :last_seen AND severity_id IN (:_severity_ids)" +
                    " AND closed_status = FALSE LIMIT :_limit";
        }
        else if (databaseCompatibility.getDatabaseType() == DatabaseType.POSTGRESQL) {
            String indexSql = "INSERT INTO event_summary_index_queue (uuid, update_time) " +
                    "SELECT uuid, " + String.valueOf(now) + " " +
                    "FROM event_summary " + 
                    " WHERE uuid IN (SELECT uuid FROM event_summary WHERE" +
                    " last_seen < :last_seen AND severity_id IN (:_severity_ids)" +
                    " AND closed_status = FALSE LIMIT :_limit)";
            this.template.update(indexSql, fields);

            // Use UPDATE ... WHERE pk IN (SELECT ... LIMIT)
            updateSql = "UPDATE event_summary SET" +
                    " status_id=:status_id,status_change=:status_change,update_time=:update_time" +
                    ",closed_status=:closed_status" +
                    " WHERE uuid IN (SELECT uuid FROM event_summary WHERE" +
                    " last_seen < :last_seen AND severity_id IN (:_severity_ids)" +
                    " AND closed_status = FALSE LIMIT :_limit)";
        }
        else {
            throw new IllegalStateException("Unsupported database type: " + databaseCompatibility.getDatabaseType());
        }
        final int numRows = this.template.update(updateSql, fields);
        if (numRows > 0) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
                @Override
                public void afterCommit() {
                    counters.addToAgedEventCount(numRows);
                }
            });
        }
        return numRows;
    }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public int addNote(String uuid, EventNote note) throws ZepException {
        // Add the uuid to the event_summary_index_queue
        final long updateTime =  System.currentTimeMillis();
        this.indexSignal(uuid, updateTime); 

        return this.eventDaoHelper.addNote(TABLE_EVENT_SUMMARY, uuid, note, template);
    }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public int updateDetails(String uuid, EventDetailSet details)
            throws ZepException {
        // Add the uuid to the event_summary_index_queue
        final long updateTime =  System.currentTimeMillis();
        this.indexSignal(uuid, updateTime); 

        return this.eventDaoHelper.updateDetails(TABLE_EVENT_SUMMARY, uuid, details.getDetailsList(), template);
    }

    private static class EventSummaryUpdateFields {
        private String currentUserUuid;
        private String currentUserName;
        private String clearedByEventUuid;

        public static final EventSummaryUpdateFields EMPTY_FIELDS = new EventSummaryUpdateFields();

        public Map<String,Object> toMap(TypeConverter<String> uuidConverter) {
            Map<String,Object> m = new HashMap<String,Object>();
            Object currentUuid = null;
            if (this.currentUserUuid != null) {
                currentUuid = uuidConverter.toDatabaseType(this.currentUserUuid);
            }
            m.put(COLUMN_CURRENT_USER_UUID, currentUuid);
            m.put(COLUMN_CURRENT_USER_NAME, currentUserName);

            Object clearedUuid = null;
            if (this.clearedByEventUuid != null) {
                clearedUuid = uuidConverter.toDatabaseType(this.clearedByEventUuid);
            }
            m.put(COLUMN_CLEARED_BY_EVENT_UUID, clearedUuid);
            return m;
        }

        public String getCurrentUserUuid() {
            return currentUserUuid;
        }

        public void setCurrentUserUuid(String currentUserUuid) {
            this.currentUserUuid = currentUserUuid;
        }

        public String getCurrentUserName() {
            return currentUserName;
        }

        public void setCurrentUserName(String currentUserName) {
            if (currentUserName == null) {
                this.currentUserName = null;
            }
            else {
                this.currentUserName = DaoUtils.truncateStringToUtf8(currentUserName, MAX_CURRENT_USER_NAME);
            }
        }

        public String getClearedByEventUuid() {
            return clearedByEventUuid;
        }

        public void setClearedByEventUuid(String clearedByEventUuid) {
            this.clearedByEventUuid = clearedByEventUuid;
        }
    }

    private int update(final List<String> uuids, final EventStatus status, final EventSummaryUpdateFields updateFields,
                       final Collection<EventStatus> currentStatuses) throws ZepException {
        if (uuids.isEmpty()) {
            return 0;
        }
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        final long now = System.currentTimeMillis();
        final Map<String,Object> fields = updateFields.toMap(uuidConverter);
        fields.put(COLUMN_STATUS_ID, status.getNumber());
        fields.put(COLUMN_STATUS_CHANGE, timestampConverter.toDatabaseType(now));
        fields.put(COLUMN_CLOSED_STATUS, ZepConstants.CLOSED_STATUSES.contains(status));
        fields.put(COLUMN_UPDATE_TIME, timestampConverter.toDatabaseType(now));
        fields.put("_uuids", TypeConverterUtils.batchToDatabaseType(uuidConverter, uuids));
        // If we aren't acknowledging events, we need to clear out the current user name / UUID values
        if (status != EventStatus.STATUS_ACKNOWLEDGED) {
            fields.put(COLUMN_CURRENT_USER_NAME, null);
            fields.put(COLUMN_CURRENT_USER_UUID, null);
        }

        StringBuilder sb = new StringBuilder("SELECT uuid,fingerprint,audit_json FROM event_summary");
        StringBuilder sbw = new StringBuilder(" WHERE uuid IN (:_uuids)");
        /*
         * This is required to support well-defined transitions between states. We only allow
         * updates to move events between states that make sense.
         */
        if (!currentStatuses.isEmpty()) {
            final List<Integer> currentStatusIds = new ArrayList<Integer>(currentStatuses.size());
            for (EventStatus currentStatus : currentStatuses) {
                currentStatusIds.add(currentStatus.getNumber());
            }
            fields.put("_current_status_ids", currentStatusIds);
            sbw.append(" AND status_id IN (:_current_status_ids)");
        }
        /*
         * Disallow acknowledging an event again as the same user name / user uuid. If the event is not
         * already acknowledged, we will allow it to be acknowledged (assuming state filter above doesn't
         * exclude it). Otherwise, we will only acknowledge it again if *either* the user name or user
         * uuid has changed. If neither of these fields have changed, it is a NO-OP.
         */
        if (status == EventStatus.STATUS_ACKNOWLEDGED) {
            fields.put("_status_acknowledged", EventStatus.STATUS_ACKNOWLEDGED.getNumber());
            sbw.append(" AND (status_id != :_status_acknowledged OR ");
            if (updateFields.getCurrentUserName() == null) {
                sbw.append("current_user_name IS NOT NULL");
            }
            else {
                sbw.append("(current_user_name IS NULL OR current_user_name != :current_user_name)");
            }
            sbw.append(" OR ");
            if (updateFields.getCurrentUserUuid() == null) {
                sbw.append("current_user_uuid IS NOT NULL");
            }
            else {
                sbw.append("(current_user_uuid IS NULL OR current_user_uuid != :current_user_uuid)");
            }
            sbw.append(")");
        }
        String selectSql = sb.toString() + sbw.toString() + " FOR UPDATE";

        final long updateTime =  System.currentTimeMillis();
        final String indexSql = "INSERT INTO event_summary_index_queue (uuid, update_time) "
                + "SELECT uuid, " + String.valueOf(updateTime) + " "
                + "FROM event_summary "
                + sbw.toString();
        this.template.update(indexSql, fields);

        /*
         * If this is a significant status change, also add an audit note
         */
        final String newAuditJson;
        if (AUDIT_LOG_STATUSES.contains(status)) {
            EventAuditLog.Builder builder = EventAuditLog.newBuilder();
            builder.setTimestamp(now);
            builder.setNewStatus(status);
            if (updateFields.getCurrentUserUuid() != null) {
                builder.setUserUuid(updateFields.getCurrentUserUuid());
            }
            if (updateFields.getCurrentUserName() != null) {
                builder.setUserName(updateFields.getCurrentUserName());
            }
            try {
                newAuditJson = JsonFormat.writeAsString(builder.build());
            } catch (IOException e) {
                throw new ZepException(e);
            }
        }
        else {
            newAuditJson = null;
        }

        List<Map<String,Object>> result = this.template.query(selectSql, new RowMapper<Map<String,Object>>() {
            @Override
            public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
                final String fingerprint = rs.getString(COLUMN_FINGERPRINT);
                final String currentAuditJson = rs.getString(COLUMN_AUDIT_JSON);

                Map<String,Object> updateFields = new HashMap<String, Object>(fields);
                final String newFingerprint;
                // When closing an event, give it a unique fingerprint hash
                if (ZepConstants.CLOSED_STATUSES.contains(status)) {
                    newFingerprint = EventDaoUtils.join('|', fingerprint, Long.toString(now));
                }
                // When re-opening an event, give it the true fingerprint_hash. This is required to correctly
                // de-duplicate events.
                else {
                    newFingerprint = fingerprint;
                }

                final StringBuilder auditJson = new StringBuilder();
                if (newAuditJson != null) {
                    auditJson.append(newAuditJson);
                }
                if (currentAuditJson != null) {
                    if (auditJson.length() > 0) {
                        auditJson.append(",\n");
                    }
                    auditJson.append(currentAuditJson);
                }
                String updatedAuditJson = (auditJson.length() > 0) ? auditJson.toString() : null;
                updateFields.put(COLUMN_FINGERPRINT_HASH, DaoUtils.sha1(newFingerprint));
                updateFields.put(COLUMN_AUDIT_JSON, updatedAuditJson);
                updateFields.put(COLUMN_UUID, rs.getObject(COLUMN_UUID));
                return updateFields;
            }
        }, fields);

        final String updateSql = "UPDATE event_summary SET status_id=:status_id,status_change=:status_change," +
              "closed_status=:closed_status,update_time=:update_time,"+
              (status != EventStatus.STATUS_CLOSED && status != EventStatus.STATUS_CLEARED ? "current_user_uuid=:current_user_uuid,current_user_name=:current_user_name,":"" ) +
               "cleared_by_event_uuid=:cleared_by_event_uuid,fingerprint_hash=:fingerprint_hash," +
                "audit_json=:audit_json WHERE uuid=:uuid";


        int numRows = 0;
        for (final Map<String,Object> update : result) {
            try {
                numRows += this.nestedTransactionService.executeInNestedTransaction(
                        new NestedTransactionCallback<Integer>() {
                            @Override
                            public Integer doInNestedTransaction(NestedTransactionContext context) throws DataAccessException {
                                return template.update(updateSql, update);
                            }
                        });
            } catch (DuplicateKeyException e) {
                /*
                 * Ignore duplicate key errors on update. This will occur if there is an active
                 * event with the same fingerprint.
                 */
            }
        }
        return numRows;
    }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public int acknowledge(List<String> uuids, String userUuid, String userName)
            throws ZepException {
        /* NEW | ACKNOWLEDGED | SUPPRESSED -> ACKNOWLEDGED */
        Set<EventStatus> currentStatuses = ZepConstants.OPEN_STATUSES;
        EventSummaryUpdateFields userfields = new EventSummaryUpdateFields();
        userfields.setCurrentUserName(userName);
        userfields.setCurrentUserUuid(userUuid);
        return update(uuids, EventStatus.STATUS_ACKNOWLEDGED, userfields, currentStatuses);
    }

    private Map<String, Object> createSharedFields(long duration, TimeUnit unit) {
        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        long delta = System.currentTimeMillis() - unit.toMillis(duration);
        Object lastSeen = timestampConverter.toDatabaseType(delta);
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("_last_seen", lastSeen);
        return fields;
    }

    @Override
    @Timed
    public long getArchiveEligibleEventCount(long duration, TimeUnit unit) {
        String sql = "SELECT COUNT(*) FROM event_summary WHERE closed_status = TRUE AND last_seen < :_last_seen";
        Map<String, Object> fields = createSharedFields(duration, unit);
        return template.queryForInt(sql, fields);
    }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public int archive(long duration, TimeUnit unit, int limit) throws ZepException {
        Map<String, Object> fields = createSharedFields(duration, unit);
        fields.put("_limit", limit);

        final String sql = "SELECT uuid FROM event_summary WHERE closed_status = TRUE AND "
                + "last_seen < :_last_seen LIMIT :_limit FOR UPDATE";
        final List<String> uuids = this.template.query(sql, new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet rs, int rowNum)
                    throws SQLException {
                return uuidConverter.fromDatabaseType(rs, COLUMN_UUID);
            }
        }, fields);
        return archive(uuids);
    }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public int close(List<String> uuids, String userUuid, String userName) throws ZepException {
        /* NEW | ACKNOWLEDGED | SUPPRESSED -> CLOSED */
        List<EventStatus> currentStatuses = Arrays.asList(EventStatus.STATUS_NEW, EventStatus.STATUS_ACKNOWLEDGED,
                EventStatus.STATUS_SUPPRESSED);
        EventSummaryUpdateFields userfields = new EventSummaryUpdateFields();
        userfields.setCurrentUserName(userName);
        userfields.setCurrentUserUuid(userUuid);
        return update(uuids, EventStatus.STATUS_CLOSED, userfields, currentStatuses);
    }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public int reopen(List<String> uuids, String userUuid, String userName) throws ZepException {
        /* CLOSED | CLEARED | AGED | ACKNOWLEDGED | SUPPRESSED -> NEW */
        List<EventStatus> currentStatuses = Arrays.asList(EventStatus.STATUS_CLOSED, EventStatus.STATUS_CLEARED,
                EventStatus.STATUS_AGED, EventStatus.STATUS_ACKNOWLEDGED, EventStatus.STATUS_SUPPRESSED);
        EventSummaryUpdateFields userfields = new EventSummaryUpdateFields();
        userfields.setCurrentUserName(userName);
        userfields.setCurrentUserUuid(userUuid);
        return update(uuids, EventStatus.STATUS_NEW, userfields, currentStatuses);
    }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public int suppress(List<String> uuids) throws ZepException {
        /* NEW -> SUPPRESSED */
        List<EventStatus> currentStatuses = Arrays.asList(EventStatus.STATUS_NEW);
        return update(uuids, EventStatus.STATUS_SUPPRESSED, EventSummaryUpdateFields.EMPTY_FIELDS, currentStatuses);
    }

    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public int archive(List<String> uuids) throws ZepException {
        if (uuids.isEmpty()) {
            return 0;
        }
        if (this.archiveColumnNames == null) {
            try {
                this.archiveColumnNames = DaoUtils.getColumnNames(this.dataSource, TABLE_EVENT_ARCHIVE);
            } catch (MetaDataAccessException e) {
                throw new ZepException(e.getLocalizedMessage(), e);
            }
        }

        TypeConverter<Long> timestampConverter = databaseCompatibility.getTimestampConverter();
        Map<String, Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_UPDATE_TIME, timestampConverter.toDatabaseType(System.currentTimeMillis()));
        fields.put("_uuids", TypeConverterUtils.batchToDatabaseType(uuidConverter, uuids));
        StringBuilder selectColumns = new StringBuilder();

        for (Iterator<String> it = this.archiveColumnNames.iterator(); it.hasNext();) {
            String columnName = it.next();
            if (fields.containsKey(columnName)) {
                selectColumns.append(':').append(columnName);
            } else {
                selectColumns.append(columnName);
            }
            if (it.hasNext()) {
                selectColumns.append(',');
            }
        }

        final long updateTime = System.currentTimeMillis();
        /* signal event_summary table rows to get indexed */ 
        this.template.update("INSERT INTO event_summary_index_queue (uuid, update_time) " 
            + "SELECT uuid, " + String.valueOf(updateTime) + " " 
            + "FROM event_summary" +
            " WHERE uuid IN (:_uuids) AND closed_status = TRUE",
                fields); 

        String insertSql = String.format("INSERT INTO event_archive (%s) SELECT %s FROM event_summary" +
                " WHERE uuid IN (:_uuids) AND closed_status = TRUE ON DUPLICATE KEY UPDATE summary=event_summary.summary",
                StringUtils.collectionToCommaDelimitedString(this.archiveColumnNames), selectColumns);

        this.template.update(insertSql, fields);
        final int updated = this.template.update("DELETE FROM event_summary WHERE uuid IN (:_uuids) AND closed_status = TRUE",
                fields);
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
            @Override
            public void afterCommit() {
                counters.addToArchivedEventCount(updated);
            }
        });
        return updated;
    }


    @Override
    @TransactionalRollbackAllExceptions
    @Timed
    public void importEvent(EventSummary eventSummary) throws ZepException {
        final long updateTime = System.currentTimeMillis();
        final EventSummary.Builder summaryBuilder = EventSummary.newBuilder(eventSummary);
        final Event.Builder eventBuilder = summaryBuilder.getOccurrenceBuilder(0);
        summaryBuilder.setUpdateTime(updateTime);
        EventDaoHelper.addMigrateUpdateTimeDetail(eventBuilder, updateTime);

        final EventSummary summary = summaryBuilder.build();
        final Map<String,Object> fields = this.eventDaoHelper.createImportedSummaryFields(summary);

        /*
         * Closed events have a unique fingerprint_hash in summary to allow multiple rows
         * but only allow one active event (where the de-duplication occurs).
         */
        if (ZepConstants.CLOSED_STATUSES.contains(eventSummary.getStatus())) {
            String uniqueFingerprint = (String) fields.get(COLUMN_FINGERPRINT) + '|' + updateTime;
            fields.put(COLUMN_FINGERPRINT_HASH, DaoUtils.sha1(uniqueFingerprint));
            fields.put(COLUMN_CLOSED_STATUS, Boolean.TRUE);
        }
        else {
            fields.put(COLUMN_FINGERPRINT_HASH, DaoUtils.sha1((String)fields.get(COLUMN_FINGERPRINT)));
            fields.put(COLUMN_CLOSED_STATUS, Boolean.FALSE);
        }

        if (eventSummary.getOccurrence(0).getSeverity() != EventSeverity.SEVERITY_CLEAR) {
            fields.put(COLUMN_CLEAR_FINGERPRINT_HASH, EventDaoUtils.createClearHash(eventSummary.getOccurrence(0)));
        }

        this.insert.execute(fields);
    }

    @TransactionalRollbackAllExceptions
    private void indexSignal(final String eventUuid, final long updateTime) throws ZepException {
        final String insertSql = "INSERT INTO event_summary_index_queue (uuid, update_time) "
                + "VALUES (:uuid, :update_time)";

        Map<String, Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_UPDATE_TIME, updateTime);
        fields.put("uuid", this.uuidConverter.toDatabaseType(eventUuid));
        
        this.template.update(insertSql, fields);
    }
}
