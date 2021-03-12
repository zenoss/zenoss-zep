/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.zenoss.zing.proto.event.Event;
import org.zenoss.zing.proto.event.Status;
import org.zenoss.zing.proto.event.Severity;
import com.google.protobuf.BoolValue;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;

/**
 * ZingEvent represents an event that will be forwarded to Zenoss Cloud. It contains
 * the logic to convert a ZingEvent pojo to Zing protobuf.
 */
public class ZingEvent {

    private static final Logger logger = LoggerFactory.getLogger(ZingEvent.class);

    private static final BoolValue boolValueTrue = BoolValue.newBuilder().setValue(true).build();
    private static final BoolValue boolValueFalse = BoolValue.newBuilder().setValue(false).build();
    private static final Map<String, Status> statusMap = new HashMap<String, Status>();
    private static final Map<String, Severity> severityMap = new HashMap<String, Severity>();
    static {
        // STATUS_NEW: Ensure Events.setAcknowledged("False")
        // STATUS_ACKNOWLEDGED: Ensure Events.setAcknowledged("True")
        statusMap.put(EventStatus.STATUS_NEW.toString(), Status.STATUS_OPEN);
        statusMap.put(EventStatus.STATUS_ACKNOWLEDGED.toString(), Status.STATUS_OPEN);
        statusMap.put(EventStatus.STATUS_SUPPRESSED.toString(), Status.STATUS_SUPPRESSED);
        statusMap.put(EventStatus.STATUS_CLOSED.toString(), Status.STATUS_CLOSED);
        statusMap.put(EventStatus.STATUS_CLEARED.toString(), Status.STATUS_CLOSED);
        statusMap.put(EventStatus.STATUS_DROPPED.toString(), Status.STATUS_CLOSED);
        statusMap.put(EventStatus.STATUS_AGED.toString(), Status.STATUS_CLOSED);

        // Ensure for CLEAR: Event.Status => Status.STATUS_CLOSED also.
        severityMap.put(EventSeverity.SEVERITY_CLEAR.toString(), Severity.SEVERITY_DEFAULT);
        severityMap.put(EventSeverity.SEVERITY_DEBUG.toString(), Severity.SEVERITY_DEBUG);
        severityMap.put(EventSeverity.SEVERITY_INFO.toString(), Severity.SEVERITY_INFO);
        severityMap.put(EventSeverity.SEVERITY_WARNING.toString(), Severity.SEVERITY_WARNING);
        severityMap.put(EventSeverity.SEVERITY_ERROR.toString(), Severity.SEVERITY_ERROR);
        severityMap.put(EventSeverity.SEVERITY_CRITICAL.toString(), Severity.SEVERITY_CRITICAL);
    }

    private final String tenant;
    private final String source;

    private final long occurrenceTime;
    private final String uuid;
    private final String fingerprint;

    private final int count;
    private final long lastSeen;
    private final long firstSeen;
    private final long updateTime;
    private final String clearedByUUID;
    private final String status;
    private final String severity;
    // parentContext attrs represent the device the event is about
    private final String parentContextUUID;
    private final String parentContextIdentifier;
    private final String parentContextTitle;
    private final String parentContextType;
    // context attrs represent the component the event is about
    private final String contextUUID;
    private final String contextIdentifier;
    private final String contextTitle;
    private final String contextType;
    private final String message;
    private final String summary;
    private final String monitor;
    private final String agent;
    private final String eventKey;
    private final String eventClass;
    private final String eventClassKey;
    private final String eventClassMappingUuid;
    private final String eventGroup;
    private final Map<String, List<String>> details;


    private  ZingEvent (Builder b) {
        this.tenant = b.tenant_;
        this.source = b.source_;
        this.occurrenceTime = b.occurrenceTime_;
        this.fingerprint = b.fingerprint_;
        this.severity = b.severity_;
        this.parentContextUUID = b.parentContextUUID_;
        this.parentContextIdentifier = b.parentContextIdentifier_;
        this.parentContextTitle = b.parentContextTitle_;
        this.parentContextType = b.parentContextType_;
        if (ZingUtils.isNullOrEmpty(b.contextUUID_)) {
            // event on the device. no component. Populate context attrs from
            // the values in parent attrs
            this.contextUUID = this.parentContextUUID;
            this.contextIdentifier = this.parentContextIdentifier;
            this.contextTitle = this.parentContextTitle;
            this.contextType = this.parentContextType;
        } else {
            this.contextUUID = b.contextUUID_;
            this.contextIdentifier = b.contextIdentifier_;
            this.contextTitle = b.contextTitle_;
            this.contextType = b.contextType_;
        }
        this.message = b.message_;
        this.summary = b.summary_;
        this.uuid = b.uuid_;
        this.monitor = b.monitor_;
        this.agent = b.agent_;
        this.eventKey = b.eventKey_;
        this.eventClass = b.eventClass_;
        this.eventClassKey = b.eventClassKey_;
        this.eventClassMappingUuid = b.eventClassMappingUuid_;
        this.eventGroup = b.eventGroup_;
        this.details = b.details_;
        this.count = b.count_;
        this.firstSeen = b.firstSeen_;
        this.lastSeen = b.lastSeen_;
        this.updateTime = b.updateTime_;
        this.clearedByUUID = b.clearedByUUID_;
        this.status = b.status_;
    }

    public String getFingerprint() {
        return this.fingerprint;
    }

    public String getUuid() {
        return this.uuid;
    }

    public String toString() {
        final StringBuffer strBuf = new StringBuffer();
        strBuf.append("\n tenant = ").append(this.tenant);
        strBuf.append("\n source = ").append(this.source);
        strBuf.append("\n uuid = ").append(this.uuid);
        strBuf.append("\n occurrenceTime = ").append(this.occurrenceTime);
        strBuf.append("\n fingerprint = ").append(this.fingerprint);
        strBuf.append("\n severity = ").append(this.severity);
        strBuf.append("\n parentContextUUID = ").append(this.parentContextUUID);
        strBuf.append("\n parentContextIdentifier = ").append(this.parentContextIdentifier);
        strBuf.append("\n parentContextTitle = ").append(this.parentContextTitle);
        strBuf.append("\n parentContextType = ").append(this.parentContextType);
        strBuf.append("\n contextUUID = ").append(this.contextUUID);
        strBuf.append("\n contextIdentifier = ").append(this.contextIdentifier);
        strBuf.append("\n contextTitle = ").append(this.contextTitle);
        strBuf.append("\n contextType = ").append(this.contextType);
        strBuf.append("\n message = ").append(this.message);
        strBuf.append("\n monitor = ").append(this.monitor);
        strBuf.append("\n agent = ").append(this.agent);
        strBuf.append("\n eventKey = ").append(this.eventKey);
        strBuf.append("\n eventClass = ").append(this.eventClass);
        strBuf.append("\n eventClassKey = ").append(this.eventClassKey);
        strBuf.append("\n eventClassMappingUuid = ").append(this.eventClassMappingUuid);
        strBuf.append("\n eventGroup = ").append(this.eventGroup);
        strBuf.append("\n details ");
        for (Map.Entry<String,List<String>> entry : this.details.entrySet()) {
            strBuf.append("\n\t ").append(entry.getKey()).append(" : ").append(entry.getValue());
        }
        strBuf.append("\n count = ").append(this.count);
        strBuf.append("\n firstSeen = ").append(this.firstSeen);
        strBuf.append("\n lastSeen = ").append(this.lastSeen);
        strBuf.append("\n updateTime = ").append(this.updateTime);
        strBuf.append("\n cleared by = ").append(this.clearedByUUID);
        strBuf.append("\n status = ").append(this.status);
        return strBuf.toString();
    }

    public boolean isValid() {
        boolean badEvent = this.occurrenceTime == 0 ||
                            ZingUtils.isNullOrEmpty(this.tenant) ||
                            ZingUtils.isNullOrEmpty(this.source) ||
                            ZingUtils.isNullOrEmpty(this.uuid) ||
                            ZingUtils.isNullOrEmpty(this.severity) ||
                            ZingUtils.isNullOrEmpty(this.fingerprint) ||
                            ZingUtils.isNullOrEmpty(this.contextUUID);
        return !badEvent;
    }

    /*
        DEFAULT_FINGERPRINT_FIELDS = ('device', 'component', 'eventClass', 'eventKey', 'severity')
        NO_EVENT_KEY_FINGERPRINT_FIELDS = ('device', 'component', 'eventClass', 'severity', 'summary')
     */
    public Event toZingEventProto() {
        Event evt = null;
        if (this.isValid()) {
            Event.Builder b = Event.newBuilder();
            b.setTenant(this.tenant);
            // The timestamp is set to the event's lastSeen (raw event occurenceTime) which
            // can be confusing when sending event updates (state change, detail/log update, etc)
            //      - lastSeen will always contain the event's last occurence time
            //      - updateTime will always contain the last time the event was updated
            //        (new occurence or event updates)
            // Therefore, it is now changed to updateTime
            b.setTimestamp(this.updateTime);
            // Dimensions
            //      source
            //      fingerprint
            //      zep event uuid
            b.putDimensions(ZingConstants.SOURCE_KEY, ZingUtils.getScalarValueFromObject(this.source));
            b.putDimensions(ZingConstants.FINGERPRINT_KEY, ZingUtils.getScalarValueFromObject(this.fingerprint));
            b.putDimensions(ZingConstants.UUID_KEY, ZingUtils.getScalarValueFromObject(this.uuid));

            //--------------------------------------------
            //  Combined Metadata and Property Settings
            //--------------------------------------------

            // Status
            if (!ZingUtils.isNullOrEmpty(this.status)) {
                // default: Status.STATUS_DEFAULT;
                b.putMetadata( ZingConstants.STATUS_KEY, ZingUtils.getScalarArray(this.status));

                // Ensure status gets mapped faithfully, else log:
                if (!statusMap.containsKey(this.status)) {
                    logger.debug("Setting default status for missing status key: {}", this.status);
                    b.setStatus(Status.STATUS_DEFAULT);
                } else {
                    logger.debug("Mapping default status for status key: {}", this.status);
                    b.setStatus(statusMap.get(this.status));
                }

                // For Acknowledged status, set the corresponding property
                if ("STATUS_ACKNOWLEDGED".equals(this.status)) {
                    logger.debug("Setting ACKNOWLEDGED true for status: {}", this.status);
                    b.setAcknowledged(boolValueTrue);
                } else if ("STATUS_NEW".equals(this.status)) {
                    logger.debug("Setting ACKNOWLEDGED false for status: {}", this.status);
                    b.setAcknowledged(boolValueFalse);
                }
            } else {
                logger.debug("Null or missing status for event summary: {}", this.summary);
            }

            // Severity Mapping
            if (!ZingUtils.isNullOrEmpty(this.severity)) {
                // default:  Severity.SEVERITY_DEFAULT;
                b.putMetadata( ZingConstants.SEVERITY_KEY, ZingUtils.getScalarArray(this.severity));

                // Ensure severity gets mapped faithfully, else log:
                if (!severityMap.containsKey(this.severity)) {
                    logger.warn("No key in severityMap for severity: {}", this.severity);
                    b.setSeverity(Severity.SEVERITY_DEFAULT);
                } else {
                    b.setSeverity(severityMap.get(this.severity));
                }

                // Ensure all cleared severities get status STATUS_CLOSED.
                if ("SEVERITY_CLEAR".equals(this.severity))
                    b.setStatus(Status.STATUS_CLOSED);
            }

            if (!ZingUtils.isNullOrEmpty(this.parentContextUUID))
                b.putMetadata( ZingConstants.PARENT_CONTEXT_UUID_KEY, ZingUtils.getScalarArray(this.parentContextUUID));
            if (!ZingUtils.isNullOrEmpty(this.parentContextIdentifier))
                b.putMetadata( ZingConstants.PARENT_CONTEXT_ID_KEY, ZingUtils.getScalarArray(this.parentContextIdentifier));
            if (!ZingUtils.isNullOrEmpty(this.parentContextTitle))
                b.putMetadata( ZingConstants.PARENT_CONTEXT_TITLE_KEY, ZingUtils.getScalarArray(this.parentContextTitle));
            if (!ZingUtils.isNullOrEmpty(this.parentContextType))
                b.putMetadata( ZingConstants.PARENT_CONTEXT_TYPE_KEY, ZingUtils.getScalarArray(this.parentContextType));
            if (!ZingUtils.isNullOrEmpty(this.contextUUID))
                b.putMetadata( ZingConstants.CONTEXT_UUID_KEY, ZingUtils.getScalarArray(this.contextUUID));
            if (!ZingUtils.isNullOrEmpty(this.contextIdentifier))
                b.putMetadata( ZingConstants.CONTEXT_ID_KEY, ZingUtils.getScalarArray(this.contextIdentifier));
            if (!ZingUtils.isNullOrEmpty(this.contextTitle))
                b.putMetadata( ZingConstants.CONTEXT_TITLE_KEY, ZingUtils.getScalarArray(this.contextTitle));
            if (!ZingUtils.isNullOrEmpty(this.contextType))
                b.putMetadata( ZingConstants.CONTEXT_TYPE_KEY, ZingUtils.getScalarArray(this.contextType));
            if (!ZingUtils.isNullOrEmpty(this.message)) {
                b.putMetadata( ZingConstants.MESSAGE_KEY, ZingUtils.getScalarArray(this.message));
                b.setBody(this.message);
            }
            if (!ZingUtils.isNullOrEmpty(this.summary)) {
                b.putMetadata( ZingConstants.SUMMARY_KEY, ZingUtils.getScalarArray(this.summary));
                b.setSummary(this.summary);
            }
            if (!ZingUtils.isNullOrEmpty(this.monitor))
                b.putMetadata( ZingConstants.MONITOR_KEY, ZingUtils.getScalarArray(this.monitor));
            if (!ZingUtils.isNullOrEmpty(this.agent))
                b.putMetadata( ZingConstants.AGENT_KEY, ZingUtils.getScalarArray(this.agent));
            if (!ZingUtils.isNullOrEmpty(this.eventKey))
                b.putMetadata( ZingConstants.EVENT_KEY_KEY, ZingUtils.getScalarArray(this.eventKey));
            if (!ZingUtils.isNullOrEmpty(this.eventClass))
                b.putMetadata( ZingConstants.EVENT_CLASS_KEY, ZingUtils.getScalarArray(this.eventClass));
            if (!ZingUtils.isNullOrEmpty(this.eventClassKey)) {
                b.putMetadata( ZingConstants.EVENT_CLASS_KEY_KEY, ZingUtils.getScalarArray(this.eventClassKey));
                b.setType(this.eventClassKey);
            }
            if (!ZingUtils.isNullOrEmpty(this.eventClassMappingUuid))
                b.putMetadata( ZingConstants.EVENT_CLASS_MAPPING_KEY, ZingUtils.getScalarArray(this.eventClassMappingUuid));
            if (!ZingUtils.isNullOrEmpty(this.eventGroup))
                b.putMetadata( ZingConstants.EVENT_GROUP_KEY, ZingUtils.getScalarArray(this.eventGroup));
            if (this.count > 0)
                b.putMetadata( ZingConstants.COUNT_KEY, ZingUtils.getScalarArray(this.count));
            if (this.firstSeen > 0)
                b.putMetadata( ZingConstants.FIRST_SEEN_KEY, ZingUtils.getScalarArray(this.firstSeen));
            if (this.lastSeen > 0)
                b.putMetadata( ZingConstants.LAST_SEEN_KEY, ZingUtils.getScalarArray(this.lastSeen));
            if (this.updateTime > 0)
                b.putMetadata( ZingConstants.UPDATE_TIME_KEY, ZingUtils.getScalarArray(this.updateTime));
            if (!ZingUtils.isNullOrEmpty(this.clearedByUUID)) {
                b.putMetadata( ZingConstants.CLEARED_BY_KEY, ZingUtils.getScalarArray(this.clearedByUUID));
            }
            // details
            for (Map.Entry<String,List<String>> entry : this.details.entrySet()) {
                final String key = ZingConstants.DETAILS_KEY_PREFIX + entry.getKey();
                final List<Object> objectList = (List)entry.getValue();
                b.putMetadata( key, ZingUtils.getScalarArrayFromList(objectList));
            }
            // Source id
            b.putMetadata(ZingConstants.SOURCE_TYPE_KEY, ZingUtils.getScalarArray(ZingConstants.SOURCE_TYPE));
            b.putMetadata(ZingConstants.SOURCE_VENDOR_KEY, ZingUtils.getScalarArray(ZingConstants.SOURCE_VENDOR));

            evt = b.build();
        }
        if (evt != null) {
            logger.debug("Returning event: {}", evt.toString());
        }
        return evt;
    }

    public static class Builder {
        private final String tenant_;
        private final String source_;
        private final long occurrenceTime_;
        private String fingerprint_;
        private String severity_;
        private String parentContextUUID_;
        private String parentContextIdentifier_;
        private String parentContextTitle_;
        private String parentContextType_;
        private String contextUUID_;
        private String contextIdentifier_;
        private String contextTitle_;
        private String contextType_;
        private String message_;
        private String summary_;
        private String uuid_;
        private String monitor_;
        private String agent_;
        private String eventKey_;
        private String eventClass_;
        private String eventClassKey_;
        private String eventClassMappingUuid_;
        private String eventGroup_;
        private int count_;
        private long lastSeen_;
        private long firstSeen_;
        private long updateTime_;
        private String clearedByUUID_;
        private String status_;

        private Map<String, List<String>> details_ = new HashMap<>();

        public Builder(String tnt, String src, long ts) {
            this.tenant_ = tnt;
            this.source_ = src;
            this.occurrenceTime_ = ts;
        }

        public Builder setUuid(String value) {
            this.uuid_ = value;
            return this;
        }

        public Builder setFingerprint(String value) {
            this.fingerprint_ = value;
            return this;
        }

        public Builder setSeverity(String value) {
            this.severity_ = value;
            return this;
        }

        public Builder setMessage(String value){
            this.message_ = value;
            return this;
        }

        public Builder setSummary(String value){
            this.summary_ = value;
            return this;
        }

        public Builder setParentContextUUID(String value){
            this.parentContextUUID_ = value;
            return this;
        }

        public Builder setParentContextIdentifier(String value){
            this.parentContextIdentifier_ = value;
            return this;
        }

        public Builder setParentContextTitle(String value){
            this.parentContextTitle_ = value;
            return this;
        }

        public Builder setParentContextType(String value){
            this.parentContextType_ = value;
            return this;
        }

        public Builder setContextUUID(String value){
            this.contextUUID_ = value;
            return this;
        }

        public Builder setContextIdentifier(String value){
            this.contextIdentifier_ = value;
            return this;
        }
        public Builder setContextTitle(String value){
            this.contextTitle_ = value;
            return this;
        }
        public Builder setContextType(String value){
            this.contextType_ = value;
            return this;
        }

        public Builder setMonitor(String value){
            this.monitor_ = value;
            return this;
        }

        public Builder setAgent(String value){
            this.agent_ = value;
            return this;
        }

        public Builder setEventKey(String value) {
            this.eventKey_ = value;
            return this;
        }
        public Builder setEventClass(String value) {
            this.eventClass_ = value;
            return this;
        }

        public Builder setEventClassKey(String value) {
            this.eventClassKey_ = value;
            return this;
        }

        public Builder setEventClassMappingUuid(String value) {
            this.eventClassMappingUuid_ = value;
            return this;
        }

        public Builder setEventGroup(String value) {
            this.eventGroup_ = value;
            return this;
        }

        public Builder setDetail(String k, List<String> v) {
            // FIXME do we need to clone v?
            this.details_.put(k, v);
            return this;
        }

        public Builder setCount(int value) {
            this.count_ = value;
            return this;
        }

        public Builder setLastSeen(long value) {
            this.lastSeen_ = value;
            return this;
        }

        public Builder setFirstSeen(long value) {
            this.firstSeen_ = value;
            return this;
        }

        public Builder setUpdateTime(long value) {
            this.updateTime_ = value;
            return this;
        }

        public Builder setClearedByUUID(String value) {
            this.clearedByUUID_ = value;
            return this;
        }

        public Builder setStatus(String value) {
            this.status_ = value;
            return this;
        }

        public ZingEvent build() {
            return new ZingEvent(this);
        }
    }
}

