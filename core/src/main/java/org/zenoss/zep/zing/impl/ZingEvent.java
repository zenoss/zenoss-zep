
package org.zenoss.zep.zing.impl;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.zenoss.zing.proto.event.Event;

public class ZingEvent {

    private final String tenant;
    private final String source;

    private final long occurrenceTime;
    private final String fingerprint;
    private final String severity;
    private final String parentContextUUID;
    private final String parentContextIdentifier;
    private final String parentContextTitle;
    private final String parentContextType;
    private final String contextUUID;
    private final String contextIdentifier;
    private final String contextTitle;
    private final String contextType;
    private final String message;
    private final String summary;
    private final String uuid;

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
        if (ZingUtils.isNullOrEmpty(b.contextUUID_)) { // event on the device. no component.
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
        return strBuf.toString();
    }

    private boolean isNullOrEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    public boolean isValid() {
        boolean badEvent = this.occurrenceTime == 0 ||
                            ZingUtils.isNullOrEmpty(this.tenant) ||
                            ZingUtils.isNullOrEmpty(this.source) ||
                            ZingUtils.isNullOrEmpty(this.severity) ||
                            ZingUtils.isNullOrEmpty(this.fingerprint) ||
                            ZingUtils.isNullOrEmpty(this.contextUUID);
        return !badEvent;
    }

    /*
        DEFAULT_FINGERPRINT_FIELDS = ('device', 'component', 'eventClass', 'eventKey', 'severity')
        NO_EVENT_KEY_FINGERPRINT_FIELDS = ('device', 'component', 'eventClass', 'severity', 'summary')
     */
    public Event toZingEvent() {
        Event evt = null;
        if (this.isValid()) {
            Event.Builder b = Event.newBuilder();
            b.setTenant(this.tenant);
            b.setTimestamp(this.occurrenceTime);
            // Dimensions - for zep events we trust the fingerprint as unique
            //      source
            b.putDimensions("source", ZingUtils.getAnyValueFromObject(this.source));
            //      fingerprint
            b.putDimensions("fingerprint", ZingUtils.getAnyValueFromObject(this.fingerprint));
            //      uuid ?? only if we care about event updates
            //-----------
            //  Metadata
            //-----------
            if (!ZingUtils.isNullOrEmpty(this.severity))
                b.putMetadata( "severity", ZingUtils.getAnyArray(this.severity));
            if (!ZingUtils.isNullOrEmpty(this.parentContextUUID))
                b.putMetadata( "parentContextUUID", ZingUtils.getAnyArray(this.parentContextUUID));
            if (!ZingUtils.isNullOrEmpty(this.parentContextIdentifier))
                b.putMetadata( "parentContextIdentifier", ZingUtils.getAnyArray(this.parentContextIdentifier));
            if (!ZingUtils.isNullOrEmpty(this.parentContextTitle))
                b.putMetadata( "parentContextTitle", ZingUtils.getAnyArray(this.parentContextTitle));
            if (!ZingUtils.isNullOrEmpty(this.parentContextType))
                b.putMetadata( "parentContextType", ZingUtils.getAnyArray(this.parentContextType));
            if (!ZingUtils.isNullOrEmpty(this.contextUUID))
                b.putMetadata( "contextUUID", ZingUtils.getAnyArray(this.contextUUID));
            if (!ZingUtils.isNullOrEmpty(this.contextIdentifier))
                b.putMetadata( "contextIdentifier", ZingUtils.getAnyArray(this.contextIdentifier));
            if (!ZingUtils.isNullOrEmpty(this.contextTitle))
                b.putMetadata( "contextTitle", ZingUtils.getAnyArray(this.contextTitle));
            if (!ZingUtils.isNullOrEmpty(this.contextType))
                b.putMetadata( "contextType", ZingUtils.getAnyArray(this.contextType));
            if (!ZingUtils.isNullOrEmpty(this.message))
                b.putMetadata( "message", ZingUtils.getAnyArray(this.message));
            if (!ZingUtils.isNullOrEmpty(this.summary))
                b.putMetadata( "summary", ZingUtils.getAnyArray(this.summary));
            if (!ZingUtils.isNullOrEmpty(this.uuid))
                b.putMetadata( "uuid", ZingUtils.getAnyArray(this.uuid));
            if (!ZingUtils.isNullOrEmpty(this.monitor))
                b.putMetadata( "monitor", ZingUtils.getAnyArray(this.monitor));
            if (!ZingUtils.isNullOrEmpty(this.agent))
                b.putMetadata( "agent", ZingUtils.getAnyArray(this.agent));
            if (!ZingUtils.isNullOrEmpty(this.eventKey))
                b.putMetadata( "eventKey", ZingUtils.getAnyArray(this.eventKey));
            if (!ZingUtils.isNullOrEmpty(this.eventClass))
                b.putMetadata( "eventClass", ZingUtils.getAnyArray(this.eventClass));
            if (!ZingUtils.isNullOrEmpty(this.eventClassKey))
                b.putMetadata( "eventClassKey", ZingUtils.getAnyArray(this.eventClassKey));
            if (!ZingUtils.isNullOrEmpty(this.eventClassMappingUuid))
                b.putMetadata( "eventClassMappingUuid", ZingUtils.getAnyArray(this.eventClassMappingUuid));
            if (!ZingUtils.isNullOrEmpty(this.eventGroup))
                b.putMetadata( "eventGroup", ZingUtils.getAnyArray(this.eventGroup));

            evt = b.build();
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

        public ZingEvent build() {
            return new ZingEvent(this);
        }
    }
}

