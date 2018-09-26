
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
    private final String contextUUID;
    private final String contextIdentifier;
    private final String contextTitle;
    private final String contextType;
    private final String childContextUUID;
    private final String childContextIdentifier;
    private final String childContextTitle;
    private final String childContextType;
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
        this.contextUUID = b.contextUUID_;
        this.contextIdentifier = b.contextIdentifier_;
        this.contextTitle = b.contextTitle_;
        this.contextType = b.contextType_;
        this.childContextUUID = b.childContextUUID_;
        this.childContextIdentifier = b.childContextIdentifier_;
        this.childContextTitle = b.childContextTitle_;
        this.childContextType = b.childContextType_;
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
        strBuf.append("\n contextUUID = ").append(this.contextUUID);
        strBuf.append("\n contextIdentifier = ").append(this.contextIdentifier);
        strBuf.append("\n contextTitle = ").append(this.contextTitle);
        strBuf.append("\n contextType = ").append(this.contextType);
        strBuf.append("\n childContextUUID = ").append(this.childContextUUID);
        strBuf.append("\n childContextIdentifier = ").append(this.childContextIdentifier);
        strBuf.append("\n childContextTitle = ").append(this.childContextTitle);
        strBuf.append("\n childContextType = ").append(this.childContextType);
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
                            this.isNullOrEmpty(this.tenant) ||
                            this.isNullOrEmpty(this.source) ||
                            this.isNullOrEmpty(this.severity) ||
                            this.isNullOrEmpty(this.fingerprint) ||
                            this.isNullOrEmpty(this.contextUUID);
        return !badEvent;
    }

    public Event toZingEvent() {
        Event.Builder b = Event.newBuilder();
        b.setTenant(this.tenant);
        b.setTimestamp(this.occurrenceTime);
        // dimensions

        // metadata

        return b.build();
    }

    public static class Builder {
        private final String tenant_;
        private final String source_;
        private final long occurrenceTime_;
        private String fingerprint_;
        private String severity_;
        private String contextUUID_;
        private String contextIdentifier_;
        private String contextTitle_;
        private String contextType_;
        private String childContextUUID_;
        private String childContextIdentifier_;
        private String childContextTitle_;
        private String childContextType_;
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

        public Builder setChildContextUUID(String value){
            this.childContextUUID_ = value;
            return this;
        }
        
        public Builder setChildContextIdentifier(String value){
            this.childContextIdentifier_ = value;
            return this;
        }
        public Builder setChildContextTitle(String value){
            this.childContextTitle_ = value;
            return this;
        }
        public Builder setChildContextType(String value){
            this.childContextType_ = value;
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

