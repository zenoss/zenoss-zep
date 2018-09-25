
package org.zenoss.zep.zing.impl;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class ZingEvent {

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
        this.occurrenceTime = b.builder_occurrenceTime;
        this.fingerprint = b.builder_fingerprint;
        this.severity = b.builder_severity;
        this.contextUUID = b.builder_contextUUID;
        this.contextIdentifier = b.builder_contextIdentifier;
        this.contextTitle = b.builder_contextTitle;
        this.contextType = b.builder_contextType;
        this.childContextUUID = b.builder_childContextUUID;
        this.childContextIdentifier = b.builder_childContextIdentifier;
        this.childContextTitle = b.builder_childContextTitle;
        this.childContextType = b.builder_childContextType;
        this.message = b.builder_message;
        this.summary = b.builder_summary;
        this.uuid = b.builder_uuid;
        this.monitor = b.builder_monitor;
        this.agent = b.builder_agent;
        this.eventKey = b.builder_eventKey;
        this.eventClass = b.builder_eventClass;
        this.eventClassKey = b.builder_eventClassKey;
        this.eventClassMappingUuid = b.builder_eventClassMappingUuid;
        this.eventGroup = b.builder_eventGroup;
        this.details = b.builder_details;
    }

    public String toString() {
        final StringBuffer strBuf = new StringBuffer();
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

    public boolean isValid() {
        boolean badEvent = this.occurrenceTime == 0 ||
                           this.severity == null    || this.severity.isEmpty() ||
                           this.fingerprint == null || this.fingerprint.isEmpty() ||
                           this.contextUUID == null || this.contextUUID.isEmpty();
        return !badEvent;
    }

    public static class Builder {
        private long builder_occurrenceTime;
        private String builder_fingerprint;
        private String builder_severity;
        private String builder_contextUUID;
        private String builder_contextIdentifier;
        private String builder_contextTitle;
        private String builder_contextType;
        private String builder_childContextUUID;
        private String builder_childContextIdentifier;
        private String builder_childContextTitle;
        private String builder_childContextType;
        private String builder_message;
        private String builder_summary;
        private String builder_uuid;
        private String builder_monitor;
        private String builder_agent;
        private String builder_eventKey;
        private String builder_eventClass;
        private String builder_eventClassKey;
        private String builder_eventClassMappingUuid;
        private String builder_eventGroup;

        private Map<String, List<String>> builder_details = new HashMap<>();

        public Builder() {
        }

        public Builder uuid(String value) {
            this.builder_uuid = value;
            return this;
        }

        public Builder occurrenceTime(long value) {
            this.builder_occurrenceTime = value;
            return this;
        }

        public Builder fingerprint(String value) {
            this.builder_fingerprint = value;
            return this;
        }

        public Builder severity(String value) {
            this.builder_severity = value;
            return this;
        }

        public Builder message(String value){
            this.builder_message = value;
            return this;
        }

        public Builder summary(String value){
            this.builder_summary = value;
            return this;
        }

        public Builder contextUUID(String value){
            this.builder_contextUUID = value;
            return this;
        }

        public Builder contextIdentifier(String value){
            this.builder_contextIdentifier = value;
            return this;
        }

        public Builder contextTitle(String value){
            this.builder_contextTitle = value;
            return this;
        }

        public Builder contextType(String value){
            this.builder_contextType = value;
            return this;
        }

        public Builder childContextUUID(String value){
            this.builder_childContextUUID = value;
            return this;
        }
        
        public Builder childContextIdentifier(String value){
            this.builder_childContextIdentifier = value;
            return this;
        }
        public Builder childContextTitle(String value){
            this.builder_childContextTitle = value;
            return this;
        }
        public Builder childContextType(String value){
            this.builder_childContextType = value;
            return this;
        }

        public Builder monitor(String value){
            this.builder_monitor = value;
            return this;
        }

        public Builder agent(String value){
            this.builder_agent = value;
            return this;
        }

        public Builder eventKey(String value) {
            this.builder_eventKey = value;
            return this;
        }
        public Builder eventClass(String value) {
            this.builder_eventClass = value;
            return this;
        }

        public Builder eventClassKey(String value) {
            this.builder_eventClassKey = value;
            return this;
        }

        public Builder eventClassMappingUuid(String value) {
            this.builder_eventClassMappingUuid = value;
            return this;
        }

        public Builder eventGroup(String value) {
            this.builder_eventGroup = value;
            return this;
        }

        public Builder detail(String k, List<String> v) {
            // FIXME do we need to clone v?
            this.builder_details.put(k, v);
            return this;
        }

        public ZingEvent build() {
            return new ZingEvent(this);
        }
    }
}

