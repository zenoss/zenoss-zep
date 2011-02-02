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
package org.zenoss.zep.index.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericField;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.zep.ZepException;

import static org.zenoss.zep.index.impl.IndexConstants.*;

public class EventIndexMapper {
    public static Analyzer createAnalyzer() {
        final PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new KeywordAnalyzer());
        analyzer.addAnalyzer(FIELD_ELEMENT_IDENTIFIER, new IdentifierAnalyzer());
        analyzer.addAnalyzer(FIELD_ELEMENT_SUB_IDENTIFIER, new IdentifierAnalyzer());
        analyzer.addAnalyzer(FIELD_SUMMARY, new SummaryAnalyzer());
        analyzer.addAnalyzer(FIELD_EVENT_CLASS, new EventClassAnalyzer());
        return analyzer;
    }

    public static Document fromEventSummary(EventSummary summary) {
        Document doc = new Document();

        // Store the entire serialized protobuf so we can reproduce the entire event from the index.
        doc.add(new Field(FIELD_PROTOBUF, summary.toByteArray(), Store.YES));

        // Store the UUID for more lightweight queries against the index
        doc.add(new Field(FIELD_UUID, summary.getUuid(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));

        doc.add(new Field(FIELD_ACKNOWLEDGED_BY_USER_NAME, summary.getAcknowledgedByUserName(), Store.NO,
                Index.NOT_ANALYZED_NO_NORMS));

        doc.add(new NumericField(FIELD_STATUS, Store.NO, true).setIntValue(summary.getStatus().getNumber()));        
        doc.add(new NumericField(FIELD_COUNT, Store.NO, true).setIntValue(summary.getCount()));
        doc.add(new NumericField(FIELD_LAST_SEEN_TIME, Store.NO, true).setLongValue(summary.getLastSeenTime()));
        doc.add(new NumericField(FIELD_FIRST_SEEN_TIME, Store.NO, true).setLongValue(summary.getFirstSeenTime()));
        doc.add(new NumericField(FIELD_STATUS_CHANGE_TIME, Store.NO, true).setLongValue(summary.getStatusChangeTime()));
        doc.add(new NumericField(FIELD_UPDATE_TIME, Store.NO, true).setLongValue(summary.getUpdateTime()));

        Event event = summary.getOccurrence(0);
        doc.add(new Field(FIELD_FINGERPRINT, event.getFingerprint(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_SUMMARY, event.getSummary(), Store.NO, Index.ANALYZED));
        doc.add(new Field(FIELD_SUMMARY_NOT_ANALYZED, event.getSummary(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new NumericField(FIELD_SEVERITY, Store.YES, true).setIntValue(event.getSeverity().getNumber()));

        doc.add(new Field(FIELD_EVENT_CLASS, event.getEventClass(), Store.NO, Index.ANALYZED));
        // Store with a trailing slash to make lookups simpler
        doc.add(new Field(FIELD_EVENT_CLASS_NOT_ANALYZED, event.getEventClass().toLowerCase() + "/", Store.NO,
                Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_AGENT, event.getAgent(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_MONITOR, event.getMonitor(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));

        for (EventTag tag : event.getTagsList()) {
            doc.add(new Field(FIELD_TAGS, tag.getUuid(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        }

        EventActor actor = event.getActor();
        String uuid = actor.getElementUuid();
        if (uuid != null && !uuid.isEmpty()) {
            doc.add(new Field(FIELD_TAGS, uuid, Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        }

        String id = actor.getElementIdentifier();
        doc.add(new Field(FIELD_ELEMENT_IDENTIFIER, id, Store.NO, Index.ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_ELEMENT_IDENTIFIER_NOT_ANALYZED, id, Store.NO, Index.NOT_ANALYZED_NO_NORMS));

        String subUuid = actor.getElementSubUuid();
        if (subUuid != null && !subUuid.isEmpty()) {
            doc.add(new Field(FIELD_TAGS, subUuid, Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        }

        String subId = actor.getElementSubIdentifier();
        doc.add(new Field(FIELD_ELEMENT_SUB_IDENTIFIER, subId, Store.NO, Index.ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_ELEMENT_SUB_IDENTIFIER_NOT_ANALYZED, subId, Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        return doc;
    }

    public static EventSummary toEventSummary(Document item) throws ZepException {
        EventSummary.Builder summaryBuilder = EventSummary.newBuilder();
        try {
            final byte[] protobuf = item.getBinaryValue(FIELD_PROTOBUF);
            if (protobuf != null) {
                summaryBuilder.mergeFrom(protobuf);
            }
            else {
                // Only other possible fields stored on index.
                final String uuid = item.get(FIELD_UUID);
                final String severityStr = item.get(FIELD_SEVERITY);
                if (uuid != null) {
                    summaryBuilder.setUuid(uuid);
                }
                if (severityStr != null) {
                    EventSeverity severity = EventSeverity.valueOf(Integer.valueOf(severityStr));
                    summaryBuilder.addOccurrence(Event.newBuilder().setSeverity(severity).build());
                }
            }
            return summaryBuilder.build();
        } catch (InvalidProtocolBufferException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }
}
