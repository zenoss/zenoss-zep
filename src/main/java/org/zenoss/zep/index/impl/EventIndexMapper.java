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

import static org.zenoss.zep.index.impl.IndexConstants.*;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.util.Version;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.zep.ZepException;

public class EventIndexMapper {
    public static Analyzer createAnalyzer() {
        PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new KeywordAnalyzer());
        analyzer.addAnalyzer(FIELD_EVENT_ACTOR_ELEMENT_IDENTIFIER, new IdentifierAnalyzer());
        analyzer.addAnalyzer(FIELD_EVENT_ACTOR_ELEMENT_SUB_IDENTIFIER, new IdentifierAnalyzer());
        analyzer.addAnalyzer(FIELD_EVENT_SUMMARY, new StandardAnalyzer(Version.LUCENE_30));
        return analyzer;
    }

    public static Document fromEventSummary(EventSummary event_summary) throws ZepException {
        Document doc = new Document();

        doc.add(new Field(FIELD_PROTOBUF, event_summary.toByteArray(), Field.Store.YES));

        doc.add(new Field(FIELD_UUID, event_summary.getUuid(), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
        EventStatus status = event_summary.getStatus();
        if (status != null) {
            doc.add(new NumericField(FIELD_STATUS, Field.Store.NO, true).setIntValue(status.getNumber()));
        }
        doc.add(new NumericField(FIELD_COUNT, Field.Store.NO, true).setIntValue(event_summary.getCount()));
        doc.add(new NumericField(FIELD_LAST_SEEN_TIME, Field.Store.NO, true).setLongValue(event_summary.getLastSeenTime()));
        doc.add(new NumericField(FIELD_FIRST_SEEN_TIME, Field.Store.NO, true).setLongValue(event_summary.getFirstSeenTime()));
        doc.add(new NumericField(FIELD_STATUS_CHANGE_TIME, Field.Store.NO, true).setLongValue(event_summary.getStatusChangeTime()));
        doc.add(new NumericField(FIELD_UPDATE_TIME, Field.Store.NO, true).setLongValue(event_summary.getUpdateTime()));

        Event event = event_summary.getOccurrence(0);
        doc.add(new Field(FIELD_EVENT_UUID, event.getUuid(), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_EVENT_SUMMARY, event.getSummary(), Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field(FIELD_EVENT_SUMMARY_SORT, event.getSummary(), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
        EventSeverity severity = event.getSeverity();
        if (severity == null) {
            severity = EventSeverity.SEVERITY_INFO;
        }
        doc.add(new NumericField(FIELD_EVENT_SEVERITY, Field.Store.YES, true).setIntValue(severity.getNumber()));
        // Store with a trailing slash to make lookups simpler
        doc.add(new Field(FIELD_EVENT_EVENT_CLASS, event.getEventClass() + "/", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));

        for (EventTag tag : event.getTagsList()) {
            doc.add(new Field(FIELD_TAGS, tag.getUuid(), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
        }

        EventActor actor = event.getActor();
        if (actor != null) {
            String uuid = actor.getElementUuid();
            if (uuid != null) {
                doc.add(new Field(FIELD_TAGS, uuid, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
            }

            String id = actor.getElementIdentifier();
            if (id != null) {
                doc.add(new Field(FIELD_EVENT_ACTOR_ELEMENT_IDENTIFIER, id, Field.Store.NO, Field.Index.ANALYZED_NO_NORMS));
                doc.add(new Field(FIELD_EVENT_ACTOR_ELEMENT_IDENTIFIER_SORT, id, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
            }

            String subUuid = actor.getElementSubUuid();
            if (subUuid != null) {
                doc.add(new Field(FIELD_TAGS, subUuid, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
            }

            String subId = actor.getElementSubIdentifier();
            if (subId != null) {
                doc.add(new Field(FIELD_EVENT_ACTOR_ELEMENT_SUB_IDENTIFIER, subId, Field.Store.NO, Field.Index.ANALYZED_NO_NORMS));
                doc.add(new Field(FIELD_EVENT_ACTOR_ELEMENT_SUB_IDENTIFIER_SORT, subId, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
            }
        }
        return doc;
    }

    public static EventSummary toEventSummary(Document item) throws ZepException {
        EventSummary.Builder summaryBuilder = EventSummary.newBuilder();
        try {
            return summaryBuilder.mergeFrom(item.getBinaryValue(FIELD_PROTOBUF)).build();
        } catch (InvalidProtocolBufferException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }
}
