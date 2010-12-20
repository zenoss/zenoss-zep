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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.zenoss.protobufs.JsonFormat;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.zep.ZepException;

public class SolrEventIndexItemMapper {
    public static SolrEventIndexItem fromEventSummary(EventSummary event_summary) throws ZepException {
        SolrEventIndexItem item = new SolrEventIndexItem();
        item.uuid = event_summary.getUuid();
        item.status = event_summary.hasStatus() ? event_summary.getStatus().getNumber() : EventStatus.STATUS_NEW.getNumber();
        item.count = event_summary.getCount();
        item.last_seen_time = new Date(event_summary.getLastSeenTime());
        item.first_seen_time = new Date(event_summary.getFirstSeenTime());
        item.status_change_time = new Date(event_summary.getStatusChangeTime());
        item.update_time = new Date(event_summary.getUpdateTime());

        Event event = event_summary.getOccurrence(0);

        item.event_uuid = event.getUuid();
        item.event_summary = event.getSummary();
        item.event_severity = event.hasSeverity() ? event.getSeverity().getNumber() : EventSeverity.SEVERITY_INFO.getNumber();
        item.event_event_class = event.getEventClass();

        if ( item.event_event_class != null && !item.event_event_class.isEmpty() ) {
            // Store with a trailing slash to make lookups simpler
            item.event_event_class_index = item.event_event_class + "/";
        }
        
        if ( event.getTagsCount() > 0 ) {
            item.tags = new ArrayList<String>(event.getTagsCount());
            for (EventTag tag : event.getTagsList()) {
                item.tags.add(tag.getUuid());
            }

            try {
                item.tag_json = JsonFormat.writeAllDelimitedAsString(event.getTagsList());
            } catch (IOException e) {
                throw new ZepException(e);
            }
        }

        EventActor actor = event.getActor();
        if (actor != null) {
            if (actor.hasElementTypeId()) {
                item.event_actor_element_type_id = actor.getElementTypeId().name();
            }
            item.event_actor_element_uuid = actor.getElementUuid();
            item.event_actor_element_identifier = actor.getElementIdentifier();

            if (actor.hasElementSubTypeId()) {
                item.event_actor_element_sub_type_id = actor.getElementSubTypeId().name();
            }
            item.event_actor_element_sub_uuid = actor.getElementSubUuid();
            item.event_actor_element_sub_identifier = actor.getElementSubIdentifier();
        }
        
        return item;
    }

    public static EventSummary toEventSummary(SolrEventIndexItem item) throws ZepException {
        EventSummary.Builder summaryBuilder = EventSummary.newBuilder();

        if (item.uuid != null) {
            summaryBuilder.setUuid(item.uuid);
        }
        if (item.status != null) {
            summaryBuilder.setStatus(EventStatus.valueOf(item.status));
        }
        if (item.count != null) {
            summaryBuilder.setCount(item.count);
        }
        if (item.last_seen_time != null) {
            summaryBuilder.setLastSeenTime(item.last_seen_time.getTime());
        }
        if (item.first_seen_time != null) {
            summaryBuilder.setFirstSeenTime(item.first_seen_time.getTime());
        }
        if (item.status_change_time != null) {
            summaryBuilder.setStatusChangeTime(item.status_change_time.getTime());
        }
        if (item.update_time != null) {
            summaryBuilder.setUpdateTime(item.update_time.getTime());
        }
        
        Event occurrence = toEventOccurrence(item);
        if (occurrence != null) {
            summaryBuilder.addOccurrence(occurrence);
        }

        return summaryBuilder.build();
    }
    
    private static Event toEventOccurrence(SolrEventIndexItem item) throws ZepException {
        int numSet = 0;
        Event.Builder eventBuilder = Event.newBuilder();
        
        if (item.event_uuid != null) {
            ++numSet;
            eventBuilder.setUuid(item.event_uuid);
        }
        if (item.event_summary != null) {
            ++numSet;
            eventBuilder.setSummary(item.event_summary);
        }
        if (item.event_severity != null) {
            ++numSet;
            eventBuilder.setSeverity(EventSeverity.valueOf(item.event_severity));
        }
        if (item.event_event_class != null) {
            ++numSet;
            eventBuilder.setEventClass(item.event_event_class);
        }
        
        if ( item.tag_json != null && !item.tag_json.isEmpty() ) {
            ++numSet;
            try {
                List<EventTag> tags = JsonFormat.mergeAllDelimitedFrom(item.tag_json, EventTag.getDefaultInstance());
                eventBuilder.addAllTags(tags);
            } catch (IOException e) {
                throw new ZepException(e);
            }
        }
       
        EventActor actor = toEventActor(item);
        if (actor != null) {
            ++numSet;
            eventBuilder.setActor(actor);
        }

        return (numSet > 0) ? eventBuilder.build() : null;
    }
    
    private static EventActor toEventActor(SolrEventIndexItem item) {
        EventActor.Builder actor = EventActor.newBuilder();
        int numSet = 0;

        if ( item.event_actor_element_type_id != null && !item.event_actor_element_type_id.isEmpty() ) {
            actor.setElementTypeId(ModelElementType.valueOf(item.event_actor_element_type_id));
            ++numSet;
        }

        if (item.event_actor_element_uuid != null) {
            actor.setElementUuid(item.event_actor_element_uuid);
            ++numSet;
        }
        if (item.event_actor_element_identifier != null) {
            actor.setElementIdentifier(item.event_actor_element_identifier);
            ++numSet;
        }

        if ( item.event_actor_element_sub_type_id != null && !item.event_actor_element_sub_type_id.isEmpty() ) {
            actor.setElementSubTypeId(ModelElementType.valueOf(item.event_actor_element_sub_type_id));
            ++numSet;
        }    

        if (item.event_actor_element_sub_uuid != null) {
            actor.setElementSubUuid(item.event_actor_element_sub_uuid);
            ++numSet;
        }
        if (item.event_actor_element_sub_identifier != null) {
            actor.setElementSubIdentifier(item.event_actor_element_sub_identifier);
            ++numSet;
        }
        return (numSet > 0) ? actor.build() : null;
    }
}
