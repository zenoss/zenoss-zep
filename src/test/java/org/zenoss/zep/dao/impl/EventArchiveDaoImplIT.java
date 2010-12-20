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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.SyslogPriority;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventDao;
import org.zenoss.zep.dao.EventSummaryDao;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventArchiveDaoImplIT extends
        AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    public EventArchiveDao eventArchiveDao;

    @Autowired
    public EventSummaryDao eventSummaryDao;

    @Autowired
    public EventDao eventDao;

    private static void compareEvents(Event event, Event eventFromDb) {
        Event event1 = Event.newBuilder().mergeFrom(event).clearUuid()
                .clearCreatedTime().build();
        Event event2 = Event.newBuilder().mergeFrom(eventFromDb).clearUuid()
                .clearCreatedTime().build();
        assertEquals(event1, event2);
    }

    private EventSummary createArchive(Event event) throws ZepException {
        return eventArchiveDao.findByUuid(eventArchiveDao.create(event));
    }

    @Test
    public void testArchiveInsert() throws ZepException, InterruptedException {
        Event event = EventDaoImplIT.createSampleEvent();
        EventSummary eventSummaryFromDb = createArchive(event);
        Event eventFromSummary = eventSummaryFromDb.getOccurrence(0);
        compareEvents(event, eventFromSummary);
        assertEquals(1, eventSummaryFromDb.getCount());
        assertEquals(EventStatus.STATUS_CLOSED, eventSummaryFromDb.getStatus());
        assertEquals(eventFromSummary.getCreatedTime(),
                eventSummaryFromDb.getFirstSeenTime());
        assertEquals(eventFromSummary.getCreatedTime(),
                eventSummaryFromDb.getStatusChangeTime());
        assertEquals(eventFromSummary.getCreatedTime(),
                eventSummaryFromDb.getLastSeenTime());
        assertFalse(eventSummaryFromDb.hasAcknowledgedByUserUuid());
        assertFalse(eventSummaryFromDb.hasClearedByEventUuid());

        eventArchiveDao.delete(eventSummaryFromDb.getUuid());
        assertNull(eventArchiveDao.findByUuid(eventSummaryFromDb.getUuid()));
    }

    public static Event createEvent() {
        Event.Builder eventBuilder = Event.newBuilder();
        eventBuilder.setUuid(UUID.randomUUID().toString());
        eventBuilder.setCreatedTime(System.currentTimeMillis());
        eventBuilder.addDetails(EventDetail.newBuilder().setName("foo")
                .addValue("bar").addValue("baz").build());
        eventBuilder.addDetails(EventDetail.newBuilder().setName("foo2")
                .addValue("bar2").addValue("baz2").build());
        eventBuilder.setActor(EventDaoImplIT.createSampleActor());
        eventBuilder.setAgent("agent");
        eventBuilder.setEventClass("/Unknown");
        eventBuilder.setEventClassKey("eventClassKey");
        eventBuilder.setEventClassMappingUuid(UUID.randomUUID().toString());
        eventBuilder.setEventGroup("event group");
        eventBuilder.setEventKey("event key");
        eventBuilder.setFingerprint("my|dedupid|foo|");
        eventBuilder.setMessage("my message");
        eventBuilder.setMonitor("monitor");
        eventBuilder.setNtEventCode(new Random().nextInt(50000));
        eventBuilder.setSeverity(EventSeverity.SEVERITY_CRITICAL);
        eventBuilder.setSummary("summary message");
        eventBuilder.setSyslogFacility(11);
        eventBuilder.setSyslogPriority(SyslogPriority.SYSLOG_PRIORITY_DEBUG);
        eventBuilder.addTags(EventDaoImplIT.createTag(ModelElementType.DEVICE,
                eventBuilder.getActor().getElementUuid()));
        eventBuilder.addTags(EventDaoImplIT.createTag(
                ModelElementType.COMPONENT, eventBuilder.getActor()
                        .getElementSubUuid()));
        return eventBuilder.build();
    }

    @Test
    public void testListByUuid() throws ZepException {
        Set<String> uuids = new HashSet<String>();
        Set<String> uuidsToSearch = new HashSet<String>();
        for (int i = 0; i < 10; i++) {
            String uuid = createArchive(createEvent()).getUuid();
            uuids.add(uuid);
            if ((i % 2) == 0) {
                uuidsToSearch.add(uuid);
            }
        }

        List<EventSummary> result = eventArchiveDao
                .findByUuids(new ArrayList<String>(uuidsToSearch));
        assertEquals(uuidsToSearch.size(), result.size());
        for (EventSummary event : result) {
            assertTrue(uuidsToSearch.contains(event.getUuid()));
        }
    }

    @Test
    public void testAddNote() throws ZepException {
        EventSummary summary = createArchive(createEvent());
        assertEquals(0, summary.getNotesCount());
        EventNote note = EventNote.newBuilder().setMessage("My Note")
                .setUserName("pkw").setUserUuid(UUID.randomUUID().toString())
                .build();
        assertEquals(1, eventArchiveDao.addNote(summary.getUuid(), note));
        EventNote note2 = EventNote.newBuilder().setMessage("My Note 2")
                .setUserName("kww").setUserUuid(UUID.randomUUID().toString())
                .build();
        assertEquals(1, eventArchiveDao.addNote(summary.getUuid(), note2));
        summary = eventArchiveDao.findByUuid(summary.getUuid());
        assertEquals(2, summary.getNotesCount());
        assertEquals("My Note", summary.getNotes(0).getMessage());
        assertEquals("pkw", summary.getNotes(0).getUserName());
        assertEquals("My Note 2", summary.getNotes(1).getMessage());
        assertEquals("kww", summary.getNotes(1).getUserName());
    }
}
