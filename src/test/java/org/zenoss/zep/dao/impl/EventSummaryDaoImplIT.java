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
import static org.zenoss.zep.dao.impl.EventConstants.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.protobufs.zep.Zep.SyslogPriority;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventDao;
import org.zenoss.zep.dao.EventSummaryDao;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventSummaryDaoImplIT extends
        AbstractTransactionalJUnit4SpringContextTests {

    private static Random random = new Random();

    @Autowired
    public EventSummaryDao eventSummaryDao;

    @Autowired
    public EventArchiveDao eventArchiveDao;

    @Autowired
    public EventDao eventDao;

    private EventSummary createSummaryNew(Event event) throws ZepException {
        return createSummary(event, EventStatus.STATUS_NEW);
    }

    private EventSummary createSummary(Event event, EventStatus status)
            throws ZepException {
        String uuid = eventSummaryDao.create(event, status);
        return eventSummaryDao.findByUuid(uuid);
    }

    private EventSummary createSummaryClear(Event event,
            Set<String> clearClasses) throws ZepException {
        String uuid = eventSummaryDao.createClearEvent(event, clearClasses);
        return eventSummaryDao.findByUuid(uuid);
    }

    private static void compareEvents(Event event, Event eventFromDb) {
        Event event1 = Event.newBuilder().mergeFrom(event).clearUuid()
                .clearCreatedTime().build();
        Event event2 = Event.newBuilder().mergeFrom(eventFromDb).clearUuid()
                .clearCreatedTime().build();
        assertEquals(event1, event2);
    }

    private static void compareSummary(EventSummary expected,
            EventSummary actual) {
        expected = EventSummary.newBuilder(expected).clearStatus()
                .clearStatusChangeTime().clearUpdateTime().build();
        actual = EventSummary.newBuilder(expected).clearStatus()
                .clearStatusChangeTime().clearUpdateTime().build();
        assertEquals(expected, actual);
    }

    @Test
    public void testSummaryInsert() throws ZepException, InterruptedException {
        Event event = EventDaoImplIT.createSampleEvent();
        EventSummary eventSummaryFromDb = createSummaryNew(event);
        Event eventFromSummary = eventSummaryFromDb.getOccurrence(0);
        compareEvents(event, eventFromSummary);
        assertEquals(1, eventSummaryFromDb.getCount());
        assertEquals(EventStatus.STATUS_NEW, eventSummaryFromDb.getStatus());
        assertEquals(eventFromSummary.getCreatedTime(),
                eventSummaryFromDb.getFirstSeenTime());
        assertEquals(eventFromSummary.getCreatedTime(),
                eventSummaryFromDb.getStatusChangeTime());
        assertEquals(eventFromSummary.getCreatedTime(),
                eventSummaryFromDb.getLastSeenTime());
        assertFalse(eventSummaryFromDb.hasAcknowledgedByUserUuid());
        assertFalse(eventSummaryFromDb.hasAcknowledgedByUserName());
        assertFalse(eventSummaryFromDb.hasClearedByEventUuid());

        /*
         * Create event with same fingerprint but again with new message,
         * summary, details.
         */
        Event.Builder newEventBuilder = Event.newBuilder().mergeFrom(event);
        newEventBuilder.setUuid(UUID.randomUUID().toString());
        newEventBuilder.setCreatedTime(System.currentTimeMillis());
        newEventBuilder.setMessage(event.getMessage() + random.nextInt(500));
        newEventBuilder.setSummary(event.getSummary() + random.nextInt(1000));
        newEventBuilder.clearDetails();
        newEventBuilder.addDetails(EventDetail.newBuilder().setName("newname1")
                .addValue("newvalue1").addValue("newvalue2"));
        Event newEvent = newEventBuilder.build();

        EventSummary newEventSummaryFromDb = createSummaryNew(newEvent);

        assertEquals(eventSummaryFromDb.getUuid(),
                newEventSummaryFromDb.getUuid());
        Event newEventFromSummary = newEventSummaryFromDb.getOccurrence(0);
        assertTrue(newEventSummaryFromDb.getLastSeenTime() > newEventSummaryFromDb
                .getFirstSeenTime());
        assertEquals(eventSummaryFromDb.getFirstSeenTime(),
                newEventSummaryFromDb.getFirstSeenTime());
        assertEquals(newEventFromSummary.getCreatedTime(),
                newEventSummaryFromDb.getLastSeenTime());
        assertEquals(newEvent.getCreatedTime(),
                newEventSummaryFromDb.getLastSeenTime());
        // Verify status didn't change (two NEW events)
        assertEquals(event.getCreatedTime(),
                newEventSummaryFromDb.getStatusChangeTime());
        assertEquals(newEvent.getMessage(), newEventFromSummary.getMessage());
        assertEquals(newEvent.getSummary(), newEventFromSummary.getSummary());
        assertEquals(newEvent.getDetailsList(),
                newEventFromSummary.getDetailsList());

        eventSummaryDao.delete(eventSummaryFromDb.getUuid());
        assertNull(eventSummaryDao.findByUuid(eventSummaryFromDb.getUuid()));
    }

    @Test
    public void testAcknowledgedToNew() throws ZepException {
        /*
         * Verify acknowledged events aren't changed to new with a new
         * occurrence.
         */
        Event event = EventDaoImplIT.createSampleEvent();
        eventSummaryDao.create(event, EventStatus.STATUS_ACKNOWLEDGED);
        Event.Builder newEventBuilder = Event.newBuilder().mergeFrom(event);
        newEventBuilder.setUuid(UUID.randomUUID().toString());
        newEventBuilder.setCreatedTime(event.getCreatedTime() + 50L);
        Event newEvent = newEventBuilder.build();

        EventSummary newEventSummaryFromDb = createSummaryNew(newEvent);

        assertEquals(EventStatus.STATUS_ACKNOWLEDGED,
                newEventSummaryFromDb.getStatus());
        assertEquals(event.getCreatedTime(),
                newEventSummaryFromDb.getStatusChangeTime());
    }

    @Test
    public void testAcknowledgedToSuppressed() throws ZepException {
        /*
         * Verify acknowledged events aren't changed to suppressed with a new
         * occurrence.
         */
        Event event = EventDaoImplIT.createSampleEvent();
        eventSummaryDao.create(event, EventStatus.STATUS_ACKNOWLEDGED);
        Event.Builder newEventBuilder = Event.newBuilder().mergeFrom(event);
        newEventBuilder.setUuid(UUID.randomUUID().toString());
        newEventBuilder.setCreatedTime(event.getCreatedTime() + 50L);
        Event newEvent = newEventBuilder.build();

        EventSummary newEventSummaryFromDb = createSummary(newEvent,
                EventStatus.STATUS_SUPPRESSED);

        assertEquals(EventStatus.STATUS_ACKNOWLEDGED,
                newEventSummaryFromDb.getStatus());
        assertEquals(event.getCreatedTime(),
                newEventSummaryFromDb.getStatusChangeTime());
    }

    private static String createRandomMaxString(int length) {
        final String alphabet = "abcdefghijklmnopqrstuvwxyz";
        final char[] chars = new char[length];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = alphabet.charAt(random.nextInt(alphabet.length()));
        }
        return new String(chars);
    }

    @Test
    public void testSummaryMaxInsert() throws ZepException,
            InterruptedException {
        Event.Builder eventBuilder = Event.newBuilder(createUniqueEvent());
        EventActor.Builder actorBuilder = EventActor.newBuilder(eventBuilder.getActor());
        eventBuilder.setFingerprint(createRandomMaxString(MAX_FINGERPRINT + 1));
        actorBuilder.setElementIdentifier(createRandomMaxString(MAX_ELEMENT_IDENTIFIER + 1));
        actorBuilder.setElementSubIdentifier(createRandomMaxString(MAX_ELEMENT_SUB_IDENTIFIER + 1));
        eventBuilder.setEventClass(createRandomMaxString(MAX_EVENT_CLASS + 1));
        eventBuilder.setEventClassKey(createRandomMaxString(MAX_EVENT_CLASS_KEY + 1));
        eventBuilder.setEventKey(createRandomMaxString(MAX_EVENT_KEY + 1));
        eventBuilder.setMonitor(createRandomMaxString(MAX_MONITOR + 1));
        eventBuilder.setAgent(createRandomMaxString(MAX_AGENT + 1));
        eventBuilder.setEventGroup(createRandomMaxString(MAX_EVENT_GROUP + 1));
        eventBuilder.setSummary(createRandomMaxString(MAX_SUMMARY + 1));
        eventBuilder.setMessage(createRandomMaxString(MAX_MESSAGE + 1));
        eventBuilder.setActor(actorBuilder.build());
        final Event event = eventBuilder.build();
        final EventSummary summary = createSummaryNew(event);
        final Event eventFromDb = summary.getOccurrence(0);
        final EventActor actorFromDb = eventFromDb.getActor();
        assertEquals(MAX_FINGERPRINT, eventFromDb.getFingerprint().length());
        assertEquals(MAX_ELEMENT_IDENTIFIER, actorFromDb.getElementIdentifier().length());
        assertEquals(MAX_ELEMENT_SUB_IDENTIFIER, actorFromDb.getElementSubIdentifier().length());
        assertEquals(MAX_EVENT_CLASS, eventFromDb.getEventClass().length());
        assertEquals(MAX_EVENT_CLASS_KEY, eventFromDb.getEventClassKey().length());
        assertEquals(MAX_EVENT_KEY, eventFromDb.getEventKey().length());
        assertEquals(MAX_MONITOR, eventFromDb.getMonitor().length());
        assertEquals(MAX_AGENT, eventFromDb.getAgent().length());
        assertEquals(MAX_EVENT_GROUP, eventFromDb.getEventGroup().length());
        assertEquals(MAX_SUMMARY, eventFromDb.getSummary().length());
        assertEquals(MAX_MESSAGE, eventFromDb.getMessage().length());

        this.eventSummaryDao.acknowledge(Collections.singletonList(summary.getUuid()), UUID.randomUUID().toString(),
                createRandomMaxString(MAX_ACKNOWLEDGED_BY_USER_NAME + 1));
        final EventSummary summaryFromDb = this.eventSummaryDao.findByUuid(summary.getUuid());
        assertEquals(MAX_ACKNOWLEDGED_BY_USER_NAME, summaryFromDb.getAcknowledgedByUserName().length());
    }

    public static Event createUniqueEvent() {
        Random r = new Random();
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
        eventBuilder.setFingerprint("my|dedupid|foo|" + r.nextInt());
        eventBuilder.setMessage("my message");
        eventBuilder.setMonitor("monitor");
        eventBuilder.setNtEventCode(r.nextInt(50000));
        eventBuilder.setSeverity(EventSeverity.SEVERITY_CRITICAL);
        eventBuilder.setSummary("summary message");
        eventBuilder.setSyslogFacility(11);
        eventBuilder.setSyslogPriority(SyslogPriority.SYSLOG_PRIORITY_DEBUG);
        return eventBuilder.build();
    }

    @Test
    public void testListByUuid() throws ZepException {
        Set<String> uuids = new HashSet<String>();
        Set<String> uuidsToSearch = new HashSet<String>();
        for (int i = 0; i < 10; i++) {
            String uuid = createSummaryNew(createUniqueEvent()).getUuid();
            uuids.add(uuid);
            if ((i % 2) == 0) {
                uuidsToSearch.add(uuid);
            }
        }

        List<EventSummary> result = eventSummaryDao
                .findByUuids(new ArrayList<String>(uuidsToSearch));
        assertEquals(uuidsToSearch.size(), result.size());
        for (EventSummary event : result) {
            assertTrue(uuidsToSearch.contains(event.getUuid()));
        }
    }

    @Test
    public void testReopen() throws ZepException {
        EventSummary summary = createSummaryNew(createUniqueEvent());
        long origStatusChange = summary.getStatusChangeTime();
        long origUpdateTime = summary.getUpdateTime();
        assertEquals(EventStatus.STATUS_NEW, summary.getStatus());
        assertFalse(summary.hasAcknowledgedByUserUuid());
        assertFalse(summary.hasAcknowledgedByUserName());

        String userUuid = UUID.randomUUID().toString();
        String userName = "user" + random.nextInt(500);
        int numUpdated = eventSummaryDao.acknowledge(
                Collections.singletonList(summary.getUuid()), userUuid, userName);
        assertEquals(1, numUpdated);
        summary = eventSummaryDao.findByUuid(summary.getUuid());
        assertEquals(userUuid, summary.getAcknowledgedByUserUuid());
        assertEquals(userName, summary.getAcknowledgedByUserName());
        assertEquals(EventStatus.STATUS_ACKNOWLEDGED, summary.getStatus());
        assertTrue(summary.getStatusChangeTime() > origStatusChange);
        assertTrue(summary.getUpdateTime() > origUpdateTime);
        origStatusChange = summary.getStatusChangeTime();
        origUpdateTime = summary.getUpdateTime();

        /* Now reopen event */
        numUpdated = eventSummaryDao.reopen(Collections.singletonList(summary
                .getUuid()));
        assertEquals(1, numUpdated);
        EventSummary origSummary = summary;
        summary = eventSummaryDao.findByUuid(summary.getUuid());
        assertFalse(summary.hasAcknowledgedByUserUuid());
        assertFalse(summary.hasAcknowledgedByUserName());
        assertEquals(EventStatus.STATUS_NEW, summary.getStatus());
        assertTrue(summary.getStatusChangeTime() > origStatusChange);
        assertTrue(summary.getUpdateTime() > origUpdateTime);

        compareSummary(origSummary, summary);
    }

    @Test
    public void testAcknowledge() throws ZepException {
        EventSummary summary = createSummaryNew(createUniqueEvent());
        long origStatusChange = summary.getStatusChangeTime();
        long origUpdateTime = summary.getUpdateTime();
        assertEquals(EventStatus.STATUS_NEW, summary.getStatus());
        assertFalse(summary.hasAcknowledgedByUserUuid());
        assertFalse(summary.hasAcknowledgedByUserName());

        String userUuid = UUID.randomUUID().toString();
        String userName = "user" + random.nextInt(500);
        EventSummary origSummary = summary;
        int numUpdated = eventSummaryDao.acknowledge(
                Collections.singletonList(summary.getUuid()), userUuid, userName);
        assertEquals(1, numUpdated);
        summary = eventSummaryDao.findByUuid(summary.getUuid());
        assertEquals(userUuid, summary.getAcknowledgedByUserUuid());
        assertEquals(userName, summary.getAcknowledgedByUserName());
        assertEquals(EventStatus.STATUS_ACKNOWLEDGED, summary.getStatus());
        assertTrue(summary.getStatusChangeTime() > origStatusChange);
        assertTrue(summary.getUpdateTime() > origUpdateTime);

        compareSummary(origSummary, summary);
    }

    @Test
    public void testSuppress() throws ZepException {
        EventSummary summary = createSummaryNew(createUniqueEvent());
        long origStatusChange = summary.getStatusChangeTime();
        long origUpdateTime = summary.getUpdateTime();
        assertEquals(EventStatus.STATUS_NEW, summary.getStatus());
        assertFalse(summary.hasAcknowledgedByUserUuid());
        assertFalse(summary.hasAcknowledgedByUserName());

        EventSummary origSummary = summary;
        int numUpdated = eventSummaryDao.suppress(Collections
                .singletonList(summary.getUuid()));
        assertEquals(1, numUpdated);
        summary = eventSummaryDao.findByUuid(summary.getUuid());
        assertEquals(EventStatus.STATUS_SUPPRESSED, summary.getStatus());
        assertTrue(summary.getStatusChangeTime() > origStatusChange);
        assertTrue(summary.getUpdateTime() > origUpdateTime);

        compareSummary(origSummary, summary);
    }

    @Test
    public void testClose() throws ZepException {
        EventSummary summary = createSummaryNew(createUniqueEvent());
        long origStatusChange = summary.getStatusChangeTime();
        long origUpdateTime = summary.getUpdateTime();
        assertEquals(EventStatus.STATUS_NEW, summary.getStatus());
        assertFalse(summary.hasAcknowledgedByUserUuid());
        assertFalse(summary.hasAcknowledgedByUserName());

        EventSummary origSummary = summary;
        int numUpdated = eventSummaryDao.close(Collections
                .singletonList(summary.getUuid()));
        assertEquals(1, numUpdated);
        summary = eventSummaryDao.findByUuid(summary.getUuid());
        assertEquals(EventStatus.STATUS_CLOSED, summary.getStatus());
        assertTrue(summary.getStatusChangeTime() > origStatusChange);
        assertTrue(summary.getUpdateTime() > origUpdateTime);

        compareSummary(origSummary, summary);
    }

    @Test
    public void testAddNote() throws ZepException {
        EventSummary summary = createSummaryNew(createUniqueEvent());
        assertEquals(0, summary.getNotesCount());
        EventNote note = EventNote.newBuilder().setMessage("My Note")
                .setUserName("pkw").setUserUuid(UUID.randomUUID().toString())
                .build();
        assertEquals(1, eventSummaryDao.addNote(summary.getUuid(), note));
        EventNote note2 = EventNote.newBuilder().setMessage("My Note 2")
                .setUserName("kww").setUserUuid(UUID.randomUUID().toString())
                .build();
        assertEquals(1, eventSummaryDao.addNote(summary.getUuid(), note2));
        summary = eventSummaryDao.findByUuid(summary.getUuid());
        assertEquals(2, summary.getNotesCount());
        // Notes returned in descending order to match previous behavior
        assertEquals("My Note 2", summary.getNotes(0).getMessage());
        assertEquals("kww", summary.getNotes(0).getUserName());
        assertEquals("My Note", summary.getNotes(1).getMessage());
        assertEquals("pkw", summary.getNotes(1).getUserName());
    }

    private Event createOldEvent(long duration, TimeUnit unit,
            EventSeverity severity) {
        Event.Builder eventBuilder = Event.newBuilder(createUniqueEvent());
        eventBuilder.setCreatedTime(System.currentTimeMillis()
                - unit.toMillis(duration));
        eventBuilder.setSeverity(severity);
        return eventBuilder.build();
    }

    @Test
    public void testAgeEvents() throws ZepException {
        EventSummary warning = createSummaryNew(createOldEvent(5,
                TimeUnit.MINUTES, EventSeverity.SEVERITY_WARNING));
        EventSummary error = createSummaryNew(createOldEvent(5,
                TimeUnit.MINUTES, EventSeverity.SEVERITY_ERROR));
        EventSummary info = createSummaryNew(createOldEvent(5,
                TimeUnit.MINUTES, EventSeverity.SEVERITY_INFO));

        /* Aging should have aged WARNING and INFO events and left ERROR */
        int numAged = eventSummaryDao.ageEvents(4, TimeUnit.MINUTES,
                EventSeverity.SEVERITY_ERROR, 100);
        assertEquals(2, numAged);
        assertEquals(error, eventSummaryDao.findByUuid(error.getUuid()));
        /* Compare ignoring status change time */
        EventSummary summaryWarning = eventSummaryDao.findByUuid(warning
                .getUuid());
        assertTrue(summaryWarning.getStatusChangeTime() > warning
                .getStatusChangeTime());
        assertEquals(EventStatus.STATUS_AGED, summaryWarning.getStatus());
        EventSummary summaryInfo = eventSummaryDao.findByUuid(info.getUuid());
        assertTrue(summaryInfo.getStatusChangeTime() > info
                .getStatusChangeTime());
        assertEquals(EventStatus.STATUS_AGED, summaryInfo.getStatus());
        compareSummary(summaryWarning, warning);
        compareSummary(summaryInfo, info);

        int archived = eventSummaryDao.archive(4, TimeUnit.MINUTES, 100);
        assertEquals(2, archived);
        assertNull(eventSummaryDao.findByUuid(warning.getUuid()));
        assertNull(eventSummaryDao.findByUuid(info.getUuid()));
        /* Compare ignoring status change time */
        EventSummary archivedWarning = eventArchiveDao.findByUuid(warning
                .getUuid());
        assertTrue(archivedWarning.getStatusChangeTime() > warning
                .getStatusChangeTime());
        EventSummary archivedInfo = eventArchiveDao.findByUuid(info.getUuid());
        assertTrue(archivedInfo.getStatusChangeTime() > info
                .getStatusChangeTime());
        compareSummary(archivedWarning, summaryWarning);
        compareSummary(archivedInfo, summaryInfo);
    }

    @Test
    public void testArchiveEventsEmpty() throws ZepException {
        // Make sure we don't fail if there are no events to archive
        int numArchived = this.eventSummaryDao.archive(0L, TimeUnit.SECONDS,
                500);
        assertEquals(0, numArchived);
    }

    @Test
    public void testClearEvents() throws ZepException {
        Event event = createUniqueEvent();
        EventSummary normalEvent = createSummaryNew(Event.newBuilder(event)
                .setSeverity(EventSeverity.SEVERITY_WARNING)
                .setEventKey("MyKey1").build());
        EventSummary clearEvent = createSummaryClear(
                Event.newBuilder(createUniqueEvent())
                        .setSeverity(EventSeverity.SEVERITY_CLEAR)
                        .setEventKey("MyKey1").build(),
                Collections.singleton(normalEvent.getOccurrence(0)
                        .getEventClass()));
        assertEquals(EventStatus.STATUS_CLOSED, clearEvent.getStatus());
        assertEquals(1, eventSummaryDao.clearEvents());
        EventSummary normalEventSummary = eventSummaryDao
                .findByUuid(normalEvent.getUuid());
        assertEquals(EventStatus.STATUS_CLEARED, normalEventSummary.getStatus());
        assertTrue(normalEventSummary.getStatusChangeTime() > normalEvent
                .getStatusChangeTime());
        compareSummary(normalEvent, normalEventSummary);
    }

    @Test
    public void testClearEventsOutOfOrder() throws ZepException {
        Event event = createUniqueEvent();
        /* Create clear event first with later timestamp */
        EventSummary clearEvent = createSummaryClear(
                Event.newBuilder(createUniqueEvent())
                        .setCreatedTime(event.getCreatedTime() + 100L)
                        .setSeverity(EventSeverity.SEVERITY_CLEAR).build(),
                Collections.singleton(event.getEventClass()));
        assertEquals(EventStatus.STATUS_CLOSED, clearEvent.getStatus());
        EventSummary normalEvent = createSummaryNew(Event.newBuilder(event)
                .setSeverity(EventSeverity.SEVERITY_WARNING).build());
        assertEquals(1, eventSummaryDao.clearEvents());
        EventSummary normalEventSummary = eventSummaryDao
                .findByUuid(normalEvent.getUuid());
        assertTrue(normalEventSummary.getStatusChangeTime() > normalEvent
                .getStatusChangeTime());
        assertEquals(EventStatus.STATUS_CLEARED, normalEventSummary.getStatus());
        compareSummary(normalEvent, normalEventSummary);
    }

    @Test
    public void testClearClasses() throws ZepException {

    }

    @Test
    public void testMergeDuplicateDetails() throws ZepException {
        String name = "dup1";
        String val1 = "dupval";
        String val2 = "dupval2";
        String val3 = "dupval3";
        Event.Builder eventBuilder = Event.newBuilder(createUniqueEvent())
                .clearDetails();
        eventBuilder.addDetails(EventDetail.newBuilder().setName(name)
                .addValue(val1).build());
        eventBuilder.addDetails(EventDetail.newBuilder().setName(name)
                .addValue(val2).addValue(val3).build());
        Event event = eventBuilder.build();
        EventSummary summary = createSummaryNew(event);
        assertEquals(1, summary.getOccurrence(0).getDetailsCount());
        EventDetail detailFromDb = summary.getOccurrence(0).getDetails(0);
        assertEquals(name, detailFromDb.getName());
        assertEquals(Arrays.asList(val1, val2, val3),
                detailFromDb.getValueList());
    }

    @Test
    public void testFilterDuplicateTags() throws ZepException {
        Event.Builder eventBuilder = Event.newBuilder(createUniqueEvent())
                .clearTags();
        String uuid = UUID.randomUUID().toString();
        eventBuilder.addTags(EventTag.newBuilder()
                .setType(ModelElementType.DEVICE.name()).setUuid(uuid).build());
        eventBuilder.addTags(EventTag.newBuilder()
                .setType(ModelElementType.DEVICE.name()).setUuid(uuid).build());
        eventBuilder.addTags(EventTag.newBuilder()
                .setType(ModelElementType.DEVICE.name()).setUuid(uuid).build());
        Event event = eventBuilder.build();
        EventSummary summary = createSummaryNew(event);
        int numFound = 0;
        for (EventTag tag : summary.getOccurrence(0).getTagsList()) {
            if (tag.getUuid().equals(uuid)) {
                numFound++;
            }
        }
        assertEquals(1, numFound);
    }
}
