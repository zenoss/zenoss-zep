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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.model.Model;
import org.zenoss.protobufs.zep.Zep;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryFilter;
import org.zenoss.protobufs.zep.Zep.EventSummaryRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryResult;
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.protobufs.zep.Zep.FilterOperator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.dao.impl.EventDaoImplIT;
import org.zenoss.zep.index.EventIndexDao;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventIndexDaoImplIT extends
        AbstractTransactionalJUnit4SpringContextTests {
    @Autowired
    public EventSummaryDao eventSummaryDao;

    @Autowired
    @Qualifier("summary")
    public EventIndexDao eventIndexDao;

    @Before
    public void setUp() throws ZepException {
        eventIndexDao.clear();
    }

    @After
    public void tearDown() throws ZepException {
        eventIndexDao.clear();
    }
    
    private EventSummary createSummaryNew(Event event) throws ZepException {
        return createSummary(event, EventStatus.STATUS_NEW);
    }

    private EventSummary createSummary(Event event, EventStatus status) throws ZepException {
        String uuid = eventSummaryDao.create(event, status);
        return eventSummaryDao.findByUuid(uuid);
    }

    @Test
    public void testList() throws ZepException {
        Set<String> uuidsToSearch = new HashSet<String>();

        EventSummary eventSummaryFromDb;

        eventSummaryFromDb = createSummaryNew(EventDaoImplIT.createSampleEvent());
        uuidsToSearch.add(eventSummaryFromDb.getUuid());

        eventIndexDao.index(eventSummaryFromDb);

        eventSummaryFromDb = createSummaryNew(EventDaoImplIT.createSampleEvent());
        uuidsToSearch.add(eventSummaryFromDb.getUuid());

        eventIndexDao.index(eventSummaryFromDb);
        eventIndexDao.commit(true);

        EventSummaryRequest.Builder requestBuilder = EventSummaryRequest.newBuilder();
        requestBuilder.setLimit(10);
        EventSummaryRequest request = requestBuilder.build();

        EventSummaryResult result = eventIndexDao.list(request);

        Set<String> uuidsFound = new HashSet<String>();
        for (EventSummary e : result.getEventsList()) {
            uuidsFound.add(e.getUuid());
        }

        assertEquals(uuidsToSearch, uuidsFound);

        assertEquals(10, result.getLimit());
        assertEquals(2, result.getEventsCount());
    }

    @Test
    public void testIndex() throws ZepException {
        Event event = EventDaoImplIT.createSampleEvent();
        EventSummary eventSummaryFromDb = createSummaryNew(event);

        eventIndexDao.index(eventSummaryFromDb);
    }

    @Test
    public void testFindByUuid() throws AssertionError, ZepException {
        Event event = EventDaoImplIT.createSampleEvent();
        EventSummary eventSummaryFromDb = createSummaryNew(event);

        eventIndexDao.index(eventSummaryFromDb);

        assertNotNull(eventIndexDao.findByUuid(eventSummaryFromDb.getUuid()));
    }

    @Test
    public void testDelete() throws AssertionError, ZepException {
        Event event = EventDaoImplIT.createSampleEvent();
        EventSummary eventSummaryFromDb = createSummaryNew(event);

        eventIndexDao.index(eventSummaryFromDb);

        assertNotNull(eventIndexDao.findByUuid(eventSummaryFromDb.getUuid()));

        eventIndexDao.delete(eventSummaryFromDb.getUuid());

        assertNull(eventIndexDao.findByUuid(eventSummaryFromDb.getUuid()));
    }

    @Test
    public void testIndexMany() throws ZepException {
        Event event = EventDaoImplIT.createSampleEvent();
        EventSummary eventSummaryFromDb = createSummaryNew(event);

        List<EventSummary> events = new ArrayList<EventSummary>();
        events.add(eventSummaryFromDb);
        events.add(eventSummaryFromDb);

        eventIndexDao.indexMany(events);
    }

    private EventSummary createEventWithSeverity(EventSeverity severity,
            EventStatus status, String... tags) throws ZepException {
        final Event.Builder eventBuilder = Event.newBuilder(EventDaoImplIT
                .createSampleEvent());
        eventBuilder.setSeverity(severity);
        eventBuilder.clearTags();
        for (String tag : tags) {
            eventBuilder.addTags(EventTag.newBuilder().setUuid(tag).setType("zenoss.device").build());
        }
        final Event event = eventBuilder.build();
        EventSummary summary = createSummary(event, status);
        eventIndexDao.index(summary);
        return summary;
    }

    @Test
    public void findWorstSeverity() throws ZepException {
        String tag1 = UUID.randomUUID().toString();
        String tag2 = UUID.randomUUID().toString();
        String tag3 = UUID.randomUUID().toString();

        /* Create error severity with two tags */
        createEventWithSeverity(EventSeverity.SEVERITY_WARNING, EventStatus.STATUS_NEW, tag1, tag2);
        /* Create critical severity with one tag */
        createEventWithSeverity(EventSeverity.SEVERITY_ERROR, EventStatus.STATUS_NEW, tag2);
        /* Create closed event with all three tags */
        createEventWithSeverity(EventSeverity.SEVERITY_CRITICAL, EventStatus.STATUS_CLOSED, tag1, tag2, tag3);

        Set<String> tags = new HashSet<String>();
        tags.add(tag1);
        tags.add(tag2);
        tags.add(tag3);
        Map<String, EventSeverity> worst = eventIndexDao.findWorstSeverity(tags);
        assertEquals(EventSeverity.SEVERITY_WARNING, worst.get(tag1));
        assertEquals(EventSeverity.SEVERITY_ERROR, worst.get(tag2));
        assertNull(worst.get(tag3));
    }

    @Test
    public void countSeverities() throws ZepException {
        String tag1 = UUID.randomUUID().toString();
        String tag2 = UUID.randomUUID().toString();
        String tag3 = UUID.randomUUID().toString();

        /* Create error severity with two tags */
        for (int i = 0; i < 5; i++) {
            createEventWithSeverity(EventSeverity.SEVERITY_ERROR,
                    EventStatus.STATUS_NEW, tag1, tag2);
        }
        /* Create critical severity with one tag */
        for (int i = 0; i < 3; i++) {
            createEventWithSeverity(EventSeverity.SEVERITY_CRITICAL,
                    EventStatus.STATUS_NEW, tag2);
        }
        /* Create some closed events for all tags - these should be ignored. */
        for (int i = 0; i < 2; i++) {
            createEventWithSeverity(EventSeverity.SEVERITY_CRITICAL,
                    EventStatus.STATUS_CLOSED, tag1, tag2, tag2);
            createEventWithSeverity(EventSeverity.SEVERITY_ERROR,
                    EventStatus.STATUS_CLOSED, tag1, tag2, tag2);
            createEventWithSeverity(EventSeverity.SEVERITY_INFO,
                    EventStatus.STATUS_CLEARED, tag1, tag2, tag2);
        }

        Set<String> tags = new HashSet<String>();
        tags.add(tag1);
        tags.add(tag2);
        tags.add(tag3);
        Map<String, Map<EventSeverity, Integer>> counts = eventIndexDao
                .countSeverities(tags);
        assertEquals(1, counts.get(tag1).size());
        assertEquals(5, counts.get(tag1).get(EventSeverity.SEVERITY_ERROR)
                .intValue());
        assertEquals(2, counts.get(tag2).size());
        assertEquals(5, counts.get(tag2).get(EventSeverity.SEVERITY_ERROR)
                .intValue());
        assertEquals(3, counts.get(tag2).get(EventSeverity.SEVERITY_CRITICAL)
                .intValue());
        assertNull(counts.get(tag3));
    }

    private EventSummaryRequest createTagRequest(FilterOperator op,
            String... tags) {
        EventSummaryRequest.Builder reqBuilder = EventSummaryRequest
                .newBuilder();
        EventSummaryFilter.Builder filterBuilder = EventSummaryFilter
                .newBuilder();
        filterBuilder.addAllTagUuids(Arrays.asList(tags));
        filterBuilder.setTagUuidsOp(op);
        reqBuilder.setFilter(filterBuilder.build());
        return reqBuilder.build();
    }

    @Test
    public void testTagFilterOp() throws ZepException {
        String tag1 = UUID.randomUUID().toString();
        String tag2 = UUID.randomUUID().toString();

        EventSummary eventBothTags = createEventWithSeverity(
                EventSeverity.SEVERITY_ERROR, EventStatus.STATUS_NEW, tag1,
                tag2);
        EventSummary eventTag1 = createEventWithSeverity(
                EventSeverity.SEVERITY_ERROR, EventStatus.STATUS_NEW, tag1);
        EventSummary eventTag2 = createEventWithSeverity(
                EventSeverity.SEVERITY_ERROR, EventStatus.STATUS_NEW, tag2);

        EventSummaryResult result = this.eventIndexDao.list(createTagRequest(
                FilterOperator.OR, tag1, tag2));
        assertEquals(3, result.getEventsCount());
        Set<String> foundUuids = new HashSet<String>();
        for (EventSummary summary : result.getEventsList()) {
            foundUuids.add(summary.getUuid());
        }
        assertTrue(foundUuids.contains(eventBothTags.getUuid()));
        assertTrue(foundUuids.contains(eventTag1.getUuid()));
        assertTrue(foundUuids.contains(eventTag2.getUuid()));

        result = this.eventIndexDao.list(createTagRequest(FilterOperator.AND,
                tag1, tag2));
        assertEquals(1, result.getEventsCount());
        assertEquals(eventBothTags.getUuid(), result.getEventsList().get(0)
                .getUuid());
    }

    public static Zep.EventActor createSampleActor(String elementId, String elementSubId) {
        Zep.EventActor.Builder actorBuilder = Zep.EventActor.newBuilder();
        actorBuilder.setElementIdentifier(elementId);
        actorBuilder.setElementTypeId(Model.ModelElementType.DEVICE);
        actorBuilder.setElementUuid(UUID.randomUUID().toString());
        actorBuilder.setElementSubIdentifier(elementSubId);
        actorBuilder.setElementSubTypeId(Model.ModelElementType.COMPONENT);
        actorBuilder.setElementSubUuid(UUID.randomUUID().toString());
        return actorBuilder.build();
    }

    @Test
    public void testIdentifierInsensitive() throws ZepException {
        EventSummary summary = createEventWithSeverity(EventSeverity.SEVERITY_ERROR, EventStatus.STATUS_NEW);
        Event occurrence = Event.newBuilder(summary.getOccurrence(0)).setActor(createSampleActor("MyHostName.zenoss.loc", "myCompName")).build();
        summary = EventSummary.newBuilder(summary).clearOccurrence().addOccurrence(occurrence).build();
        eventIndexDao.index(summary);

        List<String> queries = Arrays.asList("myhostname", "ZENOSS", "loc");
        for (String query : queries) {
            EventSummaryFilter.Builder filterBuilder = EventSummaryFilter.newBuilder();
            filterBuilder.setElementIdentifier(query);
            final EventSummaryFilter filter = filterBuilder.build();
            EventSummaryRequest.Builder reqBuilder = EventSummaryRequest.newBuilder();
            reqBuilder.setFilter(filter);
            final EventSummaryRequest req = reqBuilder.build();
            EventSummaryResult result = eventIndexDao.list(req);
            assertEquals(1, result.getEventsCount());
            assertEquals(summary, result.getEvents(0));
        }
    }
}