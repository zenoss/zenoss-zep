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
package org.zenoss.zep.rest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventFilter;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdate;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdateRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdateResponse;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.dao.impl.EventArchiveDaoImplIT;
import org.zenoss.zep.dao.impl.EventSummaryDaoImplIT;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventsResourceIT extends AbstractJUnit4SpringContextTests {

    private static final String EVENTS_URI = "/zenoss-zep/api/1.0/events";

    @Autowired
    public EventSummaryDao summaryDao;

    @Autowired
    public EventArchiveDao archiveDao;

    @Autowired
    public DataSource ds;

    private RestClient client;
    private EventSummary summaryEvent;
    private EventSummary archiveEvent;

    private EventSummary createSummaryNew(Event event) throws ZepException {
        return createSummary(event, EventStatus.STATUS_NEW);
    }

    private EventSummary createSummary(Event event, EventStatus status)
            throws ZepException {
        String uuid = summaryDao.create(event, status);
        return summaryDao.findByUuid(uuid);
    }

    private EventSummary createArchive(Event event) throws ZepException {
        return archiveDao.findByUuid(archiveDao.create(event));
    }

    @Before
    public void setup() throws ZepException {
        this.client = new RestClient(EventSummary.getDefaultInstance(),
                EventNote.getDefaultInstance(),
                EventSummaryUpdateRequest.getDefaultInstance(),
                EventSummaryUpdateResponse.getDefaultInstance());
        this.summaryEvent = createSummaryNew(EventSummaryDaoImplIT
                .createUniqueEvent());
        this.archiveEvent = createArchive(EventArchiveDaoImplIT.createEvent());
    }

    @After
    public void shutdown() throws IOException, ZepException {
        SimpleJdbcTemplate template = new SimpleJdbcTemplate(ds);
        template.update("DELETE FROM event_summary");
        template.update("DELETE FROM event_archive");
        client.close();
    }

    @Test
    public void testGetByUuid() throws ZepException, IOException {
        assertEquals(summaryEvent,
                client.getJson(EVENTS_URI + "/" + summaryEvent.getUuid())
                        .getMessage());
        assertEquals(summaryEvent,
                client.getProtobuf(EVENTS_URI + "/" + summaryEvent.getUuid())
                        .getMessage());
        assertEquals(archiveEvent,
                client.getJson(EVENTS_URI + "/" + archiveEvent.getUuid())
                        .getMessage());
        assertEquals(archiveEvent,
                client.getProtobuf(EVENTS_URI + "/" + archiveEvent.getUuid())
                        .getMessage());
    }

    @Test
    public void testAddNote() throws IOException {
        EventNote note = EventNote.newBuilder().setMessage("My Message")
                .setUserName("pkw").setUserUuid(UUID.randomUUID().toString())
                .build();
        client.postJson(EVENTS_URI + "/" + summaryEvent.getUuid() + "/notes",
                note);
        note = EventNote.newBuilder().setMessage("My Message 2")
                .setUserName("pkw").setUserUuid(UUID.randomUUID().toString())
                .build();
        client.postProtobuf(EVENTS_URI + "/" + summaryEvent.getUuid()
                + "/notes", note);
        EventSummary summary = (EventSummary) client.getJson(
                EVENTS_URI + "/" + summaryEvent.getUuid()).getMessage();
        assertEquals(2, summary.getNotesCount());
        // Notes returned in reverse order to match previous behavior
        assertEquals("My Message 2", summary.getNotes(0).getMessage());
        assertEquals("My Message", summary.getNotes(1).getMessage());

        note = EventNote.newBuilder(note).setMessage("My Message 3").build();
        client.postJson(EVENTS_URI + "/" + archiveEvent.getUuid() + "/notes",
                note);
        note = EventNote.newBuilder(note).setMessage("My Message 4").build();
        client.postProtobuf(EVENTS_URI + "/" + archiveEvent.getUuid()
                + "/notes", note);
        EventSummary archive = (EventSummary) client.getProtobuf(
                EVENTS_URI + "/" + archiveEvent.getUuid()).getMessage();
        assertEquals(2, archive.getNotesCount());
        // Notes returned in reverse order to match previous behavior
        assertEquals("My Message 4", archive.getNotes(0).getMessage());
        assertEquals("My Message 3", archive.getNotes(1).getMessage());
    }

    @Test
    @Ignore
    public void testUpdateEventSummary() throws ZepException, IOException {
        List<String> uuids = new ArrayList<String>(20);
        for (int i = 0; i < 20; i++) {
            uuids.add(createSummaryNew(
                    EventSummaryDaoImplIT.createUniqueEvent()).getUuid());
        }

        // Update first 10
        EventSummaryUpdateRequest.Builder reqBuilder = EventSummaryUpdateRequest
                .newBuilder();
        reqBuilder.setLimit(10);
        reqBuilder.setUpdateFields(EventSummaryUpdate.newBuilder()
                .setAcknowledgedByUserUuid(UUID.randomUUID().toString())
                .setStatus(EventStatus.STATUS_ACKNOWLEDGED).build());
        reqBuilder.setEventFilter(EventFilter.newBuilder().addAllUuid(uuids)
                .build());
        EventSummaryUpdateRequest req = reqBuilder.build();

        EventSummaryUpdateResponse response = (EventSummaryUpdateResponse) client
                .putProtobuf(EVENTS_URI, req).getMessage();
        assertEquals(10, response.getRemaining());
        assertEquals(10, response.getUpdated());
        assertEquals(EventSummaryUpdateRequest.newBuilder(req)
                .clearUpdateTime().build(), response.getRequest());
        assertTrue(response.getRequest().hasUpdateTime());

        // Repeat request for last 10
        EventSummaryUpdateResponse newResponse = (EventSummaryUpdateResponse) client
                .putProtobuf(EVENTS_URI, response.getRequest()).getMessage();
        assertEquals(0, newResponse.getRemaining());
        assertEquals(10, response.getUpdated());
        assertEquals(EventSummaryUpdateRequest.newBuilder(req)
                .clearUpdateTime().build(), response.getRequest());
        assertEquals(response.getRequest().getUpdateTime(), newResponse
                .getRequest().getUpdateTime());

        // Verify updates hit the database
        List<EventSummary> summaries = summaryDao.findByUuids(uuids);
        for (EventSummary summary : summaries) {
            assertEquals(req.getUpdateFields().getStatus(), summary.getStatus());
            assertEquals(req.getUpdateFields().getAcknowledgedByUserUuid(),
                    summary.getAcknowledgedByUserUuid());
        }
    }
}
