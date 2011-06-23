/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.protobufs.zep.Zep.SyslogPriority;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventDao;
import org.zenoss.zep.dao.EventSummaryDao;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventDaoImplIT extends AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    public EventSummaryDao summaryDao;

    @Autowired
    public EventDao eventDao;

    public static EventActor createSampleActor() {
        EventActor.Builder actorBuilder = EventActor.newBuilder();
        actorBuilder.setElementIdentifier("devicename");
        actorBuilder.setElementTypeId(ModelElementType.DEVICE);
        actorBuilder.setElementUuid(UUID.randomUUID().toString());
        actorBuilder.setElementSubIdentifier("compname");
        actorBuilder.setElementSubTypeId(ModelElementType.COMPONENT);
        actorBuilder.setElementSubUuid(UUID.randomUUID().toString());
        return actorBuilder.build();
    }

    public static EventTag createTag(ModelElementType type, String uuid) {
        return EventTag.newBuilder()
                .setType("zenoss." + type.name().toLowerCase()).addUuid(uuid)
                .build();
    }

    public static Event createSampleEvent() {
        EventActor actor = createSampleActor();
        Event.Builder eventBuilder = Event.newBuilder();
        eventBuilder.setUuid(UUID.randomUUID().toString());
        eventBuilder.setCreatedTime(System.currentTimeMillis());
        eventBuilder.addDetails(EventDetail.newBuilder().setName("foo")
                .addValue("bar").addValue("baz").build());
        eventBuilder.addDetails(EventDetail.newBuilder().setName("foo2")
                .addValue("bar2").addValue("baz2").build());
        eventBuilder.addTags(createTag(ModelElementType.DEVICE, UUID.randomUUID().toString()));
        eventBuilder.addTags(createTag(ModelElementType.COMPONENT, UUID.randomUUID().toString()));
        eventBuilder.addTags(createTag(ModelElementType.SERVICE, UUID.randomUUID().toString()));
        eventBuilder.addTags(createTag(actor.getElementTypeId(),
                actor.getElementUuid()));
        eventBuilder.addTags(createTag(actor.getElementSubTypeId(),
                actor.getElementSubUuid()));
        eventBuilder.setActor(actor);
        eventBuilder.setAgent("agent");
        eventBuilder.setEventClass("/Unknown");
        eventBuilder.setEventClassKey("eventClassKey");
        eventBuilder.setEventClassMappingUuid(UUID.randomUUID().toString());
        eventBuilder.setEventGroup("event group");
        eventBuilder.setEventKey("event key");
        eventBuilder.setFingerprint("my|dedupid|foo|"
                + UUID.randomUUID().toString());
        eventBuilder.setMessage("my message");
        eventBuilder.setMonitor("monitor");
        eventBuilder.setNtEventCode(new Random().nextInt(50000));
        eventBuilder.setSeverity(EventSeverity.SEVERITY_CRITICAL);
        eventBuilder.setSummary("summary message");
        eventBuilder.setSyslogFacility(11);
        eventBuilder.setSyslogPriority(SyslogPriority.SYSLOG_PRIORITY_DEBUG);
        return eventBuilder.build();
    }

    private static void compareEvents(Event original, Event fromDb) {
        // When persisting to DB, we consolidate tag UUIDs under type for storage savings
        assertEquals(Event.newBuilder(original).clearTags().addAllTags(EventDaoHelper.buildTags(original)).build(),
                fromDb);
    }

    @Test
    public void testCreateAllFields() throws ZepException {
        Event event = createSampleEvent();
        String summaryUuid = summaryDao.create(event, EventStatus.STATUS_NEW);
        event = Event.newBuilder(event).setSummaryUuid(summaryUuid).build();

        Event eventFromDb = eventDao.findByUuid(event.getUuid());
        compareEvents(event, eventFromDb);

        List<Event> occurrences = eventDao.findBySummaryUuid(summaryUuid);
        assertEquals(1, occurrences.size());
        compareEvents(event, occurrences.get(0));

        eventDao.delete(event.getUuid());
        assertNull(eventDao.findByUuid(event.getUuid()));
    }
}
