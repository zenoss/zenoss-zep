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
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.protobufs.zep.Zep.SyslogPriority;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventDao;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventDaoImplIT extends
        AbstractTransactionalJUnit4SpringContextTests {

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
                .setType("zenoss." + type.name().toLowerCase()).setUuid(uuid)
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
        eventBuilder.addTags(createTag(ModelElementType.DEVICE, UUID
                .randomUUID().toString()));
        eventBuilder.addTags(createTag(ModelElementType.COMPONENT, UUID
                .randomUUID().toString()));
        eventBuilder.addTags(createTag(ModelElementType.SERVICE, UUID
                .randomUUID().toString()));
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

    @Test
    public void testCreateAllFields() throws ZepException {
        Event event = createSampleEvent();
        String uuid = eventDao.create(event);

        Event eventFromDb = eventDao.findByUuid(uuid);
        assertEquals(uuid, eventFromDb.getUuid());
        assertEquals(event, eventFromDb);

        eventDao.delete(uuid);
        assertNull(eventDao.findByUuid(uuid));
    }
}
