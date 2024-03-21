/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.protobufs.zep.Zep.SyslogPriority;

import java.util.Random;
import java.util.UUID;

public class EventTestUtils {

    public static EventActor createSampleActor() {
        EventActor.Builder actorBuilder = EventActor.newBuilder();
        actorBuilder.setElementIdentifier("devicename");
        actorBuilder.setElementTypeId(ModelElementType.DEVICE);
        actorBuilder.setElementUuid(UUID.randomUUID().toString());
        actorBuilder.setElementTitle("My Device Title");
        actorBuilder.setElementSubIdentifier("compname");
        actorBuilder.setElementSubTypeId(ModelElementType.COMPONENT);
        actorBuilder.setElementSubUuid(UUID.randomUUID().toString());
        actorBuilder.setElementSubTitle("My Component Title");
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
                + UUID.randomUUID());
        eventBuilder.setMessage("my message");
        eventBuilder.setMonitor("monitor");
        eventBuilder.setNtEventCode(new Random().nextInt(50000));
        eventBuilder.setSeverity(EventSeverity.SEVERITY_CRITICAL);
        eventBuilder.setSummary("summary message");
        eventBuilder.setSyslogFacility(11);
        eventBuilder.setSyslogPriority(SyslogPriority.SYSLOG_PRIORITY_DEBUG);
        return eventBuilder.build();
    }
}
