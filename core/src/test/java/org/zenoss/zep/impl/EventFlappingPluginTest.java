/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.SyslogPriority;
import org.zenoss.zep.*;
import org.zenoss.zep.dao.FlapTrackerDao;

import java.io.IOException;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;


public class EventFlappingPluginTest {

    public EventFlappingPlugin eventFlappingPlugin = null;
    public FlapTrackerDao flapTrackerDao = null;
    public EventPublisher publisherMock = null;

    @Before
    public void testInit() throws IOException, ZepException {
        this.eventFlappingPlugin = new EventFlappingPlugin();
        FlapTrackerDao storage = new MemoryEventFlappingStorage();
        this.eventFlappingPlugin.setFlapTrackerDao(storage);
        this.flapTrackerDao = storage;
        this.publisherMock = createMock(EventPublisher.class);
        this.eventFlappingPlugin.setPublisher(publisherMock);
        UUIDGenerator uuidGenerator = new UUIDGeneratorImpl();
        eventFlappingPlugin.setUuidGenerator(uuidGenerator);
    }
    
    @After
    public void shutdown() throws InterruptedException {
        this.eventFlappingPlugin.stop();
    }

    private EventActor.Builder createActor() {
        EventActor.Builder actorBuilder = EventActor.newBuilder();
        actorBuilder.setElementTypeId(ModelElementType.DEVICE);
        actorBuilder.setElementIdentifier("BHM1000");
        actorBuilder.setElementTitle("BHM TITLE");
        actorBuilder.setElementUuid("MyDeviceUUID");
        actorBuilder.setElementSubTypeId(ModelElementType.COMPONENT);
        actorBuilder.setElementSubIdentifier("Fuse-10A");
        actorBuilder.setElementSubTitle("Fuse-10A Title");
        actorBuilder.setElementSubUuid("MySubUUID");
        return actorBuilder;
    }

    private Event.Builder createEventOccurrence(EventActor actor) {
        Event.Builder evtBuilder = Event.newBuilder();
        evtBuilder.setActor(actor);
        evtBuilder.setMessage("TEST - 1-2-check");
        evtBuilder.setEventClass("/Defcon/1");
        evtBuilder.setSeverity(Zep.EventSeverity.SEVERITY_ERROR);
        evtBuilder.setSyslogPriority(SyslogPriority.SYSLOG_PRIORITY_DEBUG);

        EventDetail.Builder groupBuilder = evtBuilder.addDetailsBuilder().setName(ZepConstants.DETAIL_DEVICE_GROUPS);
        groupBuilder.addValue("/US/Texas/Austin");

        EventDetail.Builder systemsBuilder = evtBuilder.addDetailsBuilder().setName(ZepConstants.DETAIL_DEVICE_SYSTEMS);
        systemsBuilder.addValue("/Production/Infrastructure");

        return evtBuilder;
    }


    @Test
    public void testEventFlapping() {
        // a flap is defined as when the event goes from a < threshold severity to a > threshold severity
        Event.Builder eventBuilder = createEventOccurrence(createActor().build());
        eventBuilder.setSeverity(Zep.EventSeverity.SEVERITY_ERROR);
        FlapTracker tracker = new FlapTracker();
        tracker.setPreviousSeverity(EventSeverity.SEVERITY_CLEAR);

        assertEquals(true, eventFlappingPlugin.isEventFlap(eventBuilder.build(), tracker, EventSeverity.SEVERITY_WARNING ));

        // make sure no change means it is not a flap
        eventBuilder.setSeverity(EventSeverity.SEVERITY_CLEAR);
        assertEquals(false, eventFlappingPlugin.isEventFlap(eventBuilder.build(), tracker, EventSeverity.SEVERITY_WARNING ));

        // make sure it goes up below the threshold it is still not a flap
        eventBuilder.setSeverity(EventSeverity.SEVERITY_DEBUG);
        assertEquals(false, eventFlappingPlugin.isEventFlap(eventBuilder.build(), tracker, EventSeverity.SEVERITY_WARNING ));
    }

    @Test
    public void testShouldGenerateFlapEvent() {
        // generate a flap event when the tracker has > flap threshold timestamps

        Event.Builder eventBuilder = createEventOccurrence(createActor().build());
        FlapTracker tracker = new FlapTracker();
        int threshold = 2;
        tracker.addCurrentTimeStamp();
        tracker.addCurrentTimeStamp();

        assertEquals(true, eventFlappingPlugin.shouldGenerateFlapEvent(tracker, eventBuilder.build(), threshold));
        tracker.clearTimestamps();
        tracker.addCurrentTimeStamp();
        assertEquals(false, eventFlappingPlugin.shouldGenerateFlapEvent(tracker, eventBuilder.build(), threshold));
    }

    @Test
    public void testDoNotGenerateFlapEventWithOldTimestamps() {
        Event.Builder eventBuilder = createEventOccurrence(createActor().build());
        FlapTracker tracker = new FlapTracker();
        int threshold = 2;
        // set a flap in the distant past
        tracker.addTimeStamp(System.currentTimeMillis()/1000l - 999999);
        tracker.addCurrentTimeStamp();

        assertEquals(false, eventFlappingPlugin.shouldGenerateFlapEvent(tracker, eventBuilder.build(), threshold));
        // now we have enough timestamps to warrant a flapping event
        tracker.addCurrentTimeStamp();
        assertEquals(true, eventFlappingPlugin.shouldGenerateFlapEvent(tracker, eventBuilder.build(), threshold));
    }
    @Test
    public void testAddTimestampToTracker() throws ZepException {
        Event.Builder eventBuilder = createEventOccurrence(createActor().build());
        String clearFingerprint = "fingerprint2";
        eventBuilder.setSeverity(EventSeverity.SEVERITY_CLEAR);
        eventFlappingPlugin.detectEventFlapping(eventBuilder.build(), clearFingerprint);

        // now create a flap
        eventBuilder.setSeverity(EventSeverity.SEVERITY_CRITICAL);
        eventFlappingPlugin.detectEventFlapping(eventBuilder.build(), clearFingerprint);

        // verify flap
        FlapTracker tracker = flapTrackerDao.getFlapTrackerByClearFingerprintHash(clearFingerprint);
        assertEquals(1, tracker.getTimestamps().length);
    }

    @Test
    public void testFlappingEventGenerated() throws ZepException {
        Event.Builder eventBuilder = createEventOccurrence(createActor().build());
        String clearFingerprint = "fingerprint2";
        FlapTracker tracker = flapTrackerDao.getFlapTrackerByClearFingerprintHash(clearFingerprint);
        for (int i=0;i<4;i++){
            tracker.addCurrentTimeStamp();
        }

        // should generate an event now
        eventFlappingPlugin.detectEventFlapping(eventBuilder.build(), clearFingerprint);

        // make sure all of the timestamps have been cleared
        assertEquals(0, tracker.getTimestamps().length);
    }

    @Test
    public void testNoFlapIfNotIdentified() throws ZepException {
        EventActor.Builder actorBuilder = createActor();
        actorBuilder.clearElementUuid();
        Event event = createEventOccurrence(actorBuilder.build()).build();
        assertFalse(eventFlappingPlugin.isEventFlap(event, flapTrackerDao.getFlapTrackerByClearFingerprintHash("test"),
                EventSeverity.SEVERITY_WARNING));
   }

    @Test
    public void testFlapTrackerPersistence() {
        FlapTracker tracker = new FlapTracker();
        tracker.addCurrentTimeStamp();
        tracker.setPreviousSeverity(EventSeverity.SEVERITY_WARNING);

        FlapTracker newTracker = FlapTracker.buildFromString(tracker.convertToString());
        assertEquals(newTracker.getPreviousSeverity(), tracker.getPreviousSeverity());
        assertEquals(newTracker.getTimestamps().length, 1);
    }
}
