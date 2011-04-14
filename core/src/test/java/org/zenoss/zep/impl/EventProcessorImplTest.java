/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import org.easymock.Capture;
import org.junit.Test;
import org.zenoss.amqp.AmqpException;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.RawEvent;
import org.zenoss.protobufs.zep.Zep.ZepRawEvent;
import org.zenoss.zep.EventContext;
import org.zenoss.zep.EventPreProcessingPlugin;
import org.zenoss.zep.PluginService;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventStoreDao;

/**
 * Unit test for {@link EventProcessorImpl}.
 */
public class EventProcessorImplTest {
    private static class SampleIdentifyPlugin extends
            AbstractPreProcessingPlugin {
        @Override
        public Event processEvent(Event evt, EventContext ctx) {
            Event.Builder eventBuilder = evt.toBuilder();
            eventBuilder.setEventClass("/TestEvent");
            return eventBuilder.build();
        }
    }

    private static class SampleTransformPlugin extends
            AbstractPreProcessingPlugin {
        @Override
        public Event processEvent(Event evt, EventContext ctx) {
            Event.Builder evBuilder = evt.toBuilder();
            evBuilder.setSummary(evt.getSummary().toUpperCase() + "!!!");
            return evBuilder.build();
        }
    }

    @Test
    public void testEventProcessor() throws ZepException, IOException,
            AmqpException {
        PluginService pluginService = createMock(PluginService.class);
        EventStoreDao eventStoreDao = createMock(EventStoreDao.class);

        Capture<Event> transformedEvent = new Capture<Event>();
        Capture<EventContext> transformedContext = new Capture<EventContext>();

        String uuid = UUID.randomUUID().toString();
        expect(eventStoreDao.create(capture(transformedEvent), capture(transformedContext))).andReturn(uuid);
        expect(pluginService.getPreProcessingPlugins())
                .andReturn(
                        Arrays.<EventPreProcessingPlugin> asList(
                                new SampleIdentifyPlugin(),
                                new SampleTransformPlugin()));
        expectLastCall();
        replay(pluginService, eventStoreDao);

        EventProcessorImpl eventProcessor = new EventProcessorImpl();
        eventProcessor.setPluginService(pluginService);
        eventProcessor.setEventStoreDao(eventStoreDao);

        RawEvent.Builder eventBuilder = RawEvent.newBuilder();
        eventBuilder.setUuid(UUID.randomUUID().toString());
        eventBuilder.setCreatedTime(System.currentTimeMillis());
        eventBuilder.setSummary("My Event Summary");
        final RawEvent event = eventBuilder.build();
        final ZepRawEvent zepEvent = ZepRawEvent.newBuilder()
                .setRawEvent(event).setStatus(EventStatus.STATUS_NEW).build();
        eventProcessor.processEvent(zepEvent);
        verify(pluginService, eventStoreDao);
        assertEquals("/TestEvent", transformedEvent.getValue().getEventClass());
        assertEquals(event.getSummary().toUpperCase() + "!!!", transformedEvent
                .getValue().getSummary());
    }

    @Test
    public void testEventProcessorNoEventClass() throws ZepException,
            IOException, AmqpException {
        PluginService pluginService = createMock(PluginService.class);
        EventStoreDao eventStoreDao = createMock(EventStoreDao.class);

        Capture<Event> transformedEvent = new Capture<Event>();
        Capture<EventContext> transformedContext = new Capture<EventContext>();

        String uuid = UUID.randomUUID().toString();
        expect(eventStoreDao.create(capture(transformedEvent), capture(transformedContext))).andReturn(uuid);
        expect(pluginService.getPreProcessingPlugins()).andReturn(Collections.<EventPreProcessingPlugin> emptyList());
        expectLastCall();
        replay(pluginService, eventStoreDao);

        EventProcessorImpl eventProcessor = new EventProcessorImpl();
        eventProcessor.setPluginService(pluginService);
        eventProcessor.setEventStoreDao(eventStoreDao);

        RawEvent.Builder eventBuilder = RawEvent.newBuilder();
        eventBuilder.setUuid(UUID.randomUUID().toString());
        eventBuilder.setCreatedTime(System.currentTimeMillis());
        eventBuilder.setSummary("My Event Summary");
        final RawEvent event = eventBuilder.build();
        final ZepRawEvent zepEvent = ZepRawEvent.newBuilder()
                .setRawEvent(event).setStatus(EventStatus.STATUS_NEW).build();
        eventProcessor.processEvent(zepEvent);
        verify(pluginService, eventStoreDao);

        Event transformed = transformedEvent.getValue();
        assertEquals("/Unknown", transformed.getEventClass());
    }
}
