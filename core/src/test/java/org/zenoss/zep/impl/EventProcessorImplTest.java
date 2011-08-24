/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import org.easymock.Capture;
import org.junit.Test;
import org.zenoss.amqp.AmqpException;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.ZepRawEvent;
import org.zenoss.zep.PluginService;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.plugins.EventPostCreateContext;
import org.zenoss.zep.plugins.EventPostCreatePlugin;
import org.zenoss.zep.plugins.EventPreCreateContext;
import org.zenoss.zep.plugins.EventPreCreatePlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

/**
 * Unit test for {@link EventProcessorImpl}.
 */
public class EventProcessorImplTest {
    private static class SampleIdentifyPlugin extends EventPreCreatePlugin {
        @Override
        public Event processEvent(Event evt, EventPreCreateContext ctx) {
            Event.Builder eventBuilder = evt.toBuilder();
            eventBuilder.setEventClass("/TestEvent");
            return eventBuilder.build();
        }
    }

    private static class SampleTransformPlugin extends EventPreCreatePlugin {
        @Override
        public Event processEvent(Event evt, EventPreCreateContext ctx) {
            Event.Builder evBuilder = evt.toBuilder();
            evBuilder.setSummary(evt.getSummary().toUpperCase() + "!!!");
            return evBuilder.build();
        }
    }

    private static class SamplePostPlugin extends EventPostCreatePlugin {
        public Event eventOccurrence;
        public EventSummary eventSummary;

        @Override
        public void processEvent(Event eventOccurrence, EventSummary event, EventPostCreateContext context)
                throws ZepException {
            this.eventOccurrence = eventOccurrence;
            this.eventSummary = event;
        }
    }

    @Test
    public void testEventProcessor() throws ZepException, IOException,
            AmqpException {
        PluginService pluginService = createMock(PluginService.class);
        EventSummaryDao eventSummaryDao = createMock(EventSummaryDao.class);
        SamplePostPlugin postPlugin = new SamplePostPlugin();

        Capture<Event> transformedEvent = new Capture<Event>();
        Capture<EventPreCreateContext> transformedContext = new Capture<EventPreCreateContext>();

        String uuid = UUID.randomUUID().toString();
        EventSummary summary = EventSummary.newBuilder().setUuid(UUID.randomUUID().toString()).build();
        expect(eventSummaryDao.create(capture(transformedEvent), capture(transformedContext))).andReturn(uuid);
        expect(pluginService.getPluginsByType(EventPreCreatePlugin.class))
                .andReturn(
                        Arrays.<EventPreCreatePlugin>asList(
                                new SampleIdentifyPlugin(),
                                new SampleTransformPlugin()));
        expectLastCall();
        expect(pluginService.getPluginsByType(EventPostCreatePlugin.class))
                .andReturn(Arrays.<EventPostCreatePlugin> asList(postPlugin));
        expectLastCall();
        expect(eventSummaryDao.findByUuid(uuid)).andReturn(summary);
        replay(pluginService, eventSummaryDao);

        EventProcessorImpl eventProcessor = new EventProcessorImpl();
        eventProcessor.setPluginService(pluginService);
        eventProcessor.setEventSummaryDao(eventSummaryDao);

        Event.Builder eventBuilder = Event.newBuilder();
        eventBuilder.setUuid(UUID.randomUUID().toString());
        eventBuilder.setCreatedTime(System.currentTimeMillis());
        eventBuilder.setSummary("My Event Summary");
        final Event event = eventBuilder.build();
        final ZepRawEvent zepEvent = ZepRawEvent.newBuilder()
                .setEvent(event).build();
        eventProcessor.processEvent(zepEvent);
        verify(pluginService, eventSummaryDao);
        assertEquals("/TestEvent", transformedEvent.getValue().getEventClass());
        assertEquals(event.getSummary().toUpperCase() + "!!!", transformedEvent
                .getValue().getSummary());

        assertEquals(transformedEvent.getValue(), postPlugin.eventOccurrence);
        assertEquals(summary, postPlugin.eventSummary);
    }

    @Test
    public void testEventProcessorNoEventClass() throws ZepException,
            IOException, AmqpException {
        PluginService pluginService = createMock(PluginService.class);
        EventSummaryDao eventSummaryDao = createMock(EventSummaryDao.class);

        Capture<Event> transformedEvent = new Capture<Event>();
        Capture<EventPreCreateContext> transformedContext = new Capture<EventPreCreateContext>();

        String uuid = UUID.randomUUID().toString();
        expect(eventSummaryDao.create(capture(transformedEvent), capture(transformedContext))).andReturn(uuid);
        expect(pluginService.getPluginsByType(EventPreCreatePlugin.class))
                .andReturn(Collections.<EventPreCreatePlugin>emptyList());
        expectLastCall();
        expect(pluginService.getPluginsByType(EventPostCreatePlugin.class))
                .andReturn(Collections.<EventPostCreatePlugin> emptyList());
        expectLastCall();
        replay(pluginService, eventSummaryDao);

        EventProcessorImpl eventProcessor = new EventProcessorImpl();
        eventProcessor.setPluginService(pluginService);
        eventProcessor.setEventSummaryDao(eventSummaryDao);

        Event.Builder eventBuilder = Event.newBuilder();
        eventBuilder.setUuid(UUID.randomUUID().toString());
        eventBuilder.setCreatedTime(System.currentTimeMillis());
        eventBuilder.setSummary("My Event Summary");
        final Event event = eventBuilder.build();
        final ZepRawEvent zepEvent = ZepRawEvent.newBuilder()
                .setEvent(event).build();
        eventProcessor.processEvent(zepEvent);
        verify(pluginService, eventSummaryDao);

        Event transformed = transformedEvent.getValue();
        assertEquals("/Unknown", transformed.getEventClass());
    }
}
