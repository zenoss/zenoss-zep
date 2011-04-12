/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2011, Zenoss Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.DaemonHeartbeat;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryResult;
import org.zenoss.protobufs.zep.Zep.RawEvent;
import org.zenoss.zep.EventPublisher;
import org.zenoss.zep.HeartbeatProcessor;
import org.zenoss.zep.ZepConstants;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.HeartbeatDao;
import org.zenoss.zep.index.EventIndexDao;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Heartbeat processing logic.
 */
public class HeartbeatProcessorImpl implements HeartbeatProcessor {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatProcessorImpl.class);
    private static final String STATUS_HEARTBEAT = "/Status/Heartbeat";

    private HeartbeatDao heartbeatDao;
    private EventPublisher eventPublisher;
    private EventIndexDao eventIndexDao;
    private final EventSummaryRequest heartbeatRequest;
    
    // Send a clear event for each "up" daemon on startup
    private final AtomicBoolean firstClearSent = new AtomicBoolean(false);
    
    public HeartbeatProcessorImpl() {
        EventSummaryRequest.Builder builder = EventSummaryRequest.newBuilder();
        builder.getEventFilterBuilder().addEventClass(STATUS_HEARTBEAT);
        builder.getEventFilterBuilder().addAllStatus(ZepConstants.OPEN_STATUSES);
        heartbeatRequest = builder.build();
    }

    public void setHeartbeatDao(HeartbeatDao heartbeatDao) {
        this.heartbeatDao = heartbeatDao;
    }

    public void setEventPublisher(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }
    
    public void setEventIndexDao(EventIndexDao eventIndexDao) {
        this.eventIndexDao = eventIndexDao;
    }

    @Override
    public void sendHeartbeatEvents() throws ZepException {
        final Set<Entry<String,String>> currentHeartbeatEvents = getCurrentHeartbeatEvents();
        
        final long now = System.currentTimeMillis();
        final List<DaemonHeartbeat> heartbeats = this.heartbeatDao.findAll();
        for (DaemonHeartbeat heartbeat : heartbeats) {
            final long expireTime = heartbeat.getLastTime() + TimeUnit.SECONDS.toMillis(heartbeat.getTimeoutSeconds());
            final boolean isClear = (expireTime > now);
            final Entry<String,String> entry = new SimpleImmutableEntry<String, String>(heartbeat.getMonitor(),
                    heartbeat.getDaemon());
            if (!isClear || firstClearSent.compareAndSet(false, true) || currentHeartbeatEvents.contains(entry)) {
                logger.debug("Publishing heartbeat event for: {}, isClear: {}", entry, isClear);
                final RawEvent event = createHeartbeatEvent(heartbeat, now, isClear);
                eventPublisher.publishRawEvent(event);
            }
        }
    }

    /**
     * Retrieves all of the current heartbeat events in OPEN status. This is used
     * to optimize how often clear events are sent to only send a clear event if
     * it has a possibility of clearing a heartbeat event.
     * 
     * @return The current open heartbeat events.
     * @throws ZepException If an exception occurs.
     */
    private Set<Entry<String,String>> getCurrentHeartbeatEvents() throws ZepException {
        Set<Entry<String,String>> events = new HashSet<Entry<String,String>>();
        EventSummaryResult result = this.eventIndexDao.list(this.heartbeatRequest);
        for (EventSummary summary : result.getEventsList()) {
            final Event occurrence = summary.getOccurrence(0);
            events.add(new SimpleImmutableEntry<String,String>(occurrence.getMonitor(), occurrence.getAgent()));
        }
        logger.debug("Current heartbeat events: {}", events);
        return events;
    }

    private static RawEvent createHeartbeatEvent(DaemonHeartbeat heartbeat, long createdTime, boolean isClear) {
        final RawEvent.Builder event = RawEvent.newBuilder();
        event.setUuid(UUID.randomUUID().toString());
        event.setCreatedTime(createdTime);
        event.getActorBuilder().setElementIdentifier(heartbeat.getMonitor())
                .setElementSubIdentifier(heartbeat.getDaemon());
        event.setMonitor(heartbeat.getMonitor());
        event.setAgent(heartbeat.getDaemon());
        // Per old behavior - alerting rules typically are configured to only fire
        // for devices in production. These events don't have a true "device" with
        // a production state a lot of the time, and so we have to set this manually.
        event.addDetailsBuilder().setName(ZepConstants.DETAIL_DEVICE_PRODUCTION_STATE)
                .addValue(Integer.toString(ZepConstants.PRODUCTION_STATE_PRODUCTION));
        if (isClear) {
            event.setSeverity(EventSeverity.SEVERITY_CLEAR);
            event.setSummary(heartbeat.getMonitor() + " " + heartbeat.getDaemon() + " heartbeat clear");
        }
        else {
            event.setSeverity(EventSeverity.SEVERITY_ERROR);
            event.setSummary(heartbeat.getMonitor() + " " + heartbeat.getDaemon() + " heartbeat failure");
        }
        event.setEventClass(STATUS_HEARTBEAT);
        return event.build();
    }
}
