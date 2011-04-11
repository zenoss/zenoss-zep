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

import org.zenoss.protobufs.zep.Zep.DaemonHeartbeat;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.RawEvent;
import org.zenoss.zep.EventPublisher;
import org.zenoss.zep.HeartbeatProcessor;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.HeartbeatDao;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Heartbeat processing logic.
 */
public class HeartbeatProcessorImpl implements HeartbeatProcessor {

    private static final String STATUS_HEARTBEAT = "/Status/Heartbeat";

    private HeartbeatDao heartbeatDao;
    private EventPublisher eventPublisher;

    public void setHeartbeatDao(HeartbeatDao heartbeatDao) {
        this.heartbeatDao = heartbeatDao;
    }

    public void setEventPublisher(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void sendHeartbeatEvents() throws ZepException {
        final long now = System.currentTimeMillis();
        final List<DaemonHeartbeat> heartbeats = this.heartbeatDao.findAll();
        for (DaemonHeartbeat heartbeat : heartbeats) {
            final long expireTime = heartbeat.getLastTime() + TimeUnit.SECONDS.toMillis(heartbeat.getTimeoutSeconds());
            final boolean isClear = (expireTime > now);
            final RawEvent event = createHeartbeatEvent(heartbeat, now, isClear);
            eventPublisher.publishRawEvent(event);
        }
    }

    private static RawEvent createHeartbeatEvent(DaemonHeartbeat heartbeat, long createdTime, boolean isClear) {
        final RawEvent.Builder event = RawEvent.newBuilder();
        event.setUuid(UUID.randomUUID().toString());
        event.setCreatedTime(createdTime);
        event.getActorBuilder().setElementIdentifier(heartbeat.getMonitor())
                .setElementSubIdentifier(heartbeat.getDaemon());
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
