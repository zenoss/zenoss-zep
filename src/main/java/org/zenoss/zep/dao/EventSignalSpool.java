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
package org.zenoss.zep.dao;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.zenoss.protobufs.zep.Zep;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.impl.DaoUtils;

/**
 * 
 *
 */
public class EventSignalSpool {
    private byte[] uuid;
    private String eventTriggerSubscriptionUuid;
    private byte[] eventSummaryUuid;
    private long flushTime;
    private long created;
    private int eventCount = 1;

    public EventSignalSpool() {
    }

    public String getUuid() {
        return (uuid != null) ? DaoUtils.uuidFromBytes(uuid) : null;
    }

    public void setUuid(String uuid) {
        this.uuid = DaoUtils.uuidToBytes(uuid);
    }

    public String getEventTriggerSubscriptionUuid() {
        return this.eventTriggerSubscriptionUuid;
    }

    public void setEventTriggerSubscriptionUuid(
            String eventTriggerSubscriptionUuid) {
        this.eventTriggerSubscriptionUuid = eventTriggerSubscriptionUuid;
    }

    public String getEventSummaryUuid() {
        return DaoUtils.uuidFromBytes(eventSummaryUuid);
    }

    public void setEventSummaryUuid(String eventSummaryUuid) {
        this.eventSummaryUuid = DaoUtils.uuidToBytes(eventSummaryUuid);
    }

    public long getFlushTime() {
        return flushTime;
    }

    public void setFlushTime(long flushTime) {
        this.flushTime = flushTime;
    }

    public long getCreated() {
        return created;
    }

    public void setCreated(long created) {
        this.created = created;
    }

    public int getEventCount() {
        return eventCount;
    }

    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }

    @Override
    public String toString() {
        return String
                .format("EventSignalSpool [uuid=%s, eventTriggerSubscriptionId=%d, eventSummaryUuid=%s, flushTime=%s, created=%s, eventCount=%s]",
                        Arrays.toString(uuid), eventTriggerSubscriptionUuid,
                        Arrays.toString(eventSummaryUuid), flushTime, created,
                        eventCount);
    }

    public static EventSignalSpool buildSpool(
            Zep.EventTriggerSubscription triggerSubscription,
            EventSummary eventSummary) throws ZepException {
        EventSignalSpool spool = new EventSignalSpool();
        long created = System.currentTimeMillis();
        spool.setCreated(created);
        spool.setEventCount(1);
        spool.setEventSummaryUuid(eventSummary.getUuid());
        spool.setEventTriggerSubscriptionUuid(triggerSubscription.getUuid());
        spool.setFlushTime(created
                + TimeUnit.SECONDS.toMillis(triggerSubscription
                        .getDelaySeconds()));
        spool.setUuid(UUID.randomUUID().toString());
        return spool;
    }

}