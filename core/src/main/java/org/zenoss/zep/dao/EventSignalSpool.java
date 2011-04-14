/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.impl.DaoUtils;

/**
 * Bean class representing a spooled event signal. These spool items are evaluated after the flushTime
 * has expired to determine if a signal should be repeated or canceled.
 */
public class EventSignalSpool {
    private String uuid;
    private String subscriptionUuid;
    private String eventSummaryUuid;
    private long flushTime;
    private long created;
    private int eventCount = 1;

    public EventSignalSpool() {
    }

    public String getUuid() {
        return this.uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSubscriptionUuid() {
        return this.subscriptionUuid;
    }

    public void setSubscriptionUuid(String subscriptionUuid) {
        this.subscriptionUuid = subscriptionUuid;
    }

    public String getEventSummaryUuid() {
        return this.eventSummaryUuid;
    }

    public void setEventSummaryUuid(String eventSummaryUuid) {
        this.eventSummaryUuid = eventSummaryUuid;
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
        return "EventSignalSpool{" +
                "uuid=" + uuid +
                ", subscriptionUuid=" + subscriptionUuid +
                ", eventSummaryUuid=" + eventSummaryUuid +
                ", flushTime=" + flushTime +
                ", created=" + created +
                ", eventCount=" + eventCount +
                '}';
    }

    /**
     * Creates a new <code>EventSignalSpool</code> from the subscription and event summary.
     *
     * @param subscription Event trigger subscription.
     * @param summary Event summary.
     * @return A new spool item.
     */
    public static EventSignalSpool buildSpool(EventTriggerSubscription subscription, EventSummary summary) {
        final int delaySeconds = subscription.getDelaySeconds();
        final int repeatSeconds = subscription.getRepeatSeconds();

        final EventSignalSpool spool = new EventSignalSpool();
        long created = System.currentTimeMillis();
        final long flushTime;
        if (delaySeconds > 0) {
            flushTime = created + TimeUnit.SECONDS.toMillis(delaySeconds);
        }
        else if (repeatSeconds > 0) {
            flushTime = created + TimeUnit.SECONDS.toMillis(repeatSeconds);
        }
        else {
            flushTime = Long.MAX_VALUE;
        }
        spool.setCreated(created);
        spool.setEventCount(1);
        spool.setEventSummaryUuid(summary.getUuid());
        spool.setSubscriptionUuid(subscription.getUuid());
        spool.setFlushTime(flushTime);
        spool.setUuid(UUID.randomUUID().toString());
        return spool;
    }
}
