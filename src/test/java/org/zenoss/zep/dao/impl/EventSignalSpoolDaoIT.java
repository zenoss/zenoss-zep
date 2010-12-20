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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.protobufs.zep.Zep.Rule;
import org.zenoss.protobufs.zep.Zep.RuleType;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSignalSpool;
import org.zenoss.zep.dao.EventSignalSpoolDao;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.dao.EventTriggerDao;
import org.zenoss.zep.dao.EventTriggerSubscriptionDao;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventSignalSpoolDaoIT extends
        AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    public EventSignalSpoolDao dao;

    @Autowired
    public EventSummaryDao eventSummaryDao;

    @Autowired
    public EventTriggerDao triggerDao;

    @Autowired
    public EventTriggerSubscriptionDao subscriptionDao;
    
    private EventSummary createSummaryNew(Event event) throws ZepException {
        return createSummary(event, EventStatus.STATUS_NEW);
    }

    private EventSummary createSummary(Event event, EventStatus status) throws ZepException {
        String uuid = eventSummaryDao.create(event, status);
        return eventSummaryDao.findByUuid(uuid);
    }

    private EventSummary createSampleSummary() throws ZepException {
        Event event = EventDaoImplIT.createSampleEvent();
        return createSummaryNew(event);
    }

    private EventTrigger createTrigger() throws ZepException {
        EventTrigger.Builder triggerBuilder = EventTrigger.newBuilder();
        triggerBuilder.setUuid(UUID.randomUUID().toString());

        Rule.Builder ruleBuilder = Rule.newBuilder();
        ruleBuilder.setApiVersion(5);
        ruleBuilder.setSource("my content");
        ruleBuilder.setType(RuleType.RULE_TYPE_JYTHON);
        triggerBuilder.setRule(ruleBuilder.build());

        final EventTrigger trigger = triggerBuilder.build();
        triggerDao.create(trigger);
        return trigger;
    }

    private EventTriggerSubscription createSubscription() throws ZepException {
        final EventTrigger trigger = createTrigger();
        EventTriggerSubscription.Builder subscriptionBuilder = EventTriggerSubscription
                .newBuilder();
        subscriptionBuilder.setSubscriberUuid(UUID.randomUUID().toString());
        subscriptionBuilder.setTriggerUuid(trigger.getUuid());
        EventTriggerSubscription triggerSubscription = subscriptionBuilder
                .build();
        String uuid = subscriptionDao.create(triggerSubscription);
        return EventTriggerSubscription.newBuilder(triggerSubscription)
                .setUuid(uuid).build();
    }

    private void compareSpool(EventSignalSpool existing, EventSignalSpool found) {
        assertEquals(existing.getUuid(), found.getUuid());
        assertEquals(existing.getEventCount(), found.getEventCount());
        assertEquals(existing.getCreated(), found.getCreated());
        assertEquals(existing.getEventSummaryUuid(),
                found.getEventSummaryUuid());
        assertEquals(existing.getEventTriggerSubscriptionUuid(),
                found.getEventTriggerSubscriptionUuid());
        assertEquals(existing.getFlushTime(), found.getFlushTime());
    }

    @Test
    public void testInsert() throws ZepException {
        EventTriggerSubscription subscription = createSubscription();
        EventSummary eventSummary = createSampleSummary();
        EventSignalSpool spool = EventSignalSpool.buildSpool(subscription,
                eventSummary);
        dao.create(spool);
        compareSpool(spool, dao.findByUuid(spool.getUuid()));
        dao.delete(spool.getUuid());
        assertNull(dao.findByUuid(spool.getUuid()));
    }

    @Test
    public void testDeleteByTriggerAndSummary() throws ZepException {
        EventTriggerSubscription subscription = createSubscription();
        EventSummary eventSummary = createSampleSummary();
        EventSignalSpool spool = EventSignalSpool.buildSpool(subscription,
                eventSummary);
        dao.create(spool);
        compareSpool(spool, dao.findByUuid(spool.getUuid()));
        assertEquals(
                1,
                dao.delete(subscription.getTriggerUuid(),
                        eventSummary.getUuid()));
        assertNull(dao.findByUuid(spool.getUuid()));
    }

    @Test
    public void testDeleteByTrigger() throws ZepException {
        EventTriggerSubscription subscription = createSubscription();
        EventSummary eventSummary = createSampleSummary();
        EventSignalSpool spool = EventSignalSpool.buildSpool(subscription,
                eventSummary);
        dao.create(spool);
        compareSpool(spool, dao.findByUuid(spool.getUuid()));
        assertEquals(1, dao.deleteByTriggerUuid(subscription.getTriggerUuid()));
        assertNull(dao.findByUuid(spool.getUuid()));
    }

    @Test
    public void testUpdateFlushTime() throws ZepException {
        EventTriggerSubscription subscription = createSubscription();
        EventSummary eventSummary = createSampleSummary();
        EventSignalSpool spool = EventSignalSpool.buildSpool(subscription,
                eventSummary);
        dao.create(spool);
        compareSpool(spool, dao.findByUuid(spool.getUuid()));
        long newFlushTime = spool.getFlushTime() + 600L;
        dao.updateFlushTime(spool.getUuid(), newFlushTime);
        assertEquals(newFlushTime, dao.findByUuid(spool.getUuid())
                .getFlushTime());
    }

    @Test
    public void testFindByTriggerAndSummary() throws ZepException {
        EventTriggerSubscription subscription = createSubscription();
        EventSummary eventSummary = createSampleSummary();
        EventSignalSpool spool = EventSignalSpool.buildSpool(subscription,
                eventSummary);
        dao.create(spool);
        compareSpool(
                spool,
                dao.findByTriggerAndEventSummaryUuids(
                        subscription.getTriggerUuid(), eventSummary.getUuid()));
    }

    @Test
    public void findAllDue() throws ZepException {
        // Create two events with past flush time, two with future
        Set<String> pastUuids = new HashSet<String>();
        Set<String> futureUuids = new HashSet<String>();
        for (int i = 0; i < 4; i++) {
            EventTriggerSubscription subscription = createSubscription();
            EventSummary eventSummary = createSampleSummary();
            EventSignalSpool spool = EventSignalSpool.buildSpool(subscription,
                    eventSummary);
            if (i < 2) {
                pastUuids.add(spool.getUuid());
                spool.setFlushTime(System.currentTimeMillis() - 5000L);
            } else {
                futureUuids.add(spool.getUuid());
                spool.setFlushTime(System.currentTimeMillis() + 5000L);
            }
            dao.create(spool);
        }
        List<EventSignalSpool> due = dao.findAllDue();
        assertEquals(pastUuids.size(), due.size());
        for (EventSignalSpool dueSpool : due) {
            assertTrue(pastUuids.contains(dueSpool.getUuid()));
        }
    }
}