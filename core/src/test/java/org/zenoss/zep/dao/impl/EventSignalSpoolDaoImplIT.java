/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.protobufs.zep.Zep.Rule;
import org.zenoss.protobufs.zep.Zep.RuleType;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSignalSpool;
import org.zenoss.zep.dao.EventSignalSpoolDao;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.dao.EventTriggerDao;
import org.zenoss.zep.dao.EventTriggerSubscriptionDao;
import org.zenoss.zep.impl.EventPreCreateContextImpl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventSignalSpoolDaoImplIT extends
        AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    public EventSignalSpoolDao dao;

    @Autowired
    public EventSummaryDao eventSummaryDao;

    @Autowired
    public EventTriggerDao triggerDao;

    @Autowired
    public EventTriggerSubscriptionDao subscriptionDao;

    @Autowired
    public UUIDGenerator uuidGenerator;
    
    private EventSummary createSummary(Event event) throws ZepException {
        String uuid = eventSummaryDao.create(event, new EventPreCreateContextImpl());
        return eventSummaryDao.findByUuid(uuid);
    }

    private EventSummary createSampleSummary() throws ZepException {
        Event event = EventTestUtils.createSampleEvent();
        return createSummary(event);
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
        EventTriggerSubscription.Builder subscriptionBuilder = EventTriggerSubscription.newBuilder();
        subscriptionBuilder.setSubscriberUuid(UUID.randomUUID().toString());
        subscriptionBuilder.setTriggerUuid(trigger.getUuid());
        EventTriggerSubscription triggerSubscription = subscriptionBuilder.build();
        String uuid = subscriptionDao.create(triggerSubscription);
        return EventTriggerSubscription.newBuilder(triggerSubscription).setUuid(uuid).build();
    }

    private void compareSpool(EventSignalSpool existing, EventSignalSpool found) {
        assertEquals(existing.getUuid(), found.getUuid());
        assertEquals(existing.getEventCount(), found.getEventCount());
        assertEquals(existing.getCreated(), found.getCreated());
        assertEquals(existing.getEventSummaryUuid(), found.getEventSummaryUuid());
        assertEquals(existing.getSubscriptionUuid(), found.getSubscriptionUuid());
        assertEquals(existing.getFlushTime(), found.getFlushTime());
        assertEquals(existing.isSentSignal(), found.isSentSignal());
    }

    @Test
    public void testInsert() throws ZepException {
        EventTriggerSubscription subscription = createSubscription();
        EventSummary eventSummary = createSampleSummary();
        EventSignalSpool spool = EventSignalSpool.buildSpool(subscription, eventSummary, uuidGenerator);
        dao.create(spool);
        compareSpool(spool, dao.findByUuid(spool.getUuid()));
        dao.delete(spool.getUuid());
        assertNull(dao.findByUuid(spool.getUuid()));
        
        // Test creating with sentSignal=true
        spool = EventSignalSpool.buildSpool(subscription, eventSummary, uuidGenerator);
        spool.setSentSignal(true);
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
                eventSummary, uuidGenerator);
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
                eventSummary, uuidGenerator);
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
                eventSummary, uuidGenerator);
        dao.create(spool);
        compareSpool(spool, dao.findByUuid(spool.getUuid()));
        spool.setFlushTime(spool.getFlushTime() + 600L);
        dao.update(spool);
        assertEquals(spool.getFlushTime(), dao.findByUuid(spool.getUuid()).getFlushTime());
    }

    @Test
    public void testFindByTriggerAndSummary() throws ZepException {
        EventTriggerSubscription subscription = createSubscription();
        EventSummary eventSummary = createSampleSummary();
        EventSignalSpool spool = EventSignalSpool.buildSpool(subscription,
                eventSummary, uuidGenerator);
        dao.create(spool);
        compareSpool(spool,
                dao.findBySubscriptionAndEventSummaryUuids(subscription.getUuid(), eventSummary.getUuid()));
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
                    eventSummary, uuidGenerator);
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

    @Test
    public void testFindBySummaryUuid() throws ZepException {
        EventTriggerSubscription subscription = createSubscription();
        EventSummary eventSummary = createSampleSummary();
        EventSignalSpool spool = EventSignalSpool.buildSpool(subscription, eventSummary, uuidGenerator);
        dao.create(spool);
        
        List<EventSignalSpool> spools = dao.findAllByEventSummaryUuid(eventSummary.getUuid());
        assertEquals(1, spools.size());
        compareSpool(spool, spools.get(0));
    }
    
    @Test
    public void testDeleteByEventSummaryUuid() throws ZepException {
        EventTriggerSubscription subscription = createSubscription();
        EventSummary eventSummary = createSampleSummary();
        EventSignalSpool spool = EventSignalSpool.buildSpool(subscription, eventSummary, uuidGenerator);
        dao.create(spool);

        assertEquals(1, dao.deleteByEventSummaryUuid(eventSummary.getUuid()));
        assertEquals(0, dao.findAllByEventSummaryUuid(eventSummary.getUuid()).size());
    }

    @Test
    public void testDeleteByEventSummaryUuids() throws ZepException {
        // Ensure deleting an empty collection doesn't trigger an error
        assertEquals(0, dao.deleteByEventSummaryUuids(Collections.<String>emptyList()));

        EventTriggerSubscription subscription1 = createSubscription();
        EventSummary eventSummary1 = createSampleSummary();
        EventSignalSpool spool1 = EventSignalSpool.buildSpool(subscription1, eventSummary1, uuidGenerator);
        dao.create(spool1);

        EventTriggerSubscription subscription2 = createSubscription();
        EventSummary eventSummary2 = createSampleSummary();
        EventSignalSpool spool2 = EventSignalSpool.buildSpool(subscription2, eventSummary2, uuidGenerator);
        dao.create(spool2);

        List<String> uuids = Arrays.asList(eventSummary1.getUuid(), eventSummary2.getUuid());
        assertEquals(2, dao.deleteByEventSummaryUuids(uuids));
        for (String uuid : uuids) {
            assertEquals(0, dao.findAllByEventSummaryUuid(uuid).size());
        }
    }

    @Test
    public void testUpdate() throws ZepException {
        EventTriggerSubscription subscription = createSubscription();
        EventSummary eventSummary = createSampleSummary();
        EventSignalSpool spool = EventSignalSpool.buildSpool(subscription, eventSummary, uuidGenerator);
        dao.create(spool);

        spool.setFlushTime(System.currentTimeMillis());
        spool.setEventCount(55);
        spool.setSentSignal(true);
        assertEquals(1, dao.update(spool));
        compareSpool(spool, dao.findByUuid(spool.getUuid()));
    }
}
