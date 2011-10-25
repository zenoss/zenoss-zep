/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.protobufs.zep.Zep.Rule;
import org.zenoss.protobufs.zep.Zep.RuleType;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventTriggerDao;
import org.zenoss.zep.dao.EventTriggerSubscriptionDao;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventTriggerSubscriptionDaoImplIT extends
        AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    public EventTriggerDao triggerDao;

    @Autowired
    public EventTriggerSubscriptionDao subscriptionDao;

    private boolean isInList(EventTriggerSubscription sub, List<EventTriggerSubscription> subscriptionList)
            throws ZepException {
        boolean found = false;
        for (EventTriggerSubscription subscription : subscriptionList) {
            if (sub.getUuid().equals(subscription.getUuid()) && sub.equals(subscription)) {
                assertFalse(found);
                found = true;
            }
        }
        return found;
    }

    private EventTrigger createTrigger() throws ZepException {
        EventTrigger.Builder triggerBuilder = EventTrigger.newBuilder();
        triggerBuilder.setEnabled(true);
        triggerBuilder.setUuid(UUID.randomUUID().toString());
        triggerBuilder.setName("my trigger");
        triggerBuilder.setRule(Rule.newBuilder().setApiVersion(1)
                .setSource("False").setType(RuleType.RULE_TYPE_JYTHON).build());
        EventTrigger trigger = triggerBuilder.build();
        triggerDao.create(trigger);
        return trigger;
    }

    @Test
    public void testCreate() throws ZepException {
        EventTrigger trigger = createTrigger();

        EventTriggerSubscription.Builder subBuilder = EventTriggerSubscription.newBuilder();
        subBuilder.setDelaySeconds(30);
        subBuilder.setRepeatSeconds(90);
        subBuilder.setSendInitialOccurrence(false);
        subBuilder.setSubscriberUuid(UUID.randomUUID().toString());
        subBuilder.setTriggerUuid(trigger.getUuid());
        subBuilder.setUuid(UUID.randomUUID().toString());
        EventTriggerSubscription sub = subBuilder.build();

        // Test create
        assertEquals(sub.getUuid(), subscriptionDao.create(sub));

        // Test findByUuid
        assertEquals(sub, subscriptionDao.findByUuid(sub.getUuid()));

        // Test findAll
        assertTrue(isInList(sub, subscriptionDao.findAll()));

        // Test findBySubscriberUuid
        assertTrue(isInList(sub, subscriptionDao.findBySubscriberUuid(sub.getSubscriberUuid())));
        assertFalse(isInList(sub, subscriptionDao.findBySubscriberUuid(UUID.randomUUID().toString())));

        subscriptionDao.delete(sub.getUuid());
        assertNull(subscriptionDao.findByUuid(sub.getUuid()));
    }

    @Test
    public void testUpdateSubscriptions() throws ZepException {
        EventTrigger trigger1 = createTrigger();
        EventTrigger trigger2 = createTrigger();
        EventTrigger trigger3 = createTrigger();

        final String subscriberUuid = UUID.randomUUID().toString();

        EventTriggerSubscription.Builder subBuilder = EventTriggerSubscription.newBuilder();
        subBuilder.setDelaySeconds(30);
        subBuilder.setRepeatSeconds(90);
        subBuilder.setSendInitialOccurrence(false);
        subBuilder.setSubscriberUuid(subscriberUuid);
        subBuilder.setTriggerUuid(trigger1.getUuid());
        subBuilder.setUuid(UUID.randomUUID().toString());
        EventTriggerSubscription subDeleted = subBuilder.build();
        subscriptionDao.create(subDeleted);

        subBuilder.clear();
        subBuilder.setDelaySeconds(15);
        subBuilder.setRepeatSeconds(45);
        subBuilder.setSendInitialOccurrence(true);
        subBuilder.setSubscriberUuid(subscriberUuid);
        subBuilder.setTriggerUuid(trigger2.getUuid());
        subBuilder.setUuid(UUID.randomUUID().toString());
        EventTriggerSubscription subBefore = subBuilder.build();
        subscriptionDao.create(subBefore);

        // We now have two subscriptions for this subscriber and trigger
        // Now we call updateSubscriptions with a new trigger and a changed trigger, and verify the changes.
        subBuilder.setDelaySeconds(90);
        subBuilder.setRepeatSeconds(120);
        subBuilder.setSendInitialOccurrence(false);
        EventTriggerSubscription subAfter = subBuilder.build();

        subBuilder.clear();
        subBuilder.setDelaySeconds(5);
        subBuilder.setRepeatSeconds(0);
        subBuilder.setSendInitialOccurrence(true);
        subBuilder.setSubscriberUuid(subscriberUuid);
        subBuilder.setTriggerUuid(trigger3.getUuid());
        subBuilder.setUuid(UUID.randomUUID().toString());
        EventTriggerSubscription subNew = subBuilder.build();

        subscriptionDao.updateSubscriptions(subscriberUuid, Arrays.asList(subAfter, subNew));
        assertNull(subscriptionDao.findByUuid(subDeleted.getUuid()));
        List<EventTriggerSubscription> newSubscriptions = this.subscriptionDao.findBySubscriberUuid(subscriberUuid);
        assertEquals(2, newSubscriptions.size());
        assertTrue(isInList(subAfter, newSubscriptions));
        assertTrue(isInList(subNew, newSubscriptions));
        assertFalse(isInList(subBefore, newSubscriptions));
        assertEquals(subAfter, this.subscriptionDao.findByUuid(subBefore.getUuid()));

        // Now update with an empty list of subscriptions - verify that all are deleted
        subscriptionDao.updateSubscriptions(subscriberUuid, Collections.<EventTriggerSubscription> emptyList());
        assertEquals(0, this.subscriptionDao.findBySubscriberUuid(subscriberUuid).size());
    }
}
