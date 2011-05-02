/*
 * Copyright (C) 2010-2011, Zenoss Inc. All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

import static org.junit.Assert.*;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventTriggerDaoImplIT extends AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    protected EventTriggerDao dao;

    @Autowired
    protected EventTriggerSubscriptionDao subscriptionDao;

    @Test
    public void testInsert() throws ZepException, IOException {
        EventTrigger.Builder triggerBuilder = EventTrigger.newBuilder();
        triggerBuilder.setUuid(UUID.randomUUID().toString());
        Rule.Builder ruleBuilder = Rule.newBuilder();
        ruleBuilder.setApiVersion(5);
        ruleBuilder.setSource("my content");
        ruleBuilder.setType(RuleType.RULE_TYPE_JYTHON);
        triggerBuilder.setRule(ruleBuilder.build());

        EventTrigger trigger = triggerBuilder.build();
        dao.create(trigger);
        EventTrigger triggerFromDb = dao.findByUuid(trigger.getUuid());
        assertEquals(trigger.getUuid(), triggerFromDb.getUuid());
        assertEquals(trigger.getRule(), triggerFromDb.getRule());
        boolean foundInAll = false;
        for (EventTrigger t : dao.findAll()) {
            if (t.getUuid().equals(trigger.getUuid())) {
                foundInAll = true;
                break;
            }
        }
        assertTrue(foundInAll);
        dao.delete(trigger.getUuid());
        assertNull(dao.findByUuid(trigger.getUuid()));
    }

    @Test
    public void testModify() throws ZepException {
        EventTrigger.Builder triggerBuilder = EventTrigger.newBuilder();
        triggerBuilder.setUuid(UUID.randomUUID().toString());
        Rule.Builder ruleBuilder = Rule.newBuilder();
        ruleBuilder.setApiVersion(5);
        ruleBuilder.setSource("my content");
        ruleBuilder.setType(RuleType.RULE_TYPE_JYTHON);
        triggerBuilder.setRule(ruleBuilder.build());
        triggerBuilder.setEnabled(true);

        EventTrigger trigger = triggerBuilder.build();
        dao.create(trigger);
        EventTrigger triggerFromDb = dao.findByUuid(trigger.getUuid());
        assertEquals(trigger, triggerFromDb);
        for (EventTrigger t : dao.findAll()) {
            if (t.getUuid().equals(trigger.getUuid())) {
                assertEquals(trigger, t);
                break;
            }
        }

        trigger = EventTrigger.newBuilder(trigger).setEnabled(false).build();
        dao.modify(trigger);
        triggerFromDb = dao.findByUuid(trigger.getUuid());
        assertEquals(trigger, triggerFromDb);
    }

    @Test
    public void testFindEnabled() throws ZepException {
        final EventTrigger enabled;
        {
            EventTrigger.Builder triggerBuilder = EventTrigger.newBuilder();
            triggerBuilder.setUuid(UUID.randomUUID().toString());
            Rule.Builder ruleBuilder = Rule.newBuilder();
            ruleBuilder.setApiVersion(5);
            ruleBuilder.setSource("my content");
            ruleBuilder.setType(RuleType.RULE_TYPE_JYTHON);
            triggerBuilder.setRule(ruleBuilder.build());
            triggerBuilder.setEnabled(true);
            enabled = triggerBuilder.build();
        }

        final EventTrigger disabled;
        {
            EventTrigger.Builder triggerBuilder = EventTrigger.newBuilder();
            triggerBuilder.setUuid(UUID.randomUUID().toString());
            Rule.Builder ruleBuilder = Rule.newBuilder();
            ruleBuilder.setApiVersion(5);
            ruleBuilder.setSource("my content");
            ruleBuilder.setType(RuleType.RULE_TYPE_JYTHON);
            triggerBuilder.setRule(ruleBuilder.build());
            triggerBuilder.setEnabled(false);
            disabled = triggerBuilder.build();
        }

        dao.create(enabled);
        dao.create(disabled);

        boolean foundEnabled = false;
        List<EventTrigger> enabledTriggers = dao.findAllEnabled();
        for (EventTrigger triggerFromDb : enabledTriggers) {
            if (triggerFromDb.getUuid().equals(disabled.getUuid())) {
                fail("Shouldn't have returned disabled trigger");
            }
            if (triggerFromDb.equals(enabled)) {
                foundEnabled = true;
            }
        }
        assertTrue("Failed to find enabled trigger", foundEnabled);
    }
    
    private void compareSubscriptions(EventTrigger triggerFromDb, Map<String,EventTriggerSubscription> subscriptionMap) {
        final Map<String,EventTriggerSubscription> subscriptionMapFromDb = new HashMap<String,EventTriggerSubscription>();
        for (EventTriggerSubscription subscriptionFromDb : triggerFromDb.getSubscriptionsList()) {
            subscriptionMapFromDb.put(subscriptionFromDb.getUuid(), subscriptionFromDb);
        }
        assertEquals(subscriptionMap, subscriptionMapFromDb);
    }

    @Test
    public void testFindWithSubscriptions() throws ZepException {
        EventTrigger.Builder triggerBuilder = EventTrigger.newBuilder();
        triggerBuilder.setUuid(UUID.randomUUID().toString());
        Rule.Builder ruleBuilder = Rule.newBuilder();
        ruleBuilder.setApiVersion(5);
        ruleBuilder.setSource("my content");
        ruleBuilder.setType(RuleType.RULE_TYPE_JYTHON);
        triggerBuilder.setRule(ruleBuilder.build());
        triggerBuilder.setEnabled(true);
        EventTrigger enabled = triggerBuilder.build();
        dao.create(enabled);

        // Create subscriptions
        EventTriggerSubscription sub1 = EventTriggerSubscription.newBuilder().setDelaySeconds(30).setRepeatSeconds(60)
            .setUuid(UUID.randomUUID().toString()).setSendInitialOccurrence(true)
            .setTriggerUuid(triggerBuilder.getUuid()).setSubscriberUuid(UUID.randomUUID().toString()).build();
        subscriptionDao.create(sub1);

        EventTriggerSubscription sub2 = EventTriggerSubscription.newBuilder().setDelaySeconds(0).setRepeatSeconds(0)
            .setUuid(UUID.randomUUID().toString()).setSendInitialOccurrence(false)
            .setTriggerUuid(triggerBuilder.getUuid()).setSubscriberUuid(UUID.randomUUID().toString()).build();
        subscriptionDao.create(sub2);

        Map<String,EventTriggerSubscription> subscriptionMap = new HashMap<String,EventTriggerSubscription>();
        subscriptionMap.put(sub1.getUuid(), sub1);
        subscriptionMap.put(sub2.getUuid(), sub2);

        compareSubscriptions(dao.findByUuid(enabled.getUuid()), subscriptionMap);
        compareSubscriptions(dao.findAllEnabled().get(0), subscriptionMap);
        compareSubscriptions(dao.findAll().get(0), subscriptionMap);
    }
}
