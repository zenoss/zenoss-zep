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

@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventTriggerSubscriptionDaoImplIT extends
        AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    public EventTriggerDao triggerDao;

    @Autowired
    public EventTriggerSubscriptionDao subscriptionDao;

    @Test
    public void testCreate() throws ZepException {
        EventTrigger.Builder triggerBuilder = EventTrigger.newBuilder();
        triggerBuilder.setEnabled(true);
        triggerBuilder.setUuid(UUID.randomUUID().toString());
        triggerBuilder.setName("my trigger");
        triggerBuilder.setRule(Rule.newBuilder().setApiVersion(1)
                .setSource("False").setType(RuleType.RULE_TYPE_JYTHON).build());
        EventTrigger trigger = triggerBuilder.build();
        triggerDao.create(trigger);

        EventTriggerSubscription.Builder subBuilder = EventTriggerSubscription
                .newBuilder();
        subBuilder.setDelaySeconds(30);
        subBuilder.setRepeatSeconds(90);
        subBuilder.setSubscriberUuid(UUID.randomUUID().toString());
        subBuilder.setTriggerUuid(trigger.getUuid());
        subBuilder.setUuid(UUID.randomUUID().toString());
        EventTriggerSubscription sub = subBuilder.build();

        assertEquals(sub.getUuid(), subscriptionDao.create(sub));
        assertEquals(sub, subscriptionDao.findByUuid(sub.getUuid()));
        subscriptionDao.delete(sub.getUuid());
        assertNull(subscriptionDao.findByUuid(sub.getUuid()));
    }
}
