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

import java.io.IOException;
import java.util.UUID;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.protobufs.zep.Zep.Rule;
import org.zenoss.protobufs.zep.Zep.RuleType;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventTriggerDao;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventTriggerDaoImplIT extends
        AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    protected EventTriggerDao dao;

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
}
