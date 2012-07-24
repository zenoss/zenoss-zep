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
import org.zenoss.zep.dao.DaoCache;

import java.util.Random;

import static org.junit.Assert.*;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class DaoCacheImplIT extends
        AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    public DaoCache daoCache;

    private Random random = new Random();

    @Test
    public void testAgentIdCache() {
        String tableName = "agent";
        String agent = "foo" + random.nextInt();
        int res = daoCache.getAgentId(agent);
        String sql = "SELECT id FROM " + tableName + " WHERE name=?";
        assertEquals(simpleJdbcTemplate.queryForInt(sql, agent), res);
        res = daoCache.getAgentId(agent);
        assertEquals(simpleJdbcTemplate.queryForInt(sql, agent), res);

        assertEquals(agent, daoCache.getAgentFromId(res));
    }

    @Test
    public void testEventClassIdCache() {
        String tableName = "event_class";
        String eventClass = "foo" + random.nextInt();
        int res = daoCache.getEventClassId(eventClass);
        String sql = "SELECT id FROM " + tableName + " WHERE name=?";
        assertEquals(simpleJdbcTemplate.queryForInt(sql, eventClass), res);
        res = daoCache.getEventClassId(eventClass);
        assertEquals(simpleJdbcTemplate.queryForInt(sql, eventClass), res);

        assertEquals(eventClass, daoCache.getEventClassFromId(res));
    }

    @Test
    public void testEventClassKeyIdCache() {
        String tableName = "event_class_key";
        String eventClassKey = "foo" + random.nextInt();
        int res = daoCache.getEventClassKeyId(eventClassKey);
        String sql = "SELECT id FROM " + tableName + " WHERE name=?";
        assertEquals(simpleJdbcTemplate.queryForInt(sql, eventClassKey), res);
        res = daoCache.getEventClassKeyId(eventClassKey);
        assertEquals(simpleJdbcTemplate.queryForInt(sql, eventClassKey), res);

        assertEquals(eventClassKey, daoCache.getEventClassKeyFromId(res));
    }

    @Test
    public void testEventGroupIdCache() {
        String tableName = "event_group";
        String eventGroup = "foo" + random.nextInt();
        int res = daoCache.getEventGroupId(eventGroup);
        String sql = "SELECT id FROM " + tableName + " WHERE name=?";
        assertEquals(simpleJdbcTemplate.queryForInt(sql, eventGroup), res);
        res = daoCache.getEventGroupId(eventGroup);
        assertEquals(simpleJdbcTemplate.queryForInt(sql, eventGroup), res);

        assertEquals(eventGroup, daoCache.getEventGroupFromId(res));
    }

    @Test
    public void testMonitorIdCache() {
        String tableName = "monitor";
        String monitor = "foo" + random.nextInt();
        int res = daoCache.getMonitorId(monitor);
        String sql = "SELECT id FROM " + tableName + " WHERE name=?";
        assertEquals(simpleJdbcTemplate.queryForInt(sql, monitor), res);
        res = daoCache.getMonitorId(monitor);
        assertEquals(simpleJdbcTemplate.queryForInt(sql, monitor), res);

        assertEquals(monitor, daoCache.getMonitorFromId(res));
    }

    @Test
    public void testEventKeyIdCache() {
        String tableName = "event_key";
        String eventKey = "foo" + random.nextInt();
        int res = daoCache.getEventKeyId(eventKey);
        String sql = "SELECT id FROM " + tableName + " WHERE name=?";
        assertEquals(simpleJdbcTemplate.queryForInt(sql, eventKey), res);
        res = daoCache.getEventKeyId(eventKey);
        assertEquals(simpleJdbcTemplate.queryForInt(sql, eventKey), res);

        assertEquals(eventKey, daoCache.getEventKeyFromId(res));
    }

}
