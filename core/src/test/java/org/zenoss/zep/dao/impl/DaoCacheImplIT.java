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

        //test case sensitivity
        String agent2 = agent.toUpperCase();
        int res2 = daoCache.getAgentId(agent2);
        assertFalse(res == res2);//this should be a new ID
        assertEquals(agent2, daoCache.getAgentFromId(res2)); //Should still work both ways
        assertEquals(agent, daoCache.getAgentFromId(res)); //We didn't overwrite the previous entry
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

        //test case sensitivity
        String eventClass2 = eventClass.toUpperCase();
        int res2 = daoCache.getEventClassId(eventClass2);
        assertFalse(res == res2);//this should be a new ID
        assertEquals(eventClass2, daoCache.getEventClassFromId(res2)); //Should still work both ways
        assertEquals(eventClass, daoCache.getEventClassFromId(res)); //We didn't overwrite the previous entry
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

        //test case sensitivity
        String eventClassKey2 = eventClassKey.toUpperCase();
        int res2 = daoCache.getEventClassKeyId(eventClassKey2);
        assertFalse(res == res2);//this should be a new ID
        assertEquals(eventClassKey2, daoCache.getEventClassKeyFromId(res2)); //Should still work both ways
        assertEquals(eventClassKey, daoCache.getEventClassKeyFromId(res)); //We didn't overwrite the previous entry
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

        //test case sensitivity
        String eventGroup2 = eventGroup.toUpperCase();
        int res2 = daoCache.getEventGroupId(eventGroup2);
        assertFalse(res == res2);//this should be a new ID
        assertEquals(eventGroup2, daoCache.getEventGroupFromId(res2)); //Should still work both ways
        assertEquals(eventGroup, daoCache.getEventGroupFromId(res)); //We didn't overwrite the previous entry
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

        //test case sensitivity
        String monitor2 = monitor.toUpperCase();
        int res2 = daoCache.getMonitorId(monitor2);
        assertFalse(res == res2);//this should be a new ID
        assertEquals(monitor2, daoCache.getMonitorFromId(res2)); //Should still work both ways
        assertEquals(monitor, daoCache.getMonitorFromId(res)); //We didn't overwrite the previous entry
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

        //test case sensitivity
        String eventKey2 = eventKey.toUpperCase();
        int res2 = daoCache.getEventKeyId(eventKey2);
        assertFalse(res == res2);//this should be a new ID
        assertEquals(eventKey2, daoCache.getEventKeyFromId(res2)); //Should still work both ways
        assertEquals(eventKey, daoCache.getEventKeyFromId(res)); //We didn't overwrite the previous entry
    }

}
