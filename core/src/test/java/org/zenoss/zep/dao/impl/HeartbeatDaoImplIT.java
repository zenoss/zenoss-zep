/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.DaemonHeartbeat;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.HeartbeatDao;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for HeartbeatDao.
 */
@ContextConfiguration({ "classpath:zep-config.xml" })
public class HeartbeatDaoImplIT extends AbstractTransactionalJUnit4SpringContextTests {
    @Autowired
    public HeartbeatDao heartbeatDao;

    private void findHeartbeat(DaemonHeartbeat hb, List<DaemonHeartbeat> heartbeats) throws ZepException {
        boolean found = false;
        for (DaemonHeartbeat heartbeat : heartbeats) {
            // Clear last_time for comparisons
            heartbeat = DaemonHeartbeat.newBuilder(heartbeat).clearLastTime().build();
            if (hb.getMonitor().equals(heartbeat.getMonitor()) &&
                hb.getDaemon().equals(heartbeat.getDaemon())) {
                assertEquals(hb, heartbeat);
                found = true;
            }
        }
        assertTrue(found);
    }

    @Before
    public void init() {
        this.simpleJdbcTemplate.update("DELETE FROM daemon_heartbeat");
    }

    private static DaemonHeartbeat createHeartbeat(String monitor, String daemon, int timeoutSeconds) {
        return DaemonHeartbeat.newBuilder().setMonitor(monitor).setDaemon(daemon).setTimeoutSeconds(timeoutSeconds)
                .build();
    }

    @Test
    public void testCreate() throws ZepException {
        DaemonHeartbeat hb = createHeartbeat("localhost", "zenactiond", 90);
        heartbeatDao.createHeartbeat(hb);
        findHeartbeat(hb, heartbeatDao.findAll());

        // Change timeout - tests ON DUPLICATE KEY UPDATE
        hb = createHeartbeat("localhost", "zenactiond", 900);
        heartbeatDao.createHeartbeat(hb);
        findHeartbeat(hb, heartbeatDao.findAll());
    }

    @Test
    public void testMonitor() throws ZepException {
        DaemonHeartbeat localhost = createHeartbeat("localhost", "zenactiond", 90);
        heartbeatDao.createHeartbeat(localhost);
        DaemonHeartbeat devsvcs = createHeartbeat("devsvcs", "zenactiond", 90);
        heartbeatDao.createHeartbeat(devsvcs);

        assertEquals(1, heartbeatDao.findByMonitor("localhost").size());
        findHeartbeat(localhost, heartbeatDao.findByMonitor("localhost"));

        assertEquals(1, heartbeatDao.findByMonitor("devsvcs").size());
        findHeartbeat(devsvcs, heartbeatDao.findByMonitor("devsvcs"));

        assertEquals(1, heartbeatDao.deleteByMonitor("localhost"));
        assertEquals(0, heartbeatDao.findByMonitor("localhost").size());

        assertEquals(1, heartbeatDao.deleteByMonitor("devsvcs"));
        assertEquals(0, heartbeatDao.findByMonitor("devsvcs").size());
    }
}
