/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
        this.jdbcTemplate.update("DELETE FROM daemon_heartbeat");
    }

    private static DaemonHeartbeat createHeartbeat(String monitor, String daemon, int timeoutSeconds) {
        return DaemonHeartbeat.newBuilder().setMonitor(monitor).setDaemon(daemon).setTimeoutSeconds(timeoutSeconds)
                .build();
    }

    private static DaemonHeartbeat clearLastTime(DaemonHeartbeat heartbeat) {
        return DaemonHeartbeat.newBuilder(heartbeat).clearLastTime().build();
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

    @Test
    public void testDeleteByMonitorAndDaemon() throws ZepException {
        DaemonHeartbeat zenmodeler = createHeartbeat("localhost", "zenmodeler", 1440);
        heartbeatDao.createHeartbeat(zenmodeler);
        DaemonHeartbeat zenactiond = createHeartbeat("localhost", "zenactiond", 90);
        heartbeatDao.createHeartbeat(zenactiond);

        assertEquals(2, heartbeatDao.findByMonitor(zenmodeler.getMonitor()).size());
        heartbeatDao.deleteByMonitorAndDaemon(zenmodeler.getMonitor(), zenmodeler.getDaemon());

        assertEquals(zenactiond, clearLastTime(heartbeatDao.findByMonitor(zenactiond.getMonitor()).get(0)));
        heartbeatDao.deleteByMonitorAndDaemon(zenactiond.getMonitor(), zenactiond.getDaemon());

        assertTrue(heartbeatDao.findByMonitor(zenactiond.getMonitor()).isEmpty());
    }
}
