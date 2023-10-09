/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao;

import org.zenoss.protobufs.zep.Zep.DaemonHeartbeat;
import org.zenoss.zep.ZepException;

import java.util.List;

/**
 * Used to send daemon heartbeats.
 */
public interface HeartbeatDao {
    /**
     * Creates a record for the heartbeat from the specified daemon in the
     * database.
     *
     * @param heartbeat Heartbeat from daemon.
     * @throws ZepException If an exception occurs.
     */
    void createHeartbeat(DaemonHeartbeat heartbeat) throws ZepException;

    /**
     * Returns all heartbeats in the ZEP database.
     *
     * @return List of all heartbeat records in the database.
     * @throws ZepException If an exception occurs.
     */
    List<DaemonHeartbeat> findAll() throws ZepException;

    /**
     * Returns all heartbeats in the ZEP database for the specified monitor.
     *
     * @param monitor The monitor name.
     * @return List of all heartbeat records in the database for the specified monitor.
     * @throws ZepException If an exception occurs.
     */
    List<DaemonHeartbeat> findByMonitor(String monitor) throws ZepException;

    /**
     * Deletes all heartbeats in the ZEP database.
     *
     * @return The number of affected rows.
     * @throws ZepException If an exception occurs.
     */
    int deleteAll() throws ZepException;

    /**
     * Deletes all heartbeats in the ZEP database for the specified monitor.
     *
     * @param monitor The monitor name.
     * @return The number of affected rows.
     * @throws ZepException If an exception occurs.
     */
    int deleteByMonitor(String monitor) throws ZepException;

    /**
     * Deletes the heartbeat record in the ZEP database for the specified monitor and daemon.
     *
     * @param monitor The monitor name.
     * @param daemon The daemon name.
     * @return The number of affected rows.
     * @throws ZepException If an exception occurs.
     */
    int deleteByMonitorAndDaemon(String monitor, String daemon) throws ZepException;
}
