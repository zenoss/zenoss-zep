/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2011, Zenoss Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * For complete information please visit: http://www.zenoss.com/oss/
 */
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
    public void createHeartbeat(DaemonHeartbeat heartbeat) throws ZepException;

    /**
     * Returns all heartbeats in the ZEP database.
     *
     * @return List of all heartbeat records in the database.
     * @throws ZepException If an exception occurs.
     */
    public List<DaemonHeartbeat> findAll() throws ZepException;

    /**
     * Returns all heartbeats in the ZEP database for the specified monitor.
     *
     * @param monitor The monitor name.
     * @return List of all heartbeat records in the database for the specified monitor.
     * @throws ZepException If an exception occurs.
     */
    public List<DaemonHeartbeat> findByMonitor(String monitor) throws ZepException;

    /**
     * Deletes all heartbeats in the ZEP database.
     *
     * @return The number of affected rows.
     * @throws ZepException If an exception occurs.
     */
    public int deleteAll() throws ZepException;

    /**
     * Deletes all heartbeats in the ZEP database for the specified monitor.
     *
     * @param monitor The monitor name.
     * @return The number of affected rows.
     * @throws ZepException If an exception occurs.
     */
    public int deleteByMonitor(String monitor) throws ZepException;
}
