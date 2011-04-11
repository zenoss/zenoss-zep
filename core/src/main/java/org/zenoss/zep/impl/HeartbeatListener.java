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
package org.zenoss.zep.impl;

import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.amqp.AmqpConnectionManager;
import org.zenoss.amqp.QueueConfiguration;
import org.zenoss.amqp.QueueListener;
import org.zenoss.amqp.ZenossQueueConfig;
import org.zenoss.protobufs.zep.Zep.DaemonHeartbeat;
import org.zenoss.zep.dao.HeartbeatDao;

import java.io.IOException;

/**
 * Listener for daemon heartbeats.
 */
public class HeartbeatListener extends QueueListener {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatListener.class);

    private HeartbeatDao heartbeatDao;

    public void setConnectionManager(AmqpConnectionManager connectionManager)
            throws IOException {
        QueueConfiguration queueCfg = ZenossQueueConfig.getConfig().getQueue("$ZepHeartbeats");
        connectionManager.addListener(queueCfg, this);
    }

    public void setHeartbeatDao(HeartbeatDao heartbeatDao) {
        this.heartbeatDao = heartbeatDao;
    }

    @Override
    public void handle(Message message) throws Exception {
        if (!(message instanceof DaemonHeartbeat)) {
            logger.warn("Skipping unsupported message: {}", message);
            return;
        }
        DaemonHeartbeat heartbeat = (DaemonHeartbeat) message;
        logger.debug("Creating heartbeat record: {}", heartbeat);
        this.heartbeatDao.createHeartbeat(heartbeat);
    }
}
