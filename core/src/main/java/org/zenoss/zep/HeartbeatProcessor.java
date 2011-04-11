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
package org.zenoss.zep;

/**
 * Component used to process heartbeat events.
 */
public interface HeartbeatProcessor {
    /**
     * Sends heartbeat events for all daemons which have not reported
     * heartbeats within the timeout interval. If a daemon has reported
     * a heartbeat within the interval, then a clear event is sent to
     * clear any existing heartbeat events for the daemon.
     *
     * @throws ZepException If an error occurs.
     */
    public void sendHeartbeatEvents() throws ZepException;
}
