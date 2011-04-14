/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
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
