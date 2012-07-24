/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
