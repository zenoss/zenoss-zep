/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.index;

/**
 * Interface used to trigger a rebuild of the event index when configuration changes.
 */
public interface EventIndexRebuilder {
    /**
     * Shuts down the index rebuilder.
     *
     * @throws InterruptedException If the rebuilder is interrupted while shutting down.
     */
    public void shutdown() throws InterruptedException;
}
