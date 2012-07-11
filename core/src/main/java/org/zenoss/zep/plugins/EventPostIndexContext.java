/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.plugins;

/**
 * Context passed to {@link EventPostIndexPlugin}.
 */
public interface EventPostIndexContext {
    /**
     * Returns true if the event is in the event archive, false otherwise.
     *
     * @return True if the event is in the event archive, false otherwise.
     */
    public boolean isArchive();
}
