/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
