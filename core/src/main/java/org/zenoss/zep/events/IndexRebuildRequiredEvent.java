/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.events;

/**
 * Fired when the index configuration has changed and the index must be rebuilt.
 */
public class IndexRebuildRequiredEvent extends ZepEvent {
    public IndexRebuildRequiredEvent(Object source) {
        super(source);
    }
}
