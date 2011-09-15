/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.events;

/**
 * Fired when the index configuration has changed and the index must be rebuilt.
 */
public class IndexRebuildRequiredEvent extends ZepEvent {
    public IndexRebuildRequiredEvent(Object source) {
        super(source);
    }
}
