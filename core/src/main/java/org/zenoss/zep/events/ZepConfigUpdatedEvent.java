/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.events;

import org.zenoss.protobufs.zep.Zep.ZepConfig;

/**
 * Application event sent when the ZEP configuration changes.
 */
public class ZepConfigUpdatedEvent extends ZepEvent {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final ZepConfig config;

    public ZepConfigUpdatedEvent(Object source, ZepConfig config) {
        super(source);
        this.config = config;
    }

    public ZepConfig getConfig() {
        return this.config;
    }

}
