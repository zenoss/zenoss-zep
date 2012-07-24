/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
