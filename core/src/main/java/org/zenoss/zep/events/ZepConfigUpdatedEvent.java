/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
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
