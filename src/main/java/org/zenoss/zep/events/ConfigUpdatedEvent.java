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

import java.util.Collections;
import java.util.Map;

import org.springframework.context.ApplicationEvent;
import org.zenoss.protobufs.zep.Zep.ZepConfig;

public class ConfigUpdatedEvent extends ApplicationEvent {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final ZepConfig config;

    public ConfigUpdatedEvent(Object source, ZepConfig config) {
        super(source);
        this.config = config;
    }

    public ZepConfig getConfig() {
        return this.config;
    }

}
