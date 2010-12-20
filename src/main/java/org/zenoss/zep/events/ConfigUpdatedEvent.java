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

public class ConfigUpdatedEvent extends ApplicationEvent {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final Map<String, String> config;
    private final boolean multiple;

    public ConfigUpdatedEvent(Object source, String name) {
        super(source);
        this.config = Collections.singletonMap(name, null);
        this.multiple = false;
    }

    public ConfigUpdatedEvent(Object source, String name, String value) {
        super(source);
        this.config = Collections.singletonMap(name, value);
        this.multiple = false;
    }

    public ConfigUpdatedEvent(Object source, Map<String, String> config) {
        super(source);
        this.config = config;
        this.multiple = true;
    }

    public Map<String, String> getConfig() {
        return Collections.unmodifiableMap(config);
    }

    public boolean isMultiple() {
        return multiple;
    }
}
