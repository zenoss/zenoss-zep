/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.zenoss.zep.EventPlugin;

/**
 * Abstract class which implements methods on {@link EventPlugin}.
 */
public abstract class AbstractPlugin implements EventPlugin {

    protected Map<String, String> properties = Collections.emptyMap();

    @Override
    public String getId() {
        return getClass().getSimpleName();
    }

    @Override
    public String getName() {
        return getId();
    }

    @Override
    public Set<String> getDependencies() {
        return Collections.emptySet();
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(this.properties);
    }

    @Override
    public void init(Map<String, String> properties) {
        this.properties = properties;
    }

}
