/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.plugins;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Abstract class defined for all plug-ins.
 */
public abstract class EventPlugin {

    protected Map<String, String> properties = Collections.emptyMap();

    /**
     * Returns properties of this plug-in.
     *
     * @return Properties of this plug-in.
     */
    public Map<String,String> getProperties() {
        return Collections.unmodifiableMap(this.properties);
    }

    /**
     * Returns the unique id for the plug-in.
     *
     * @return The unique id for the plug-in.
     */
    public String getId() {
        return getClass().getSimpleName();
    }

    /**
     * Returns the dependent plug-in ids.
     *
     * @return The plug-in ids that this plug-in depends on.
     */
    public Set<String> getDependencies() {
        return Collections.emptySet();
    }

    /**
     * Starts the plug-in with the specified configuration properties.
     *
     * @param properties Configuration properties for the plug-in.
     */
     public void start(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * Stops the plug-in.
     */
    public void stop() {
    }
}
