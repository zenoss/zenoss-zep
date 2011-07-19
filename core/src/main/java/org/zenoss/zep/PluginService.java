/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep;

import org.zenoss.zep.plugins.EventPlugin;

import java.util.List;

/**
 * Service which can look up event plug-ins of various types.
 */
public interface PluginService {

    /**
     * Returns a list of configured plug-ins of the specified type.
     * 
     * @return A list of configured plug-ins of the specified type.
     */
    public <T extends EventPlugin> List<T> getPluginsByType(Class<T> clazz);

    /**
     * Shuts down the PluginService.
     */
    public void shutdown();
    
}
