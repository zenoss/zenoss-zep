/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
    <T extends EventPlugin> List<T> getPluginsByType(Class<T> clazz);

    /**
     * Shuts down the PluginService.
     */
    void shutdown();

    void initializePlugins();

}
