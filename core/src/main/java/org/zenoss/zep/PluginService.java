/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep;

import java.util.List;

/**
 * Service which can look up event plug-ins of various types.
 */
public interface PluginService {
    /**
     * Returns a list of configured pre-processing plug-ins, or an empty list if
     * none are configured. The pre-processing plug-ins must run in the order
     * returned by this list.
     * 
     * @return A list of configured pre-processing plug-ins, or an empty list if
     *         none are configured.
     */
    public List<EventPreProcessingPlugin> getPreProcessingPlugins();

    /**
     * Returns a list of configured post-processing plug-ins. Post-processing
     * plug-ins can run in any order.
     * 
     * @return A list of configured post-processing plug-ins, or an empty list
     *         if none are configured.
     */
    public List<EventPostProcessingPlugin> getPostProcessingPlugins();
}
