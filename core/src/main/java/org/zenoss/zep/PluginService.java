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
