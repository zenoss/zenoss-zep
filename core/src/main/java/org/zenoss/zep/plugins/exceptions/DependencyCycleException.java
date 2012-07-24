/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.plugins.exceptions;

/**
* Exception thrown when a dependency cycle is detected.
*/
public class DependencyCycleException extends Exception {
    private static final long serialVersionUID = 1L;
    private final String pluginId;

    public DependencyCycleException(String pluginId) {
        this.pluginId = pluginId;
    }

    public String getPluginId() {
        return pluginId;
    }
}
