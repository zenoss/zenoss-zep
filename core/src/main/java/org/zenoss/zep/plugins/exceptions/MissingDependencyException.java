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
* Exception when a plug-in dependency cannot be satisfied.
*/
public class MissingDependencyException extends Exception {
    private static final long serialVersionUID = 1L;
    private final String pluginId;
    private final String dependencyId;

    public MissingDependencyException(String pluginId, String dependencyId) {
        this.pluginId = pluginId;
        this.dependencyId = dependencyId;
    }

    public String getPluginId() {
        return pluginId;
    }

    public String getDependencyId() {
        return dependencyId;
    }
}
