/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
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
