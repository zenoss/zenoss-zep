/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
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
