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
package org.zenoss.zep.dao;

import java.util.Map;

import org.zenoss.zep.ZepException;

/**
 * Interface used to manage ZEP's configuration settings.
 */
public interface ConfigDao {
    /**
     * Returns the configuration of ZEP.
     * 
     * @return The ZEP configuration.
     * @throws ZepException
     *             If an error occurs loading the configuration.
     */
    public Map<String, String> getConfig() throws ZepException;

    /**
     * Retrieves the value of the configuration entry with the specified value,
     * or null if it does not exist.
     * 
     * @param name
     *            The name of the configuration entry to retrieve.
     * @return The value, or null if it does not exist.
     * @throws ZepException
     *             If an error occurs loading the configuration.
     */
    public String getConfigValue(String name) throws ZepException;

    /**
     * Removes the configuration value with the specified name from the
     * database.
     * 
     * @param name
     *            The name of the configuration value to remove.
     * @return The number of affected rows.
     * @throws ZepException
     *             If an error occurs removing the configuration value.
     */
    public int removeConfigValue(String name) throws ZepException;

    /**
     * Overwrites any existing configuration for ZEP with the specified
     * configuration.
     * 
     * @param config
     *            The new configuration settings to use.
     * @throws ZepException
     *             If an error occurs overwriting the configuration.
     */
    public void setConfig(Map<String, String> config) throws ZepException;

    /**
     * Adds or updates one configuration entry with the specified name.
     * 
     * @param name
     *            The name of the configuration entry.
     * @param value
     *            The new value.
     * @throws ZepException
     *             If an error occurs modifying the configuration.
     */
    public void setConfigValue(String name, String value) throws ZepException;
}
