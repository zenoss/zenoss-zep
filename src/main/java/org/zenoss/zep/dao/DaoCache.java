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


/**
 * Provides caching support for normalized tables to speed up retrieving an auto
 * generated ID from the database.
 */
public interface DaoCache {
    /**
     * Returns the auto generated id corresponding to the model event class.
     * 
     * @param eventClass
     *            Event class.
     * @return Auto generated id.
     */
    public int getEventClassId(String eventClass);

    public String getEventClassFromId(int id);

    /**
     * Returns the auto generated id corresponding to the event class key.
     * 
     * @param eventClassKey
     *            Event class key.
     * @return Auto generated id.
     */
    public int getEventClassKeyId(String eventClassKey);

    public String getEventClassKeyFromId(int id);

    /**
     * Returns the auto generated id corresponding to the monitor.
     * 
     * @param monitor
     *            Monitor.
     * @return Auto generated id.
     */
    public int getMonitorId(String monitor);

    public String getMonitorFromId(int id);

    /**
     * Returns the auto generated id corresponding to the agent.
     * 
     * @param agent
     *            Agent.
     * @return Auto generated id.
     */
    public int getAgentId(String agent);

    public String getAgentFromId(int id);

    /**
     * Returns the auto generated id corresponding to the event group.
     * 
     * @param eventGroup
     *            Event group.
     * @return Auto generated id.
     */
    public int getEventGroupId(String eventGroup);

    public String getEventGroupFromId(int id);

    public int getEventKeyId(String eventKey);

    public String getEventKeyFromId(int id);
}
