/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
