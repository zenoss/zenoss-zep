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
    int getEventClassId(String eventClass);

    String getEventClassFromId(int id);

    /**
     * Returns the auto generated id corresponding to the event class key.
     * 
     * @param eventClassKey
     *            Event class key.
     * @return Auto generated id.
     */
    int getEventClassKeyId(String eventClassKey);

    String getEventClassKeyFromId(int id);

    /**
     * Returns the auto generated id corresponding to the monitor.
     * 
     * @param monitor
     *            Monitor.
     * @return Auto generated id.
     */
    int getMonitorId(String monitor);

    String getMonitorFromId(int id);

    /**
     * Returns the auto generated id corresponding to the agent.
     * 
     * @param agent
     *            Agent.
     * @return Auto generated id.
     */
    int getAgentId(String agent);

    String getAgentFromId(int id);

    /**
     * Returns the auto generated id corresponding to the event group.
     * 
     * @param eventGroup
     *            Event group.
     * @return Auto generated id.
     */
    int getEventGroupId(String eventGroup);

    String getEventGroupFromId(int id);

    int getEventKeyId(String eventKey);

    String getEventKeyFromId(int id);
}
