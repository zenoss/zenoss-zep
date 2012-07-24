/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao;

import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.zep.ZepException;

import java.util.Map;

/*
 * DAO to provide interface to index configuration of event details
 */
public interface EventDetailsConfigDao {

    /**
     * Populates the database with the default set of indexed details.
     *
     * @throws ZepException If an exception occurs.
     */
    public void init() throws ZepException;

    /**
     * Creates the event detail item in the database (adds the specified event
     * detail to be indexed). If the detail item exists in the database, it will
     * be replaced.
     *
     * @param item The item to add.
     * @throws ZepException If an exception occurs.
     */
    public void create(EventDetailItem item) throws ZepException;

    /**
     * Deletes the event detail item in the database with the specified event
     * detail name.
     *
     * @param eventDetailName The name of the event detail item to remove.
     * @return The number of affected rows.
     * @throws ZepException If an exception occurs.
     */
    public int delete(String eventDetailName) throws ZepException;

    /**
     * Returns the EventDetailItem matching the specified name.
     *
     * @param eventDetailName Event detail item name.
     * @return The EventDetailItem, or null if not found.
     * @throws ZepException If an exception occurs.
     */
    public EventDetailItem findByName(String eventDetailName) throws ZepException;

    /**
     * Retrieves configuration information for indexing items added on
     * in the EventDetails array field, for custom event data
     *
     * @return A map of event details configuration definitions, keyed by
     *         the detail name as it would be found in the EventDetails
     *         array (not necessarily the same name that will be used for
     *         indexing).
     * @throws org.zenoss.zep.ZepException If an error occurs.
     */
    public Map<String, EventDetailItem> getEventDetailItemsByName() throws ZepException;
}
