/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2011, Zenoss Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.dao;

import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.zep.ZepException;

import java.util.Map;

/*
 * DAO to provide interface to index configuration of event details
 */
public interface EventDetailsConfigDao {

    /**
     * Retrieves configuration information for indexing items added on
     * in the EventDetails array field, for custom event data
     *
     * @return Map<String, EventDetailItem>
     *            A map of event details configuration definitions, keyed by
     *            the detail name as it would be found in the EventDetails
     *            array (not necessarily the same name that will be used for
     *            indexing).
     *
     * @throws org.zenoss.zep.ZepException
     *             If an error occurs.
     */
    Map<String, EventDetailItem> getEventDetailsIndexConfiguration() throws
            ZepException;
}
