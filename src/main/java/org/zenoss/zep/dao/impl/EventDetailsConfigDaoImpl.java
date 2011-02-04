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
package org.zenoss.zep.dao.impl;


import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventDetailsConfigDao;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EventDetailsConfigDaoImpl implements EventDetailsConfigDao {

    private Map<String, EventDetailItem> detailsItemMap;
    public void init() {
        this.detailsItemMap = new HashMap<String, EventDetailItem>();

        // TODO - populate detailsItemMap using properties files in zep properties dir

    }

    @Override
    public Map<String, EventDetailItem> getEventDetailsIndexConfiguration() throws ZepException {
        return Collections.unmodifiableMap(this.detailsItemMap);
    }
}
