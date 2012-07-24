/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventDetailItem.EventDetailType;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventDetailsConfigDao;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Integration test for EventDetailsConfigDaoImpl.
 */
@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventDetailsConfigDaoImplIT extends AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    public EventDetailsConfigDao eventDetailsConfigDao;

    private static EventDetailItem createDetailItem(String key, EventDetailType type, String name) {
        return EventDetailItem.newBuilder().setKey(key).setType(type).setName(name).build();
    }

    @Test
    public void testCrud() throws ZepException {
        EventDetailItem item1 = createDetailItem("mykey", EventDetailType.STRING, "My Key");
        eventDetailsConfigDao.create(item1);
        assertEquals(item1, eventDetailsConfigDao.findByName(item1.getKey()));

        EventDetailItem item2 = EventDetailItem.newBuilder(item1).setType(EventDetailType.DOUBLE).build();
        eventDetailsConfigDao.create(item2);
        assertEquals(item2, eventDetailsConfigDao.findByName(item2.getKey()));

        Map<String,EventDetailItem> items = eventDetailsConfigDao.getEventDetailItemsByName();
        assertEquals(item2, items.get(item2.getKey()));

        assertEquals(1, eventDetailsConfigDao.delete(item2.getKey()));
        assertNull(eventDetailsConfigDao.findByName(item2.getKey()));
        assertEquals(0, eventDetailsConfigDao.delete(item2.getKey()));
    }
}
