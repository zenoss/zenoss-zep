/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/
package org.zenoss.zep.impl;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.dao.impl.EventTestUtils;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Integration test for {@link MigratedEventQueueListener}.
 */
@ContextConfiguration({ "classpath:zep-config.xml" })
public class MigratedEventQueueListenerIT extends AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    @Qualifier("summary")
    public MigratedEventQueueListener summaryListener;

    @Autowired
    @Qualifier("archive")
    public MigratedEventQueueListener archiveListener;

    @Autowired
    public EventSummaryDao eventSummaryDao;

    @Autowired
    public EventArchiveDao eventArchiveDao;

    private EventSummary createSummary() {
        EventSummary.Builder summaryBuilder = EventSummary.newBuilder();
        summaryBuilder.addOccurrence(EventTestUtils.createSampleEvent());
        long now = System.currentTimeMillis();
        summaryBuilder.setFirstSeenTime(now);
        summaryBuilder.setLastSeenTime(now);
        summaryBuilder.setStatusChangeTime(now);
        summaryBuilder.setStatus(EventStatus.STATUS_CLOSED);
        summaryBuilder.setUuid(UUID.randomUUID().toString());
        summaryBuilder.setCount(1);
        return summaryBuilder.build();
    }

    @Test
    public void testDuplicateUuidsSummaryFirst() throws Exception {
        EventSummary summary = createSummary();
        summaryListener.handle(summary);
        archiveListener.handle(summary);

        assertNotNull(eventSummaryDao.findByUuid(summary.getUuid()));
        assertNull(eventArchiveDao.findByUuid(summary.getUuid()));
    }

    @Test
    public void testDuplicateUuidsArchiveFirst() throws Exception {
        EventSummary summary = createSummary();
        archiveListener.handle(summary);
        summaryListener.handle(summary);

        assertNotNull(eventArchiveDao.findByUuid(summary.getUuid()));
        assertNull(eventSummaryDao.findByUuid(summary.getUuid()));
    }
}
