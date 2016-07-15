/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2011, 2014 all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventIndexHandler;
import org.zenoss.zep.dao.EventIndexQueueDao;
import org.zenoss.zep.dao.EventSummaryBaseDao;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.dao.IndexQueueID;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.TypeConverter;
import org.zenoss.zep.impl.EventPreCreateContextImpl;
import org.zenoss.zep.index.EventIndexDao;
import org.zenoss.zep.index.impl.MultiBackendEventIndexDao;
import org.zenoss.zep.index.impl.RedisWorkQueue;
import org.zenoss.zep.index.impl.lucene.LuceneEventIndexBackend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Integration tests for EventIndexQueueDao.
 */
@ContextConfiguration({"classpath:zep-config.xml"})
public class EventIndexQueueDaoImplIT extends AbstractTransactionalJUnit4SpringContextTests {
    @Autowired
    public RedisWorkQueue workQueue;

    @Autowired
    public EventSummaryDao eventSummaryDao;

    @Autowired
    public EventArchiveDao eventArchiveDao;

    @Autowired
    @Qualifier("summary")
    public EventIndexDao eventSummaryIndexDao;

    @Autowired
    @Qualifier("summary")
    public LuceneEventIndexBackend eventSummaryLuceneIndexBackend;

    @Autowired
    @Qualifier("archive")
    public EventIndexDao eventArchiveIndexDao;

    @Autowired
    @Qualifier("archive")
    public LuceneEventIndexBackend eventArchiveLuceneIndexBackend;

    @Autowired
    @Qualifier("summary")
    public EventIndexQueueDao eventSummaryIndexQueueDao;

    @Autowired
    @Qualifier("archive")
    public EventIndexQueueDao eventArchiveIndexQueueDao;

    @Autowired
    public DatabaseCompatibility databaseCompatibility;

    @Before
    public void setup() throws ZepException {
        ((MultiBackendEventIndexDao) eventSummaryIndexDao).disableRebuilders();
        ((MultiBackendEventIndexDao) eventArchiveIndexDao).disableRebuilders();
        ((MultiBackendEventIndexDao) eventSummaryIndexDao).disableAsyncProcessing();
        ((MultiBackendEventIndexDao) eventArchiveIndexDao).disableAsyncProcessing();
        eventSummaryIndexDao.clear();
        eventArchiveIndexDao.clear();
        eventSummaryLuceneIndexBackend.setReaderReopenInterval(0);
        eventArchiveLuceneIndexBackend.setReaderReopenInterval(0);
        eventSummaryDao.setTxSynchronizedQueue(false);
        this.workQueue.clearAll();
        this.simpleJdbcTemplate.update("TRUNCATE TABLE event_archive_index_queue");
    }

    private EventSummary create(EventSummaryBaseDao eventSummaryBaseDao, boolean archive) throws ZepException {
        Event event = EventTestUtils.createSampleEvent();
        if (archive) {
            event = Event.newBuilder(event).setStatus(EventStatus.STATUS_CLOSED).build();
        }
        String uuid = eventSummaryBaseDao.create(event, new EventPreCreateContextImpl());
        return eventSummaryBaseDao.findByUuid(uuid);
    }

    private static class TestEventIndexHandler implements EventIndexHandler {
        private List<EventSummary> indexed = new ArrayList<EventSummary>();
        private List<String> deleted = new ArrayList<String>();
        private AtomicBoolean completed = new AtomicBoolean(false);

        @Override
        public void prepareToHandle(Collection<EventSummary> events) throws Exception {
            //noop
        }

        @Override
        public void handle(EventSummary event) throws Exception {
            this.indexed.add(event);
        }

        @Override
        public void handleDeleted(String uuid) throws Exception {
            this.deleted.add(uuid);
        }

        @Override
        public void handleComplete() throws Exception {
            if (!completed.compareAndSet(false, true)) {
                throw new IllegalStateException("handleComplete should only be called once");
            }
        }
    }

    private EventSummary testCreate(EventSummaryBaseDao eventSummaryBaseDao, EventIndexQueueDao eventIndexQueueDao,
                                    boolean archive) throws ZepException {
        EventSummary eventSummary = create(eventSummaryBaseDao, archive);
        TestEventIndexHandler handler = new TestEventIndexHandler();
        List<IndexQueueID> indexQueueIds = eventIndexQueueDao.indexEvents(handler, 1000);
        assertEquals(1, indexQueueIds.size());
        assertTrue(handler.completed.get());
        assertEquals(1, handler.indexed.size());
        assertEquals(removeIsArchiveDetail(eventSummary), removeIsArchiveDetail(handler.indexed.get(0)));
        assertTrue(handler.deleted.isEmpty());

        eventIndexQueueDao.deleteIndexQueueIds(indexQueueIds);

        /* Run it again and verify that the event has been deleted */
        handler = new TestEventIndexHandler();
        assertEquals(0, eventIndexQueueDao.indexEvents(handler, 1000).size());
        assertFalse(handler.completed.get());
        assertTrue(handler.indexed.isEmpty());
        assertTrue(handler.deleted.isEmpty());
        return eventSummary;
    }

    public static EventSummary removeIsArchiveDetail(EventSummary input) {
        for (int i = 0; i < input.getOccurrence(0).getDetailsCount(); i++) {
            if ("is_archive".equals(input.getOccurrence(0).getDetails(i).getName())) {
                EventSummary.Builder builder = EventSummary.newBuilder(input);
                Event.Builder eventBuilder = builder.getOccurrenceBuilder(0);
                eventBuilder.removeDetails(i);
                return builder.build();
            }
        }
        return input;
    }

    @Test
    public void testIndexCreatedEvents() throws ZepException {
        testCreate(eventSummaryDao, eventSummaryIndexQueueDao, false);
        testCreate(eventArchiveDao, eventArchiveIndexQueueDao, true);
    }

    private void testModify(EventSummaryBaseDao eventSummaryBaseDao, EventIndexQueueDao eventIndexQueueDao,
                            boolean archive) throws ZepException {
        EventSummary summary = testCreate(eventSummaryBaseDao, eventIndexQueueDao, archive);
        EventNote note = EventNote.newBuilder().setCreatedTime(System.currentTimeMillis())
                .setMessage("My note").setUserName("pkw").setUserUuid(UUID.randomUUID().toString())
                .setUuid(UUID.randomUUID().toString()).build();

        eventSummaryBaseDao.addNote(summary.getUuid(), note);
        EventSummary summaryWithNote = eventSummaryBaseDao.findByUuid(summary.getUuid());

        TestEventIndexHandler handler = new TestEventIndexHandler();
        List<IndexQueueID> indexQueueIds = eventIndexQueueDao.indexEvents(handler, 1000);
        assertEquals(1, indexQueueIds.size());
        assertTrue(handler.completed.get());
        assertEquals(1, handler.indexed.size());
        assertEquals(removeIsArchiveDetail(summaryWithNote), removeIsArchiveDetail(handler.indexed.get(0)));
        assertTrue(handler.deleted.isEmpty());

        eventIndexQueueDao.deleteIndexQueueIds(indexQueueIds);

        /* Run it again and verify that the event has been deleted */
        handler = new TestEventIndexHandler();
        assertEquals(0, eventIndexQueueDao.indexEvents(handler, 1000).size());
        assertFalse(handler.completed.get());
        assertTrue(handler.indexed.isEmpty());
        assertTrue(handler.deleted.isEmpty());
    }

    @Test
    public void testIndexModifiedEvents() throws ZepException {
        testModify(eventSummaryDao, eventSummaryIndexQueueDao, false);
        testModify(eventArchiveDao, eventArchiveIndexQueueDao, true);
    }

    private void testDelete(EventSummaryBaseDao eventSummaryBaseDao, EventIndexQueueDao eventIndexQueueDao,
                            boolean archive) throws ZepException {
        TypeConverter<String> uuidConverter = databaseCompatibility.getUUIDConverter();
        EventSummary summary = testCreate(eventSummaryBaseDao, eventIndexQueueDao, archive);

        String tableName = (archive) ? EventConstants.TABLE_EVENT_ARCHIVE : EventConstants.TABLE_EVENT_SUMMARY;
        // event_summary triggers were dropped after adding the percona external tool for optimize
        // emulating the old event_summary triggers...
        if (!archive) {
            int closed = eventSummaryDao.close(Collections.singletonList(summary.getUuid()), UUID.randomUUID().toString(), "test");
            assertEquals(1, closed);
            TestEventIndexHandler handler = new TestEventIndexHandler();
            List<IndexQueueID> indexQueueIds = eventIndexQueueDao.indexEvents(handler, 1000);
            assertEquals(1, indexQueueIds.size());
            assertTrue(handler.completed.get());
            assertEquals(1, handler.indexed.size());
            assertTrue(handler.deleted.isEmpty());
            int archived = eventSummaryDao.archive(Collections.singletonList(summary.getUuid()));
            assertEquals(1, archived);
        } else {
            int numDeleted = this.simpleJdbcTemplate.update("DELETE FROM " + tableName + " WHERE uuid=?",
                    uuidConverter.toDatabaseType(summary.getUuid()));
            assertEquals(1, numDeleted);
        }

        TestEventIndexHandler handler = new TestEventIndexHandler();
        List<IndexQueueID> indexQueueIds = eventIndexQueueDao.indexEvents(handler, 1000);
        assertEquals(1, indexQueueIds.size());
        assertTrue(handler.completed.get());
        assertTrue(handler.indexed.isEmpty());
        assertEquals(1, handler.deleted.size());
        assertEquals(summary.getUuid(), handler.deleted.get(0));

        eventIndexQueueDao.deleteIndexQueueIds(indexQueueIds);

        /* Run it again and verify that the event has been deleted */
        handler = new TestEventIndexHandler();
        assertEquals(0, eventIndexQueueDao.indexEvents(handler, 1000).size());
        assertFalse(handler.completed.get());
        assertTrue(handler.indexed.isEmpty());
        assertTrue(handler.deleted.isEmpty());
    }

    @Test
    public void testIndexDeletedEvents() throws ZepException {
        testDelete(eventSummaryDao, eventSummaryIndexQueueDao, false);
    }

    @Test
    public void testIndexArchiveDeletedEvents() throws ZepException {
        testDelete(eventArchiveDao, eventArchiveIndexQueueDao, true);
    }

    @Test
    public void testSummaryQueueLength() throws ZepException {
        eventSummaryIndexQueueDao.getQueueLength();
    }

    @Test
    public void testArchiveQueueLength() throws ZepException {
        long actual = eventSummaryIndexQueueDao.getQueueLength();
        assertEquals(0L, actual);
    }

    @Test
    public void testIndexGrouping() throws ZepException {
        // Verifies that during indexing, we will only process the same event once.

        // Create one event 500 times
        Event event = EventTestUtils.createSampleEvent();
        for (int i = 0; i < 500; i++) {
            eventSummaryDao.create(event, new EventPreCreateContextImpl());
        }
        TestEventIndexHandler handler = new TestEventIndexHandler();
        List<IndexQueueID> indexQueueIds = eventSummaryIndexQueueDao.indexEvents(handler, 1000);
        // The number of queue ids should be 500
        assertEquals(500, indexQueueIds.size());
        assertEquals(1, handler.indexed.size());
        assertEquals(0, handler.deleted.size());
        assertTrue(handler.completed.get());
    }

}
