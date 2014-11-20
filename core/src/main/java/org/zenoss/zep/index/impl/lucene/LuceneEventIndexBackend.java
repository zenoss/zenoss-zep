/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010, 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.zep.index.impl.lucene;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.OpenBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventFilter;
import org.zenoss.protobufs.zep.Zep.EventQuery;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventSort;
import org.zenoss.protobufs.zep.Zep.EventSort.Direction;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryResult;
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.zep.Messages;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepConstants;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSummaryBaseDao;
import org.zenoss.zep.index.IndexedDetailsConfiguration;
import org.zenoss.zep.index.SavedSearchProcessor;
import org.zenoss.zep.index.impl.BaseEventIndexBackend;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.zenoss.zep.index.impl.IndexConstants.*;

public class LuceneEventIndexBackend extends BaseEventIndexBackend<LuceneSavedSearch> {

    private static final Logger logger = LoggerFactory.getLogger(LuceneEventIndexBackend.class);

    private final String name;
    private final IndexWriter writer;
    private final SearcherManager searcherManager;
    private final boolean archive;
    private final EventSummaryBaseDao eventSummaryBaseDao;
    private final LuceneFilterCacheManager filterCacheManager;
    private final TrackingIndexWriter trackingIndexWriter;
    private ControlledRealTimeReopenThread nrtManagerReopenThread;
    private int readerReopenInterval;
    private volatile boolean ready = false;


    private int queryLimit = ZepConstants.DEFAULT_QUERY_LIMIT;

    private IndexedDetailsConfiguration indexedDetailsConfiguration;

    private MetricRegistry metrics;
    private int indexResultsCount = -1;
    private int luceneSearchTimeout = 0;

    public LuceneEventIndexBackend(String name, IndexWriter writer, EventSummaryBaseDao eventSummaryBaseDao,
                                   Integer maxClauseCount, LuceneFilterCacheManager filterCacheManager, int readerRefreshInterval,
                                   Messages messages, TaskScheduler scheduler, UUIDGenerator uuidGenerator)
            throws IOException
    {
        super(messages, scheduler, uuidGenerator);
        this.name = name;
        this.writer = writer;
        this.trackingIndexWriter = new TrackingIndexWriter(this.writer);
        this.searcherManager = new SearcherManager(this.writer, true, null);
        this.eventSummaryBaseDao = eventSummaryBaseDao;
        this.archive = "event_archive".equals(name);
        this.filterCacheManager = filterCacheManager;
        this.readerReopenInterval = readerRefreshInterval;
        BooleanQuery.setMaxClauseCount(maxClauseCount);

        // Deal with the reader reopen thread
        if (this.readerReopenInterval != 0) {
            startReopenThread();
        } else {
            this.nrtManagerReopenThread = null;
        }
    }

    private String getMetricName(String metricName) {
        if(this.archive) {
            metricName = MetricRegistry.name(this.getClass().getCanonicalName(), "archive" + metricName);
        }
        else {
            metricName = MetricRegistry.name(this.getClass().getCanonicalName(), "summary" + metricName);
        }
        return metricName;
    }

    @Resource(name="metrics")
    public void setBean( MetricRegistry metrics ) {
        this.metrics = metrics;
        String metricName = this.getMetricName("IndexResultsCount");
        this.metrics.register(metricName, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return indexResultsCount;
            }
        });
    }

    public void init() {
        //Do Zenoss' default query to warm the cache on a thread, as not to delay startup
        class CacheWarmer implements Runnable {
            @Override
            public void run() {
                logger.info("Warming cache for {}", name);
                IndexSearcher searcher = null;
                try {
                    searcher = getSearcher();
                    EventFilter filter = EventFilter.newBuilder()
                            .addAllStatus(Lists.newArrayList(EventStatus.values()))
                            .addAllSeverity(Lists.newArrayList(EventSeverity.values()))
                            .build();
                    Query query = buildQuery(searcher.getIndexReader(), filter, null);
                    List<EventSort> sortList = new ArrayList<EventSort>(2);
                    sortList.add(EventSort.newBuilder().setField(EventSort.Field.SEVERITY).setDirection(Direction.DESCENDING).build());
                    sortList.add(EventSort.newBuilder().setField(EventSort.Field.LAST_SEEN).setDirection(Direction.DESCENDING).build());
                    Sort sort = buildSort(sortList);
                    logger.info("Warming cache for {}", name);
                    searchToEventSummaryResult(searcher, query, sort, Sets.newHashSet(FIELD_PROTOBUF), 0, 1000);
                    logger.info("Done warming cache for {}!", name);
                    ready = true;
                } catch (Exception e) {
                    logger.error("Failed to warm cache for {}", name);
                    e.printStackTrace();
                } finally {
                    try {
                        returnSearcher(searcher);
                    } catch (ZepException e) {
                        logger.error("Failed to return searcher");
                        e.printStackTrace();
                    }
                }
            }
        }
        Thread warmer = new Thread(new CacheWarmer());
        warmer.setName(name + " Cache Warmer Thread");
        warmer.start();
    }

    private synchronized void startReopenThread() {
        stopReopenThread();
        logger.debug("Starting NRT Reopen Thread");
        // Max = min on this because min only matters when you call
        // waitForGeneration() on the NRTManager, which we never do
        this.nrtManagerReopenThread = new ControlledRealTimeReopenThread<IndexSearcher>(this.trackingIndexWriter, this.searcherManager,
                this.readerReopenInterval, this.readerReopenInterval);
        this.nrtManagerReopenThread.setName(name + "NRT Reopen Thread");
        this.nrtManagerReopenThread.setPriority(Math.min(Thread.currentThread().getPriority() + 2, Thread.MAX_PRIORITY));
        this.nrtManagerReopenThread.setDaemon(true);
        this.nrtManagerReopenThread.start();
    }

    private synchronized void stopReopenThread() {
        if (this.nrtManagerReopenThread != null) {
            if (this.nrtManagerReopenThread.isAlive()) {
                logger.debug("Stopping NRT Reopen Thread");
                this.nrtManagerReopenThread.close();
            } else {
                this.nrtManagerReopenThread = null;
            }
        }
    }

    public synchronized void setReaderReopenInterval(int interval) {
        if (this.readerReopenInterval == interval) {
            return;
        } else if (this.readerReopenInterval != 0 && interval == 0) {
            this.readerReopenInterval = interval;
            stopReopenThread();
        } else {
            this.readerReopenInterval = interval;
            startReopenThread();
        }
    }

    public synchronized void close() {
        super.close();
        closeSearcherManager();
    }

    public void setLuceneSearchTimeout(int luceneSearchTimeout) {
        if (luceneSearchTimeout > 0) {
            this.luceneSearchTimeout = luceneSearchTimeout;
            logger.info("Lucene search timeout set to " + this.luceneSearchTimeout + " seconds.");
        }
    }

    @Override
    public boolean isReady() {
        return ready;
    }

    @Override
    public boolean ping() {
        return true;
    }

    @Override
    public long count() throws ZepException {
        return this.writer.numDocs();
    }

    @Override
    public long sizeInBytes() {
        try {
            Directory directory = writer.getDirectory();
            long size = 0L;
            for (String name : directory.listAll()) {
                size += directory.fileLength(name);
            }
            return size;
        } catch (IOException e) {
            logger.warn("Cannot get index size.");
            return -1L;
        }
    }

    private synchronized void closeSearcherManager() {
        stopReopenThread();
        try {
            this.searcherManager.close();
        } catch (IOException e) {
            logger.error("Unable to close SearcherManager: {}", e);
        }
    }

    public void setIndexDetailsConfiguration(IndexedDetailsConfiguration indexedDetailsConfiguration) {
        this.indexedDetailsConfiguration = indexedDetailsConfiguration;
    }

    /**
     * Sets the maximum number of results returned in a query from ZEP.
     *
     * @param limit Maximum number of results returned in a query from ZEP.
     */
    public void setQueryLimit(int limit) {
        if (limit > 0) {
            this.queryLimit = limit;
        } else {
            logger.warn("Invalid query limit: {}, using default: {}", limit, this.queryLimit);
        }
    }

    @Override
    public void index(EventSummary event) throws ZepException {
        Document doc = LuceneEventIndexMapper.fromEventSummary(
                event,
                indexedDetailsConfiguration.getEventDetailItemsByName(),
                this.archive);
        try {
            this.trackingIndexWriter.updateDocument(
                    new Term(FIELD_UUID, event.getUuid()),
                    doc);
        } catch (IOException e) {
            throw new ZepException(e);
        } catch (OutOfMemoryError e) {
            closeSearcherManager();
            throw e;
        }
    }

    @Override
    public void index(Collection<EventSummary> events) throws ZepException {
        for (EventSummary event : events)
            index(event);
    }

    @Override
    public void delete(String eventUuid) throws ZepException {
        try {
            this.trackingIndexWriter.deleteDocuments(new Term(FIELD_UUID, eventUuid));
        } catch (IOException e) {
            throw new ZepException(e);
        }
        logger.debug("Deleted eventUuid: {}", eventUuid);
    }

    @Override
    public void delete(Collection<String> eventUuids) throws ZepException {
        if (eventUuids.isEmpty()) return;
        try {
            Term[] terms = new Term[eventUuids.size()];
            int i = 0;
            for (String uuid : eventUuids) {
                terms[i] = new Term(FIELD_UUID, uuid);
                i++;
            }
            this.trackingIndexWriter.deleteDocuments(terms);
        } catch (IOException e) {
            throw new ZepException(e);
        } catch (OutOfMemoryError e) {
            closeSearcherManager();
            throw e;
        }
        logger.debug("Deleted eventUuids: {}", eventUuids);
    }

    @Override
    public void flush() throws ZepException {
        try {
            this.writer.commit();
        } catch (IOException e) {
            throw new ZepException(e);
        } catch (OutOfMemoryError e) {
            closeSearcherManager();
            throw e;
        }
        logger.debug("Flushed");
    }

    private IndexSearcher getSearcher() throws IOException {
        if (this.readerReopenInterval == 0) {
            this.searcherManager.maybeRefresh();
        }
        return this.searcherManager.acquire();
    }

    private void returnSearcher(IndexSearcher searcher) throws ZepException {
        try {
            if (searcher != null) this.searcherManager.release(searcher);
        } catch (IOException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }

    // Load the serialized protobuf (entire event)
    private static final ImmutableSet<String> PROTO_FIELDS = ImmutableSet.of(FIELD_PROTOBUF);

    // Load just the event UUID
    private static final ImmutableSet<String> UUID_FIELDS = ImmutableSet.of(FIELD_UUID);

    @Override
    public EventSummaryResult list(EventSummaryRequest request) throws ZepException {
        return listInternal(request, PROTO_FIELDS);
    }

    @Override
    public EventSummaryResult listUuids(EventSummaryRequest request) throws ZepException {
        return listInternal(request, UUID_FIELDS);
    }

    private EventSummaryResult listInternal(EventSummaryRequest request, Set<String> fieldsToLoad) throws ZepException {
        IndexSearcher searcher = null;
        long now = System.currentTimeMillis();
        Query query = null;
        try {
            searcher = getSearcher();
            query = buildQuery(searcher.getIndexReader(), request.getEventFilter(), request.getExclusionFilter());
            Sort sort = buildSort(request.getSortList());
            return searchToEventSummaryResult(searcher, query, sort, fieldsToLoad, request.getOffset(), request.getLimit());
        } catch (IOException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        } catch (OutOfMemoryError e) {
            closeSearcherManager();
            throw e;
        } finally {
            returnSearcher(searcher);
            if (query != null) {
                logger.debug("Query {} finished in {} milliseconds", query.toString(), System.currentTimeMillis() - now);
            }
        }
    }

    private TopDocs timeLimitedSearch(final IndexSearcher searcher, final Query query,
                                         final Sort sort, final int offset, final int limit, final int numDocs)
        throws ZepException {

        TopDocs docs;

        Callable search_call = new Callable<TopDocs>() {
            public TopDocs call() throws IOException, Exception {
                TopDocs tdocs;
                if (sort != null) {
                    logger.debug("Query: {}, Sort: {}, Offset: {}, Limit: {}", new Object[]{query, sort, offset, limit});
                    tdocs = searcher.search(query, null, numDocs, sort);
                } else {
                    logger.debug("Query: {}, Offset: {}, Limit: {}", new Object[]{query, offset, limit});
                    tdocs = searcher.search(query, null, numDocs);
                }
                return tdocs;
            }
        };

        try {
            if (this.luceneSearchTimeout > 0) {
                TimeLimiter limiter = new SimpleTimeLimiter();
                docs = (TopDocs)limiter.callWithTimeout(search_call, this.luceneSearchTimeout, TimeUnit.SECONDS, true);
            }
            else
                docs = (TopDocs)search_call.call();
        }
        catch (UncheckedTimeoutException e) {
            String msg = "Lucene search exceeded time limit ( " + this.luceneSearchTimeout + " seconds.)";
            if (sort != null)
                logger.warn(msg + "Query: {}, Sort: {}, Offset: {}, Limit: {}", new Object[]{query, sort, offset, limit});
            else
                logger.warn(msg + "Query: {}, Offset: {}, Limit: {}", new Object[]{query, offset, limit});
            throw new ZepException(msg + e.getLocalizedMessage(), e);
        }
        catch (OutOfMemoryError oome) {
            throw oome;
        }
        catch (Exception e) {
            logger.error("Exception performing timed search: ", e);
            throw new ZepException(e.getLocalizedMessage(), e);
        }

        return docs;
    }

    private EventSummaryResult searchToEventSummaryResult(IndexSearcher searcher, Query query, Sort sort,
                                                          Set<String> fieldsToLoad, int offset, int limit)
            throws IOException, ZepException {
        if (limit < 0) {
            throw new ZepException(messages.getMessage("invalid_query_limit", limit));
        }
        if (limit > queryLimit) {
            limit = queryLimit;
        }
        if (offset < 0) {
            offset = 0;
        }

        // Lucene doesn't like querying for 0 documents - search for at least one here
        final int numDocs = Math.max(limit + offset, 1);

        final TopDocs docs = this.timeLimitedSearch(searcher, query, sort, offset, limit, numDocs);

        this.indexResultsCount = docs.totalHits;
        logger.debug("Found {} results", docs.totalHits);
        EventSummaryResult.Builder result = EventSummaryResult.newBuilder();
        result.setTotal(docs.totalHits);
        result.setLimit(limit);
        if (docs.totalHits > limit + offset) {
            result.setNextOffset(limit + offset);
        }

        // Return the number of results they asked for (the query has to return at least one match
        // but the request may specified a limit of zero).
        final int lastDocument = Math.min(limit + offset, docs.scoreDocs.length);

        if (this.archive && !UUID_FIELDS.equals(fieldsToLoad)) {
            // Event archive only stores UUID & last_seen - have to query results from database
            Map<String, EventSummary> sortedResults = Maps.newLinkedHashMap();
            Set<EventSummary> toLookup = Sets.newHashSetWithExpectedSize(lastDocument);
            Set<String> uuidAndLastSeen = Sets.newHashSet(FIELD_UUID, FIELD_LAST_SEEN_TIME);
            for (int i = offset; i < lastDocument; i++) {
                Document doc = searcher.doc(docs.scoreDocs[i].doc, uuidAndLastSeen);
                EventSummary event = LuceneEventIndexMapper.toEventSummary(doc);
                sortedResults.put(event.getUuid(), null);
                toLookup.add(event);
            }

            if (!toLookup.isEmpty()) {
                final long beforeLookup = System.currentTimeMillis();
                logger.debug("Looking up {} events by UUID", toLookup.size());
                List<EventSummary> events = eventSummaryBaseDao.findByKey(toLookup);
                if (events.size() != toLookup.size()) {
                    logger.info("Event archive index out of sync - expected {} results, found {} results",
                            toLookup.size(), events.size());
                }
                for (EventSummary event : events)
                    sortedResults.put(event.getUuid(), event); // a re-insertion -- lucene sort is preserved.

                for (EventSummary event : sortedResults.values())
                    if (event != null)
                        result.addEvents(event);
                logger.debug("Query spent {} milliseconds to lookup {} events by UUID.",
                        System.currentTimeMillis() - beforeLookup, toLookup.size());
            } else {
                logger.debug("Query did not have to lookup any events by UUID");
            }

        } else {
            for (int i = offset; i < lastDocument; i++) {
                result.addEvents(LuceneEventIndexMapper.toEventSummary(searcher.doc(docs.scoreDocs[i].doc, fieldsToLoad)));
            }
        }
        return result.build();
    }

    @Override
    public EventSummary findByUuid(String uuid) throws ZepException {
        TermQuery query = new TermQuery(new Term(FIELD_UUID, uuid));
        EventSummary summary = null;
        IndexSearcher searcher = null;
        try {
            searcher = getSearcher();
            TopDocs docs = searcher.search(query, 1);
            if (docs.scoreDocs.length > 0) {
                // Not the most efficient way to search the archive, however this should give consistent results
                // with other queries of the index (only returning indexed documents).
                if (this.archive) {
                    Document doc = searcher.doc(docs.scoreDocs[0].doc, UUID_FIELDS);
                    summary = eventSummaryBaseDao.findByUuid(doc.get(FIELD_UUID));
                    if (summary == null) {
                        logger.info("Event archive index out of sync - expected event {} not found", uuid);
                    }
                } else {
                    summary = LuceneEventIndexMapper.toEventSummary(searcher.doc(docs.scoreDocs[0].doc));
                }
            }
        } catch (IOException e) {
            throw new ZepException(e);
        } catch (OutOfMemoryError e) {
            closeSearcherManager();
            throw e;
        } finally {
            returnSearcher(searcher);
        }
        return summary;
    }

    @Override
    public void purge(Date threshold) throws ZepException {
        IndexSearcher searcher = null;
        try {
            searcher = getSearcher();
            LuceneQueryBuilder query = new LuceneQueryBuilder(filterCacheManager,
                    searcher.getIndexReader(), this.indexedDetailsConfiguration);
            query.addRange(FIELD_LAST_SEEN_TIME, null, threshold.getTime());
            this.trackingIndexWriter.deleteDocuments(query.build());
            flush();
        } catch (IOException e) {
            throw new ZepException(e);
        } catch (OutOfMemoryError e) {
            closeSearcherManager();
            throw e;
        } finally {
            returnSearcher(searcher);
        }
    }

    @Override
    public void clear() throws ZepException {
        logger.debug("Deleting all events for: {}", name);
        try {
            this.trackingIndexWriter.deleteAll();
            flush();
        } catch (IOException e) {
            throw new ZepException(e);
        } catch (OutOfMemoryError e) {
            closeSearcherManager();
            throw e;
        }
    }

    private Sort buildSort(List<EventSort> sortList) throws ZepException {
        if (sortList.isEmpty()) {
            return null;
        }
        List<SortField> fields = new ArrayList<SortField>(sortList.size());
        for (EventSort sort : sortList) {
            fields.addAll(createSortField(sort));
        }
        return new Sort(fields.toArray(new SortField[fields.size()]));
    }

    private List<SortField> createSortField(EventSort sort) throws ZepException {
        final List<SortField> sortFields = new ArrayList<SortField>(2);
        boolean reverse = (sort.getDirection() == Direction.DESCENDING);

        FieldComparatorSource termOrdValcomparator = new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                return new FieldComparator.TermOrdValComparator(numHits, fieldname);
            }
        };

        switch (sort.getField()) {
            case COUNT:
                sortFields.add(new SortField(FIELD_COUNT, SortField.Type.INT, reverse));
                break;
            case ELEMENT_IDENTIFIER:
                sortFields.add(new SortField(FIELD_ELEMENT_IDENTIFIER_NOT_ANALYZED, termOrdValcomparator, reverse));
                break;
            case ELEMENT_SUB_IDENTIFIER:
                sortFields.add(new SortField(FIELD_ELEMENT_SUB_IDENTIFIER_NOT_ANALYZED, termOrdValcomparator, reverse));
                break;
            case ELEMENT_TITLE:
                sortFields.add(new SortField(FIELD_ELEMENT_TITLE_NOT_ANALYZED, termOrdValcomparator, reverse));
                break;
            case ELEMENT_SUB_TITLE:
                sortFields.add(new SortField(FIELD_ELEMENT_SUB_TITLE_NOT_ANALYZED, termOrdValcomparator, reverse));
                break;
            case EVENT_CLASS:
                sortFields.add(new SortField(FIELD_EVENT_CLASS_NOT_ANALYZED, SortField.Type.STRING, reverse));
                break;
            case EVENT_SUMMARY:
                sortFields.add(new SortField(FIELD_SUMMARY_NOT_ANALYZED, SortField.Type.STRING, reverse));
                break;
            case FIRST_SEEN:
                sortFields.add(new SortField(FIELD_FIRST_SEEN_TIME, SortField.Type.LONG, reverse));
                break;
            case LAST_SEEN:
                sortFields.add(new SortField(FIELD_LAST_SEEN_TIME, SortField.Type.LONG, reverse));
                break;
            case SEVERITY:
                sortFields.add(new SortField(FIELD_SEVERITY, SortField.Type.INT, reverse));
                break;
            case STATUS:
                sortFields.add(new SortField(FIELD_STATUS, SortField.Type.INT, reverse));
                break;
            case STATUS_CHANGE:
                sortFields.add(new SortField(FIELD_STATUS_CHANGE_TIME, SortField.Type.LONG, reverse));
                break;
            case UPDATE_TIME:
                sortFields.add(new SortField(FIELD_UPDATE_TIME, SortField.Type.LONG, reverse));
                break;
            case CURRENT_USER_NAME:
                sortFields.add(new SortField(FIELD_CURRENT_USER_NAME, SortField.Type.STRING, reverse));
                break;
            case AGENT:
                sortFields.add(new SortField(FIELD_AGENT, SortField.Type.STRING, reverse));
                break;
            case MONITOR:
                sortFields.add(new SortField(FIELD_MONITOR, SortField.Type.STRING, reverse));
                break;
            case EVENT_KEY:
                sortFields.add(new SortField(FIELD_EVENT_KEY, SortField.Type.STRING, reverse));
                break;
            case UUID:
                sortFields.add(new SortField(FIELD_UUID, SortField.Type.STRING, reverse));
                break;
            case FINGERPRINT:
                sortFields.add(new SortField(FIELD_FINGERPRINT, SortField.Type.STRING, reverse));
                break;
            case DETAIL:
                EventDetailItem item = indexedDetailsConfiguration.getEventDetailItemsByName().get(sort.getDetailKey());
                if (item == null) {
                    throw new IllegalArgumentException("Unknown event detail: " + sort.getDetailKey());
                }
                final String fieldName = LuceneEventIndexMapper.DETAIL_INDEX_PREFIX + sort.getDetailKey();
                switch (item.getType()) {
                    case DOUBLE:
                        sortFields.add(new SortField(fieldName, SortField.Type.DOUBLE, reverse));
                        break;
                    case INTEGER:
                        sortFields.add(new SortField(fieldName, SortField.Type.INT, reverse));
                        break;
                    case STRING:
                        sortFields.add(new SortField(fieldName, SortField.Type.STRING, reverse));
                        break;
                    case FLOAT:
                        sortFields.add(new SortField(fieldName, SortField.Type.FLOAT, reverse));
                        break;
                    case LONG:
                        sortFields.add(new SortField(fieldName, SortField.Type.LONG, reverse));
                        break;
                    case IP_ADDRESS:
                        // Sort IPv4 before IPv6
                        sortFields.add(new SortField(fieldName + IP_ADDRESS_TYPE_SUFFIX, SortField.Type.STRING, reverse));
                        sortFields.add(new SortField(fieldName + SORT_SUFFIX, SortField.Type.STRING, reverse));
                        break;
                    case PATH:
                        sortFields.add(new SortField(fieldName + SORT_SUFFIX, SortField.Type.STRING, reverse));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported detail type: " + item.getType());
                }
                break;
            case EVENT_CLASS_KEY:
                sortFields.add(new SortField(FIELD_EVENT_CLASS_KEY, SortField.Type.STRING, reverse));
                break;
            case EVENT_GROUP:
                sortFields.add(new SortField(FIELD_EVENT_GROUP, SortField.Type.STRING, reverse));
                break;
        }
        if (sortFields.isEmpty()) {
            throw new IllegalArgumentException("Unsupported sort field: " + sort.getField());
        }
        return sortFields;
    }

    private Query buildQuery(IndexReader reader, EventFilter filter, EventFilter exclusionFilter) throws ZepException {
        final BooleanQuery filterQuery = buildQueryFromFilter(reader, filter);
        final BooleanQuery exclusionQuery = buildQueryFromFilter(reader, exclusionFilter);
        final Query query;

        if (filterQuery == null && exclusionQuery == null) {
            query = new MatchAllDocsQuery();
        } else if (filterQuery != null) {
            if (exclusionQuery != null) {
                filterQuery.add(exclusionQuery, Occur.MUST_NOT);
            }
            query = filterQuery;
        } else {
            BooleanQuery bq = new BooleanQuery();
            bq.add(exclusionQuery, Occur.MUST_NOT);
            bq.add(new MatchAllDocsQuery(), Occur.MUST);
            query = bq;
        }
        logger.debug("Filter: {}, Exclusion filter: {}, Query: {}", new Object[]{filter, exclusionFilter, query});
        return query;
    }

    private BooleanQuery buildQueryFromFilter(IndexReader reader, EventFilter filter) throws ZepException {
        if (filter == null) {
            return null;
        }
        LuceneQueryBuilder qb = new LuceneQueryBuilder(filter.getOperator(), filterCacheManager,
                reader, indexedDetailsConfiguration);
        qb.addFilter(filter);
        return qb.build();
    }

    protected void searchEventTagSeverities(EventFilter filter, EventTagSeverityCounter counter) throws ZepException {
        final boolean hasTagsFilter = filter.getTagFilterCount() > 0;
        IndexSearcher searcher = null;
        try {
            searcher = getSearcher();
            final Query query = buildQueryFromFilter(searcher.getIndexReader(), filter);
            final OpenBitSet docs = new OpenBitSet(searcher.getIndexReader().maxDoc());
            searcher.search(query, new Collector() {
                private int docBase;

                @Override
                public void setScorer(Scorer scorer) throws IOException {
                }

                @Override
                public void collect(int doc) throws IOException {
                    docs.set(docBase + doc);
                }

                @Override
                public void setNextReader(AtomicReaderContext atomicReaderContext) throws IOException {
                    this.docBase = atomicReaderContext.docBase;
                }

                @Override
                public boolean acceptsDocsOutOfOrder() {
                    return true;
                }
            });
            int docId;
            final DocIdSetIterator it = docs.iterator();
            while ((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                final EventSummary summary;
                if (this.archive) {
                    // TODO: This isn't very cheap - would be better to batch by UUID in separate calls
                    // This doesn't get called on the event archive right now, so leave it until need to optimize.
                    Document doc = searcher.doc(docId, UUID_FIELDS);
                    summary = this.eventSummaryBaseDao.findByUuid(doc.get(FIELD_UUID));
                } else {
                    Document doc = searcher.doc(docId);
                    // this is an optimization for getting the non-archived tags from an organizer for ticket
                    // see ZEN-7239. For this ticket we updated the index to store what we needed for generating the
                    // tags severities. Since we do not want a migrate of completely deleting the index this
                    // method is backwards compatible by uncompressing the protobuf
                    if (doc.get(FIELD_SEVERITY) != null) {
                        int count = Integer.parseInt(doc.get(FIELD_COUNT));
                        boolean acknowledged = EventStatus.STATUS_ACKNOWLEDGED.equals(EventStatus.valueOf(Integer.parseInt(doc.get(FIELD_STATUS))));
                        EventSeverity severity = EventSeverity.valueOf(Integer.parseInt(doc.get(FIELD_SEVERITY)));

                        // get the map for each filter and update the count
                        for (String tag : doc.getValues(FIELD_TAGS))
                            counter.update(tag, severity, count, acknowledged);
                        continue;
                    } else {
                        summary = LuceneEventIndexMapper.toEventSummary(doc);
                    }
                }
                boolean acknowledged = EventStatus.STATUS_ACKNOWLEDGED == summary.getStatus();
                Event occurrence = summary.getOccurrence(0);
                EventSeverity severity = occurrence.getSeverity();
                int count = occurrence.getCount();
                EventActor actor = occurrence.getActor();

                // Build tags from element_uuids - no tags specified in filter
                if (!hasTagsFilter) {
                    if (actor.hasElementUuid())
                        counter.update(actor.getElementUuid(), severity, count, acknowledged);
                }
                // Build tag severities from passed in filter
                else {
                    for (String uuid : Arrays.asList(actor.getElementUuid(), actor.getElementSubUuid()))
                        counter.update(uuid, severity, count, acknowledged);
                    for (EventTag tag : occurrence.getTagsList())
                        for (String tagUuid : tag.getUuidList())
                            counter.update(tagUuid, severity, count, acknowledged);
                }
            }
        } catch (IOException e) {
            throw new ZepException(e);
        } catch (OutOfMemoryError e) {
            closeSearcherManager();
            throw e;
        } finally {
            returnSearcher(searcher);
        }
    }

    private class Processor implements SavedSearchProcessor<LuceneSavedSearch> {
        private final Set<String> fieldsToLoad;
        public Processor(Set<String> fieldsToLoad) {
            this.fieldsToLoad = fieldsToLoad;
        }
        @Override
        public EventSummaryResult result(LuceneSavedSearch search, int offset, int limit) throws ZepException {
            IndexReader reader = search.getReader();
            reader.incRef();
            try {
                IndexSearcher searcher = new IndexSearcher(reader);
                return searchToEventSummaryResult(searcher, search.getQuery(), search.getSort(), fieldsToLoad, offset, limit);
            } catch (IOException e) {
                try {
                    reader.decRef();
                } catch (IOException ex) {
                    logger.warn("Exception decrementing reference count", ex);
                }
                throw new ZepException(e);
            }
        }
    }

    private final Processor searchProcessor = new Processor(PROTO_FIELDS);
    private final Processor searchUuidsProcessor = new Processor(UUID_FIELDS);

    @Override
    public SavedSearchProcessor<LuceneSavedSearch> savedSearchProcessor() {
        return this.searchProcessor;
    }

    @Override
    public SavedSearchProcessor<LuceneSavedSearch> savedSearchUuidsProcessor() {
        return this.searchUuidsProcessor;
    }

    @Override
    public LuceneSavedSearch buildSavedSearch(String uuid, EventQuery eventQuery) throws ZepException {
        if (eventQuery.getTimeout() < 1)
            throw new ZepException("Invalid timeout: " + eventQuery.getTimeout());

        IndexReader reader;
        try {
            reader = DirectoryReader.open(writer, false);
        } catch (IOException e) {
            String msg = "Unable to get Lucene reader";
            logger.warn(msg, e);
            throw new ZepException(msg, e);
        }
        try {
            final Query query = buildQuery(reader, eventQuery.getEventFilter(), eventQuery.getExclusionFilter());
            final Sort sort = buildSort(eventQuery.getSortList());
            return new LuceneSavedSearch(uuid, reader, query, sort, eventQuery.getTimeout());
        } catch (Exception e) {
            try {
                reader.decRef();
            } catch (IOException ex) {
                logger.warn("Exception decrementing reference count", ex);
            }
            if (e instanceof ZepException) {
                throw (ZepException) e;
            }
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }
}
