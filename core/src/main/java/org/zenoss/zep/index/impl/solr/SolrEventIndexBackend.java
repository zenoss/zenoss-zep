/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010, 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.index.impl.solr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.zenoss.protobufs.zep.Zep.*;
import org.zenoss.protobufs.zep.Zep.EventSort.Field;
import org.zenoss.zep.Messages;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventSummaryBaseDao;
import org.zenoss.zep.index.IndexedDetailsConfiguration;
import org.zenoss.zep.index.SavedSearchProcessor;
import org.zenoss.zep.index.impl.BaseEventIndexBackend;
import org.zenoss.zep.index.impl.IndexConstants;
import org.zenoss.zep.utils.AtomicThresholdCounter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.zenoss.zep.index.impl.IndexConstants.*;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_EVENT_GROUP;

public class SolrEventIndexBackend extends BaseEventIndexBackend<SolrSavedSearch> {

    public static final int MAX_RESULTS = 100;
    private static Logger logger = LoggerFactory.getLogger(SolrEventIndexBackend.class);

    private static final Map<Field, String> SORT_MAP;
    static {
        Map<Field, String> m = Maps.newEnumMap(Field.class);
        m.put(Field.COUNT, FIELD_COUNT);
        m.put(Field.ELEMENT_IDENTIFIER, FIELD_ELEMENT_IDENTIFIER_NOT_ANALYZED);
        m.put(Field.ELEMENT_SUB_IDENTIFIER, FIELD_ELEMENT_SUB_IDENTIFIER_NOT_ANALYZED);
        m.put(Field.ELEMENT_TITLE, FIELD_ELEMENT_TITLE_NOT_ANALYZED);
        m.put(Field.ELEMENT_SUB_TITLE, FIELD_ELEMENT_SUB_TITLE_NOT_ANALYZED);
        m.put(Field.EVENT_CLASS, FIELD_EVENT_CLASS_NOT_ANALYZED);
        m.put(Field.EVENT_SUMMARY, FIELD_SUMMARY_NOT_ANALYZED);
        m.put(Field.FIRST_SEEN, FIELD_FIRST_SEEN_TIME);
        m.put(Field.LAST_SEEN, FIELD_LAST_SEEN_TIME);
        m.put(Field.SEVERITY, FIELD_SEVERITY);
        m.put(Field.STATUS, FIELD_STATUS);
        m.put(Field.STATUS_CHANGE, FIELD_STATUS_CHANGE_TIME);
        m.put(Field.UPDATE_TIME, FIELD_UPDATE_TIME);
        m.put(Field.CURRENT_USER_NAME, FIELD_CURRENT_USER_NAME);
        m.put(Field.AGENT, FIELD_AGENT);
        m.put(Field.MONITOR, FIELD_MONITOR);
        m.put(Field.EVENT_KEY, FIELD_EVENT_KEY);
        m.put(Field.UUID, FIELD_UUID);
        m.put(Field.FINGERPRINT, FIELD_FINGERPRINT);
        m.put(Field.EVENT_CLASS_KEY, FIELD_EVENT_CLASS_KEY);
        m.put(Field.EVENT_GROUP, FIELD_EVENT_GROUP);
        SORT_MAP = Collections.unmodifiableMap(m);
    }

    public final String name;
    private boolean ready = false;
    private int optimizeThreshold = -1; // disabled, for now.
    private OptimizeThread optimizeThread = null;
    private final AtomicThresholdCounter eventsSinceOptimize = new AtomicThresholdCounter(0);
    private final EventArchiveDao archiveDao;
    private final IndexedDetailsConfiguration indexedDetailsConfiguration;
    private final SolrServer queryServer;
    private final int shards;
    private final int replicationFactor;
    private final int maxShardsPerNode;
    private final int concurrentUploadQueueSize;
    private final int concurrentThreads;
    private SolrServer updateServer;

    public SolrEventIndexBackend(String name, String server, IndexedDetailsConfiguration indexedDetailsConfiguration,
                                 EventArchiveDao archiveDao, int shards, int replicationFactor, int maxShardsPerNode,
                                 int concurrentUploadQueueSize, int concurrentThreads,
                                 Messages messages, TaskScheduler scheduler, UUIDGenerator uuidGenerator) {
        super(messages, scheduler, uuidGenerator);
        this.name = name;
        this.indexedDetailsConfiguration = indexedDetailsConfiguration;
        this.archiveDao = archiveDao;
        this.shards = shards;
        this.replicationFactor = replicationFactor;
        this.maxShardsPerNode = maxShardsPerNode;
        this.concurrentUploadQueueSize = concurrentUploadQueueSize;
        this.concurrentThreads = concurrentThreads;
        if (server.toLowerCase().startsWith("http://") || server.toLowerCase().startsWith("https://")) {
            this.queryServer = new HttpSolrServer(server);
        } else {
            CloudSolrServer cloudServer = new CloudSolrServer(server);
            cloudServer.setIdField("uuid");
            this.updateServer = this.queryServer = cloudServer;
        }
    }

    public static class CollectionListAdminRequest extends CollectionAdminRequest {
        @Override
        public SolrParams getParams() {
            ModifiableSolrParams params = new ModifiableSolrParams();
            params.set(CoreAdminParams.ACTION, "LIST");
            return params;
        }
    }

    public synchronized boolean initializeSolr() {
        try {
            CollectionAdminResponse response = new CollectionListAdminRequest().process(queryServer);
            Set<String> collections = Sets.newHashSet((Collection<String>) response.getResponse().get("collections"));
            if (!collections.contains(name)) {
                response = CollectionAdminRequest.createCollection(name, shards, replicationFactor, maxShardsPerNode, null, "zenoss_events", "uuid", queryServer);
                if (!response.isSuccess()) {
                    logger.error("Failed to initialize Solr: " + response.toString());
                    return false;
                }
            }
            if (queryServer instanceof HttpSolrServer) {
                String baseUrl = ((HttpSolrServer) queryServer).getBaseURL();
                String newBaseUrl = baseUrl.matches("/zenoss_events/?$") ? baseUrl : baseUrl.replaceAll("/*$", "/zenoss_events/");
                ((HttpSolrServer) queryServer).setBaseURL(baseUrl.replaceAll("/*$", "/zenoss_events/"));
                updateServer = new ConcurrentUpdateSolrServer(newBaseUrl, concurrentUploadQueueSize, concurrentThreads);
            } else if (queryServer instanceof CloudSolrServer) {
                ((CloudSolrServer) queryServer).setDefaultCollection("zenoss_events");
            }
            this.ready = true;
            return true;
        } catch (SolrException e) {
            logger.error("Failed to initialize Solr: " + e.getMessage(), e);
            return false;
        } catch (SolrServerException e) {
            logger.error("Failed to initialize Solr: " + e.getMessage(), e);
            return false;
        } catch (IOException e) {
            logger.error("Failed to initialize Solr: " + e.getMessage(), e);
            return false;
        }
    }

    private class OptimizeThread extends Thread {
        private volatile boolean shutdown = false;
        public OptimizeThread() {
            setName(SolrEventIndexBackend.class.getName() + " optimize thread");
            setPriority(Math.min(Thread.currentThread().getPriority() + 2, Thread.MAX_PRIORITY));
            setDaemon(true);
        }

        public void run() {
            boolean interrupted = false;
            try {
                while (!shutdown) {
                    try {
                        final int threshold = optimizeThreshold;
                        if (threshold <= 0 || !ready)
                            Thread.sleep(100);
                        else if (eventsSinceOptimize.awaitAndReset(threshold, 100, TimeUnit.MILLISECONDS))
                            queryServer.optimize();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    } catch (IOException e) {
                        logger.warn("exception while optimizing: " + e.getMessage(), e);
                    } catch (SolrServerException e) {
                        logger.warn("exception while optimizing: " + e.getMessage(), e);
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public boolean isDown() {
            return shutdown || !this.isAlive();
        }

        public void close() {
            shutdown = true;
        }
    }

    private synchronized void startOptimizeThread() {
        stopOptimizeThread();
        if (optimizeThreshold > 0) {
            logger.debug("Starting %s optimize thread", getClass().getName());
            optimizeThread = new OptimizeThread();
            optimizeThread.start();
        }
    }

    private synchronized void stopOptimizeThread() {
        if (optimizeThread != null) {
            if (optimizeThread.isAlive()) {
                logger.debug("Stopping %s optimize thread", getClass().getName());
                optimizeThread.close();
            }
            optimizeThread = null;
        }
    }

    public synchronized void setOptimizeThreshold(final int threshold) {
        final int old = optimizeThreshold;
        if (old != threshold) {
            optimizeThreshold = threshold;
            if (threshold <= 0)
                stopOptimizeThread();
            else if (old <= 0 || optimizeThread == null || optimizeThread.isDown())
                startOptimizeThread();
        }
    }

    private class SolrInitializationThread extends Thread {
        @Override
        public void run() {
            try {
                while (!initializeSolr()) {
                    logger.info("Could not initialize solr backend.");
                    //TODO implement exponential backoff or something more sophisticated
                    sleep(1000);
                }
                logger.info("Solr Indexing enabled");
                startOptimizeThread();
            }
            catch (InterruptedException e) {
                // ignore it
            }
        }
    }

    public synchronized boolean start() {
        SolrInitializationThread initThread = new SolrInitializationThread();
        initThread.start();
        return this.ready;
    }

    public synchronized void shutdown() {
        stopOptimizeThread();
        this.queryServer.shutdown();
        if (this.updateServer != null)
            this.updateServer.shutdown();
    }

    @Override
    public boolean isReady() {
        return ready;
    }

    @Override
    public boolean ping() {
        boolean pingable = false;
        if (ready) {
            try {
                this.queryServer.ping();
                pingable = true;
            }
            catch (SolrServerException e) {
                logger.debug(e.getMessage(), e);
            }
            catch (SolrException e) {
                logger.debug(e.getMessage(), e);
            }
            catch (IOException e) {
                logger.debug(e.getMessage(), e);
            }
        }
        return pingable;
    }

    private void assertReady() {
        if (!ready) throw new IllegalStateException("Solr failed to initialize");
    }

    private boolean checkReady() {
        if (!ready && logger.isDebugEnabled())
            logger.debug("Request ignored. Solr failed to initialize.");
        return ready;
    }

    @Override
    public long count() throws ZepException {
        assertReady();
        try {
            SolrQuery query = new SolrQuery().setQuery("*:*").setRows(0);
            return queryServer.query(query).getResults().getNumFound();
        } catch (SolrServerException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public long sizeInBytes() throws UnsupportedOperationException {
        assertReady();
        throw new UnsupportedOperationException();
    }

    @Override
    public void index(EventSummary event) throws ZepException {
        if (!checkReady()) return;
        SolrInputDocument doc = SolrEventIndexMapper.fromEventSummary(event, indexedDetailsConfiguration.getEventDetailItemsByName());
        logger.debug("Indexing {}", event.getUuid());
        try {
            updateServer.add(doc);
            eventsSinceOptimize.increment();
        } catch (IOException e) {
            throw new ZepException(e);
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } catch (SolrException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public void index(Collection<EventSummary> events) throws ZepException {
        if (!checkReady()) return;
        List<SolrInputDocument> docs = Lists.newArrayListWithExpectedSize(events.size());
        for (EventSummary event : events) {
            docs.add(SolrEventIndexMapper.fromEventSummary(event, indexedDetailsConfiguration.getEventDetailItemsByName()));
        }
        try {
            updateServer.add(docs);
            eventsSinceOptimize.increment(docs.size());
        } catch (SolrException e) {
            throw new ZepException(e);
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public void delete(String eventUuid) throws ZepException {
        if (!checkReady()) return;
        try {
            updateServer.deleteById(eventUuid);
            eventsSinceOptimize.increment();
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public void delete(Collection<String> eventUuids) throws ZepException {
        if (!checkReady()) return;
        try {
            if (eventUuids instanceof List) {
                updateServer.deleteById((List<String>)eventUuids);
            } else {
                updateServer.deleteById(Lists.newArrayList(eventUuids));
            }
            eventsSinceOptimize.increment(eventUuids.size());
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public void flush() throws ZepException {
        if (!checkReady()) return;
        try {
            updateServer.commit(true, true, true);
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public void purge(Date threshold) throws ZepException {
        if (!checkReady()) return;
        SolrQueryBuilder query = new SolrQueryBuilder(indexedDetailsConfiguration);
        query.addRange(IndexConstants.FIELD_LAST_SEEN_TIME, null, threshold.getTime());
        logger.info("Purging events older than {}", threshold);
        logger.debug("Solr query: {}", query);
        try {
            updateServer.deleteByQuery(query.build());
            eventsSinceOptimize.incrementTo(optimizeThreshold);
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public void clear() throws ZepException {
        if (!checkReady()) return;
        try {
            updateServer.deleteByQuery("*:*");
            flush();
            eventsSinceOptimize.incrementTo(optimizeThreshold);
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public EventSummaryResult list(EventSummaryRequest request) throws ZepException {
        assertReady();
        List<EventSort> sorts = request.getSortCount() == 0 ? Collections.<EventSort>emptyList() : request.getSortList();
        SolrQuery solrQuery = buildSolrQuery(request.getEventFilter(),
                                             request.getExclusionFilter(),
                                             request.hasLimit() ? request.getLimit() : null,
                                             request.hasOffset() ? request.getOffset() : null,
                                             sorts,
                                             SolrFieldFilter.UUID_LAST_SEEN_AND_PROTOBUF);
        return execute(solrQuery, false);
    }

    @Override
    public EventSummaryResult listUuids(EventSummaryRequest request) throws ZepException {
        assertReady();
        List<EventSort> sorts = request.getSortCount() == 0 ? Collections.<EventSort>emptyList() : request.getSortList();
        SolrQuery solrQuery = buildSolrQuery(request.getEventFilter(),
                request.getExclusionFilter(),
                request.hasLimit() ? request.getLimit() : null,
                request.hasOffset() ? request.getOffset() : null,
                sorts,
                SolrFieldFilter.JUST_UUID);
        return execute(solrQuery, true);
    }

    private EventSummaryResult execute(final SolrQuery query, boolean justUuids) throws ZepException {
        EventSummaryResult.Builder result = EventSummaryResult.newBuilder();

        logger.debug("Searching SOLR for events matching: {}", query.getQuery());
        final long now = logger.isDebugEnabled() ? System.currentTimeMillis() : 0;
        final QueryResponse response;
        try {
            response = queryServer.query(query);
            final int numFound = (int) response.getResults().getNumFound();
            result.setTotal(numFound);
            if (query.getRows() != null) {
                final int limit = query.getRows();
                final int offset = (query.getStart() == null) ? 0 : query.getStart() ;
                result.setLimit(limit);
                if (numFound > offset + limit)
                    result.setNextOffset(offset + limit);
            }

            logger.debug("Found {} results in SOLR", numFound);

            if (justUuids) {
                for (SolrDocument doc : response.getResults())
                    result.addEvents(SolrEventIndexMapper.toEventSummary(doc));
            } else {
                Map<String, EventSummary> sortedResults = Maps.newLinkedHashMap();
                Set<EventSummary> toLookup = Sets.newHashSet();
                for (SolrDocument doc : response.getResults()) {
                    EventSummary event = SolrEventIndexMapper.toEventSummary(doc);
                    if (event.hasFirstSeenTime()) {
                        sortedResults.put(event.getUuid(), event);
                    } else {
                        // We only store keys in the index for archived events. This must be one of those.
                        // Set a place-holder now. We'll find it in the database shortly.
                        sortedResults.put(event.getUuid(), null);
                        toLookup.add(event);
                    }
                }
                if (!toLookup.isEmpty()) {
                    final long beforeLookup = System.currentTimeMillis();
                    logger.debug("Looking up {} events by UUID", toLookup.size());
                    List<EventSummary> events = archiveDao.findByKey(toLookup);
                    if (events.size() != toLookup.size()) {
                        int missing = toLookup.size() - events.size();
                        logger.info("Event archive index out of sync - {} of {} event UUIDs are in Solr index, but not in database.", missing, toLookup.size());
                    }
                    for (EventSummary event : events)
                        sortedResults.put(event.getUuid(), event); // a re-insertion -- solr sort is preserved.
                    logger.debug("Query spent {} milliseconds to lookup {} events by UUID.",
                            System.currentTimeMillis() - beforeLookup, toLookup.size());
                } else {
                    logger.debug("Query did not have to lookup any events by UUID");
                }
                for (EventSummary event : sortedResults.values())
                    if (event != null)
                        result.addEvents(event);
            }
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } finally {
            if (logger.isDebugEnabled())
                logger.debug("Query {} finished in {} milliseconds", query, System.currentTimeMillis() - now);
        }
        return result.build();
    }

    @Override
    public EventSummary findByUuid(String uuid) throws ZepException {
        assertReady();
        SolrQuery solrQuery = new SolrQuery().
                setQuery(IndexConstants.FIELD_UUID + ':' + uuid).
                setFields(IndexConstants.FIELD_UUID, IndexConstants.FIELD_PROTOBUF).
                setRows(1);

        SolrDocumentList docs;

        try {
            QueryResponse response = queryServer.query(solrQuery);
            docs = response.getResults();
        } catch (SolrServerException e) {
            throw new ZepException(e);
        }

        if ( docs.size() > 0) {
            return SolrEventIndexMapper.toEventSummary(docs.get(0));
        }
        else {
            return null;
        }
    }

    @Override
    public SavedSearchProcessor<SolrSavedSearch> savedSearchProcessor() {
        assertReady();
        return new SavedSearchProcessor<SolrSavedSearch>(){
            @Override
            public EventSummaryResult result(SolrSavedSearch search, int offset, int limit) throws ZepException {
                SolrQuery query = search.getSolrQuery().getCopy();
                query.setRows(limit);
                query.setStart(offset);
                query.setFields(IndexConstants.FIELD_UUID, IndexConstants.FIELD_PROTOBUF);
                return execute(query, false);
            }
        };
    }

    @Override
    public SavedSearchProcessor<SolrSavedSearch> savedSearchUuidsProcessor() {
        assertReady();
        return new SavedSearchProcessor<SolrSavedSearch>() {
            @Override
            public EventSummaryResult result(SolrSavedSearch search, int offset, int limit) throws ZepException {
                SolrQuery query = search.getSolrQuery().getCopy();
                query.setRows(limit);
                query.setStart(offset);
                query.setFields(IndexConstants.FIELD_UUID);
                return execute(query, true);
            }
        };
    }

    @Override
    public SolrSavedSearch buildSavedSearch(String uuid, EventQuery query) throws ZepException {
        assertReady();
        List<EventSort> sorts = query.getSortCount() == 0 ? Collections.<EventSort>emptyList() : query.getSortList();
        SolrQuery solrQuery = buildSolrQuery(query.getEventFilter(),
                query.getExclusionFilter(),
                null, null, sorts, null);
        return new SolrSavedSearch(uuid, query.getTimeout(), solrQuery);
    }

    private SolrQuery buildSolrQuery(EventFilter filter, EventFilter exclusionFilter,
                                     Integer limit, Integer offset,
                                     List<EventSort> sortList, SolrFieldFilter fieldFilter)
            throws ZepException {
        final String query = buildQuery(filter, exclusionFilter);
        SolrQuery solrQuery = new SolrQuery().setQuery(query);

        if (limit != null && limit < MAX_RESULTS && limit > 0)
            solrQuery.setRows(limit);
        else
            solrQuery.setRows(MAX_RESULTS);

        if (offset != null)
            solrQuery.setStart(offset);

        if (sortList == null)
            solrQuery.clearSorts();
        else if (sortList.isEmpty())
            solrQuery.addSort(SortClause.desc(IndexConstants.FIELD_LAST_SEEN_TIME));
        else
            for (EventSort sort : sortList)
                for (SortClause clause : createSortClauses(sort))
                    solrQuery.addSort(clause);

        if (fieldFilter != null) {
            switch (fieldFilter) {
                case DEFAULTS:
                    break;
                case JUST_UUID:
                    solrQuery.setFields(IndexConstants.FIELD_UUID);
                    break;
                case UUID_LAST_SEEN_AND_PROTOBUF:
                    solrQuery.setFields(
                            IndexConstants.FIELD_UUID,
                            IndexConstants.FIELD_LAST_SEEN_TIME,
                            IndexConstants.FIELD_PROTOBUF);
                    break;
                case SEARCH_EVENT_TAG_SEVERITIES:
                    solrQuery.setFields(
                            IndexConstants.FIELD_ELEMENT_IDENTIFIER,
                            IndexConstants.FIELD_ELEMENT_SUB_IDENTIFIER,
                            IndexConstants.FIELD_SEVERITY,
                            IndexConstants.FIELD_STATUS,
                            IndexConstants.FIELD_TAGS,
                            IndexConstants.FIELD_COUNT);
                    break;
                default:
                    throw new IllegalStateException("Unexpected fieldFilter: " + fieldFilter);
            }
        }
        solrQuery.setIncludeScore(false);
        solrQuery.setHighlight(false);
        solrQuery.setTerms(false);
        solrQuery.setFacet(false);

        return solrQuery;
    }

    private String buildQuery(EventFilter filter, EventFilter exclusionFilter) throws ZepException {
        final StringBuilder sb = new StringBuilder();
        if (filter == null)
            sb.append("*:*");
        else
            sb.append("(").append(buildQueryFromFilter(filter)).append(")");

        if (exclusionFilter != null) {
            String exclusionQuery = buildQueryFromFilter(exclusionFilter);
            if (exclusionQuery != null && !exclusionQuery.isEmpty())
                sb.append(" AND NOT (").append(exclusionQuery).append(")");
        }

        final String query = sb.toString();
        logger.debug("Filter: {}, Exclusion filter: {}, Query: {}", new Object[]{filter, exclusionFilter, query});
        return query;
    }

    private String buildQueryFromFilter(EventFilter filter) throws ZepException {
        if (filter == null) return null;
        SolrQueryBuilder qb = new SolrQueryBuilder(indexedDetailsConfiguration);
        qb.addFilter(filter);
        return qb.build();
    }

    @Override
    protected void searchEventTagSeverities(EventFilter filter, final EventTagSeverityCounter counter) throws ZepException {
        assertReady();
        if (filter.getTagFilterCount() == 0) {
            SolrQuery solrQuery = buildSolrQuery(filter, null, null, null, null, SolrFieldFilter.DEFAULTS);
            solrQuery.setRows(0);
            solrQuery.setFields();
            solrQuery.setFacet(true);
            solrQuery.setFacetMinCount(1);
            solrQuery.setFacetLimit(-1);
            solrQuery.addFacetPivotField(
                    IndexConstants.FIELD_ELEMENT_IDENTIFIER,
                    IndexConstants.FIELD_SEVERITY,
                    IndexConstants.FIELD_STATUS,
                    IndexConstants.FIELD_COUNT);
            try {
                QueryResponse response = queryServer.query(solrQuery);
                for (PivotField pivotElementId : response.getFacetPivot().getVal(0)) {
                    final String uuid = (String) pivotElementId.getValue();
                    for (PivotField pivotSeverity : pivotElementId.getPivot()) {
                        final EventSeverity severity = EventSeverity.valueOf(Integer.parseInt((String) pivotSeverity.getValue()));
                        for (PivotField pivotStatus : pivotSeverity.getPivot()) {
                            final EventStatus status = EventStatus.valueOf(Integer.parseInt((String) pivotStatus.getValue()));
                            final boolean acknowledged = EventStatus.STATUS_ACKNOWLEDGED.equals(status);
                            for (PivotField pivotCount : pivotStatus.getPivot()) {
                                final Integer count = pivotCount.getCount() * (Integer) pivotCount.getValue();
                                counter.update(uuid, severity, count, acknowledged);
                            }
                        }
                    }
                }
            } catch (SolrServerException e) {
                throw new ZepException(e);
            }
        } else {
            SolrQuery solrQuery = buildSolrQuery(filter, null, null, null, null, SolrFieldFilter.SEARCH_EVENT_TAG_SEVERITIES);
            try {
                queryServer.queryAndStreamResponse(solrQuery, new StreamingResponseCallback() {
                    @Override
                    public void streamSolrDocument(SolrDocument doc) {
                        final EventSeverity severity = EventSeverity.valueOf(Integer.parseInt((String) doc.getFieldValue(FIELD_SEVERITY)));
                        final EventStatus status = EventStatus.valueOf(Integer.parseInt((String) doc.getFieldValue(FIELD_STATUS)));
                        final boolean acknowledged = EventStatus.STATUS_ACKNOWLEDGED.equals(status);
                        final int count = Integer.parseInt((String) doc.getFieldValue(FIELD_COUNT));
                        for (String fieldName : new String[]{FIELD_ELEMENT_IDENTIFIER, FIELD_ELEMENT_SUB_IDENTIFIER}) {
                            final String uuid = (String) doc.getFieldValue(fieldName);
                            counter.update(uuid, severity, count, acknowledged);
                        }
                        for (Object tag : doc.getFieldValues(FIELD_TAGS)) {
                            counter.update((String)tag, severity, count, acknowledged);
                        }
                    }

                    @Override
                    public void streamDocListInfo(long numFound, long start, Float maxScore) {
                        // ignored
                    }
                });
            } catch (SolrServerException e) {
                throw new ZepException(e);
            } catch (IOException e) {
                throw new ZepException(e);
            }
        }
    }

    private List<SortClause> createSortClauses(EventSort sort) throws ZepException {
        SolrQuery.ORDER order = (sort.getDirection() == EventSort.Direction.ASCENDING) ? ORDER.asc : ORDER.desc;
        Field field = sort.getField();
        if (field == Field.DETAIL) {
            EventDetailItem item = indexedDetailsConfiguration.getEventDetailItemsByName().get(sort.getDetailKey());
            if (item == null) {
                throw new IllegalArgumentException("Unknown event detail: " + sort.getDetailKey());
            }
            final String fieldName = SolrEventIndexMapper.DETAIL_INDEX_PREFIX + sort.getDetailKey();
            switch (item.getType()) {
                case STRING:
                    return Lists.newArrayList(SortClause.create(fieldName + "_s_sort", order));
                case DOUBLE:
                    return Lists.newArrayList(SortClause.create(fieldName + "_d", order));
                case INTEGER:
                    return Lists.newArrayList(SortClause.create(fieldName + "_i", order));
                case FLOAT:
                    return Lists.newArrayList(SortClause.create(fieldName + "_f", order));
                case LONG:
                    return Lists.newArrayList(SortClause.create(fieldName + "_l", order));
                case IP_ADDRESS:
                    return Lists.newArrayList(
                            SortClause.create(fieldName + "_ip_type", order),
                            SortClause.create(fieldName + "_ip_sort", order));
                case PATH:
                    //ignored, because some path fields (zenoss.device.group) are multi-valued.
                    return Collections.emptyList();
                default:
                    throw new IllegalArgumentException("Unsupported detail type: " + item.getType());
            }
        } else {
            String sortField = SORT_MAP.get(field);
            if (sortField == null)
                throw new IllegalArgumentException("Unsupported sort field: " + field);
            return Lists.newArrayList(SortClause.create(sortField, order));
        }
    }
}
