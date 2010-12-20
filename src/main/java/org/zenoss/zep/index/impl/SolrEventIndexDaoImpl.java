/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.index.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.util.Util.TimestampRange;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventSort;
import org.zenoss.protobufs.zep.Zep.EventSort.Direction;
import org.zenoss.protobufs.zep.Zep.EventSort.Field;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryFilter;
import org.zenoss.protobufs.zep.Zep.EventSummaryRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryResult;
import org.zenoss.protobufs.zep.Zep.FilterOperator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.index.EventIndexDao;

public class SolrEventIndexDaoImpl implements EventIndexDao {
    private final SolrServer server;
    private final String name;

    public static final int MAX_RESULTS = 100;
    public static final int OPTIMIZE_AT_NUM_EVENTS = 5000;
    private AtomicInteger eventsSinceOptimize = new AtomicInteger(0);

    private static Logger logger = LoggerFactory
            .getLogger(SolrEventIndexDaoImpl.class);


    private static final Map<Field, String> SORT_MAP = new EnumMap<Field, String>(Field.class);
    private static final Map<Direction, SolrQuery.ORDER> SORT_DIRECTION_MAP = new EnumMap<Direction, SolrQuery.ORDER>(Direction.class);
    private static final Map<String,String> PROTOBUF_TO_INDEX_MAP = new HashMap<String, String>();
    
    static {
        SORT_MAP.put(Field.SEVERITY, SolrEventIndexItem.FIELD_EVENT_SEVERITY);
        SORT_MAP.put(Field.STATUS, SolrEventIndexItem.FIELD_STATUS);
        SORT_MAP.put(Field.EVENT_CLASS, SolrEventIndexItem.FIELD_EVENT_EVENT_CLASS_INDEX);
        SORT_MAP.put(Field.FIRST_SEEN, SolrEventIndexItem.FIELD_FIRST_SEEN_TIME);
        SORT_MAP.put(Field.LAST_SEEN, SolrEventIndexItem.FIELD_LAST_SEEN_TIME);
        SORT_MAP.put(Field.STATUS_CHANGE, SolrEventIndexItem.FIELD_STATUS_CHANGE_TIME);
        SORT_MAP.put(Field.COUNT, SolrEventIndexItem.FIELD_COUNT);
        SORT_MAP.put(Field.ELEMENT_IDENTIFIER, SolrEventIndexItem.FIELD_EVENT_ACTOR_ELEMENT_IDENTIFIER_SORT);
        SORT_MAP.put(Field.ELEMENT_SUB_IDENTIFIER, SolrEventIndexItem.FIELD_EVENT_ACTOR_ELEMENT_SUB_IDENTIFIER_SORT);
        SORT_MAP.put(Field.EVENT_SUMMARY, SolrEventIndexItem.FIELD_EVENT_SUMMARY_SORT);
        SORT_MAP.put(Field.UPDATE_TIME, SolrEventIndexItem.FIELD_UPDATE_TIME);
        
        SORT_DIRECTION_MAP.put(Direction.ASCENDING, SolrQuery.ORDER.asc);
        SORT_DIRECTION_MAP.put(Direction.DESCENDING, SolrQuery.ORDER.desc);
        
        // Mapping of EventSummaryRequest key -> index name
        PROTOBUF_TO_INDEX_MAP.put("uuid", SolrEventIndexItem.FIELD_UUID);
    }
    
    public SolrEventIndexDaoImpl(String name, SolrServer server) {
        this.name = name;
        this.server = server;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void index(EventSummary event) throws ZepException {
        stage(event);
        commit();
    }

    @Override
    public void stage(EventSummary event) throws ZepException {
        SolrEventIndexItem item = SolrEventIndexItemMapper.fromEventSummary(event);
        logger.debug("Indexing {}", item.uuid);

        try {
            server.addBean(item);
            eventsSinceOptimize.incrementAndGet();
        } catch (IOException e) {
            throw new ZepException(e);
        } catch (SolrServerException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public void commit() throws ZepException {
        commit(false);
    }

    @Override
    public void commit(boolean forceOptimize) throws ZepException {
        try {
            server.commit();

            if ( forceOptimize || eventsSinceOptimize.get() >= OPTIMIZE_AT_NUM_EVENTS ) {
                optimize();
            }
        } catch (IOException e) {
            throw new ZepException(e);
        } catch (SolrServerException e) {
            throw new ZepException(e);
        }
    }

    private void optimize() throws IOException, SolrServerException {
        server.optimize();
        eventsSinceOptimize.set(0);
    }

    @Override
    public void indexMany(List<EventSummary> events) throws ZepException {
        for ( EventSummary event : events ) {            
            stage(event);
        }

        commit();
    }

    @Override
    public EventSummaryResult list(EventSummaryRequest request) throws ZepException {
        // TODO limit fields and use filter
        SolrQuery solrQuery = buildSolrQuery(request);

        if ( solrQuery.getRows() == null || solrQuery.getRows() > MAX_RESULTS || solrQuery.getRows() < 1 ) {
            solrQuery.setRows(MAX_RESULTS);
        }

        EventSummaryResult.Builder result = EventSummaryResult.newBuilder();

        logger.info("Searching SOLR for events matching: {}", solrQuery.getQuery());

        try {
            QueryResponse response = server.query(solrQuery);

            result.setTotal((int) response.getResults().getNumFound());
            result.setLimit(solrQuery.getRows());
            if ( result.getTotal() > response.getResults().getStart() + solrQuery.getRows() ) {
                result.setNextOffset((int) response.getResults().getStart() + solrQuery.getRows());
            }

            logger.info("Found {} results in SOLR", result.getTotal());            

            List<SolrEventIndexItem> items = response.getBeans(SolrEventIndexItem.class);
            for (Iterator<SolrEventIndexItem> iter = items.iterator(); iter.hasNext(); ) {
                result.addEvents(SolrEventIndexItemMapper.toEventSummary(iter.next()));
            }
        } catch (SolrServerException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }

        return result.build();
    }

    @Override
    public void delete(String uuid) throws ZepException {
        try {
            server.deleteById(uuid);
            commit();
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public void delete(List<String> uuids) throws ZepException {
        try {
            server.deleteById(uuids);

            // Optimize only when we delete in batches as optimizing one at a time would be expensive
            commit(true);
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public EventSummary findByUuid(String uuid) throws ZepException {
        SolrQuery solrQuery = new SolrQuery().
                setQuery(SolrEventIndexItem.FIELD_UUID + ':' + uuid).
                setRows(1);

        List<SolrEventIndexItem> items;

        try {
            QueryResponse response = server.query(solrQuery);
            items = response.getBeans(SolrEventIndexItem.class);
        } catch (SolrServerException e) {
            throw new ZepException(e);
        }

        if ( items.size() > 0) {
            return SolrEventIndexItemMapper.toEventSummary(items.get(0));
        }
        else {
            return null;
        }
    }

    @Override
    public void clear() throws ZepException {
        logger.info("Deleting all events");

        try {
            server.deleteByQuery("*:*");
            commit(true);
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public void delete(EventSummaryRequest request) throws ZepException {
        String query = buildQuery(request);

        logger.info("Deleting events matching: {}", query);

        try {
            server.deleteByQuery(query);
            commit(true);
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public void purge(int duration, TimeUnit unit) throws ZepException {
        if (duration < 0) {
            throw new IllegalArgumentException("Duration must be >= 0");
        }
        final long millis = unit.toMillis(duration);
        final long pruneTimestamp = System.currentTimeMillis() - millis;

        SolrQueryBuilder query = new SolrQueryBuilder();
        query.addRange(SolrEventIndexItem.FIELD_LAST_SEEN_TIME, null, new Date(pruneTimestamp));

        logger.info("Purging events older than {}", new Date(pruneTimestamp));
        logger.debug("Solr query: {}", query);
        
        try {
            server.deleteByQuery(query.toString());
            commit(true);
        } catch (SolrServerException e) {
            throw new ZepException(e);
        } catch (IOException e) {
            throw new ZepException(e);
        }
    }

    private void buildDateRange(SolrQueryBuilder builder, String key, TimestampRange range) {
        if ( range.hasStartTime() && range.hasEndTime() ) {
            builder.addRange(key, new Date(range.getStartTime()), new Date(range.getEndTime()));
        }
        else if ( range.hasStartTime() ) {
            builder.addRange(key, new Date(range.getStartTime()), null);
        }
        else if ( range.hasEndTime() ) {
            builder.addRange(key, null, new Date(range.getEndTime()));
        }
    }

    private SolrQuery buildSolrQuery(EventSummaryRequest request) throws ZepException {
        SolrQuery solrQuery = new SolrQuery().setQuery(buildQuery(request));

        if ( request.hasLimit() ) {
            solrQuery.setRows(request.getLimit());
        }

        if ( request.hasOffset() ) {
            solrQuery.setStart(request.getOffset());
        }

        if ( request.getSortCount() > 0) {
            for ( EventSort sort : request.getSortList() ) {
                solrQuery.addSortField(SORT_MAP.get(sort.getField()), SORT_DIRECTION_MAP.get(sort.getDirection()));
            }
        }
        else {
            solrQuery.addSortField(SolrEventIndexItem.FIELD_LAST_SEEN_TIME, SolrQuery.ORDER.desc);
        }
        
        for (String fieldName : request.getKeysList()) {
            String solrField = PROTOBUF_TO_INDEX_MAP.get(fieldName);
            if (solrField != null) {
                solrQuery.addField(solrField);
            } else {
                logger.warn("Unsupported key: {}", fieldName);
            }
        }

        return solrQuery;
    }

    private String buildQuery(EventSummaryRequest request) throws ZepException {
        return buildQuery(request.getFilter());
    }

    private String buildQuery(EventSummaryFilter filter) throws ZepException {
        SolrQueryBuilder sb = new SolrQueryBuilder();

        if ( filter.hasCount() ) {
            sb.addField(SolrEventIndexItem.FIELD_COUNT, filter.getCount());
        }

        if ( filter.hasElementIdentifier() ) {
            sb.addWildcardField(SolrEventIndexItem.FIELD_EVENT_ACTOR_ELEMENT_IDENTIFIER, filter.getElementIdentifier());
        }

        if ( filter.hasElementSubIdentifier() ) {
            sb.addWildcardField(SolrEventIndexItem.FIELD_EVENT_ACTOR_ELEMENT_SUB_IDENTIFIER, filter.getElementSubIdentifier());
        }

        if ( filter.hasEventSummary() ) {
            sb.addWildcardField(SolrEventIndexItem.FIELD_EVENT_SUMMARY, filter.getEventSummary());
        }

        if ( filter.hasFirstSeen() ) {
            buildDateRange(sb, SolrEventIndexItem.FIELD_FIRST_SEEN_TIME, filter.getFirstSeen());
        }

        if ( filter.hasLastSeen() ) {
            buildDateRange(sb, SolrEventIndexItem.FIELD_LAST_SEEN_TIME, filter.getLastSeen());
        }
        
        if ( filter.hasStatusChange() ) {
            buildDateRange(sb, SolrEventIndexItem.FIELD_STATUS_CHANGE_TIME, filter.getStatusChange());
        }
        
        if ( filter.hasUpdateTime() ) {
            buildDateRange(sb, SolrEventIndexItem.FIELD_UPDATE_TIME, filter.getUpdateTime());
        }

        if ( filter.getStatusCount() > 0 ) {
            sb.addFieldOfEnumNumbers(SolrEventIndexItem.FIELD_STATUS, filter.getStatusList());
        }

        if ( filter.getSeverityCount() > 0 ) {
            sb.addFieldOfEnumNumbers(SolrEventIndexItem.FIELD_EVENT_SEVERITY, filter.getSeverityList());
        }

        if ( filter.hasEventClass() ) {
            String eventClass = filter.getEventClass();
            if ( eventClass.endsWith("/") ) {
                // This is a "startswith" search
                eventClass += '*';
            }       
            else if ( !eventClass.endsWith("*") ) {
                // This is an exact match
                eventClass += '/';
            }

            sb.addWildcardField(SolrEventIndexItem.FIELD_EVENT_EVENT_CLASS_INDEX, eventClass);
        }

        if ( filter.getTagUuidsCount() > 0 ) {
            List<String> tags = new ArrayList<String>(filter.getTagUuidsCount());
            for ( String t : filter.getTagUuidsList() ) {
                tags.add(t);
            }

            sb.addField(SolrEventIndexItem.FIELD_TAGS, tags, filter.getTagUuidsOp());
        }

        if ( filter.getUuidCount() > 0 ) {
            sb.addField(SolrEventIndexItem.FIELD_UUID, filter.getUuidList(), FilterOperator.OR);
        }

        String query = sb.toString();
        if ( query.isEmpty() ) {
            // Special SOLR query to get all results
            query = "*:*";
        }

        return query;
    }
    
    private Map<EventSeverity,Integer> countSeveritiesForTag(String tag) throws ZepException {
        SolrQueryBuilder builder = new SolrQueryBuilder();
        builder.addField(SolrEventIndexItem.FIELD_TAGS, tag);
        List<EventStatus> status = Arrays.asList(EventStatus.STATUS_NEW, EventStatus.STATUS_ACKNOWLEDGED);
        builder.addFieldOfEnumNumbers(SolrEventIndexItem.FIELD_STATUS, status);
        
        SolrQuery query = new SolrQuery();
        query.setRows(0);
        query.setQuery(builder.toString());
        query.setFacet(true);
        query.setFacetMinCount(1);
        query.setFacetLimit(-1);
        query.addFacetField(SolrEventIndexItem.FIELD_EVENT_SEVERITY);
        try {
            QueryResponse response = this.server.query(query);
            FacetField field = response.getFacetField(SolrEventIndexItem.FIELD_EVENT_SEVERITY);
            if (field.getValueCount() == 0) {
                return null;
            }
            Map<EventSeverity, Integer> severities = new HashMap<EventSeverity, Integer>();
            for (Count count : field.getValues()) {
                EventSeverity severity = EventSeverity.valueOf(Integer.valueOf(count.getName()));
                severities.put(severity, (int)count.getCount());
            }
            return severities;
        } catch (SolrServerException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }

    @Override
    public Map<String,Map<EventSeverity,Integer>> countSeverities(Set<String> tags)
            throws ZepException {
        Map<String,Map<EventSeverity,Integer>> severities = new HashMap<String, Map<EventSeverity,Integer>>(tags.size());
        for (String tag : tags) {
            Map<EventSeverity,Integer> tagSeverities = countSeveritiesForTag(tag);
            if (tagSeverities != null) {
                severities.put(tag, tagSeverities);
            }
        }
        return severities;
    }
    
    private EventSeverity findWorstSeverity(String tag) throws ZepException {
        SolrQueryBuilder builder = new SolrQueryBuilder();
        builder.addField(SolrEventIndexItem.FIELD_TAGS, tag);
        List<EventStatus> status = Arrays.asList(EventStatus.STATUS_NEW, EventStatus.STATUS_ACKNOWLEDGED);
        builder.addFieldOfEnumNumbers(SolrEventIndexItem.FIELD_STATUS, status);
        
        SolrQuery query = new SolrQuery();
        query.addSortField(SolrEventIndexItem.FIELD_EVENT_SEVERITY, ORDER.desc);
        query.setRows(1);
        query.setQuery(builder.toString());
        query.addField(SolrEventIndexItem.FIELD_EVENT_SEVERITY);
        QueryResponse response;
        EventSeverity severity = null;
        try {
            response = this.server.query(query);
            SolrDocumentList docs = response.getResults();
            if (!docs.isEmpty()) {
                SolrDocument doc = docs.get(0);
                Object val = doc.getFieldValue(SolrEventIndexItem.FIELD_EVENT_SEVERITY);
                severity = EventSeverity.valueOf(((Integer)val).intValue());
            }
            return severity;
        } catch (SolrServerException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }

    @Override
    public Map<String, EventSeverity> findWorstSeverity(Set<String> tags)
            throws ZepException {
        Map<String,EventSeverity> severities = new LinkedHashMap<String, EventSeverity>(tags.size());
        for (String tag : tags) {
            EventSeverity worstSeverity = findWorstSeverity(tag);
            if (worstSeverity != null) {
                severities.put(tag, worstSeverity);
            }
        }
        return severities;
    }
}
