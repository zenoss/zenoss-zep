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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.EventPostProcessingPlugin;
import org.zenoss.zep.EventPublisher;
import org.zenoss.zep.PluginService;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepInstance;
import org.zenoss.zep.dao.impl.DaoUtils;
import org.zenoss.zep.dao.impl.EventDaoHelper;
import org.zenoss.zep.dao.impl.EventSummaryRowMapper;
import org.zenoss.zep.index.EventIndexDao;
import org.zenoss.zep.index.EventIndexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zenoss.zep.dao.impl.EventConstants.*;

public class EventIndexerImpl implements EventIndexer {
    private static final Logger logger = LoggerFactory.getLogger(EventIndexerImpl.class);
    
    private final NamedParameterJdbcTemplate template;
    private final byte[] zepInstanceIdBytes;
    private EventDaoHelper eventDaoHelper;
    private final Object indexLock = new Object();
    private EventIndexDao eventSummaryIndexDao;
    private EventIndexDao eventArchiveIndexDao;
    private PluginService pluginService;
    private EventPublisher eventPublisher;

    private final int BATCH_SIZE = 1000;

    // Start dirty so we scan anything new on startup
    private AtomicBoolean summaryDirty = new AtomicBoolean(true);
    private AtomicBoolean archiveDirty = new AtomicBoolean(true);

    private static final String sqlTemplate =
            String.format("SELECT * FROM %%s WHERE %1$s > :since && %1$s <= :max_update_time ORDER BY %1$s ASC LIMIT :limit OFFSET :offset",
                    COLUMN_UPDATE_TIME);
    private static final String sqlGetSummary = String.format(sqlTemplate, TABLE_EVENT_SUMMARY);

    // FIXME Include the archive partitioning columns to take advantage of pruning   
    private static final String sqlGetArchive = String.format(sqlTemplate, TABLE_EVENT_ARCHIVE);

    public EventIndexerImpl(JdbcTemplate jdbcTemplate, ZepInstance zepInstance) {
        this.template = new NamedParameterJdbcTemplate(jdbcTemplate);
        this.zepInstanceIdBytes = DaoUtils.uuidToBytes(zepInstance.getId());
    }

    public void setEventSummaryIndexDao(EventIndexDao eventSummaryIndexDao) {
        this.eventSummaryIndexDao = eventSummaryIndexDao;
    }

    public void setEventArchiveIndexDao(EventIndexDao eventIndexDao) {
        this.eventArchiveIndexDao = eventIndexDao;
    }

    public void setEventDaoHelper(EventDaoHelper eventDaoHelper) {
        this.eventDaoHelper = eventDaoHelper;
    }
    
    public void setPluginService(PluginService pluginService) {
        this.pluginService = pluginService;
    }
    
    public void setEventPublisher(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @Override
    @Transactional(readOnly = true)
    public void init() throws ZepException {
        rebuildIndex(this.eventSummaryIndexDao, TABLE_EVENT_SUMMARY);
        rebuildIndex(this.eventArchiveIndexDao, TABLE_EVENT_ARCHIVE);
    }

    private void rebuildIndex(final EventIndexDao dao, final String tableName) throws ZepException {
        long lastIndexTime = getLastIndexTime(dao);
        int numDocs = dao.getNumDocs();

        /*
         * Check if we have never indexed according to database but have documents in the index. This could
         * occur if someone wiped the database but left the previous index behind.
         */
        if (lastIndexTime == 0) {
            if (numDocs > 0) {
                logger.info("Inconsistent state between index and database. Clearing index.");
                dao.clear();
            }
            return;
        }

        /*
         * If we have indexed before according to the database and we have documents in our index, assume
         * that the index is in sync with the database.
         */
        if (numDocs > 0) {
            logger.debug("Rebuilding index not required for: {}", dao.getName());
            return;
        }
        
        logger.info("Rebuilding index on table {} through {}", tableName, new Date(lastIndexTime));
        final EventSummaryRowMapper mapper = new EventSummaryRowMapper(eventDaoHelper);
        final Map<String,?> fields = Collections.singletonMap("max_update_time", lastIndexTime);
        final String sql = String.format("SELECT * FROM %s WHERE update_time <= :max_update_time", tableName);
        final int[] numRows = new int[1];
        this.template.query(sql, fields, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet rs) throws SQLException {
                final EventSummary summary = mapper.mapRow(rs, rs.getRow());
                try {
                    dao.stage(summary);
                } catch (ZepException e) {
                    throw new SQLException(e.getLocalizedMessage(), e);
                }
                ++numRows[0];
            }
        });
        if (numRows[0] > 0) {
            logger.info("Committing changes to index on table: {}", tableName);
            dao.commit(true);
        }
        logger.info("Successfully rebuilt index for {} events on table {}", numRows[0], tableName);
    }

    @Override
    public void markSummaryDirty() {
        summaryDirty.set(true);
    }

    @Override
    public void markArchiveDirty() {
        archiveDirty.set(true);
    }

    private long getLastIndexTime(EventIndexDao dao) {
        // Get the first ID that we haven't finished indexing
        String sql = "SELECT last_index_time FROM index_version WHERE zep_instance = :zep_instance AND index_name = :index_name";

        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("zep_instance", zepInstanceIdBytes);
        fields.put("index_name", dao.getName());
        try {
            return this.template.queryForLong(sql, fields);
        }
        catch ( EmptyResultDataAccessException e ) {
            return 0;
        }
    }

    private void setNextIndexTime(EventIndexDao dao, long time) {
        String sql = "REPLACE INTO index_version (zep_instance, index_name, last_index_time) VALUES (:zep_instance, :index_name, :last_index_time)";
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("last_index_time", time);
        fields.put("zep_instance", zepInstanceIdBytes);
        fields.put("index_name", dao.getName());

        this.template.update(sql, fields);
    }
    
    @Override
    @Transactional
    public void index() throws ZepException {
        index(false);
    }

    @Override
    @Transactional
    public void index(boolean force) throws ZepException {
        synchronized (indexLock) {
            if ( force || summaryDirty.compareAndSet(true, false)) {
                doIndex(eventSummaryIndexDao, null, sqlGetSummary);
            }

            if ( force || archiveDirty.compareAndSet(true, false)) {
                doIndex(eventArchiveIndexDao, eventSummaryIndexDao, sqlGetArchive);
            }
        }
    }

    /**
     * Get all the recently updated event from both summary and archive in ascending
     * order (so we index oldest first). Index them all and return the next time to
     * index.
     * 
     * @param dao The DAO to use to index
     * @param deleteFromDao The DAO to delete from for each item found in <code>dao</code>.
     * @param sql The query to index
     * @throws org.zenoss.zep.ZepException If an error occurs.
     */
    private void doIndex(EventIndexDao dao, EventIndexDao deleteFromDao, String sql) throws ZepException {
        long since = getLastIndexTime(dao);

        logger.debug("Indexing {} events since {}", dao.getName(), new Date(since));

        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("since", since);
        fields.put("max_update_time", System.currentTimeMillis());
        // Grab 1 more record than needed so we know if we have moreToIndex
        fields.put("limit", BATCH_SIZE + 1);

        int i = 0;
        Result indexResults;
        do {
            fields.put("offset", i * BATCH_SIZE);

            logger.debug("Indexing events for {} with offset {}", dao.getName(), fields.get("offset"));

            ResultSetIndexer indexer = new ResultSetIndexer(dao, this.pluginService.getPostProcessingPlugins(), this.eventPublisher);
            indexResults = template.query(sql, fields, indexer);

            if ( deleteFromDao != null && !indexResults.uuids.isEmpty() ) {
                deleteFromDao.delete(new ArrayList<String>(indexResults.uuids));
            }

            i += 1;
        } while ( indexResults.moreToIndex);

        if ( indexResults.lastUpdateTime > 0 ) {
            setNextIndexTime(dao, indexResults.lastUpdateTime);
            logger.debug("Done indexing {}", dao.getName());
        }
        else {
            logger.debug("Nothing to index.");
        }
    }

    protected class ResultSetIndexer implements ResultSetExtractor<Result> {
        private final EventIndexDao dao;
        private List<EventPostProcessingPlugin> plugins;
        private EventPublisher publisher;

        public ResultSetIndexer(EventIndexDao eventIndexDao, List<EventPostProcessingPlugin> plugins, EventPublisher publisher) {
            this.dao = eventIndexDao;
            this.plugins = plugins;
            this.publisher = publisher;
        }

        @Override
        public Result extractData(ResultSet resultSet) throws SQLException, DataAccessException {
            Result result = new Result();
            EventSummaryRowMapper mapper = new EventSummaryRowMapper(eventDaoHelper);
            for ( result.processed = 0; result.processed < BATCH_SIZE && resultSet.next(); ++result.processed ) {

                EventSummary summary = mapper.mapRow(resultSet, resultSet.getRow());
                result.uuids.add(summary.getUuid());
                result.lastUpdateTime = summary.getUpdateTime();
                
                for (EventPostProcessingPlugin plugin : this.plugins) {
                    try {
                        plugin.processEvent(summary);
                    } catch (Exception e) {
                        // Post-processing plug-in failures are not fatal errors.
                        logger.warn("Failed to run post-processing plug-in on event", e);
                    }
                }

                try {
                    dao.stage(summary);
                    this.publisher.addEvent(summary);
                } catch (ZepException e) {
                    throw new SQLException(e);
                }

            }

            if ( result.processed > 0 ) {
                try {
                    dao.commit();
                    publisher.publish();
                } catch (ZepException e) {
                    throw new SQLException(e);
                }
            }
            
            if ( resultSet.next() ) {
                result.moreToIndex = true;
            }

            return result;
        }
    }

    protected static class Result {
        private int processed = 0;
        private Set<String> uuids = new HashSet<String>();
        private long lastUpdateTime = 0L;
        private boolean moreToIndex = false;
    }
}
