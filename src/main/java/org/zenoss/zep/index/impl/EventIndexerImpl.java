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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.EventPostProcessingPlugin;
import org.zenoss.zep.PluginService;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventDetailsConfigDao;
import org.zenoss.zep.dao.IndexMetadata;
import org.zenoss.zep.dao.IndexMetadataDao;
import org.zenoss.zep.dao.impl.DaoUtils;
import org.zenoss.zep.dao.impl.EventDaoHelper;
import org.zenoss.zep.dao.impl.EventSummaryRowMapper;
import org.zenoss.zep.impl.ThreadRenamingRunnable;
import org.zenoss.zep.index.EventIndexDao;
import org.zenoss.zep.index.EventIndexer;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zenoss.zep.dao.impl.EventConstants.*;

public class EventIndexerImpl implements EventIndexer {
    private static final Logger logger = LoggerFactory.getLogger(EventIndexerImpl.class);

    private final int indexInterval;
    private final NamedParameterJdbcTemplate template;
    private EventDaoHelper eventDaoHelper;
    private final Object indexLock = new Object();
    private EventIndexDao eventSummaryIndexDao;
    private EventIndexDao eventArchiveIndexDao;
    private PluginService pluginService;
    private IndexMetadataDao indexMetadataDao;

    private final byte[] indexVersionHash;
    private final int BATCH_SIZE = 1000;

    // Start dirty so we scan anything new on startup
    private final AtomicBoolean summaryDirty = new AtomicBoolean(true);
    private final AtomicBoolean archiveDirty = new AtomicBoolean(true);

    private static final String sqlTemplate =
            String.format("SELECT * FROM %%s WHERE %1$s > :since && %1$s <= :max_update_time ORDER BY %1$s ASC LIMIT :limit OFFSET :offset",
                    COLUMN_UPDATE_TIME);
    private static final String sqlGetSummary = String.format(sqlTemplate, TABLE_EVENT_SUMMARY);

    // FIXME Include the archive partitioning columns to take advantage of pruning   
    private static final String sqlGetArchive = String.format(sqlTemplate, TABLE_EVENT_ARCHIVE);

    @Autowired
    private TaskScheduler scheduler;
    private ScheduledFuture<?> eventIndexerFuture = null;

    public EventIndexerImpl(int indexInterval, DataSource dataSource,
                            EventDetailsConfigDao detailsConfigDao) throws ZepException {
        this.indexInterval = indexInterval;
        this.template = new NamedParameterJdbcTemplate(dataSource);
        this.indexVersionHash = calculateIndexVersionHash(detailsConfigDao);
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

    public void setIndexMetadataDao(IndexMetadataDao indexMetadataDao) {
        this.indexMetadataDao = indexMetadataDao;
    }

    private static byte[] calculateIndexVersionHash(EventDetailsConfigDao detailsConfigDao) throws ZepException {
        TreeMap<String,EventDetailItem> sorted = new TreeMap<String,EventDetailItem>();
        sorted.putAll(detailsConfigDao.getInitialEventDetailItemsByName());
        StringBuilder indexConfigStr = new StringBuilder();
        for (EventDetailItem item : sorted.values()) {
            // Only key and type affect the indexing behavior - ignore changes to display name
            indexConfigStr.append('|');
            indexConfigStr.append(item.getKey());
            indexConfigStr.append('|');
            indexConfigStr.append(item.getType().name());
            indexConfigStr.append('|');
        }
        if (indexConfigStr.length() == 0) {
            return null;
        }
        return DaoUtils.sha1(indexConfigStr.toString());
    }

    @Override
    @Transactional
    public void init() throws ZepException {
        rebuildIndex(this.eventSummaryIndexDao, TABLE_EVENT_SUMMARY);
        rebuildIndex(this.eventArchiveIndexDao, TABLE_EVENT_ARCHIVE);
        startEventIndexer();
    }

    private void startEventIndexer() {
        cancelFuture(this.eventIndexerFuture);
        this.eventIndexerFuture = null;

        logger.info("Starting event indexing at interval: {} second(s)", this.indexInterval);
        Date startTime = new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(this.indexInterval));
        this.eventIndexerFuture = scheduler.scheduleWithFixedDelay(
                new ThreadRenamingRunnable(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            index();
                        } catch (Exception e) {
                            logger.warn("Failed to index events", e);
                            /* If we fail to index, make sure we retry again next time */
                            markSummaryDirty();
                            markArchiveDirty();
                        }
                    }
                }, "ZEP_EVENT_INDEXER"), startTime, TimeUnit.SECONDS.toMillis(this.indexInterval));
    }

    @Override
    @Transactional
    public void shutdown() throws ZepException {
        // Stop indexing process
        cancelFuture(this.eventIndexerFuture);
        
        // Commit indexes on shutdown
        {
            final long lastTime = getLastIndexTime(this.eventSummaryIndexDao);
            this.eventSummaryIndexDao.commit();
            setNextIndexTime(this.eventSummaryIndexDao, lastTime, true);
        }
        {
            final long lastTime = getLastIndexTime(this.eventArchiveIndexDao);
            this.eventArchiveIndexDao.commit();
            setNextIndexTime(this.eventArchiveIndexDao, lastTime, true);
        }
    }

    private static void cancelFuture(Future<?> future) {
        if (future != null) {
            future.cancel(true);
            try {
                future.get();
            } catch (CancellationException e) {
                // Expected exception - we just canceled above
            } catch (ExecutionException e) {
                logger.warn("exception", e);
            } catch (InterruptedException e) {
                logger.debug("Interrupted", e);
            }
        }
    }

    private void rebuildIndex(final EventIndexDao dao, final String tableName)
            throws ZepException {
        final IndexMetadata indexMetadata = this.indexMetadataDao.findIndexMetadata(dao.getName());
        final long maxIndexTime = this.indexMetadataDao.findMaxLastIndexTime();
        final int numDocs = dao.getNumDocs();
        
        /*
         * Check if we have never indexed according to database but have documents in the index. This could
         * occur if someone wiped the database but left the previous index behind or the ZEP instance ID
         * changed for some reason.
         */
        if (indexMetadata == null) {
            if (numDocs > 0) {
                logger.info("Inconsistent state between index and database. Clearing index.");
                dao.clear();
            }
            /* Reindex everything to catch up with any other ZEP instances */
            indexEventsInRange(dao, tableName, 0, maxIndexTime);
            return;
        }

        /*
         * Check to see if the index version or version hash has changed. In this case, we will
         * need to rebuild all the currently indexed events.
         */
        if (indexVersionChanged(indexMetadata)) {
            dao.reindex();
            this.indexMetadataDao.updateIndexVersion(dao.getName(), IndexConstants.INDEX_VERSION,
                    this.indexVersionHash);
        }

        /*
         * If there are no documents in index and we have a non-zero last commit time, then the index
         * may have been manually wiped.
         */
        final long lastCommitTime = (numDocs > 0) ? indexMetadata.getLastCommitTime() : 0;

        /*
         * Index all events up to the maximum index time to ensure we don't duplicate post-processing
         * or fan-out of previous events.
         */
        if (lastCommitTime != maxIndexTime) {
            indexEventsInRange(dao, tableName, lastCommitTime, maxIndexTime);
        }
    }

    private boolean indexVersionChanged(IndexMetadata indexMetadata) {
        boolean changed = false;
        if (IndexConstants.INDEX_VERSION != indexMetadata.getIndexVersion()) {
            logger.info("Index version changed: previous={}, new={}", indexMetadata.getIndexVersion(),
                    IndexConstants.INDEX_VERSION);
            changed = true;
        }
        else if (!Arrays.equals(this.indexVersionHash, indexMetadata.getIndexVersionHash())) {
            logger.info("Index configuration changed.");
            changed= true;
        }
        return changed;
    }

    private void indexEventsInRange(final EventIndexDao dao, String tableName, long from, long to) throws ZepException {
        // No need to index
        if (to == 0L || from == to) {
            return;
        }
        logger.info("Indexing events in table {} from {} to {}",
                new Object[] { tableName, new Date(from), new Date(to) });
        
        final EventSummaryRowMapper mapper = new EventSummaryRowMapper(eventDaoHelper);
        final Map<String,Object> fields = new HashMap<String,Object>();
        fields.put("_max_update_time", to);
        fields.put("_min_update_time", from);
        final StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(tableName).append(" WHERE ");
        if (from > 0) {
            sql.append("update_time >= :_min_update_time AND ");
        }
        sql.append("update_time <= :_max_update_time");
        
        final int[] numRows = new int[1];
        this.template.query(sql.toString(), fields, new RowCallbackHandler() {
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
        setNextIndexTime(dao, to, true);
    }

    @Override
    public void markSummaryDirty() {
        summaryDirty.set(true);
    }

    @Override
    public void markArchiveDirty() {
        archiveDirty.set(true);
    }

    private long getLastIndexTime(EventIndexDao dao) throws ZepException {
        // Get the first ID that we haven't finished indexing
        IndexMetadata md = this.indexMetadataDao.findIndexMetadata(dao.getName());
        return (md != null) ? md.getLastIndexTime() : 0;
    }

    private void setNextIndexTime(EventIndexDao dao, long time, boolean isCommit) throws ZepException {
        this.indexMetadataDao.updateIndexMetadata(dao.getName(), IndexConstants.INDEX_VERSION, this.indexVersionHash,
                time, isCommit);
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
                doIndex(eventSummaryIndexDao, sqlGetSummary);
            }

            if ( force || archiveDirty.compareAndSet(true, false)) {
                doIndex(eventArchiveIndexDao, sqlGetArchive);
            }
        }
    }

    /**
     * Get all the recently updated event from both summary and archive in ascending
     * order (so we index oldest first). Index them all and return the next time to
     * index.
     * 
     * @param dao The DAO to use to index
     * @param sql The query to index
     * @throws org.zenoss.zep.ZepException If an error occurs.
     */
    private void doIndex(EventIndexDao dao, String sql) throws ZepException {
        final long since = getLastIndexTime(dao);
        final long now = System.currentTimeMillis();

        logger.debug("Indexing {} events since {}", dao.getName(), new Date(since));

        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("since", since);
        fields.put("max_update_time", now);
        // Grab 1 more record than needed so we know if we have moreToIndex
        fields.put("limit", BATCH_SIZE + 1);

        int i = 0;
        Result indexResults;
        do {
            fields.put("offset", i * BATCH_SIZE);

            logger.debug("Indexing events for {} with offset {}", dao.getName(), fields.get("offset"));

            ResultSetIndexer indexer = new ResultSetIndexer(dao, this.pluginService.getPostProcessingPlugins());
            indexResults = template.query(sql, fields, indexer);

            if (indexResults.lastUpdateTime > 0) {
                setNextIndexTime(dao, indexResults.lastUpdateTime, false);
            }

            i += 1;
        } while ( indexResults.moreToIndex);

        if ( indexResults.lastUpdateTime > 0 ) {
            logger.debug("Done indexing {}", dao.getName());
        }
        else {
            logger.debug("Nothing to index.");
        }
        setNextIndexTime(dao, now, false);
    }

    protected class ResultSetIndexer implements ResultSetExtractor<Result> {
        private final EventIndexDao dao;
        private final List<EventPostProcessingPlugin> plugins;

        public ResultSetIndexer(EventIndexDao eventIndexDao, List<EventPostProcessingPlugin> plugins) {
            this.dao = eventIndexDao;
            this.plugins = plugins;
        }

        @Override
        public Result extractData(ResultSet resultSet) throws SQLException, DataAccessException {
            Result result = new Result();
            EventSummaryRowMapper mapper = new EventSummaryRowMapper(eventDaoHelper);
            for ( result.processed = 0; result.processed < BATCH_SIZE && resultSet.next(); ++result.processed ) {

                EventSummary summary = mapper.mapRow(resultSet, resultSet.getRow());
                result.lastUpdateTime = summary.getUpdateTime();

                try {
                    dao.stage(summary);
                } catch (ZepException e) {
                    throw new SQLException(e);
                }
                
                for (EventPostProcessingPlugin plugin : this.plugins) {
                    try {
                        plugin.processEvent(summary);
                    } catch (Exception e) {
                        // Post-processing plug-in failures are not fatal errors.
                        logger.warn("Failed to run post-processing plug-in on event", e);
                    }
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
        private long lastUpdateTime = 0L;
        private boolean moreToIndex = false;
    }
}
