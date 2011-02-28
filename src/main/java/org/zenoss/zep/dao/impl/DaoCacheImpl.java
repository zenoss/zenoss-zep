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
package org.zenoss.zep.dao.impl;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.zenoss.zep.dao.DaoCache;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class DaoCacheImpl implements DaoCache {

    private static final int DEFAULT_MAX_CACHE_ENTRIES = 500;
    //private static final Logger logger = LoggerFactory.getLogger(DaoCacheImpl.class);

    private final DaoTableStringLruCache eventClassCache;
    private final DaoTableStringLruCache eventClassKeyCache;
    private final DaoTableStringLruCache monitorCache;
    private final DaoTableStringLruCache agentCache;
    private final DaoTableStringLruCache groupCache;
    private final DaoTableStringLruCache eventKeyCache;

    public DaoCacheImpl(DataSource ds) {
        JdbcTemplate template = new JdbcTemplate(ds);
        this.eventClassCache = new DaoTableStringLruCache(template, "event_class");
        this.eventClassKeyCache = new DaoTableStringLruCache(template, "event_class_key");
        this.monitorCache = new DaoTableStringLruCache(template, "monitor");
        this.agentCache = new DaoTableStringLruCache(template, "agent");
        this.groupCache = new DaoTableStringLruCache(template, "event_group");
        this.eventKeyCache = new DaoTableStringLruCache(template, "event_key");
    }

    public void init() {
        this.eventClassCache.init();
        this.eventClassKeyCache.init();
        this.monitorCache.init();
        this.agentCache.init();
        this.groupCache.init();
        this.eventKeyCache.init();
    }

    private <T> int getIdFromName(final DaoTableCache<T> cache, final T name) {
        Integer cached = cache.getCache().getIdFromName(name);
        if (cached == null) {
            final int id = cache.insertOrSelect(name);
            TransactionSynchronizationManager
                    .registerSynchronization(new TransactionSynchronizationAdapter() {
                        @Override
                        public void afterCommit() {
                            cache.getCache().cache(name, id);
                        }
                    });
            cached = id;
        }
        return cached;
    }

    private <T> T getNameFromId(final DaoTableCache<T> cache, final int id) {
        T cached = cache.getCache().getNameFromId(id);
        if (cached == null) {
            final T name = cache.findNameFromId(id);
            if (TransactionSynchronizationManager.isSynchronizationActive()) {
                TransactionSynchronizationManager.registerSynchronization(
                        new TransactionSynchronizationAdapter() {
                            @Override
                            public void afterCommit() {
                                cache.getCache().cache(name, id);
                            }
                        });
            }
            else {
                // Not part of a transaction - can safely cache
                cache.getCache().cache(name, id);
            }
            cached = name;
        }
        return cached;
    }

    @Override
    @Transactional
    public int getEventClassId(String eventClass) {
        return getIdFromName(this.eventClassCache, eventClass);
    }

    @Override
    @Transactional(readOnly = true)
    public String getEventClassFromId(int id) {
        return getNameFromId(this.eventClassCache, id);
    }

    @Override
    @Transactional
    public int getEventClassKeyId(String eventClassKey) {
        return getIdFromName(this.eventClassKeyCache, eventClassKey);
    }

    @Override
    @Transactional(readOnly = true)
    public String getEventClassKeyFromId(int id) {
        return getNameFromId(this.eventClassKeyCache, id);
    }

    @Override
    @Transactional
    public int getMonitorId(String monitor) {
        return getIdFromName(this.monitorCache, monitor);
    }

    @Override
    @Transactional(readOnly = true)
    public String getMonitorFromId(int id) {
        return getNameFromId(this.monitorCache, id);
    }

    @Override
    @Transactional
    public int getAgentId(String agent) {
        return getIdFromName(this.agentCache, agent);
    }

    @Override
    @Transactional(readOnly = true)
    public String getAgentFromId(int id) {
        return getNameFromId(this.agentCache, id);
    }

    @Override
    @Transactional
    public int getEventGroupId(String eventGroup) {
        return getIdFromName(this.groupCache, eventGroup);
    }

    @Override
    @Transactional(readOnly = true)
    public String getEventGroupFromId(int id) {
        return getNameFromId(this.groupCache, id);
    }

    @Override
    @Transactional
    public int getEventKeyId(String eventKey) {
        return getIdFromName(this.eventKeyCache, eventKey);
    }

    @Override
    @Transactional(readOnly = true)
    public String getEventKeyFromId(int id) {
        return getNameFromId(this.eventKeyCache, id);
    }

    private static class BiMap<T> {
        private final Map<T, Integer> nameToIdMap;
        private final Map<Integer, T> idToNameMap;
        private final int maxCache;

        /**
         * Creates a BiMap which is optionally bounded by the specified size.
         * 
         * @param maxCache
         *            The maximum size of the maps to keep. If this is greater
         *            than zero, then this BiMap will function as a LRU cache.
         */
        public BiMap(int maxCache) {
            this.nameToIdMap = new HashMap<T, Integer>();
            if (maxCache <= 0) {
                this.idToNameMap = new HashMap<Integer, T>();
            } else {
                this.idToNameMap = new LinkedHashMap<Integer, T>();
            }
            this.maxCache = maxCache;
        }

        public synchronized Integer getIdFromName(T name) {
            return this.nameToIdMap.get(name);
        }

        public synchronized T getNameFromId(int id) {
            return this.idToNameMap.get(id);
        }

        public synchronized void cache(T name, int id) {
            this.idToNameMap.put(id, name);
            this.nameToIdMap.put(name, id);
            if (maxCache > 0 && idToNameMap.size() > maxCache) {
                final Iterator<Map.Entry<Integer, T>> it = this.idToNameMap
                        .entrySet().iterator();
                final Map.Entry<Integer, T> lru = it.next();
                it.remove();
                this.nameToIdMap.remove(lru.getValue());
            }
        }
    }

    private static abstract class DaoTableCache<T> {
        protected final JdbcTemplate template;
        protected final SimpleJdbcInsert insert;
        protected final String tableName;
        protected final BiMap<T> cache;
        static final String COLUMN_ID = "id";
        static final String COLUMN_NAME = "name";

        public DaoTableCache(JdbcTemplate template, String tableName, BiMap<T> cache) {
            this.template = template;
            this.insert = new SimpleJdbcInsert(template).withTableName(tableName)
                    .usingColumns(COLUMN_NAME).usingGeneratedKeyColumns(COLUMN_ID);
            this.tableName = tableName;
            this.cache = cache;
        }

        public void init() {
            this.insert.compile();
        }

        public BiMap<T> getCache() {
            return cache;
        }

        public abstract int insertOrSelect(T name);

        public abstract T findNameFromId(int id);
    }

    private static class DaoTableStringLruCache extends DaoTableCache<String> {
        public DaoTableStringLruCache(JdbcTemplate template, String tableName) {
            this(template, tableName, DEFAULT_MAX_CACHE_ENTRIES);
        }

        public DaoTableStringLruCache(JdbcTemplate template, String tableName, final int limit) {
            super(template, tableName, new BiMap<String>(limit));
        }

        @Override
        public int insertOrSelect(String name) {
            try {
                final Map<String, Object> args = Collections
                        .<String, Object> singletonMap(COLUMN_NAME, name);
                return this.insert.executeAndReturnKey(args).intValue();
            } catch (DuplicateKeyException e) {
                return this.template.queryForInt("SELECT "
                        + COLUMN_ID + " FROM " + tableName + " WHERE "
                        + COLUMN_NAME + "=?", name);
            }
        }

        @Override
        public String findNameFromId(int id) {
            return template.queryForObject("SELECT " + COLUMN_NAME + " FROM "
                    + this.tableName + " WHERE " + COLUMN_ID + "=?",
                    String.class, id);
        }
    }
}
