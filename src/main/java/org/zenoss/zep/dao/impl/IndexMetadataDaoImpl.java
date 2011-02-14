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

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepInstance;
import org.zenoss.zep.dao.IndexMetadata;
import org.zenoss.zep.dao.IndexMetadataDao;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of IndexMetadataDao service.
 */
public class IndexMetadataDaoImpl implements IndexMetadataDao {

    //private static final Logger logger = LoggerFactory.getLogger(IndexMetadataDaoImpl.class);
    private final SimpleJdbcTemplate template;
    private final byte[] zepInstanceBytes;

    private static final String COLUMN_ZEP_INSTANCE = "zep_instance";
    private static final String COLUMN_INDEX_NAME = "index_name";
    private static final String COLUMN_INDEX_VERSION = "index_version";
    private static final String COLUMN_INDEX_VERSION_HASH = "index_version_hash";
    private static final String COLUMN_LAST_INDEX_TIME = "last_index_time";
    private static final String COLUMN_LAST_COMMIT_TIME = "last_commit_time";

    public IndexMetadataDaoImpl(DataSource ds, ZepInstance instance) {
        this.template = new SimpleJdbcTemplate(ds);
        this.zepInstanceBytes = DaoUtils.uuidToBytes(instance.getId());
    }

    @Override
    public long findMaxLastIndexTime() throws ZepException {
        final String sql = "SELECT MAX(last_index_time) FROM index_metadata";
        return this.template.queryForLong(sql);
    }

    @Override
    public IndexMetadata findIndexMetadata(String indexName) throws ZepException {
        final String sql = "SELECT * FROM index_metadata WHERE zep_instance=:zep_instance AND index_name=:index_name";
        Map<String,Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_ZEP_INSTANCE, zepInstanceBytes);
        fields.put(COLUMN_INDEX_NAME, indexName);
        final List<IndexMetadata> l = this.template.query(sql, new RowMapper<IndexMetadata>() {
            @Override
            public IndexMetadata mapRow(ResultSet rs, int rowNum) throws SQLException {
                IndexMetadata md = new IndexMetadata();
                md.setZepInstance(DaoUtils.uuidFromBytes(rs.getBytes(COLUMN_ZEP_INSTANCE)));
                md.setIndexName(rs.getString(COLUMN_INDEX_NAME));
                md.setIndexVersion(rs.getInt(COLUMN_INDEX_VERSION));
                md.setIndexVersionHash(rs.getBytes(COLUMN_INDEX_VERSION_HASH));
                md.setLastIndexTime(rs.getLong(COLUMN_LAST_INDEX_TIME));
                md.setLastCommitTime(rs.getLong(COLUMN_LAST_COMMIT_TIME));
                return md;
            }
        }, fields);
        return (l.isEmpty()) ? null : l.get(0);
    }

    @Override
    public int updateIndexMetadata(String indexName, int indexVersion, byte[] indexVersionHash, long lastIndexTime,
                                    boolean isCommit) throws ZepException {
        final Map<String,Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_ZEP_INSTANCE, zepInstanceBytes);
        fields.put(COLUMN_INDEX_NAME, indexName);
        fields.put(COLUMN_INDEX_VERSION, indexVersion);
        fields.put(COLUMN_INDEX_VERSION_HASH, indexVersionHash);
        fields.put(COLUMN_LAST_INDEX_TIME, lastIndexTime);

        final String updateSql;
        if (isCommit) {
            updateSql = "UPDATE index_metadata SET index_version=:index_version,index_version_hash=:index_version_hash," +
                    "last_index_time=:last_index_time,last_commit_time=:last_commit_time " +
                    "WHERE zep_instance=:zep_instance AND index_name=:index_name";
            fields.put(COLUMN_LAST_COMMIT_TIME, lastIndexTime);
        }
        else {
            updateSql = "UPDATE index_metadata SET index_version=:index_version,index_version_hash=:index_version_hash," +
                    "last_index_time=:last_index_time " +
                    "WHERE zep_instance=:zep_instance AND index_name=:index_name";
        }

        try {
            int numRows = this.template.update(updateSql, fields);
            if (numRows == 0) {
                final String insertSql = DaoUtils.createNamedInsert("index_metadata", fields.keySet());
                numRows = this.template.update(insertSql, fields);
            }
            return numRows;
        } catch (DataAccessException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }

    @Override
    public int updateIndexVersion(String indexName, int indexVersion, byte[] indexHash) throws ZepException {
        final Map<String,Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_ZEP_INSTANCE, zepInstanceBytes);
        fields.put(COLUMN_INDEX_NAME, indexName);
        fields.put(COLUMN_INDEX_VERSION, indexVersion);
        fields.put(COLUMN_INDEX_VERSION_HASH, indexHash);

        final String updateSql = "UPDATE index_metadata SET index_version=:index_version,index_version_hash=:index_version_hash" +
                    " WHERE zep_instance=:zep_instance AND index_name=:index_name";

        try {
            return this.template.update(updateSql, fields);
        } catch (DataAccessException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }
}
