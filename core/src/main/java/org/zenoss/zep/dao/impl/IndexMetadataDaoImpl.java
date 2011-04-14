/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */

package org.zenoss.zep.dao.impl;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
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

    public IndexMetadataDaoImpl(DataSource ds, ZepInstance instance) {
        this.template = new SimpleJdbcTemplate(ds);
        this.zepInstanceBytes = DaoUtils.uuidToBytes(instance.getId());
    }

    @Override
    @Transactional(readOnly = true)
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
                return md;
            }
        }, fields);
        return (l.isEmpty()) ? null : l.get(0);
    }

    @Override
    @Transactional
    public int updateIndexVersion(String indexName, int indexVersion, byte[] indexHash) throws ZepException {
        final Map<String,Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_ZEP_INSTANCE, zepInstanceBytes);
        fields.put(COLUMN_INDEX_NAME, indexName);
        fields.put(COLUMN_INDEX_VERSION, indexVersion);
        fields.put(COLUMN_INDEX_VERSION_HASH, indexHash);

        final String sql = "INSERT INTO index_metadata (zep_instance,index_name,index_version,index_version_hash) " +
                "VALUES(:zep_instance,:index_name,:index_version,:index_version_hash) ON DUPLICATE KEY UPDATE " +
                "index_version=VALUES(index_version),index_version_hash=VALUES(index_version_hash)";

        try {
            return this.template.update(sql, fields);
        } catch (DataAccessException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }
}
