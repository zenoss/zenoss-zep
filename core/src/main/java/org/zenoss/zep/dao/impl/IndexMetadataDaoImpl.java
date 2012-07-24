/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepInstance;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.IndexMetadata;
import org.zenoss.zep.dao.IndexMetadataDao;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.NestedTransactionService;
import org.zenoss.zep.dao.impl.compat.TypeConverter;

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
    private final String zepInstanceId;

    private static final String COLUMN_ZEP_INSTANCE = "zep_instance";
    private static final String COLUMN_INDEX_NAME = "index_name";
    private static final String COLUMN_INDEX_VERSION = "index_version";
    private static final String COLUMN_INDEX_VERSION_HASH = "index_version_hash";

    private TypeConverter<String> uuidConverter;
    private NestedTransactionService nestedTransactionService;

    public IndexMetadataDaoImpl(DataSource ds, ZepInstance instance) {
        this.template = new SimpleJdbcTemplate(ds);
        this.zepInstanceId = instance.getId();
    }

    public void setDatabaseCompatibility(DatabaseCompatibility databaseCompatibility) {
        this.uuidConverter = databaseCompatibility.getUUIDConverter();
    }

    public void setNestedTransactionService(NestedTransactionService nestedTransactionService) {
        this.nestedTransactionService = nestedTransactionService;
    }

    @Override
    @TransactionalReadOnly
    public IndexMetadata findIndexMetadata(String indexName) throws ZepException {
        final String sql = "SELECT * FROM index_metadata WHERE zep_instance=:zep_instance AND index_name=:index_name";
        Map<String,Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_ZEP_INSTANCE, uuidConverter.toDatabaseType(zepInstanceId));
        fields.put(COLUMN_INDEX_NAME, indexName);
        final List<IndexMetadata> l = this.template.query(sql, new RowMapper<IndexMetadata>() {
            @Override
            public IndexMetadata mapRow(ResultSet rs, int rowNum) throws SQLException {
                IndexMetadata md = new IndexMetadata();
                md.setZepInstance(uuidConverter.fromDatabaseType(rs, COLUMN_ZEP_INSTANCE));
                md.setIndexName(rs.getString(COLUMN_INDEX_NAME));
                md.setIndexVersion(rs.getInt(COLUMN_INDEX_VERSION));
                md.setIndexVersionHash(rs.getBytes(COLUMN_INDEX_VERSION_HASH));
                return md;
            }
        }, fields);
        return (l.isEmpty()) ? null : l.get(0);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void updateIndexVersion(String indexName, int indexVersion, byte[] indexHash) throws ZepException {
        final Map<String,Object> fields = new HashMap<String,Object>();
        fields.put(COLUMN_ZEP_INSTANCE, uuidConverter.toDatabaseType(zepInstanceId));
        fields.put(COLUMN_INDEX_NAME, indexName);
        fields.put(COLUMN_INDEX_VERSION, indexVersion);
        fields.put(COLUMN_INDEX_VERSION_HASH, indexHash);

        String insertSql = "INSERT INTO index_metadata (zep_instance,index_name,index_version,index_version_hash) " +
                        "VALUES(:zep_instance,:index_name,:index_version,:index_version_hash)";
        String updateSql = "UPDATE index_metadata SET index_version=:index_version,index_version_hash=:index_version_hash " +
                    "WHERE zep_instance=:zep_instance AND index_name=:index_name";
        DaoUtils.insertOrUpdate(nestedTransactionService, template, insertSql, updateSql, fields);
    }
}
