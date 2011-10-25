/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl.compat;

import org.zenoss.utils.dao.RangePartitioner;
import org.zenoss.utils.dao.impl.MySqlRangePartitioner;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Database compatibility layer for MySQL.
 */
public class DatabaseCompatibilityMySQL implements DatabaseCompatibility {

    private final TypeConverter<String> uuidConverter = new UUIDConverterMySQL();

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.MYSQL;
    }

    @Override
    public TypeConverter<Long> getTimestampConverter() {
        return new TypeConverter<Long>() {
            @Override
            public Long fromDatabaseType(ResultSet rs, String columnName) throws SQLException {
                long l = rs.getLong(columnName);
                return (rs.wasNull()) ? null : l;
            }

            @Override
            public Object toDatabaseType(Long timestampInMillis) {
                return timestampInMillis;
            }
        };
    }

    @Override
    public TypeConverter<String> getUUIDConverter() {
        return uuidConverter;
    }

    @Override
    public RangePartitioner getRangePartitioner(DataSource ds,
            String databaseName, String tableName, String columnName,
            long duration, TimeUnit unit) {
        return new MySqlRangePartitioner(ds, databaseName, tableName,
                columnName, duration, unit);
    }
}
