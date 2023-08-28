/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl.compat;

import org.zenoss.utils.dao.RangePartitioner;
import org.zenoss.utils.dao.impl.PostgreSqlRangePartitioner;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Database compatibility interface for PostgreSQL support.
 */
public class DatabaseCompatibilityPostgreSQL implements DatabaseCompatibility {

    private final TypeConverter<String> uuidConverter = new UUIDConverterPostgreSQL();

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.POSTGRESQL;
    }

    @Override
    public TypeConverter<Long> getTimestampConverter() {
        return new TypeConverter<Long>() {
            @Override
            public Long fromDatabaseType(ResultSet rs, String columnName) throws SQLException {
                Timestamp ts = rs.getTimestamp(columnName);
                return (ts != null) ? ts.getTime() : null;
            }

            @Override
            public Object toDatabaseType(Long timestampInMillis) {
                if (timestampInMillis == null) {
                    return null;
                }
                return new Timestamp(timestampInMillis);
            }
        };
    }

    @Override
    public TypeConverter<String> getUUIDConverter() {
        return this.uuidConverter;
    }

    @Override
    public RangePartitioner getRangePartitioner(DataSource ds,
            String tableName, String columnName,
            long duration, TimeUnit unit) {
        return new PostgreSqlRangePartitioner(ds, tableName,
                columnName, duration, unit);
    }
}
