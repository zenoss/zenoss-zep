/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl.compat;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.zenoss.utils.dao.RangePartitioner;
import org.zenoss.utils.dao.impl.PostgreSqlRangePartitioner;

/**
 * Database compatibility interface for PostgreSQL support.
 */
public class DatabaseCompatibilityPostgreSQL implements DatabaseCompatibility {

    private final TypeConverter<String> uuidConverter = new UUIDConverterPostgreSQL();

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.POSTGRESQL;
    }

    @Override
    public TypeConverter<Long> getTimestampConverter() {
        return new TypeConverter<Long>() {
            @Override
            public Long fromDatabaseType(Object object) {
                return ((Timestamp)object).getTime();
            }

            @Override
            public Object toDatabaseType(Long timestampInMillis) {
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
            String databaseName, String tableName, String columnName,
            long duration, TimeUnit unit) {
        return new PostgreSqlRangePartitioner(ds, databaseName, tableName,
                columnName, duration, unit);
    }
}
