/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl.compat;

import javax.sql.DataSource;
import java.util.concurrent.TimeUnit;

/**
 * Database compatibility interface.
 */
public interface DatabaseCompatibility {
    /**
     * Returns the database type (used to conditionalize queries where needed).
     *
     * @return The database type.
     */
    public DatabaseType getDatabaseType();

    /**
     * Returns the timestamp conversion interface for the database.
     *
     * @return Timestamp converter.
     */
    public TypeConverter<Long> getTimestampConverter();

    /**
     * Returns the UUID conversion interface for the database.
     *
     * @return UUID converter.
     */
    public TypeConverter<String> getUUIDConverter();

    /**
     * Returns a range partitioner for the specified database.
     *
     * @param ds Datasource.
     * @param databaseName Database name.
     * @param tableName Table name.
     * @param columnName Column name.
     * @param duration Duration.
     * @param unit Unit of time.
     * @return The RangePartitioner.
     */
    public RangePartitioner getRangePartitioner(DataSource ds, String databaseName, String tableName,
                                                String columnName, long duration, TimeUnit unit);
}
