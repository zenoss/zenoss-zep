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
    public RangePartitioner getRangePartitioner(DataSource ds,
            String databaseName, String tableName, String columnName,
            long duration, TimeUnit unit);
}
