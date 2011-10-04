/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl.compat;

/**
 * Basic interface for partition meta-data.
 */
public interface Partition {
    /**
     * Returns the table catalog name.
     *
     * @return The table catalog name.
     */
    public String getTableCatalog();

    /**
     * Returns the table schema name.
     *
     * @return The table schema name.
     */
    public String getTableSchema();

    /**
     * Returns the table name.
     *
     * @return The table name.
     */
    public String getTableName();

    /**
     * Returns the partition name.
     *
     * @return The partition name.
     */
    public String getPartitionName();

    /**
     * Returns the range of values this partition contains.
     *
     * @return The maximum (non-inclusive) value that this partition can contain.
     */
    public Long getRangeLessThan();
}
