/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl.compat;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Generic interface used to convert parameters to/from database representations.
 */
public interface TypeConverter<T> {
    /**
     * Converts the original type to the database type.
     * 
     * @param originalType Original type.
     * @return Underlying type expected by the database.
     */
    Object toDatabaseType(T originalType);

    /**
     * Converts the type returned by the database to the appropriate application type.
     *
     * @param rs ResultSet.
     * @param columnName Column name.
     * @return Application type.
     * @throws SQLException If the column can't be read.
     */
    T fromDatabaseType(ResultSet rs, String columnName) throws SQLException;
}
