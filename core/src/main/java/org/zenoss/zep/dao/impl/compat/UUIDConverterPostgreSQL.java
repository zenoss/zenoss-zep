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
import java.util.UUID;

/**
 * UUID converter for PostgreSQL native types.
 */
public class UUIDConverterPostgreSQL implements TypeConverter<String> {
    @Override
    public String fromDatabaseType(ResultSet rs, String columnName) throws SQLException {
        return rs.getString(columnName);
    }

    @Override
    public Object toDatabaseType(String uuid) {
        return (uuid != null) ? UUID.fromString(uuid) : null;
    }
}
