/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
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
