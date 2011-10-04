/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl.compat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * UUID converter for PostgreSQL native types.
 */
public class UUIDConverterPostgreSQL implements TypeConverter<String> {
    @Override
    public String fromDatabaseType(Object uuid) {
        return uuid.toString();
    }

    @Override
    public Object toDatabaseType(String uuid) {
        return UUID.fromString(uuid);
    }
}
