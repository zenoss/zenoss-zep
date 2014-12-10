/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.dao.impl;

import org.springframework.jdbc.core.RowMapper;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.TypeConverter;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.zenoss.zep.dao.impl.EventConstants.COLUMN_LAST_SEEN;
import static org.zenoss.zep.dao.impl.EventConstants.COLUMN_UUID;

public class EventKeyRowMapper implements RowMapper<EventSummary> {

    private final TypeConverter<Long> timestampConverter;
    private final TypeConverter<String> uuidConverter;

    public EventKeyRowMapper(DatabaseCompatibility databaseCompatibility) {
        this.timestampConverter = databaseCompatibility.getTimestampConverter();
        this.uuidConverter = databaseCompatibility.getUUIDConverter();
    }

    @Override
    public EventSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
        return EventSummary.newBuilder()
                .setUuid(uuidConverter.fromDatabaseType(rs, COLUMN_UUID))
                .setLastSeenTime(timestampConverter.fromDatabaseType(rs, COLUMN_LAST_SEEN))
                .build();
    }

}
