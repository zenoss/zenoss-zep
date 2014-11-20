package org.zenoss.zep.dao.impl;

import org.springframework.jdbc.core.RowMapper;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.TypeConverter;
import java.sql.ResultSet;
import java.sql.SQLException;
import static org.zenoss.zep.dao.impl.EventConstants.*;

public class EventUuidRowMapper implements RowMapper<String> {
    private final TypeConverter<String> uuidConverter;

    public EventUuidRowMapper(DatabaseCompatibility databaseCompatibility) {
        this.uuidConverter = databaseCompatibility.getUUIDConverter();
    }

    @Override
    public String mapRow(ResultSet rs, int rowNum) throws SQLException {
        return uuidConverter.fromDatabaseType(rs, COLUMN_UUID);
    }
}
