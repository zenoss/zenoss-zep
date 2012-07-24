/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl.compat;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * UUID converter for MySQL (stored as BINARY(16) type).
 */
public class UUIDConverterMySQL implements TypeConverter<String> {
    @Override
    public String fromDatabaseType(ResultSet rs, String columnName) throws SQLException {
        return uuidFromBytes(rs.getBytes(columnName));
    }

    @Override
    public Object toDatabaseType(String uuid) {
        return uuidToBytes(uuid);
    }
    
    /**
     * Converts the UUID string to a 16-byte byte array.
     *
     * @param uuidStr
     *            UUID string.
     * @return 16 byte array.
     */
    private static byte[] uuidToBytes(String uuidStr) {
        if (uuidStr == null) {
            return null;
        }
        final ByteBuffer bb = ByteBuffer.allocate(16);
        final UUID uuid = UUID.fromString(uuidStr);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

    /**
     * Converts a 16-byte byte array to a UUID string.
     *
     * @param bytes
     *            16-byte byte array.
     * @return UUID string.
     */
    private static String uuidFromBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        final ByteBuffer bb = ByteBuffer.wrap(bytes);
        return new UUID(bb.getLong(), bb.getLong()).toString();
    }
}
