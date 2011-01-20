/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.dao.impl;

import org.springframework.jdbc.support.DatabaseMetaDataCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

public final class DaoUtils {
    private DaoUtils() {
    }

    /**
     * Converts the UUID string to a 16-byte byte array.
     * 
     * @param uuidStr
     *            UUID string.
     * @return 16 byte array.
     */
    public static byte[] uuidToBytes(String uuidStr) {
        final ByteBuffer bb = ByteBuffer.allocate(16);
        final UUID uuid = UUID.fromString(uuidStr);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

    /**
     * Returns bytes from a random UUID.
     * 
     * @return Bytes from a randomly created UUID.
     */
    @SuppressWarnings("unused")
    public static byte[] createRandomUuid() {
        final ByteBuffer bb = ByteBuffer.allocate(16);
        final UUID uuid = UUID.randomUUID();
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
    public static String uuidFromBytes(byte[] bytes) {
        final ByteBuffer bb = ByteBuffer.wrap(bytes);
        return new UUID(bb.getLong(), bb.getLong()).toString();
    }

    /**
     * Convert a collection of String UUIDs to a list of byte[] UUIDs.
     * 
     * @param strUuids
     *            UUIDs to convert.
     * @return A list of UUID byte arrays (suitable for passing to the
     *         database).
     */
    public static List<byte[]> uuidsToBytes(Collection<String> strUuids) {
        List<byte[]> uuids = new ArrayList<byte[]>(strUuids.size());
        for (String strUuid : strUuids) {
            uuids.add(uuidToBytes(strUuid));
        }
        return uuids;
    }

    /**
     * Calculate a SHA-1 hash from the specified string.
     * 
     * @param str
     *            String to hash.
     * @return SHA-1 hash for string.
     */
    public static byte[] sha1(String str) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            return sha1.digest(str.getBytes(Charset.forName("UTF-8")));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Must support SHA-1", e);
        }
    }

    public static Set<String> getFieldsInResultSet(ResultSetMetaData metaData)
            throws SQLException {
        final Set<String> fields = new TreeSet<String>();
        final int count = metaData.getColumnCount();
        for (int i = 1; i <= count; i++) {
            fields.add(metaData.getColumnName(i));
        }
        return fields;
    }

    /**
     * Truncates the specified string to fit in the specified maximum number of
     * UTF-8 bytes. This method will not split strings in the middle of
     * surrogate pairs.
     * 
     * @param original
     *            The original string.
     * @param maxBytes
     *            The maximum number of UTF-8 bytes available to store the
     *            string.
     * @return If the string doesn't overflow the number of specified bytes,
     *         then the original string is returned, otherwise the string is
     *         truncated to the number of bytes available to encode
     */
    public static String truncateStringToUtf8(final String original,
            final int maxBytes) {
        final int length = original.length();
        int newLength = 0;
        int currentBytes = 0;
        while (newLength < length) {
            final char c = original.charAt(newLength);
            boolean isSurrogate = false;
            if (c <= 0x7f) {
                ++currentBytes;
            } else if (c <= 0x7FF) {
                currentBytes += 2;
            } else if (c <= Character.MAX_HIGH_SURROGATE) {
                currentBytes += 4;
                isSurrogate = true;
            } else if (c <= 0xFFFF) {
                currentBytes += 3;
            } else {
                currentBytes += 4;
            }
            if (currentBytes > maxBytes) {
                break;
            }
            if (isSurrogate) {
                newLength += 2;
            } else {
                ++newLength;
            }
        }
        return (newLength == length) ? original : original.substring(0,
                newLength);
    }

    /**
     * Create an insert SQL string for the table with the specified insert columns.
     *
     * @param tableName Table name.
     * @param columnNames Column names for insert.
     * @return An insert SQL statement with the names (suitable for passing to Spring named
     *         parameter template).
     */
    public static String createNamedInsert(String tableName, Collection<String> columnNames) {
        StringBuilder names = new StringBuilder();
        StringBuilder values = new StringBuilder();
        Iterator<String> it = columnNames.iterator();
        while (it.hasNext()) {
            final String columnName = it.next();
            names.append(columnName);
            values.append(':').append(columnName);
            if (it.hasNext()) {
                names.append(',');
                values.append(',');
            }
        }
        return "INSERT INTO " + tableName + " (" + names + ") VALUES (" + values + ")";
    }

    /**
     * Returns a list of column names in the specified table.
     *
     * @param dataSource DataSource to use.
     * @param tableName Table name.
     * @return A list of column names in the table.
     * @throws MetaDataAccessException If an exception occurs.
     */
    public static List<String> getColumnNames(final DataSource dataSource, final String tableName)
            throws MetaDataAccessException {
        final List<String> columnNames = new ArrayList<String>();
        JdbcUtils.extractDatabaseMetaData(dataSource, new DatabaseMetaDataCallback() {
            @Override
            public Object processMetaData(DatabaseMetaData dbmd) throws SQLException, MetaDataAccessException {
                ResultSet rs = dbmd.getColumns(null, null, tableName, null);
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    columnNames.add(columnName);
                }
                rs.close();
                return null;
            }
        });
        return columnNames;
    }
}