/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import com.google.protobuf.Message;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.DatabaseMetaDataCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;
import org.zenoss.protobufs.JsonFormat;
import org.zenoss.zep.ZepInstance;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibilityMySQL;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibilityPostgreSQL;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class DaoUtils {
    private static final Logger logger = LoggerFactory.getLogger(DaoUtils.class.getName());
    
    private static final String MYSQL_PROTOCOL = "mysql";
    private static final String POSTGRESQL_PROTOCOL = "postgresql";

    private DaoUtils() {
    }

    private static int getIntProperty(String value, int defaultValue) {
        int intVal = defaultValue;
        if (value!= null) {
            try {
                intVal = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                logger.warn("Invalid value for property: {}", value);
            }
        }
        return intVal;
    }

    private static boolean getBoolProperty(String value, boolean defaultValue) {
        boolean boolVal = defaultValue;
        if (value != null) {
            boolVal = Boolean.valueOf(value);
        }
        return boolVal;
    }

    public static BasicDataSource createDataSource(Properties globalConf, ZepInstance zepInstance) {
        // TODO: Update to look in global.conf first, then defer to ZEP's own configuration file
        final Map<String,String> zepConfig = zepInstance.getConfig();
        final String protocol = zepConfig.get("zep.jdbc.protocol");
        final String hostname = zepConfig.get("zep.jdbc.hostname");
        final String port = zepConfig.get("zep.jdbc.port");
        final String dbname = zepConfig.get("zep.jdbc.dbname");
        final String username = zepConfig.get("zep.jdbc.username");
        final String password = zepConfig.get("zep.jdbc.password");
        final int initialSize = getIntProperty(zepConfig.get("zep.jdbc.pool.initial_size"), 3);
        final int maxActive = getIntProperty(zepConfig.get("zep.jdbc.pool.max_active"), 10);
        final boolean poolPreparedStatements = getBoolProperty(zepConfig.get("zep.jdbc.pool.pool_prepared_statements"), true);
        final int maxOpenPreparedStatements = getIntProperty(zepConfig.get("zep.jdbc.pool.max_open_prepared_statements"), 1000);
        final String driverClassName;
        final String jdbcParameters;
        if (MYSQL_PROTOCOL.equals(protocol)) {
            driverClassName = "com.mysql.jdbc.Driver";
            // Make this configurable?
            jdbcParameters = "characterEncoding=UTF-8&amp;autoReconnect=true&amp;rewriteBatchedStatements=true";
        }
        else if (POSTGRESQL_PROTOCOL.equals(protocol)) {
            driverClassName = "org.postgresql.Driver";
            jdbcParameters = "";
        }
        else {
            throw new RuntimeException("Unsupported database protocol: " + protocol);
        }

        final BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(driverClassName);
        ds.setUrl(String.format("jdbc:%s://%s:%s/%s?%s", protocol, hostname, port, dbname, jdbcParameters));
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setInitialSize(initialSize);
        ds.setMaxActive(maxActive);
        ds.setTestWhileIdle(true);
        ds.setTestOnBorrow(false);
        ds.setTestOnReturn(false);
        ds.setDefaultAutoCommit(false);
        ds.setValidationQuery("SELECT 1");
        ds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        ds.setPoolPreparedStatements(poolPreparedStatements);
        ds.setMaxOpenPreparedStatements(maxOpenPreparedStatements);
        return ds;
    }

    public static DatabaseCompatibility createDatabaseCompatibility(Properties globalConf, ZepInstance zepInstance) {
        final Map<String,String> zepConfig = zepInstance.getConfig();
        final String protocol = zepConfig.get("zep.jdbc.protocol");
        if (MYSQL_PROTOCOL.equals(protocol)) {
            return new DatabaseCompatibilityMySQL();
        }
        else if (POSTGRESQL_PROTOCOL.equals(protocol)) {
            return new DatabaseCompatibilityPostgreSQL();
        }
        throw new RuntimeException("Unsupported database protocol: " + protocol);
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

    /**
     * Converts the protobuf message to JSON (wrapping exceptions).
     *
     * @param message Protobuf message.
     * @param <T> Type of protobuf.
     * @return JSON string representation of protobuf.
     * @throws RuntimeException If an exception occurs.
     */
    public static <T extends Message> String protobufToJson(T message) throws RuntimeException {
        try {
            return JsonFormat.writeAsString(message);
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Converts the JSON to the protobuf message (wrapping exceptions).
     *
     * @param json JSON string representation of protobuf.
     * @param defaultInstance Default instance of protobuf.
     * @param <T> Type of protobuf.
     * @return The deserialized message from the JSON representation.
     * @throws RuntimeException If an error occurs.
     */
    @SuppressWarnings({"unchecked"})
    public static <T extends Message> T protobufFromJson(String json, T defaultInstance) throws RuntimeException {
        try {
            return (T) JsonFormat.merge(json, defaultInstance.newBuilderForType());
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(), e);
        }
    }
}
