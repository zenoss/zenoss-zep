/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.EventSeverity;

import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * General utility functions used by ZEP.
 */
public final class ZepUtils {
    private static final Logger logger = LoggerFactory
            .getLogger(ZepUtils.class);

    private ZepUtils() {
    }

    /**
     * Closes the resource and logs any exceptions.
     * 
     * @param closeable
     *            Closeable resource.
     */
    public static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    /**
     * Create a map which can store up to the specified amount of entries.
     * 
     * @param <K>
     *            Key type.
     * @param <V>
     *            Value type.
     * @param limit
     *            The maximum number of entries to store in the map.
     * @return A new bounded map.
     */
    public static <K, V> Map<K, V> createBoundedMap(final int limit) {
        return new LinkedHashMap<K, V>() {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
                return size() > limit;
            }
        };
    }
    
    /**
     * Severities ordered (in ascending order).
     */
    public static final List<EventSeverity> ORDERED_SEVERITIES = Collections.unmodifiableList(Arrays
            .asList(EventSeverity.SEVERITY_CLEAR, EventSeverity.SEVERITY_DEBUG,
                    EventSeverity.SEVERITY_INFO,
                    EventSeverity.SEVERITY_WARNING,
                    EventSeverity.SEVERITY_ERROR,
                    EventSeverity.SEVERITY_CRITICAL));

    /**
     * Returns a hex string of the byte array.
     *
     * @param bytes Bytes to convert to hex.
     * @return A hex string of the byte array.
     */
    public static String hexstr(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            final String s = Integer.toHexString(b & 0xff);
            if (s.length() == 1) {
                sb.append('0');
            }
            sb.append(s);
        }
        return sb.toString();
    }

    /** Returns a byte array of the hex string.
     *
     * @param hexString hex characters to convert to bytes
     * @return A byte array of the hex string.
     *
     * @throws NumberFormatException if string is not parsable.
     */
    public static byte[] fromHex(String hexString) {
        if (hexString.length() % 2 != 0) {
            throw new IllegalArgumentException("Hex string should be an even number of characters");
        }
        byte[] bytes = new byte[hexString.length()/2];
        for (int i = 0; i < bytes.length; i++) {
            int strIndex = i * 2;
            bytes[i] = (byte) Integer.parseInt(hexString.substring(strIndex, strIndex+2), 16);
        }
        return bytes;
    }

    /**
     * Returns true if the exception (or its cause) is of the specified type.
     *
     * @param t The exception to test..
     * @param type The type to check.
     * @return True if the exception (or its cause) is of the specified type, false otherwise.
     */
    public static boolean isExceptionOfType(Throwable t, Class<? extends Throwable> type) {
        boolean isOfType = false;
        while (t != null) {
            if (type.isAssignableFrom(t.getClass())) {
                isOfType = true;
                break;
            }
            t = t.getCause();
        }
        return isOfType;
    }

    private static final SimpleDateFormat UTC;
    static {
        UTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S 'UTC'");
        UTC.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /**
     * Returns a string in "yyyy-MM-dd HH:mm:ss.S 'UTC'" format.
     *
     * @param time milliseconds since the epoch
     * @return a string in "yyyy-MM-dd HH:mm:ss.S 'UTC'" format.
     */
    public static String formatUTC(long time) {
        synchronized (UTC) {
            return UTC.format(new Date(time));
        }
    }

    public static Date parseUTC(String s) throws ParseException {
        synchronized (UTC) {
            return UTC.parse(s);
        }
    }


}
