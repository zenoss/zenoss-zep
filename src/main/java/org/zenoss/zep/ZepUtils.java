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
package org.zenoss.zep;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.EventSeverity;

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
}
