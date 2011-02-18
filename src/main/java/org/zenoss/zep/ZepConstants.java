/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2011, Zenoss Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * For complete information please visit: http://www.zenoss.com/oss/
 */

package org.zenoss.zep;

/**
 * Constants for ZEP.
 */
public final class ZepConstants {
    private ZepConstants() {}

    /**
     * The default query limit (can be overridden with a properties setting).
     */
    public static final int DEFAULT_QUERY_LIMIT = 1000;

    /**
     * Constant for the device priority stored in event details.
     */
    public static final String DETAIL_DEVICE_PRIORITY = "zenoss.device.priority";

    /**
     * Constant for the device production state stored in event details.
     */
    public static final String DETAIL_DEVICE_PRODUCTION_STATE = "zenoss.device.production_state";
}
