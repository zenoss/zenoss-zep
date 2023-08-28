/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep;

import org.zenoss.protobufs.zep.Zep.EventStatus;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

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
     * The default device priority if one is not specified.
     */
    public static final int DEVICE_PRIORITY_NORMAL = 3;

    /**
     * Constant for the device production state stored in event details.
     */
    public static final String DETAIL_DEVICE_PRODUCTION_STATE = "zenoss.device.production_state";

    /**
     * Constant for the device class stored in event details.
     */
    public static final String DETAIL_DEVICE_CLASS = "zenoss.device.device_class";

    /**
     * Constant for the device systems stored in event details.
     */
    public static final String DETAIL_DEVICE_SYSTEMS = "zenoss.device.systems";

    /**
     * Constant for the device groups stored in event details.
     */
    public static final String DETAIL_DEVICE_GROUPS = "zenoss.device.groups";

    /**
     * Constant for the device ip address stored in event details.
     */
    public static final String DETAIL_DEVICE_IP_ADDRESS = "zenoss.device.ip_address";

    /**
     * Constant for the device location stored in event details.
     */
    public static final String DETAIL_DEVICE_LOCATION = "zenoss.device.location";

    /**
     * Constant for migrated events update time (used to bypass post-processing of events which have already
     * been processed by the system.
     */
    public static final String DETAIL_MIGRATE_UPDATE_TIME = "zenoss.event.migrate_update_time";

    /**
     * The constant representing devices in production.
     */
    public static final int PRODUCTION_STATE_PRODUCTION = 1000;

    /**
     * Open event statuses.
     */
    public static final Set<EventStatus> OPEN_STATUSES = Collections.unmodifiableSet(
            EnumSet.of(EventStatus.STATUS_NEW, EventStatus.STATUS_ACKNOWLEDGED, EventStatus.STATUS_SUPPRESSED));

    /**
     * Closed event statuses.
     */
    public static final Set<EventStatus> CLOSED_STATUSES = Collections.unmodifiableSet(
            EnumSet.of(EventStatus.STATUS_CLOSED, EventStatus.STATUS_AGED, EventStatus.STATUS_CLEARED));
}
