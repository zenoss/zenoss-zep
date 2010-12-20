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

import java.util.concurrent.TimeUnit;

import org.zenoss.protobufs.zep.Zep.EventSeverity;

/**
 * Contains ZEP configuration constants for configuration names and
 * default/maximum values.
 */
public class ConfigConstants {
    private ConfigConstants() {
    }

    /**
     * Configuration setting which controls which events are aged from the
     * summary table to the archive table. Only events with a lower severity
     * than this are automatically aged to the history table. The default value
     * for this setting is {@link #DEFAULT_EVENT_AGE_DISABLE_SEVERITY}.
     */
    public static final String CONFIG_EVENT_AGE_DISABLE_SEVERITY = "event_age_disable_severity";

    /**
     * The default severity of events which should not be aged.
     */
    public static final EventSeverity DEFAULT_EVENT_AGE_DISABLE_SEVERITY = EventSeverity.SEVERITY_ERROR;

    /**
     * Configuration setting which controls the aging interval (in minutes). If
     * this is <= 0, then event aging is disabled.The default value for this
     * setting is {@link #DEFAULT_EVENT_AGE_INTERVAL_MINUTES}.
     */
    public static final String CONFIG_EVENT_AGE_INTERVAL_MINUTES = "event_age_interval_minutes";

    /**
     * Event age interval unit.
     */
    public static final TimeUnit EVENT_AGE_INTERVAL_UNIT = TimeUnit.MINUTES;

    /**
     * The default event aging interval.
     */
    public static final int DEFAULT_EVENT_AGE_INTERVAL_MINUTES = 4 * 60;

    /**
     * Configuration setting which controls the event purge interval of events
     * in the event archive (in days). This must be set to a value between 1 and
     * {@link #MAX_EVENT_ARCHIVE_PURGE_INTERVAL_DAYS}.
     */
    public static final String CONFIG_EVENT_ARCHIVE_PURGE_INTERVAL_DAYS = "event_archive_purge_interval_days";

    /**
     * The event archive purge interval unit.
     */
    public static final TimeUnit EVENT_ARCHIVE_PURGE_INTERVAL_UNIT = TimeUnit.DAYS;

    /**
     * The default value for the event archive purge interval.
     */
    public static final int DEFAULT_EVENT_ARCHIVE_PURGE_INTERVAL_DAYS = 90;

    /**
     * Configuration setting which controls the event purge interval for the
     * event occurrences table (in days).
     */
    public static final String CONFIG_EVENT_OCCURRENCE_PURGE_INTERVAL_DAYS = "event_occurrence_purge_interval_days";

    /**
     * The event occurrence purge interval unit.
     */
    public static final TimeUnit EVENT_OCCURRENCE_PURGE_INTERVAL_UNIT = TimeUnit.DAYS;

    /**
     * The default value for the event occurrence purge interval.
     */
    public static final int DEFAULT_EVENT_OCCURRENCE_PURGE_INTERVAL_DAYS = 30;

    /**
     * Configuration setting which controls how often we move events to archive
     * in days
     **/
    public static final String CONFIG_EVENT_ARCHIVE_INTERVAL_DAYS = "event_archive_interval_days";

    /**
     * The default value for the event archive interval (
     * {@link #MAX_EVENT_ARCHIVE_INTERVAL_DAYS}).
     */
    public static final int DEFAULT_EVENT_ARCHIVE_INTERVAL_DAYS = 3;

    /**
     * The time unit for archiving events from summary.
     */
    public static TimeUnit EVENT_ARCHIVE_INTERVAL_UNIT = TimeUnit.DAYS;

    /**
     * The maximum value for the event archive interval.
     */
    public static final int MAX_EVENT_ARCHIVE_INTERVAL_DAYS = 30;
}
