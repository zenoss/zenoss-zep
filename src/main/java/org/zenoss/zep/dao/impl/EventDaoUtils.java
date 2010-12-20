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

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.SyslogPriority;

public class EventDaoUtils {
    public static final int syslogPriorityToInt(final SyslogPriority priority) {
        switch (priority) {
        case SYSLOG_PRIORITY_EMERG:
            return 0;
        case SYSLOG_PRIORITY_ALERT:
            return 1;
        case SYSLOG_PRIORITY_CRIT:
            return 2;
        case SYSLOG_PRIORITY_ERR:
            return 3;
        case SYSLOG_PRIORITY_WARNING:
            return 4;
        case SYSLOG_PRIORITY_NOTICE:
            return 5;
        case SYSLOG_PRIORITY_INFO:
            return 6;
        case SYSLOG_PRIORITY_DEBUG:
            return 7;
        }
        throw new IllegalArgumentException("Invalid priority: " + priority);
    }

    public static final SyslogPriority syslogPriorityFromInt(
            final int priorityInt) {
        switch (priorityInt) {
        case 0:
            return SyslogPriority.SYSLOG_PRIORITY_EMERG;
        case 1:
            return SyslogPriority.SYSLOG_PRIORITY_ALERT;
        case 2:
            return SyslogPriority.SYSLOG_PRIORITY_CRIT;
        case 3:
            return SyslogPriority.SYSLOG_PRIORITY_ERR;
        case 4:
            return SyslogPriority.SYSLOG_PRIORITY_WARNING;
        case 5:
            return SyslogPriority.SYSLOG_PRIORITY_NOTICE;
        case 6:
            return SyslogPriority.SYSLOG_PRIORITY_INFO;
        case 7:
            return SyslogPriority.SYSLOG_PRIORITY_DEBUG;
        }
        throw new IllegalArgumentException("Invalid priority: " + priorityInt);
    }

    /**
     * Calculates a hash which is used to clear events. The clear hash is
     * comprised of the element id, sub element id (if it exists), event class,
     * and event key.
     * 
     * @param event
     *            Event used to calculate hash.
     * @return The clear hash for the event, or null if the event is missing
     *         fields which are required for a clear hash.
     */
    public static byte[] createClearHash(Event event) {
        final EventActor actor = event.getActor();
        if (actor == null) {
            return null;
        }
        final String elementId = actor.getElementIdentifier();
        final String subElementId = actor.getElementSubIdentifier();
        final String eventKey = event.getEventKey();
        final String eventClass = event.getEventClass();
        // To clear an event, need a minimum of event class and element id specified.
        if (elementId.isEmpty() || eventClass.isEmpty()) {
            return null;
        }
        final char separator = '|';
        final String hash = elementId + separator + subElementId + separator
                + eventClass + separator + eventKey;
        try {
            return MessageDigest.getInstance("SHA-1").digest(
                    hash.getBytes("UTF-8"));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<byte[]> createClearHashes(Event event,
            Set<String> clearClasses) {
        List<byte[]> clearHashes = new ArrayList<byte[]>();
        final EventActor actor = event.getActor();
        if (actor == null) {
            return clearHashes;
        }
        final String elementId = actor.getElementIdentifier();
        final String subElementId = actor.getElementSubIdentifier();
        final String eventKey = event.getEventKey();
        // To clear an event, need element id specified.
        if (elementId.isEmpty()) {
            return clearHashes;
        }
        final char separator = '|';
        for (String clearClass : clearClasses) {
            final String hash = elementId + separator + subElementId
                    + separator + clearClass + separator + eventKey;
            try {
                clearHashes.add(MessageDigest.getInstance("SHA-1").digest(
                        hash.getBytes("UTF-8")));
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        return clearHashes;
    }
}
