/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class EventDaoUtils {
    private static final Charset UTF8 = Charset.forName("UTF-8");

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
        final String subElementUuid = actor.getElementSubUuid();
        final String eventKey = event.getEventKey();
        final String eventClass = event.getEventClass();
        // To clear an event, need a minimum of event class and element id specified.
        if (elementId.isEmpty() || eventClass.isEmpty()) {
            return null;
        }
        final char separator = '|';

        final String hash;
        if (!subElementUuid.isEmpty()) {
            hash = join(separator, subElementUuid, eventClass, eventKey);
        }
        else {
            hash = join(separator, elementId, subElementId, eventClass, eventKey);
        }
        try {
            return MessageDigest.getInstance("SHA-1").digest(hash.getBytes(UTF8));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<byte[]> createClearHashes(Event event, Set<String> clearClasses) {
        final EventActor actor = event.getActor();
        if (actor == null) {
            return Collections.emptyList();
        }
        final String elementId = actor.getElementIdentifier();
        final String subElementId = actor.getElementSubIdentifier();
        final String subElementUuid = actor.getElementSubUuid();
        final String eventKey = event.getEventKey();
        // To clear an event, need element id specified.
        if (elementId.isEmpty()) {
            return Collections.emptyList();
        }
        final List<byte[]> clearHashes = new ArrayList<byte[]>(clearClasses.size() * 2);
        final char separator = '|';
        try {
            final MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            for (String clearClass : clearClasses) {
                // Create old-style clear hash to match events without UUID information
                final String hash = join(separator, elementId, subElementId, clearClass, eventKey);
                clearHashes.add(sha1.digest(hash.getBytes(UTF8)));

                // Create new-style clear hash to match events with UUID information
                if (!subElementUuid.isEmpty()) {
                    final String newHash = join(separator, subElementUuid, clearClass, eventKey);
                    clearHashes.add(sha1.digest(newHash.getBytes(UTF8)));
                }
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return clearHashes;
    }

    public static String join(char separator, String... args) {
        final StringBuilder sb = new StringBuilder();
        for (String arg : args) {
            if (sb.length() > 0) {
                sb.append(separator);
            }
            sb.append(arg);
        }
        return sb.toString();
    }
}
