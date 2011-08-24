/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.zep.ClearFingerprintGenerator;
import org.zenoss.zep.plugins.EventPreCreateContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class EventDaoUtils {
    /**
     * Default implementation of ClearFingerprintGenerator.
     */
    public static final ClearFingerprintGenerator DEFAULT_GENERATOR = new ClearFingerprintGenerator() {
        @Override
        public String generateClearFingerprint(Event event) {
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
            return hash;
        }

        @Override
        public List<String> generateClearFingerprints(Event event, Set<String> clearClasses) {
            final EventActor actor = event.getActor();
            if (actor == null) {
                return Collections.emptyList();
            }
            final String elementId = actor.getElementIdentifier();
            final String subElementId = actor.getElementSubIdentifier();
            final String subElementUuid = actor.getElementSubUuid();
            final boolean hasSubElementUuid = !subElementUuid.isEmpty();
            final String eventKey = event.getEventKey();
            // To clear an event, need element id specified.
            if (elementId.isEmpty()) {
                return Collections.emptyList();
            }
            final int listSize = (hasSubElementUuid) ? clearClasses.size() * 2 : clearClasses.size();
            final List<String> clearFingerprints = new ArrayList<String>(listSize);
            final char separator = '|';
            for (String clearClass : clearClasses) {
                // Create old-style clear hash to match events without UUID information
                clearFingerprints.add(join(separator, elementId, subElementId, clearClass, eventKey));

                // Create new-style clear hash to match events with UUID information
                if (hasSubElementUuid) {
                    clearFingerprints.add(join(separator, subElementUuid, clearClass, eventKey));
                }
            }
            return clearFingerprints;
        }

        @Override
        public String toString() {
            return "DEFAULT_FINGERPRINT_GENERATOR";
        }
    };

    /**
     * Create a clear hash using the default ClearFingerprintGenerator.
     *
     * @param event Event to generate a clear hash for.
     * @return The clear hash.
     */
    public static byte[] createClearHash(Event event) {
        return createClearHash(event, null);
    }

    /**
     * Create a clear hash using the specified ClearFingerprintGenerator.
     * @param event Event to generate a clear hash for.
     * @param generator Generator used to generate a clear fingerprint.
     * @return The clear hash.
     */
    public static byte[] createClearHash(Event event, ClearFingerprintGenerator generator) {
        ClearFingerprintGenerator fingerprintGenerator = generator;
        if (fingerprintGenerator == null) {
            fingerprintGenerator = DEFAULT_GENERATOR;
        }
        final String fingerprint = fingerprintGenerator.generateClearFingerprint(event);
        return (fingerprint != null) ? DaoUtils.sha1(fingerprint) : null;
    }

    /**
     * Creates clear hashes for the specified event. Uses the clear classes and the fingerprint
     * generator specified in the {@link org.zenoss.zep.plugins.EventPreCreateContext} (or the default if the generator is null)
     * to create the clear hashes.
     *
     * @param event Clear event to generate clear hashes for.
     * @param context Event context containing the clear classes and the clear fingerprint
     * generator.
     * @return A list of clear hashes for the event.
     */
    public static List<byte[]> createClearHashes(Event event, EventPreCreateContext context) {
        ClearFingerprintGenerator fingerprintGenerator = context.getClearFingerprintGenerator();
        if (fingerprintGenerator == null) {
            fingerprintGenerator = DEFAULT_GENERATOR;
        }
        final List<String> clearFingerprints = fingerprintGenerator.generateClearFingerprints(event,
                context.getClearClasses());
        if (clearFingerprints == null || clearFingerprints.isEmpty()) {
            return Collections.emptyList();
        }
        final List<byte[]> clearHashes = new ArrayList<byte[]>(clearFingerprints.size());
        for (String clearFingerprint : clearFingerprints) {
            clearHashes.add(DaoUtils.sha1(clearFingerprint));
        }
        return clearHashes;
    }

    /**
     * Joins the specified strings using the character separator.
     *
     * @param separator Separator used to join strings.
     * @param args Strings to join.
     * @return A String delimited by the specified separator.
     */
    public static String join(char separator, String... args) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                sb.append(separator);
            }
            sb.append(args[i]);
        }
        return sb.toString();
    }
}
