/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep;

import org.zenoss.protobufs.zep.Zep.Event;

import java.util.List;
import java.util.Set;

/**
 * <p>Used to generate clear fingerprints for an event. Events with severity of CLEAR can return
 * more than one clear fingerprint which will match the fingerprints of all events this event
 * should clear. Events with other severities should return one fingerprint (returning zero
 * fingerprints will disable event clearing for the event, and returning more than one fingerprint
 * will only result in the first fingerprint being used).</p>
 *
 * <p>The default implementation uses this clear fingerprint for CLEAR severity events:</p>
 *
 * <p>For all of the clear classes in {@link org.zenoss.zep.plugins.EventPreCreateContext#getClearClasses()},
 * create a pipe-delimited string of the following fields in order:</p>
 *
 * <ul>
 *     <li>event.actor.element_identifier</li>
 *     <li>event.actor.element_sub_identifier</li>
 *     <li>clearClass</li>
 *     <li>event.event_key</li>
 * </ul>
 *
 * <p>and for events which have a element_sub_uuid:</p>
 *
 * <ul>
 *     <li>event.actor.element_sub_uuid</li>
 *     <li>clearClass</li>
 *     <li>event.event_key</li>
 * </ul>
 *
 * <p>Using a sub-element UUID for event clearing supports clearing events on components which
 * can be moved to different devices (i.e. VMotion events for VMware VMs).</p>
 *
 * <p>For non-CLEAR events, the fingerprint is created using either the first or second algorithm
 * above (using the event's event class as the clearClass) depending on the presence of the
 * <code>event.actor.element_sub_uuid</code>.</p>
 */
public interface ClearFingerprintGenerator {

    /**
     * Generates a clear fingerprint for the specified
     * non-{@link org.zenoss.protobufs.zep.Zep.EventSeverity#SEVERITY_CLEAR} event.
     * 
     * @param event Event to generate clear fingerprint from.
     * @return A clear fingerprint for the event, or null to disable event clearing (not
     * recommended).
     */
    String generateClearFingerprint(Event event);

    /**
     * Generates clear fingerprint(s) for the specified
     * {@link org.zenoss.protobufs.zep.Zep.EventSeverity#SEVERITY_CLEAR} event and clear classes.
     * CLEAR events can return any number of clear fingerprints - these will be used to clear all
     * of the active events with the same clear fingerprint. A clear event which returns zero clear
     * fingerprints will be dropped by the system (as it can't clear any associated events).
     *
     * @param event Event to generate clear fingerprint from.
     * @param clearClasses Set of clear classes used to generate fingerprints.
     * @return A list of clear fingerprint(s) for the specified event and clear classes.
     */
    List<String> generateClearFingerprints(Event event, Set<String> clearClasses);
    
}
