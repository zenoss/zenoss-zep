/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.plugins;

import org.zenoss.zep.ClearFingerprintGenerator;

import java.util.Set;

/**
 * Context information available to event plug-ins.
 */
public interface EventPreCreateContext {
    
    /**
     * Returns a mutable set of clear classes for the event (set in clear events).
     * 
     * @return The set of clear classes for the event.
     */
    Set<String> getClearClasses();

    /**
     * Sets the clear classes for this event.
     *
     * @param clearClasses Clear classes for the event.
     * @throws NullPointerException If the set of clear classes is null.
     */
    void setClearClasses(Set<String> clearClasses);

    /**
     * Returns the current clear fingerprint generator, or null if the default algorithm will be used.
     *
     * @return The clear fingerprint generator for the event, or null if the default algorithm will be used.
     */
    ClearFingerprintGenerator getClearFingerprintGenerator();

    /**
     * Sets the clear fingerprint generator for the event, or null if the default algorithm will be used.
     *
     * @param clearFingerprintGenerator The clear fingerprint generator for the event, or null to use the
     * default algorithm.
     */
    void setClearFingerprintGenerator(ClearFingerprintGenerator clearFingerprintGenerator);

}
