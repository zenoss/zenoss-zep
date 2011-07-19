/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep;

import java.util.Set;

/**
 * Context information available to event plug-ins.
 */
public interface EventContext {
    
    /**
     * Returns a mutable set of clear classes for the event (set in clear events).
     * 
     * @return The set of clear classes for the event.
     */
    public Set<String> getClearClasses();

    /**
     * Sets the clear classes for this event.
     *
     * @param clearClasses Clear classes for the event.
     * @throws NullPointerException If the set of clear classes is null.
     */
    public void setClearClasses(Set<String> clearClasses);

    /**
     * Returns the current clear fingerprint generator, or null if the default algorithm will be used.
     *
     * @return The clear fingerprint generator for the event, or null if the default algorithm will be used.
     */
    public ClearFingerprintGenerator getClearFingerprintGenerator();

    /**
     * Sets the clear fingerprint generator for the event, or null if the default algorithm will be used.
     *
     * @param clearFingerprintGenerator The clear fingerprint generator for the event, or null to use the
     * default algorithm.
     */
    public void setClearFingerprintGenerator(ClearFingerprintGenerator clearFingerprintGenerator);

}
