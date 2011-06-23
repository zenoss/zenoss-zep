/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep;

import java.util.Set;

/**
 * Context information available to event plug-ins.
 */
public interface EventContext {
    
    /**
     * Returns the set of clear classes for the event (set in clear events).
     * 
     * @return The set of clear classes for the event.
     */
    public Set<String> getClearClasses();

}
