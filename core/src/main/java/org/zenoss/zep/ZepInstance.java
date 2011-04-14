/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep;

import java.util.Map;

/**
 * Returns properties of the currently running instance of ZEP.
 */
public interface ZepInstance {
    /**
     * Returns the unique identifier for this instance of ZEP.
     * 
     * @return The unique identifier used for this instance of ZEP.
     */
    public String getId();

    /**
     * Returns the configuration properties loaded from the
     * <code>zenoss-zep.properties</code> files.
     * 
     * @return The configuration properties for this ZEP instance.
     */
    public Map<String, String> getConfig();
}
