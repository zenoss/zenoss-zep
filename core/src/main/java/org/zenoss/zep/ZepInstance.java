/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
     * <code>zeneventserver.properties</code> files.
     * 
     * @return The configuration properties for this ZEP instance.
     */
    public Map<String, String> getConfig();
}
