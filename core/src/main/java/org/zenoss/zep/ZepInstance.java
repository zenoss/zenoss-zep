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
