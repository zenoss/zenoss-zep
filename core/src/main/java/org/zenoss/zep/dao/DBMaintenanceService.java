/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao;

import org.zenoss.zep.ZepException;

/**
 * Utility service used to perform maintenance tasks against the database.
 */
public interface DBMaintenanceService {
    /**
     * Optimizes tables used in the database schema (reclaims deleted space and optimizes the indexes).
     *
     * @throws ZepException If the database cannot be optimized.
     */
    public void optimizeTables() throws ZepException;
}
