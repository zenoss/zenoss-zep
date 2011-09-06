/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
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
