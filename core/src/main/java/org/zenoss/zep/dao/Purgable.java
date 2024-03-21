/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao;

import org.zenoss.zep.ZepException;

import java.util.concurrent.TimeUnit;

/**
 * Interface implemented by DAOs that can be purged regularly.
 */
public interface Purgable {
    /**
     * Purge records which are older than the specified time.
     *
     * @param duration
     *            Duration of time.
     * @param unit
     *            Time unit.
     * @throws org.zenoss.zep.ZepException
     *             If an exception occurs purging
     */
    void purge(int duration, TimeUnit unit)
            throws ZepException;
}
