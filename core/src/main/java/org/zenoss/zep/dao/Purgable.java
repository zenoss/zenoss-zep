/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */

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
    public void purge(int duration, TimeUnit unit)
            throws ZepException;
}
