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
