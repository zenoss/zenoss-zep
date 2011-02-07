/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2011, Zenoss Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/**
 * Renames threads while they are running, then restores the name back to
 * the original.
 */
public class ThreadRenamingRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ThreadRenamingRunnable.class);
    private final Runnable runnable;
    private final String name;

    public ThreadRenamingRunnable(Runnable runnable, String name) {
        if (runnable == null || name == null) {
            throw new NullPointerException();
        }
        this.runnable = runnable;
        this.name = name;
    }

    @Override
    public void run() {
        final String previousName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName(this.name);
        } catch (SecurityException e) {
            logger.debug("Exception changing name", e);
        }
        try {
            this.runnable.run();
        } finally {
            try {
                Thread.currentThread().setName(previousName);
            } catch (SecurityException e) {
                logger.debug("Exception changing name", e);
            }
        }
    }
}
