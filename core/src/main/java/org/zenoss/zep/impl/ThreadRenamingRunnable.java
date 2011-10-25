/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
