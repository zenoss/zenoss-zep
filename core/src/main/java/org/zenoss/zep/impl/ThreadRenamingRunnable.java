/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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

    protected ThreadRenamingRunnable(String name) {
        if (name == null) {
            throw new NullPointerException();
        }
        this.runnable = null;
        this.name = name;
    }

    @Override
    public final void run() {
        final String previousName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName(this.name);
        } catch (SecurityException e) {
            logger.debug("Exception changing name", e);
        }
        try {
            if (this.runnable == null)
                this._run();
            else
                this.runnable.run();
        } finally {
            try {
                Thread.currentThread().setName(previousName);
            } catch (SecurityException e) {
                logger.debug("Exception changing name", e);
            }
        }
    }

    protected void _run() {
        String msg = "you must override the _run() method";
        logger.error(msg);
        throw new IllegalStateException(msg);
    }
}
