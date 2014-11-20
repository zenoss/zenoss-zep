/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/
package org.zenoss.zep.index.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledFuture;

public abstract class SavedSearch implements Closeable {
    private final String uuid;
    private final int timeout;
    private ScheduledFuture<?> timeoutFuture;

    public SavedSearch(String uuid, int timeout) {
        this.uuid = uuid;
        this.timeout = timeout;
    }

    public final String getUuid() {
        return uuid;
    }

    public final int getTimeout() {
        return timeout;
    }

    public final synchronized void setTimeoutFuture(ScheduledFuture<?> timeoutFuture) {
        if (this.timeoutFuture != null) {
            this.timeoutFuture.cancel(false);
        }
        this.timeoutFuture = timeoutFuture;
    }

    public abstract void close() throws IOException;

    @Override
    public String toString() {
        return "SavedSearch{uuid=" + uuid + ", timeout=" + timeout + ", timeoutFuture=" + timeoutFuture + '}';
    }
}
