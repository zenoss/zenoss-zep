package org.zenoss.zep.index.impl;

import org.python.google.common.base.Strings;
import org.zenoss.zep.index.EventIndexBackend;
import org.zenoss.zep.index.impl.MultiBackendEventIndexDao.BackendStatus;

import java.util.regex.Pattern;

public final class EventIndexBackendConfiguration {

    private static final Pattern VALID_ID = Pattern.compile("\\A[\\w\\.\\-]+\\z"); // alphanum, underscore, dash, & dot

    public final String id;
    public final EventIndexBackend backend;
    public final BackendStatus status;
    public final boolean asyncUpdates;
    public final boolean honorDeletes;
    public final boolean enableRebuilder;
    public final int batchSize;


    public static EventIndexBackendConfiguration createInstance(String id, EventIndexBackend backend,
                                                                BackendStatus status, boolean asyncUpdates,
                                                                boolean honorDeletes, boolean enableRebuilder,
                                                                int batchSize) throws Exception {
        if (status == null || status == BackendStatus.DISABLED)
            return null;
        EventIndexBackendConfiguration result = new EventIndexBackendConfiguration(
                id, backend, status, asyncUpdates, honorDeletes, enableRebuilder, batchSize);
        return result;
    }


    private EventIndexBackendConfiguration(String id, EventIndexBackend backend, BackendStatus status,
                                           boolean asyncUpdates, boolean honorDeletes, boolean enableRebuilder,
                                           int batchSize) {
        if (Strings.isNullOrEmpty(id))
            throw new IllegalArgumentException("backend id must be specified");
        if (!VALID_ID.matcher(id).matches())
            throw new IllegalArgumentException("backend id can only contain alpha-numerics, underscores, dashes, and dots. Unacceptable: " + id);
        if (backend == null)
            throw new IllegalArgumentException("EventIndexBackend must not be null");
        if (status == null)
            throw new IllegalArgumentException("BackendStatus must not be null");
        this.id = id;
        this.backend = backend;
        this.status = status;
        this.asyncUpdates = asyncUpdates;
        this.honorDeletes = honorDeletes;
        this.enableRebuilder = enableRebuilder;
        this.batchSize = batchSize;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("EventIndexBackend[");
        sb.append("id:");
        sb.append(id);
        sb.append(", status:");
        sb.append(status);
        sb.append(", updates:");
        sb.append(asyncUpdates ? "async" : "sync");
        sb.append(", deletes:");
        sb.append(honorDeletes ? "honor" : "ignore");
        sb.append(", rebuilder:");
        sb.append(enableRebuilder ? "enabled" : "disabled");
        sb.append(", batchSize:");
        sb.append(batchSize);
        sb.append("]");
        return sb.toString();
    }
}
