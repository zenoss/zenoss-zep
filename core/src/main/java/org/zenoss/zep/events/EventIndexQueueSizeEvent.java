/*
 * Copyright (C) 2012, Zenoss Inc.  All Rights Reserved.
 */

package org.zenoss.zep.events;

public class EventIndexQueueSizeEvent extends ZepEvent {

    private final long size;
    private final String tableName;
    private final int limit;

    public EventIndexQueueSizeEvent(Object source, String tableName, long size, int limit) {
        super(source);
        this.size = size;
        this.tableName = tableName;
        this.limit = limit;
    }

    public long getSize() {
        return this.size;
    }

    public String getTableName() {
        return this.tableName;
    }

    public int getLimit() {
        return this.limit;
    }



}
