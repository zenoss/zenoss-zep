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

package org.zenoss.zep.dao;

import org.zenoss.zep.ZepUtils;

/**
 * Bean for storing the meta data associated with an event index.
 */
public class IndexMetadata {
    private String zepInstance;
    private String indexName;
    private int indexVersion;
    private byte[] indexVersionHash;
    private long lastIndexTime;
    private long lastCommitTime;

    public IndexMetadata() {
    }

    public String getZepInstance() {
        return zepInstance;
    }

    public void setZepInstance(String zepInstance) {
        this.zepInstance = zepInstance;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public int getIndexVersion() {
        return indexVersion;
    }

    public void setIndexVersion(int indexVersion) {
        this.indexVersion = indexVersion;
    }

    public byte[] getIndexVersionHash() {
        return indexVersionHash;
    }

    public void setIndexVersionHash(byte[] indexDetailsHash) {
        this.indexVersionHash = indexDetailsHash;
    }

    public long getLastIndexTime() {
        return lastIndexTime;
    }

    public void setLastIndexTime(long lastIndexTime) {
        this.lastIndexTime = lastIndexTime;
    }

    public long getLastCommitTime() {
        return lastCommitTime;
    }

    public void setLastCommitTime(long lastCommitTime) {
        this.lastCommitTime = lastCommitTime;
    }

    @Override
    public String toString() {
        return "IndexMetadata{" +
                "zepInstance='" + zepInstance + '\'' +
                ", indexName='" + indexName + '\'' +
                ", indexVersion=" + indexVersion +
                ", indexVersionHash='" + ZepUtils.hexstr(indexVersionHash) + '\'' +
                ", lastIndexTime=" + lastIndexTime +
                ", lastCommitTime=" + lastCommitTime +
                '}';
    }
}
