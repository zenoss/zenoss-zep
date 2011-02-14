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

import org.zenoss.zep.ZepException;

/**
 * DAO for the metadata stored for the event indexing. This contains details
 * about the index name, version number of the index, a checksum calculated
 * of the version of the event index configuration, and timestamps
 * for the last time events were indexed and when the index was last committed.
 */
public interface IndexMetadataDao {
    /**
     * Finds the latest index time for any instance of ZEP in the database. This
     * is used to catch-up indexing to the latest processed time.
     *
     * @return The maximum index time for any ZEP instance. If no ZEP instances
     *         have indexed the database, this will return 0.
     * @throws ZepException If an exception occurs querying the database.
     */
    public long findMaxLastIndexTime() throws ZepException;

    /**
     * Returns the IndexMetadata for the specified index, or null if not found.
     *
     * @param indexName The index name to retrieve.
     * @return The IndexMetadata for the index, or null if not found.
     * @throws ZepException If an exception occurs querying the database.
     */
    public IndexMetadata findIndexMetadata(String indexName) throws ZepException;

    /**
     * Updates the index time for the specified index. If the <code>isCommit</code>
     * parameter is true, then the last commit time is updated to be the same
     * as the last index time. If no record exists for the index, a new one is
     * created.
     *
     * @param indexName The index name to update
     * @param indexVersion The version number of the index.
     * @param indexHash The hash of the index configuration.
     * @param lastIndexTime The last index time.
     * @param isCommit If true, the last commit time is updated to be the same
     *                 as the last index time.
     * @throws ZepException If an exception occurs querying the database.
     */
    public int updateIndexMetadata(String indexName, int indexVersion, byte[] indexHash, long lastIndexTime,
                                   boolean isCommit) throws ZepException;

    /**
     * Updates the version information for the specified index name.
     *
     * @param indexName The index name to update.
     * @param indexVersion The version number of the index.
     * @param indexHash The hash of the index configuration.
     * @return The number of affected rows by the query.
     * @throws ZepException If an exception occurs updating the database.
     */
    public int updateIndexVersion(String indexName, int indexVersion, byte[] indexHash) throws ZepException;
}
