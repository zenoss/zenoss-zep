/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
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
     * Returns the IndexMetadata for the specified index, or null if not found.
     *
     * @param indexName The index name to retrieve.
     * @return The IndexMetadata for the index, or null if not found.
     * @throws ZepException If an exception occurs querying the database.
     */
    public IndexMetadata findIndexMetadata(String indexName) throws ZepException;

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
