/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao;

import org.zenoss.zep.ZepException;

/**
 * DAO for the metadata stored for the event indexing. This contains details
 * about the index name, version number of the index, and a checksum calculated
 * of the version of the event index configuration.
 */
public interface IndexMetadataDao {
    /**
     * Returns the IndexMetadata for the specified index, or null if not found.
     *
     * @param indexName The index name to retrieve.
     * @return The IndexMetadata for the index, or null if not found.
     * @throws ZepException If an exception occurs querying the database.
     */
    IndexMetadata findIndexMetadata(String indexName) throws ZepException;

    /**
     * Updates the version information for the specified index name.
     *
     * @param indexName The index name to update.
     * @param indexVersion The version number of the index.
     * @param indexHash The hash of the index configuration.
     * @throws ZepException If an exception occurs updating the database.
     */
    void updateIndexVersion(String indexName, int indexVersion, byte[] indexHash) throws ZepException;
}
