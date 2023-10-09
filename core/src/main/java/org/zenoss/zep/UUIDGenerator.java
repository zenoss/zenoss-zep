/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep;

import java.util.UUID;

/**
 * Interface used to generate UUIDs in ZEP.
 */
public interface UUIDGenerator {
    /**
     * Generates a UUID for use in ZEP.
     *
     * @return Generated UUID.
     */
    UUID generate();
}
