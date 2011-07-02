/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
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
    public UUID generate();
}
