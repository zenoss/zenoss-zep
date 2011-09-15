/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.index;

import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.zep.ZepException;

import java.util.Map;

/**
 * Returns the current event details configuration.
 */
public interface IndexedDetailsConfiguration {
    /**
     * Returns the currently configured indexed details.
     *
     * @return A map of event details configuration definitions, keyed by
     *         the detail name as it would be found in the EventDetails
     *         array (not necessarily the same name that will be used for
     *         indexing).
     * @throws org.zenoss.zep.ZepException If an error occurs.
     */
    public Map<String, EventDetailItem> getEventDetailItemsByName() throws ZepException;
}
