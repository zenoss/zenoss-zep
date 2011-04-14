/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */

package org.zenoss.zep.events;

import org.zenoss.protobufs.zep.Zep.EventDetailItem;

import java.util.Collections;
import java.util.Map;

/**
 * Event which is sent when the index detail configuration changes.
 */
public class IndexDetailsUpdatedEvent extends ZepEvent {
    private final Map<String,EventDetailItem> indexedDetails;

    public IndexDetailsUpdatedEvent(Object source, Map<String,EventDetailItem> indexedDetails) {
        super(source);
        this.indexedDetails = Collections.unmodifiableMap(indexedDetails);
    }

    /**
     * Returns the currently indexed details.
     *
     * @return The currently indexed details.
     */
    public Map<String, EventDetailItem> getIndexedDetails() {
        return indexedDetails;
    }
}
