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
