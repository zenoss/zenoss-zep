package org.zenoss.zep.dao;

import org.zenoss.protobufs.zep.Zep.EventSummary;

import java.util.Collections;
import java.util.List;

public class EventBatch {
    public final EventBatchParams nextParams;
    public final List<EventSummary> events;

    public EventBatch(List<EventSummary> events, long nextLastSeen, String nextUuid) {
        this.events = Collections.unmodifiableList(events);
        this.nextParams = new EventBatchParams(nextLastSeen, nextUuid);
    }
}
