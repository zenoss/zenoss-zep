package org.zenoss.zep.dao;

public class EventBatchParams {
    public final long nextLastSeen;
    public final String nextUuid;

    public EventBatchParams(long nextLastSeen, String nextUuid) {
        this.nextLastSeen = nextLastSeen;
        this.nextUuid = nextUuid;
    }

    public String toString() {
        return "{nextLastSeen:" + nextLastSeen + ", nextUuid:" + nextUuid + "}";
    }
}
