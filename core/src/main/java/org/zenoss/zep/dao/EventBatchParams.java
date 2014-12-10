package org.zenoss.zep.dao;

public class EventBatchParams {
    public final Long nextLastSeen;
    public final String nextUuid;

    public EventBatchParams(Long nextLastSeen, String nextUuid) {
        this.nextLastSeen = nextLastSeen;
        this.nextUuid = nextUuid;
    }

    public String toString() {
        return "{nextLastSeen:" + nextLastSeen + ", nextUuid:" + nextUuid + "}";
    }
}
