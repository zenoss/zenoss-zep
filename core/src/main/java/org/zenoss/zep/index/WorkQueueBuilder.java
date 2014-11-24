package org.zenoss.zep.index;

public interface WorkQueueBuilder {
    WorkQueue build(String queueId);
}
