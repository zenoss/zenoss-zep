package org.zenoss.zep.index.impl;

import org.zenoss.zep.index.WorkQueue;
import org.zenoss.zep.index.WorkQueueBuilder;
import org.zenoss.zep.utils.JedisPoolUtil;

public class RedisWorkQueueBuilder implements WorkQueueBuilder {

    private final JedisPoolUtil pool;
    private final long requeueThresholdInSeconds;

    public RedisWorkQueueBuilder(JedisPoolUtil pool, long requeueThresholdInSeconds) {
        this.pool = pool;
        this.requeueThresholdInSeconds = requeueThresholdInSeconds;
    }

    @Override
    public WorkQueue build(String queueId) {
        return new RedisWorkQueue(pool, queueId, requeueThresholdInSeconds);
    }
}
