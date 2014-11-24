package org.zenoss.zep.index.impl;

import org.zenoss.zep.index.WorkQueue;
import org.zenoss.zep.index.WorkQueueBuilder;
import org.zenoss.zep.utils.JedisPoolUtil;

public class RedisWorkQueueBuilder implements WorkQueueBuilder {

    private final JedisPoolUtil pool;

    public RedisWorkQueueBuilder(JedisPoolUtil pool) {
        this.pool = pool;
    }

    @Override
    public WorkQueue build(String queueId) {
        return new RedisWorkQueue(pool, queueId);
    }
}
