package org.zenoss.zep.utils;

import redis.clients.jedis.Jedis;

public interface JedisUser<T> {
    T use(Jedis jedis) throws RedisTransactionCollision;
}
