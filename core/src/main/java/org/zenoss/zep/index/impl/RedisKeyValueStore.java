package org.zenoss.zep.index.impl;

import com.google.common.base.Function;
import org.zenoss.zep.utils.JedisPoolUtil;
import org.zenoss.zep.utils.JedisUser;
import org.zenoss.zep.utils.KeyValueStore;
import org.zenoss.zep.utils.RedisTransactionCollision;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class RedisKeyValueStore implements KeyValueStore {

    private final byte[] storeKey;
    private final JedisPoolUtil pool;

    public RedisKeyValueStore(String storeKey, JedisPoolUtil pool) {
        this.storeKey = ("org.zenoss.zep:" + storeKey).getBytes();
        this.pool = pool;
    }

    @Override
    public void store(final byte[] key, final byte[] value) throws IOException {
        pool.useJedis(new JedisUser<Object>() {
            @Override
            public Object use(Jedis jedis) throws RedisTransactionCollision {
                jedis.hset(storeKey, key, value);
                return null;
            }
        });
    }

    @Override
    public byte[] load(final byte[] key) throws IOException {
        return pool.useJedis(new JedisUser<byte[]>() {
            @Override
            public byte[] use(Jedis jedis) throws RedisTransactionCollision {
                return jedis.hget(storeKey, key);
            }
        });
    }

    @Override
    public void checkAndSetAll(final Function<Map<byte[], byte[]>,Map<byte[], byte[]>> mapper) {
        pool.useJedis(new JedisUser<Object>() {
            @Override
            public Object use(Jedis jedis) throws RedisTransactionCollision {
                jedis.watch(storeKey);
                Map<byte[],byte[]> data = mapper.apply(jedis.hgetAll(storeKey));
                Transaction tx = jedis.multi();
                tx.del(storeKey);
                for (Entry<byte[], byte[]> entry : data.entrySet())
                    tx.hset(storeKey, entry.getKey(), entry.getValue());
                if (tx.exec() == null)
                    throw new RedisTransactionCollision(new String(storeKey));
                return null;
            }
        });
    }

    @Override
    public Map<byte[], byte[]> loadAll() throws IOException {
        return pool.useJedis(new JedisUser<Map<byte[], byte[]>>() {
            @Override
            public Map<byte[], byte[]> use(Jedis jedis) throws RedisTransactionCollision {
                return jedis.hgetAll(storeKey);
            }
        });
    }
}
