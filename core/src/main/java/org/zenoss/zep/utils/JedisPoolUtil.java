package org.zenoss.zep.utils;

import com.google.api.client.util.ExponentialBackOff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;

public final class JedisPoolUtil {

    private static final Logger logger = LoggerFactory.getLogger(JedisPoolUtil.class);

    private final JedisPool pool;
    private volatile Boolean supportsEval;
    private volatile boolean ready;
    private final int maxConnectionWaitMillis;
    private final int maxTransactionWaitMillis;

    public JedisPoolUtil(JedisPool pool, int maxConnectionWaitMillis, int maxTransactionWaitMillis)
    {
        this.pool = pool;
        this.maxConnectionWaitMillis = maxConnectionWaitMillis;
        this.maxTransactionWaitMillis = maxTransactionWaitMillis;
        this.ready = false;
    }

    public boolean isReady() {
        if(!this.ready) {
            synchronized (this) {
                if(!this.ready) {
                    Jedis jedis = null;
                    try {
                        jedis = this.pool.getResource();
                        this.ready = true;
                    } catch (JedisConnectionException e) {
                        // Not connected
                    } finally {
                        if (jedis!=null)
                            pool.returnResource(jedis);
                    }
                }
            }
        }
        return this.ready;
    }

    public boolean supportsEval() {
        Boolean result = supportsEval;
        if (result == null) {
            synchronized (this) {
                result = supportsEval;
                if (result == null) {
                    try {
                        useJedis(new JedisUser<Object>() {
                            @Override
                            public Object use(Jedis jedis) throws RedisTransactionCollision {
                                return jedis.eval("do return end".getBytes());
                            }
                        });
                        supportsEval = result = true;
                    } catch (JedisDataException e) {
                        if (e.getMessage().contains("unknown command")) {
                            logger.info("Redis EVAL command is unsupported. Resorting to watch/multi/exec.");
                            supportsEval = result = false;
                        } else {
                            throw e;
                        }
                    }
                }
            }
        }
        return result;
    }

    /** Provides a managed {@link Jedis} resource for temporary use.
     * Automatically re-executes the given block of code if there is a
     * connection problem (and maxConnectionAttempts is greater than zero).
     * Automatically re-executes the given block of code if it throws
     * {@link RedisTransactionCollision} (and maxTransactionAttempts is
     * greater than zero).
     */

    public <T> T useJedis(JedisUser<T> user) {
        ExponentialBackOff backoffTracker = null;
        int collisions = 0;
        while (true) {
            try {
                return useJedisOnce(user);
            } catch (RedisTransactionCollision e) {
                collisions++;
                if (backoffTracker == null) {
                    backoffTracker = new ExponentialBackOff.Builder().
                            setMaxElapsedTimeMillis(maxTransactionWaitMillis).
                            build();
                }
                long backOff;
                try {
                    backOff = backoffTracker.nextBackOffMillis();
                } catch (IOException ioe) {
                    // should never happen
                    throw new RuntimeException(ioe);
                }
                long elapsed = backoffTracker.getElapsedTimeMillis();
                if (ExponentialBackOff.STOP == backOff) {
                    if (logger.isDebugEnabled())
                        logger.error("Too many Redis collisions (" + collisions + "). Gave up after " + elapsed + "ms.", e);
                    else
                        logger.error("Too many Redis collisions (" + collisions + "). Gave up after " + elapsed + "ms.");
                    throw e;
                } else {
                    if (logger.isDebugEnabled())
                        logger.debug("Collision detected (" + collisions + " in " + elapsed + "ms). Backing off for " + backOff + "ms");
                    try {
                        Thread.sleep(backOff);
                    } catch (InterruptedException ie) { /* no biggie */ }
                }
            }
        }
    }

    private <T> T useJedisOnce(JedisUser<T> user) throws RedisTransactionCollision {
        ExponentialBackOff backoffTracker = null;
        Jedis jedis = null;
        int exceptions = 0;
        while (true) {
            try {
                if (jedis == null)
                    jedis = pool.getResource();
                T result = user.use(jedis);
                pool.returnResource(jedis);
                return result;
            } catch (JedisConnectionException e) {
                exceptions++;
                if (jedis != null)
                    pool.returnBrokenResource(jedis);
                if (backoffTracker == null) {
                    backoffTracker = new ExponentialBackOff.Builder().
                            setMaxElapsedTimeMillis(maxConnectionWaitMillis).
                            build();
                }
                long backOff;
                try {
                    backOff = backoffTracker.nextBackOffMillis();
                } catch (IOException ioe) {
                    // should never happen
                    throw new RuntimeException(ioe);
                }
                long elapsed = backoffTracker.getElapsedTimeMillis();
                if (ExponentialBackOff.STOP == backOff) {
                    if (logger.isDebugEnabled())
                        logger.error("Giving up after " + elapsed + "ms, and " + exceptions + " consecutive Redis connection exceptions. Most recent: " + e.getMessage(), e);
                    else
                        logger.error("Giving up after " + elapsed + "ms, and " + exceptions + " consecutive Redis connection exceptions. Most recent: " + e.getMessage());
                    throw e;
                } else {
                    if (logger.isDebugEnabled())
                        logger.debug("Redis connection exception (" + exceptions + " in " + elapsed + "ms). Backing off for " + backOff + "ms");
                    try {
                        Thread.sleep(backOff);
                    } catch (InterruptedException ie) { /* no biggie */ }
                    jedis = pool.getResource();
                }
            } catch (RuntimeException e) {
                if (jedis != null)
                    pool.returnResource(jedis);
                throw e;
            }
        }
    }

}
