/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/
package org.zenoss.zep.index.impl;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zep.index.WorkQueue;
import org.zenoss.zep.utils.JedisPoolUtil;
import org.zenoss.zep.utils.JedisUser;
import org.zenoss.zep.utils.RedisTransactionCollision;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * An unbounded de-duping task queue backed by Redis data structures.
 *
 * This queue orders tasks FIFO (first-in-first-out).
 *
 * The <em>head</em> of the queue is that task that has been on the queue
 * the longest time.  The <em>tail</em> of the queue is that task that has
 * been on the queue the shortest time. New tasks are inserted at the tail
 * of the queue, unless the queue already contains them, in which case they
 * are not re-inserted. The queue retrieval operations obtain tasks at the
 * head of the queue.
 *
 * Copies of retrieved tasks are temporarily kept in an "in-progress"
 * holding area until they are marked "complete", after which time the copies
 * are removed. Tasks which are "in-progress" for too long are assumed to
 * have failed, and are automatically re-inserted at the tail of the queue.
 */
public class RedisWorkQueue implements WorkQueue {

    private static Logger logger = LoggerFactory.getLogger(RedisWorkQueue.class);

    private long pollIntervalInNanos;
    private long inProgressDurationInMillis;
    private final JedisPoolUtil pool;
    private final String name;
    private final String queueListKey;
    private final String queueSetKey;
    private final String holdZsetKey;
    private volatile JedisUser<Long> requeueJedisUser;
    private final JedisUser<Long> sizeJedisUser;
    private final List<String> queueKeys;
    private final List<String> allKeys;

    private static final String NEGATIVE_INF = "-inf";
    private static final long MINIMUM_POLL_INTERVAL = TimeUnit.MICROSECONDS.toNanos(100);
    private static final long MAXIMUM_POLL_INTERVAL = TimeUnit.SECONDS.toNanos(1);
    private static final long MINIMUM_IN_PROGRESS_DURATION = TimeUnit.SECONDS.toMillis(1);
    private static final long MAXIMUM_IN_PROGRESS_DURATION = TimeUnit.DAYS.toMillis(30);



    public RedisWorkQueue(JedisPoolUtil pool, String name) {
        this.pool = pool;
        this.name = name;
        this.queueListKey = "zep.work.queue.list:" + name;
        this.queueSetKey = "zep.work.queue.set:" + name;
        this.holdZsetKey = "zep.work.inprogress.zset:" + name;
        this.queueKeys = Lists.newArrayList(queueListKey, queueSetKey);
        this.allKeys = Lists.newArrayList(queueListKey, queueSetKey, holdZsetKey);
        requeueJedisUser = null;
        sizeJedisUser = new SizeJedisUser();
        setInProgressDuration(1, TimeUnit.MINUTES);
        setPollInterval(1, TimeUnit.MILLISECONDS);
    }

    public String toString() {
        return new StringBuilder()
                .append(RedisWorkQueue.class.getSimpleName())
                .append("[")
                .append(name)
                .append("]")
                .toString();
    }

    public void clearAll() {
        pool.useJedis(new JedisUser<Object>() {
            @Override
		public Object use(Jedis jedis) throws RedisTransactionCollision {
                jedis.del(queueListKey);
                jedis.del(queueSetKey);
                jedis.del(holdZsetKey);
                return null;
            }
	    });
    }

    @Override
    public boolean isReady() {
        return this.pool.isReady();
    }

    @Override
    public void complete(EventIndexBackendTask task) {
        if (task == null) return;
        complete(serialize(task));
    }

    @Override
    public void completeAll(Collection<EventIndexBackendTask> tasks) {
        if (tasks == null || tasks.isEmpty()) return;
        String[] serialized = new String[tasks.size()];
        int i=0;
        for (EventIndexBackendTask task : tasks) {
            serialized[i++] = serialize(task);
        }
        complete(serialized);
    }

    @Override
    public void add(EventIndexBackendTask task) {
        if (task == null) return;
        addAll(Collections.singleton(task));
    }

    @Override
    public void addAll(Collection<EventIndexBackendTask> tasks) {
        if (tasks == null || tasks.isEmpty())
            return;
        if (pool.supportsEval())
            pool.useJedis(new PushEvalJedisUser(tasks));
        else
            pool.useJedis(new PushTxJedisUser(tasks));
    }

    private JedisUser<Long> getRequeueJedisUser() {
        JedisUser<Long> result = this.requeueJedisUser;
        if (result == null) {
            synchronized(this) {
                result = this.requeueJedisUser;
                if (result == null) {
                    if (pool.supportsEval()) {
                        this.requeueJedisUser = result = new RequeueEvalJedisUser();
                    } else {
                        this.requeueJedisUser = result = new RequeueTxJedisUser();
                    }
                }
            }
        }
        return result;
    }

    @Override
    public long requeueOldTasks() {
        return pool.useJedis(this.getRequeueJedisUser());
    }

    @Override
    public long size() {
        return pool.useJedis(sizeJedisUser);
    }

    public void setPollInterval(long timeout, TimeUnit unit) {
        long nanos = unit.toNanos(timeout);
        if (nanos < MINIMUM_POLL_INTERVAL)
            nanos = MINIMUM_POLL_INTERVAL;
        else if (nanos > MAXIMUM_POLL_INTERVAL)
            nanos = MAXIMUM_POLL_INTERVAL;
        this.pollIntervalInNanos = nanos;
    }

    public void setInProgressDuration(long duration, TimeUnit unit) {
        long millis = unit.toMillis(duration);
        if (millis < MINIMUM_IN_PROGRESS_DURATION)
            millis = MINIMUM_IN_PROGRESS_DURATION;
        else if (millis > MAXIMUM_IN_PROGRESS_DURATION)
            millis = MAXIMUM_IN_PROGRESS_DURATION;
        this.inProgressDurationInMillis = millis;
    }

    private String serialize(EventIndexBackendTask task) {
        return task.toString();
    }

    private EventIndexBackendTask deserialize(String task) {
        String s = new String(task);
        try {
            return EventIndexBackendTask.parse(s);
        } catch (NullPointerException e) {
            logger.warn("Encountered unparsable task: " + s, e);
            //TODO: increment some metric
            complete(task);
            return null;
        } catch (IllegalArgumentException e) {
            logger.warn("Encountered unparsable task: " + s, e);
            //TODO: increment some metric
            complete(task);
            return null;
        }
    }

    private void complete(final String... tasks) {
        pool.useJedis(new JedisUser<Boolean>() {
            @Override
            public Boolean use(Jedis jedis) throws RedisTransactionCollision {
                return jedis.zrem(holdZsetKey, tasks) > 0;
            }
        });
    }

    @Override
    public List<EventIndexBackendTask> poll(int maxSize, int timeout, TimeUnit unit) throws InterruptedException {
        List<EventIndexBackendTask> result = poll(maxSize);
        final long due = System.nanoTime() + unit.toNanos(timeout);
        while (result == null || result.isEmpty()) {
            final long remaining = due - System.nanoTime();
            if (remaining <= 0)
                return Collections.emptyList();
            final long nanos = (remaining < pollIntervalInNanos) ? remaining : pollIntervalInNanos;
            Thread.sleep(nanos / 1000000, (int)nanos % 1000000);
            result = poll(maxSize);
        }
        return result;
    }

    private List<EventIndexBackendTask> poll(int maxSize) {
        List<String> fetched;
        if (pool.supportsEval())
            fetched = pool.useJedis(new PollEvalJedisUser(maxSize));
        else
            fetched = pool.useJedis(new PollTxJedisUser(maxSize));
        if (fetched == null || fetched.isEmpty())
            return Collections.emptyList();
        List<EventIndexBackendTask> tasks = Lists.newArrayListWithExpectedSize(fetched.size());
        for (String s : fetched) {
            tasks.add(deserialize(s));
        }
        return tasks;
    }

    private class SizeJedisUser implements JedisUser<Long> {
        @Override
        public Long use(Jedis jedis) {
            return jedis.llen(queueListKey);
        }
    }

    private static final ThreadLocal<Random> THREAD_LOCAL_RANDOM = new ThreadLocal<Random>(){
        @Override
        protected Random initialValue() {
            return new Random();
        }
    };
    private static char[] RANDOM_KEY_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
    private static String randomKey() {
        Random random = THREAD_LOCAL_RANDOM.get();
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<64; i++) {
            sb.append(RANDOM_KEY_CHARS[random.nextInt(RANDOM_KEY_CHARS.length)]);
        }
        return sb.toString();
    }



    /* -------------------------------------------------------------------- */
    /* Methods based on "eval" support in Redis, available since Redis 2.6. */
    /* -------------------------------------------------------------------- */

    /**
     * KEYS[1]: queueListKey
     * KEYS[2]: queueSetKey
     * ARGV[*]: serialized tasks
     */
    private static final String LUA_PUSH = (""
            + " for i, t in ipairs(ARGV) do"
            + "   if redis.call('sadd', KEYS[2], t) > 0 then"
            + "     redis.call('lpush', KEYS[1], t);"
            + "   end"
            + " end"
    ).replaceAll("\\s+"," ");

    private class PushEvalJedisUser implements JedisUser<Object> {
        private final List<String> args;

        public PushEvalJedisUser(Collection<EventIndexBackendTask> tasks) {
            args = Lists.newArrayListWithExpectedSize(tasks.size());
            for (EventIndexBackendTask task : tasks) {
                args.add(serialize(task));
            }
        }

        @Override
        public Object use(Jedis jedis) {
            jedis.eval(LUA_PUSH, queueKeys, args);
            return null;
        }
    }

    /**
     * KEYS[1]: queueListKey
     * KEYS[2]: queueSetKey
     * KEYS[3]: holdZsetKey
     * ARGV[1]: current milliseconds since epoch
     * ARGV[2]: maxSize
     */
    private static final String LUA_POLL = (""
            + " do"
            + "   local len = redis.call('llen', KEYS[1]);"
            + "   if len > 0 then"
            + "     local n = tonumber(ARGV[2]);"
            + "     if len < n then"
            + "       n = len;"
            + "     end"
            + "     local v = redis.call('lrange', KEYS[1], -n, -1);"
            + "     redis.call('ltrim', KEYS[1], 0, -(1+n));"
            + "     for i, e in ipairs(v) do"
            + "       redis.call('srem', KEYS[2], e);"
            + "       redis.call('zadd', KEYS[3], ARGV[1], e);"
            + "     end"
            + "     return v;"
            + "   end"
            + " end"
    ).replaceAll("\\s+"," ");

    private class PollEvalJedisUser implements JedisUser<List<String>> {
        private final int maxSize;

        public PollEvalJedisUser(int maxSize) {
            this.maxSize = maxSize;
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<String> use(Jedis jedis) {
            String currentTime = Long.toString(System.currentTimeMillis());
            String maxSize = Long.toString(this.maxSize);
            List<String> result = (List<String>) jedis.eval(LUA_POLL, allKeys, Lists.newArrayList(currentTime, maxSize));
            return result == null ? null : Lists.reverse(result);
        }
    }

    /**
     * KEYS[1]: queueListKey
     * KEYS[2]: queueSetKey
     * KEYS[3]: holdZsetKey
     * ARGV[1]: minStartTime
     * ARGV[2]: maxStartTime
     *
     * Returns a count of the tasks that were taken out of the "in-progress"
     * queue and (possibly) put back into the main queue.
     */
    private static final String LUA_REQUEUE = (""
            + " do"
            + "   local c = 0;"
            + "   for t in redis.call('zrangebyscore', KEYS[3], ARGV[1], ARGV[2]) do"
            + "     c = c + 1"
            + "     if redis.call('sadd', KEYS[2], t) > 0 then"
            + "       redis.call('lpush', KEYS[1], t);"
            + "     end"
            + "     redis.call('zrem', KEYS[3], t);"
            + "   end"
            + "   return c;"
            + " end"
    ).replaceAll("\\s+", " ");

    private class RequeueEvalJedisUser implements JedisUser<Long> {
        @Override
        public Long use(Jedis jedis) throws RedisTransactionCollision {
            long maxStartTime = System.currentTimeMillis() - inProgressDurationInMillis;
            String cutoff = Long.toString(maxStartTime);
            return (Long) jedis.eval(LUA_REQUEUE, allKeys, Lists.newArrayList(NEGATIVE_INF, cutoff));
        }
    }


    /* -------------------------------------------------------------------- */
    /* Methods based on watch/multi/exec optimistic transactions in Redis.  */
    /* -------------------------------------------------------------------- */

    private class PushTxJedisUser implements JedisUser<Boolean> {
        private final String[] values;

        public PushTxJedisUser(Collection<EventIndexBackendTask> tasks) {
            values = new String[tasks.size()];
            int i=0;
            for (EventIndexBackendTask task : tasks)
                values[i++] = serialize(task);
        }

        @Override
        public Boolean use(Jedis jedis) throws RedisTransactionCollision {
            String randomKey = randomKey();
            try {
                jedis.sadd(randomKey, values);
                jedis.watch(queueSetKey);
                Set<String> toPush = jedis.sdiff(randomKey, queueSetKey);
                if (toPush.isEmpty()) {
                    jedis.unwatch();
                    return false;
                } else {
                    Transaction tx = jedis.multi();
                    String[] values = toPush.toArray(new String[toPush.size()]);
                    tx.sadd(queueSetKey, values);
                    tx.lpush(queueListKey, values);
                    if (tx.exec() == null)
                        throw new RedisTransactionCollision(new String(queueSetKey));
                    return true;
                }
            } finally {
                jedis.del(randomKey);
            }
        }
    }

    private class PollTxJedisUser implements JedisUser<List<String>> {
        private final int maxSize;
        public PollTxJedisUser(int maxSize) {
            this.maxSize = maxSize;
        }
        @Override
        public List<String> use(Jedis jedis) throws RedisTransactionCollision {
            jedis.watch(queueListKey);
            if (jedis.llen(queueListKey) == 0) {
                jedis.unwatch();
                return null;
            } else {
                List<String> elements = Lists.reverse(jedis.lrange(queueListKey, -maxSize, -1));
                Transaction tx = jedis.multi();
                tx.ltrim(queueListKey, 0, -(1+elements.size()));
                tx.srem(queueSetKey, elements.toArray(new String[elements.size()]));
                //TODO: perhaps combine all the ZADD calls into one, but probably it doesn't matter since it's a multi.
                long now = System.currentTimeMillis();
                for (String e : elements) {
                    tx.zadd(holdZsetKey, now, e);
                }
                if (tx.exec() == null)
                    throw new RedisTransactionCollision(new String(queueListKey));
                return elements;
            }
        }
    }

    private class RequeueTxJedisUser implements JedisUser<Long> {
        @Override
        public Long use(Jedis jedis) throws RedisTransactionCollision {
            long maxStartTime = System.currentTimeMillis() - inProgressDurationInMillis;
            String cutoff = Long.toString(maxStartTime);
            long count = 0;

            jedis.watch(holdZsetKey, queueSetKey);

            Set<String> tasks = jedis.zrangeByScore(holdZsetKey, NEGATIVE_INF, cutoff);
            Set<String> tasksAlreadyInQueue = new HashSet<String>();
            for (String task : tasks) {
                if (jedis.sismember(queueSetKey, task)) {
                    tasksAlreadyInQueue.add(task);
                }
            }
            tasks.removeAll(tasksAlreadyInQueue);

            Transaction tx = jedis.multi();
            for (String task : tasks) {
                count++;
                if (!tasksAlreadyInQueue.contains(task)) {
                    tx.sadd(queueSetKey, task);
                    tx.lpush(queueListKey, task);
                }
                tx.zrem(holdZsetKey, task);
            }
            if (tx.exec() == null)
                throw new RedisTransactionCollision(new String(holdZsetKey) + " or " + new String(queueSetKey));
            return count;
        }
    }
}
