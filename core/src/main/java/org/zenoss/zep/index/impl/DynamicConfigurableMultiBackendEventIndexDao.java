package org.zenoss.zep.index.impl;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.scheduling.TaskScheduler;
import org.zenoss.zep.Messages;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSummaryBaseDao;
import org.zenoss.zep.index.WorkQueueBuilder;
import org.zenoss.zep.utils.KeyValueStore;

import java.io.IOException;
import java.util.*;

public class DynamicConfigurableMultiBackendEventIndexDao extends MultiBackendEventIndexDao {

    private final KeyValueStore configStore;
    private volatile boolean closed = false;

    public DynamicConfigurableMultiBackendEventIndexDao(String name, EventSummaryBaseDao eventDao,
            WorkQueueBuilder queueBuilder, KeyValueStore stateStore, KeyValueStore configStore,
            Messages messages, TaskScheduler scheduler, UUIDGenerator uuidGenerator) {
        super(name, eventDao, queueBuilder, stateStore, messages, scheduler, uuidGenerator);
        this.configStore = configStore;
    }

    public void init() throws ZepException {
        super.init();
        if (!useRedis) {
            return;
        }
        updateRedis();

        Thread thread = new Thread() {
            @Override
            public void run() {
                logger.info(getName() + " - started");
                while (!closed) {
                    try {
                        // Sleep for a second
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) { /* ignore */ }
                        syncBackendsFromRedis();
                    } catch (ZepException e) {
                        logger.error(getName() + " - error while updating configuration from Redis: " + e.getMessage(), e);
                    }
                }
                logger.info(getName() + " - stopped");
            }
        };
        thread.setName("Multi-backend event indexer configuration monitoring thread (" + DynamicConfigurableMultiBackendEventIndexDao.this.getName() + ")");
        thread.setDaemon(true);
        thread.start();
    }

    private void updateRedis() throws ZepException {
        try {
            logger.info("copying backend configuration of " + getName() + " to Redis");
            configStore.checkAndSetAll(new Function<Map<byte[], byte[]>, Map<byte[], byte[]>>() {
                @Override
                public Map<byte[], byte[]> apply(Map<byte[], byte[]> input) {
                    Map<byte[], byte[]> data = Maps.newHashMap();
                    for (EventIndexBackendConfiguration config : getInitialBackendConfigurations()) {
                        //overwrite any existing config in config store,
                        //i.e. on startup always use initial config from file
                        final byte[] key = config.getName().getBytes();
                        data.put(key, config.toString().getBytes());
                    }
                    return data;
                }
            });
            logger.debug("done copying backend configuration of " + getName() + " to Redis");
        } catch (IOException e) {
            logger.debug("failed copying backend configuration of " + getName() + " to Redis", e);
            throw new ZepException(e);
        } catch (RuntimeException e) {
            logger.debug("failed copying backend configuration of " + getName() + " to Redis", e);
            throw e;
        }
    }

    private void syncBackendsFromRedis() throws ZepException {
        try {
            final List<EventIndexBackendConfiguration> backends = Lists.newArrayList();
            final Set<String> forceRebuild = Sets.newHashSet();
            final Set<String> toClear = Sets.newHashSet();
            configStore.checkAndSetAll(new Function<Map<byte[], byte[]>, Map<byte[], byte[]>>(){
                @Override
                public Map<byte[], byte[]> apply(Map<byte[], byte[]> input) {
                    backends.clear();
                    for (byte[] keyBytes : Sets.newHashSet(input.keySet())) {
                        final String backendId = new String(keyBytes);
                        EventIndexBackendConfiguration current = getBackendConfiguration(backendId);
                        final byte[] bytes = input.get(keyBytes);
                        final String s = new String(bytes);
                        final EventIndexBackendConfiguration c = EventIndexBackendConfiguration.parse(s);
                        if (c == null) {
                            logger.error("Unparsable configuration: " + s);
                            if (current != null)
                                input.put(keyBytes, current.toString().getBytes());
                        } else {
                            if (c.getLastRebuilt() != null && c.getLastRebuilt() <= 0) {
                                forceRebuild.add(c.getName());
                                c.setLastRebuilt(System.currentTimeMillis());
                            }
                            if (c.getLastCleared() != null && c.getLastCleared() <= 0) {
                                toClear.add(c.getName());
                                c.setLastCleared(System.currentTimeMillis());
                            }
                            if (current != null)
                                current.merge(c);
                            else
                                current = c;
                            input.put(keyBytes, current.toString().getBytes());
                        }
                        backends.add(current);
                    }
                    return input;
                }
            });
            super.setBackends(backends);
            for (String backendId : toClear)
                clear(backendId);
            for (String backendId : forceRebuild)
                forceRebuild(backendId);
        } catch (IOException e) {
            throw new ZepException(e);
        } catch (RuntimeException e) {
            throw new ZepException(e);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.closed = true;
    }

}
