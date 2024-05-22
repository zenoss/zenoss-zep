/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/

package org.zenoss.zep.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.FlapTrackerDao;
import org.zenoss.zep.impl.FlapTracker;
import org.zenoss.zep.utils.JedisPoolUtil;
import redis.clients.jedis.params.SetParams;

import java.util.concurrent.TimeUnit;

public class FlapTrackerDaoImpl implements FlapTrackerDao{

    private static final Logger logger = LoggerFactory.getLogger(FlapTrackerDaoImpl.class);

    private final String REDIS_FLAP_KEY = "zenoss_event_flapping";

    @Autowired
    private JedisPoolUtil pool = null;


    private String createKey(String clearFingerPrintHash) {
        return REDIS_FLAP_KEY + clearFingerPrintHash;
    }
    @Override
    public FlapTracker getFlapTrackerByClearFingerprintHash(String clearFingerPrintHash) throws ZepException {
        String result = pool.useJedis(jedis -> jedis.get(createKey(clearFingerPrintHash)));
        if (result != null) {
            // return existing flap tracker
            logger.debug("String gotten back from redis " + result);
            return FlapTracker.buildFromString(result);
        }
        // there is no tracker for this guy yet
        return new FlapTracker();

    }

    @Override
    public void persistTracker(String clearFingerprintHash, FlapTracker tracker, long timeToKeep) throws ZepException {
        logger.debug("Setting string key {}  value {}", createKey(clearFingerprintHash), tracker.convertToString());
        SetParams params = new SetParams();
        params.ex(timeToKeep);
        pool.useJedis(jedis -> {
            jedis.set(createKey(clearFingerprintHash), tracker.convertToString(), params);
            return null;
        });
    }
}
