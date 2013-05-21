/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/

package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.zenoss.zep.EventFlappingStorage;

import java.util.concurrent.TimeUnit;

public class RedisEventFlappingStorage implements EventFlappingStorage{

    private static final Logger logger = LoggerFactory.getLogger(RedisEventFlappingStorage.class);

    private final String REDIS_FLAP_KEY = "zenoss_event_flapping";

    private StringRedisTemplate template = null;

    public void setRedisTemplate(StringRedisTemplate template) {
        this.template = template;
    }

    private String createKey(String clearFingerPrintHash) {
        return REDIS_FLAP_KEY + clearFingerPrintHash;
    }
    @Override
    public FlapTracker getFlapTrackerByClearFingerprintHash(String clearFingerPrintHash) {
        String result = template.opsForValue().get(createKey(clearFingerPrintHash));
        if (result != null) {
            // return existing flap tracker
            logger.debug("String gotten back from redis " + result);
            return FlapTracker.buildFromString(result);
        }
        // there is no tracker for this guy yet
        return new FlapTracker();

    }

    @Override
    public void persistTracker(String clearFingerprintHash, FlapTracker tracker, long timeToKeep) {
        // convert the object into an array with the first index being the last severity
        // and the rest being the flap timestamps
        logger.debug("Setting string key {}  value {}", createKey(clearFingerprintHash), tracker.convertToString());
        template.opsForValue().set(createKey(clearFingerprintHash), tracker.convertToString(), timeToKeep, TimeUnit.SECONDS);
    }
}
