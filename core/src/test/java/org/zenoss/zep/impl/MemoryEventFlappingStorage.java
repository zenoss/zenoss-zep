/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/

package org.zenoss.zep.impl;

import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.FlapTrackerDao;

import java.util.HashMap;
import java.util.Map;

/**
 * Uses a hashmap to persist flap tracking in memory. This should only be used during tests
 * as it doesn't really clean up old or stale entries from the hashmap.
 */
public class MemoryEventFlappingStorage implements FlapTrackerDao{

    // create a hashmap indexed by clear finger print hash
    Map<String, FlapTracker> trackers = new HashMap<String, FlapTracker>();

    @Override
    public FlapTracker getFlapTrackerByClearFingerprintHash(String clearFingerPrintHash) throws ZepException {
        if (trackers.containsKey(clearFingerPrintHash)) {
            return trackers.get(clearFingerPrintHash);
        }

        FlapTracker tracker = new FlapTracker();
        trackers.put(clearFingerPrintHash, tracker);
        return tracker;
    }

    public void persistTracker(String clearFingerprintHash, FlapTracker tracker, long timeToKeep) throws ZepException{
        trackers.put(clearFingerprintHash, tracker);
    }

    @Override
    public String toString() {
        return "MemoryEventFlappingStorage{" +
                "trackers=" + trackers +
                '}';
    }
}
