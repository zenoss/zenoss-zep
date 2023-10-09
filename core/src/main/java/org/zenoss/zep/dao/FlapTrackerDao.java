package org.zenoss.zep.dao;


import org.zenoss.zep.ZepException;
import org.zenoss.zep.impl.FlapTracker;

/**
 * Used by the event flapping plugin. This interfaces provides a mechanism
 * where we keep track of the number of "flaps" an event has in a given period.
 */
public interface FlapTrackerDao{

    /**
     * Returns the FlapTracker model object.
     * @param clearFingerprintHash
     * @return FlapTracker
     */
    FlapTracker getFlapTrackerByClearFingerprintHash(String clearFingerprintHash) throws ZepException;

    /**
     * After the flap tracker has been updated call this method to persist it. The next
     * time you call getFlapTrackerByClearFingerprintHash it will return the updated FlapTracker
     *
     * @param clearFingerprintHash how we store and retrieve this flap tracker.
     * @param tracker the flap tracker associated with the clearFingerprinthash
     * @param timeToKeep After this amount of time the entry is not guaranteed to be there.
     */
    void persistTracker(String clearFingerprintHash, FlapTracker tracker, long timeToKeep) throws ZepException;
}
