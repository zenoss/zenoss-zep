/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/

package org.zenoss.zep.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.EventSeverity;

/**
 * This is used by the event flap detection plugin to count how many times the
 * event has flapped.
 *
 * There should be one flap tracker per set of events. The events are correlated by the clear fingerprint hash.
 */
public class FlapTracker implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(FlapTracker.class);

    EventSeverity previousSeverity = EventSeverity.SEVERITY_CLEAR;
    List<Long> timestamps = new ArrayList<Long>();

    /**
     * Sets the last severity encountered by the events
     * @param severity
     */
    public void setPreviousSeverity(EventSeverity severity) {
        previousSeverity = severity;
    }

    /**
     * The last severity for this event set
     * @return EventSeverity
     */
    public EventSeverity getPreviousSeverity() {
        return previousSeverity;
    }

    /**
     * The timestamps are a list of flaps that the event has had.
     * @param timestamp
     */
    public void addTimeStamp(long timestamp) {
        timestamps.add(timestamp);
    }

    public void addCurrentTimeStamp() {
        addTimeStamp(System.currentTimeMillis() / 1000l);
    }
    public Long[] getTimestamps() {
        Long[] results = new Long[timestamps.size()];
        int i = 0;
        for (Long l : timestamps) {
            results[i] = l;
            i++;
        }
        return results;
    }

    public void clearTimestamps() {
        timestamps.clear();
    }

    public void discardTimestampsOlderThan(long windowStart) {
        Long[] ts = getTimestamps();
        // remove the older timestamps
        timestamps.clear();
        for (Long t : ts) {
            if (t >= windowStart) {
                timestamps.add(t);
            }
        }
    }

    @Override
    public String toString() {
        return "FlapTracker{" +
                "previousSeverity=" + previousSeverity +
                ", timestamps=" + timestamps +
                '}';
    }


    /**
     * Represent the state of this flap tracker as a string so that it can be persisted
     * in redis easily. This simply returns a comma delimited string. The first place
     * is the Previous Severity, and the rest of the string are timestamps of flaps.
     * @return String a string representation of this tracker.
     */
    public String convertToString() {
        StringBuilder sb = new StringBuilder(100);
        sb.append(previousSeverity.getNumber());
        sb.append(",");
        for (long t : timestamps) {
            sb.append(t);
            sb.append(",");
        }
        return sb.toString();
    }

    /**
     * Factory method for constructing a flap tracker from a string. This takes the
     * convertToString as the input.
     * @param str See the convertToString method for the format of the string.
     * @return FlapTracker
     */
    public static FlapTracker buildFromString(String str) {
        FlapTracker tracker = new FlapTracker();
        String[] l = str.split(",");
        if (l.length > 0) {
            try {
                tracker.setPreviousSeverity(EventSeverity.valueOf(Integer.valueOf(l[0])));
            }catch (IllegalArgumentException e) {
                logger.warn("Invalid value for Event Severity " + l[0]);
                tracker.setPreviousSeverity(EventSeverity.SEVERITY_CLEAR);
            }
        }
        for (int i=1; i < l.length; i++ ) {
            tracker.addTimeStamp(Long.valueOf(l[i]));
        }
        return tracker;
    }

}
