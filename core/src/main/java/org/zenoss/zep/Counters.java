package org.zenoss.zep;

public interface Counters {

    long getArchivedEventCount();

    long getAgedEventCount();

    long getClearedEventCount();

    long getDedupedEventCount();

    long getDroppedEventCount();

    long getProcessedEventCount();

    void addToAgedEventCount(long delta);

    void addToArchivedEventCount(long delta);

    void addToClearedEventCount(long delta);

    void addToDedupedEventCount(long delta);

    void addToDroppedEventCount(long delta);

    void addToProcessedEventCount(long delta);
}
