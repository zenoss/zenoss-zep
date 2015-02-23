package org.zenoss.zep.impl;

import org.zenoss.zep.Counters;

import java.util.concurrent.atomic.AtomicLong;


public class CountersImpl implements Counters {

    @Override
    public long getArchivedEventCount() {
        return archivedEvents.get();
    }


    @Override
    public long getAgedEventCount() {
        return agedEvents.get();
    }

    @Override
    public long getClearedEventCount() {
        return clearedEvents.get();
    }

    @Override
    public long getDedupedEventCount() {
        return dedupedEvents.get();
    }

    @Override
    public long getDroppedEventCount() {
        return droppedEvents.get();
    }

    @Override
    public long getProcessedEventCount() {
        return processedEvents.get();
    }

    @Override
    public void addToAgedEventCount(long delta) {
        agedEvents.addAndGet(delta);
    }

    @Override
    public void addToArchivedEventCount(long delta) {
        archivedEvents.addAndGet(delta);
    }

    @Override
    public void addToClearedEventCount(long delta) {
        clearedEvents.addAndGet(delta);
    }

    @Override
    public void addToDedupedEventCount(long delta) {
        dedupedEvents.addAndGet(delta);
    }

    @Override
    public void addToDroppedEventCount(long delta) {
        droppedEvents.addAndGet(delta);
    }

    @Override
    public void addToProcessedEventCount(long delta) {
        processedEvents.addAndGet(delta);
    }

    private final AtomicLong agedEvents = new AtomicLong();
    private final AtomicLong archivedEvents = new AtomicLong();
    private final AtomicLong clearedEvents = new AtomicLong();
    private final AtomicLong dedupedEvents = new AtomicLong();
    private final AtomicLong droppedEvents = new AtomicLong();
    private final AtomicLong processedEvents = new AtomicLong();

}
