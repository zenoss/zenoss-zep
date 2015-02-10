package org.zenoss.zep.index;

import org.zenoss.zep.index.impl.EventIndexBackendTask;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface WorkQueue {
    List<EventIndexBackendTask> poll(int maxSize, int timeout, TimeUnit unit) throws InterruptedException;

    void complete(EventIndexBackendTask task);

    void completeAll(Collection<EventIndexBackendTask> tasks);

    void add(EventIndexBackendTask task);

    void addAll(Collection<EventIndexBackendTask> tasks);

    long requeueOldTasks();

    long size();

    boolean isReady();

}
