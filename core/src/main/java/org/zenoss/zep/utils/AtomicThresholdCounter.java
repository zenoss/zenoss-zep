package org.zenoss.zep.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class AtomicThresholdCounter
{
    private long value;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition incremented = lock.newCondition();

    public AtomicThresholdCounter(int initialValue) {
        this.value = initialValue;
    }

    public void increment() {
        increment(1);
    }

    public void increment(int value) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            this.value += value;
            incremented.signalAll();
        } finally { lock.unlock(); }
    }

    public void incrementTo(int value) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (this.value < value) {
                this.value = value;
                incremented.signalAll();
            }
        } finally { lock.unlock(); }
    }

    public boolean awaitAndDecrement(int value, long timeout, TimeUnit unit)
            throws InterruptedException {
        long nanoseconds = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (this.value < value && nanoseconds > 0) {
                nanoseconds = incremented.awaitNanos(nanoseconds);
            }
            if (this.value >= value) {
                this.value -= value;
                return true;
            }
        } finally { lock.unlock(); }
        return false;
    }

    public boolean awaitAndReset(int value, long timeout, TimeUnit unit)
            throws InterruptedException {
        long nanoseconds = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (this.value < value && nanoseconds > 0) {
                nanoseconds = incremented.awaitNanos(nanoseconds);
            }
            if (this.value >= value) {
                this.value = 0;
                return true;
            }
        } finally { lock.unlock(); }
        return false;
    }
}
