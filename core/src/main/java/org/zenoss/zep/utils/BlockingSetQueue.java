/*
 * Based on java.util.concurrent.BlockingQueue, which was
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.zenoss.zep.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A {@link BlockingQueue} that ignores duplicates when storing elements.
 *
 * @param <E> the type of elements held in this collection
 */
public interface BlockingSetQueue<E> extends BlockingQueue<E> {

    /**
     * Inserts the specified element into this queue if it is possible to do
     * so immediately without exceeding the queue's capacity, returning {@code
     * true} upon success, {@code false} if already present in the queue, and
     * throwing an {@code IllegalStateException} if not present in the queue
     * and the queue is full.
     *
     * @param e the element to add
     * @return {@code true} if this queue changed as a result of the call
     * @throws IllegalStateException if this queue is full and does not
     *                               contain the specified element
     * @throws NullPointerException if the specified element is null
     * @throws ClassCastException if the class of the specified element
     *         prevents it from being added to this queue
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     */
    boolean add(E e);

    /**
     * Inserts the specified element into this queue if it is not already
     * present and it is possible to do so immediately without exceeding the
     * queue's capacity, returning {@code true} if the element already present
     * or upon successful insertion, and {@code false} if the element is not
     * present and this queue is full.
     *
     * @throws NullPointerException if the specified element is null
     */
    boolean offer(E e);

    /**
     * Inserts the specified element into this queue unless it is already
     * present, waiting for space to become available if the queue is full.
     *
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     */
    void put(E e) throws InterruptedException;

    /**
     * Inserts the specified element into this queue unless it is already
     * present, waiting up to the specified wait time for space to become
     * available if the queue is full.
     *
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     */
    boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException;
}
