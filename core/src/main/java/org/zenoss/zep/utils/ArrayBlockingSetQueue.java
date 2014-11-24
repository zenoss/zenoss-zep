/*
 * Based on java.util.concurrent.ArrayBlockingQueue, which was
 * written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
package org.zenoss.zep.utils;

import java.io.Serializable;
import java.util.concurrent.locks.*;
import java.util.concurrent.*;
import java.util.*;


/**
 * A bounded {@linkplain org.zenoss.zep.utils.BlockingSetQueue blocking set
 * queue} backed by an array and hash set.  This queue orders elements FIFO
 * (first-in-first-out).  The <em>head</em> of the queue is that element that
 * has been on the queue the longest time.  The <em>tail</em> of the queue is
 * that element that has been on the queue the shortest time. New elements are
 * inserted at the tail of the queue, unless the queue already contains them,
 * and the queue retrieval operations obtain elements at the head of the queue.
 *
 * <p>This is a classic &quot;bounded buffer&quot;, in which a fixed-sized
 * array holds elements inserted by producers and extracted by consumers.
 * Once created, the capacity cannot be changed.  Attempts to {@code put} an
 * element into a full queue may result in the operation blocking (it may
 * succeed without blocking if the queue contains the element); attempts to
 * {@code take} an element from an empty queue will similarly block.
 *
 * <p>This class supports an optional fairness policy for ordering
 * waiting producer and consumer threads.  By default, this ordering
 * is not guaranteed. However, a queue constructed with fairness set
 * to {@code true} grants threads access in FIFO order. Fairness
 * generally decreases throughput but reduces variability and avoids
 * starvation.
 *
 * <p>This class and its iterator implement all of the
 * <em>optional</em> methods of the {@link java.util.Collection} and {@link
 * java.util.Iterator} interfaces.
 *
 * @param <E> the type of elements held in this collection
 */
public class ArrayBlockingSetQueue<E>
        extends AbstractQueue<E>
        implements BlockingSetQueue<E>, Serializable {

    private static final long serialVersionUID = 1L;

    /** The queued items */
    final Object[] items;

    /** The queued items, in a set. */
    final Set<E> set;

    /** items index for next take, poll, peek or remove */
    int takeIndex;

    /** items index for next put, offer, or add */
    int putIndex;

    /** Number of elements in the queue */
    int count;

    /*
     * Concurrency control uses the classic two-condition algorithm
     * found in any textbook.
     */

    /** Main lock guarding all access */
    protected final ReentrantLock lock;
    /** Condition for waiting takes */
    protected final Condition notEmpty;
    /** Condition for waiting puts */
    protected final Condition notFull;

    // Internal helper methods

    /**
     * Circularly increment i.
     */
    protected final int inc(int i) {
        return (++i == items.length) ? 0 : i;
    }

    /**
     * Circularly decrement i.
     */
    protected final int dec(int i) {
        return ((i == 0) ? items.length : i) - 1;
    }

    /**
     * Returns item at index i.
     */
    @SuppressWarnings("unchecked")
    protected final E itemAt(int i) {
        return (E)(items[i]);
    }

    /**
     * Throws NullPointerException if argument is null.
     *
     * @param v the element
     */
    private static void checkNotNull(Object v) {
        if (v == null)
            throw new NullPointerException();
    }

    /**
     * Inserts element at current put position, advances, and signals.
     * Call only when holding lock and set.add(x) just returned true.
     */
    private void insert(E x) {
        items[putIndex] = x;
        putIndex = inc(putIndex);
        ++count;
        notEmpty.signal();
    }

    /**
     * Extracts element at current take position, advances, and signals.
     * Call only when holding lock, and be sure to set.remove(value).
     */
    @SuppressWarnings("unchecked")
    private E extract() {
        final Object[] items = this.items;
        E x = (E)(items[takeIndex]);
        items[takeIndex] = null;
        takeIndex = inc(takeIndex);
        --count;
        notFull.signal();
        return x;
    }

    /**
     * Deletes item at position i.
     * Utility for remove and iterator.remove.
     * Call only when holding lock, and be sure to set.remove(value).
     */
    private void removeAt(int i) {
        final Object[] items = this.items;
        // if removing front item, just advance
        if (i == takeIndex) {
            items[takeIndex] = null;
            takeIndex = inc(takeIndex);
        } else {
            // slide over all others up through putIndex.
            for (;;) {
                int nexti = inc(i);
                if (nexti != putIndex) {
                    items[i] = items[nexti];
                    i = nexti;
                } else {
                    items[i] = null;
                    putIndex = i;
                    break;
                }
            }
        }
        --count;
        notFull.signal();
    }

    /**
     * Creates an {@code ArrayBlockingSetQueue} with the given (fixed)
     * capacity and default access policy.
     *
     * @param capacity the capacity of this queue
     * @throws IllegalArgumentException if {@code capacity < 1}
     */
    public ArrayBlockingSetQueue(int capacity) {
        this(capacity, false);
    }

    /**
     * Creates an {@code ArrayBlockingSetQueue} with the given (fixed)
     * capacity and the specified access policy.
     *
     * @param capacity the capacity of this queue
     * @param fair if {@code true} then queue accesses for threads blocked
     *        on insertion or removal, are processed in FIFO order;
     *        if {@code false} the access order is unspecified.
     * @throws IllegalArgumentException if {@code capacity < 1}
     */
    public ArrayBlockingSetQueue(int capacity, boolean fair) {
        if (capacity <= 0)
            throw new IllegalArgumentException();
        this.items = new Object[capacity];
        this.set = new HashSet<E>(capacity);
        lock = new ReentrantLock(fair);
        notEmpty = lock.newCondition();
        notFull =  lock.newCondition();
    }

    /**
     * Creates an {@code ArrayBlockingSetQueue} with the given (fixed)
     * capacity, the specified access policy and initially containing the
     * elements of the given collection,
     * added in traversal order of the collection's iterator.
     *
     * @param capacity the capacity of this queue
     * @param fair if {@code true} then queue accesses for threads blocked
     *        on insertion or removal, are processed in FIFO order;
     *        if {@code false} the access order is unspecified.
     * @param c the collection of elements to initially contain
     * @throws IllegalArgumentException if {@code capacity} is less than
     *         {@code c.size()}, or less than 1.
     * @throws NullPointerException if the specified collection or any
     *         of its elements are null
     */
    public ArrayBlockingSetQueue(int capacity, boolean fair,
                                 Collection<? extends E> c) {
        this(capacity, fair);

        final ReentrantLock lock = this.lock;
        lock.lock(); // Lock only for visibility, not mutual exclusion
        try {
            int i = 0;
            try {
                for (E e : c) {
                    checkNotNull(e);
                    items[i++] = e;
                }
            } catch (ArrayIndexOutOfBoundsException ex) {
                throw new IllegalArgumentException();
            }
            count = i;
            putIndex = (i == capacity) ? 0 : i;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Inserts the specified element at the tail of this queue if it is
     * possible to do so immediately without exceeding the queue's capacity,
     * returning (@code true} upon success, {@code false} if already present
     * in the queue, and throwing an <tt>IllegalStateException</tt> if not
     * present in the queue and the queue is full.
     *
     * @param e the element to add
     * @return {@code true} or {@code false} (as specified by {@link Collection#add})
     * @throws IllegalStateException if this queue is full and does not contain the specified element
     * @throws NullPointerException if the specified element is null
     */
    public boolean add(E e) {
        checkNotNull(e);
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (set.contains(e)) return false;
            if (count == items.length)
                throw new IllegalStateException("Queue full");
            else if (set.add(e)) {
                insert(e);
                return true;
            } else {
                throw new RuntimeException("Internal bug: someone forgot to lock");
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Inserts the specified element at the tail of this queue if it is
     * not already present and it is possible to do so immediately without
     * exceeding the queue's capacity, returning {@code true} if the element
     * is already present or upon successful insertion, and {@code false} if
     * the element is not present and this queue is full.
     *
     * @throws NullPointerException if the specified element is null
     */
    public boolean offer(E e) {
        checkNotNull(e);
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (set.contains(e)) return true;
            if (count == items.length)
                return false;
            else if (set.add(e)) {
                insert(e);
                return true;
            } else {
                throw new RuntimeException("Internal bug: someone forgot to update the set, or else forgot to lock");
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Inserts the specified element at the tail of this queue unless it is
     * already present, waiting for space to become available if the queue is
     * full.
     *
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     */
    public void put(E e) throws InterruptedException {
        checkNotNull(e);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            if (set.contains(e))
                return;
            while (count == items.length)
                notFull.await();
            if (set.add(e))
                insert(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Inserts the specified element at the tail of this queue unless it is
     * already present, waiting up to the specified wait time for space to
     * become available if the queue is full.
     *
     * @return {@code true} if the element is present or was successfully
     *         inserted into the queue; {@code false} otherwise
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean offer(E e, long timeout, TimeUnit unit)
            throws InterruptedException {
        checkNotNull(e);
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            if (set.contains(e))
                return true;
            while (count == items.length) {
                if (set.contains(e))
                    return true;
                if (nanos <= 0)
                    return false;
                nanos = notFull.awaitNanos(nanos);
            }
            if (set.add(e))
                insert(e);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public E poll() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (count == 0)
                return null;
            else {
                final E x = extract();
                set.remove(x);
                return x;
            }
        } finally {
            lock.unlock();
        }
    }

    public E take() throws InterruptedException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (count == 0)
                notEmpty.await();
            final E x = extract();
            set.remove(x);
            return x;
        } finally {
            lock.unlock();
        }
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (count == 0) {
                if (nanos <= 0)
                    return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            final E x = extract();
            set.remove(x);
            return x;
        } finally {
            lock.unlock();
        }
    }

    public E peek() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return (count == 0) ? null : itemAt(takeIndex);
        } finally {
            lock.unlock();
        }
    }

    // this doc comment is overridden to remove the reference to collections
    // greater in size than Integer.MAX_VALUE
    /**
     * Returns the number of elements in this queue.
     *
     * @return the number of elements in this queue
     */
    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    // this doc comment is a modified copy of the inherited doc comment,
    // without the reference to unlimited queues.
    /**
     * Returns the number of additional elements that this queue can ideally
     * (in the absence of memory or resource constraints) accept without
     * blocking. This is always equal to the initial capacity of this queue
     * less the current {@code size} of this queue.
     *
     * <p>Note that you <em>cannot</em> always tell if an attempt to insert
     * an element will succeed by inspecting {@code remainingCapacity}
     * because it may be the case that another thread is about to
     * insert or remove an element.
     */
    public int remainingCapacity() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return items.length - count;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes a single instance of the specified element from this queue,
     * if it is present.  More formally, removes an element {@code e} such
     * that {@code o.equals(e)}, if this queue contains one or more such
     * elements.
     * Returns {@code true} if this queue contained the specified element
     * (or equivalently, if this queue changed as a result of the call).
     *
     * <p>Removal of interior elements in circular array based queues
     * is an intrinsically slow and disruptive operation, so should
     * be undertaken only in exceptional circumstances, ideally
     * only when the queue is known not to be accessible by other
     * threads.
     *
     * @param o element to be removed from this queue, if present
     * @return {@code true} if this queue changed as a result of the call
     */
    public boolean remove(Object o) {
        if (o == null) return false;
        final Object[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (remove(o)) {
                for (int i = takeIndex, k = count; k > 0; i = inc(i), k--) {
                    if (o.equals(items[i])) {
                        removeAt(i);
                        return true;
                    }
                }
                throw new RuntimeException("Internal bug: array and hash got out of sync");
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns {@code true} if this queue contains the specified element.
     * More formally, returns {@code true} if and only if this queue contains
     * at least one element {@code e} such that {@code o.equals(e)}.
     *
     * @param o object to be checked for containment in this queue
     * @return {@code true} if this queue contains the specified element
     */
    public boolean contains(Object o) {
        if (o == null) return false;
        final Set<E> set = this.set;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return set.contains(o);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns an array containing all of the elements in this queue, in
     * proper sequence.
     *
     * <p>The returned array will be "safe" in that no references to it are
     * maintained by this queue.  (In other words, this method must allocate
     * a new array).  The caller is thus free to modify the returned array.
     *
     * <p>This method acts as bridge between array-based and collection-based
     * APIs.
     *
     * @return an array containing all of the elements in this queue
     */
    public Object[] toArray() {
        final Object[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            final int count = this.count;
            Object[] a = new Object[count];
            for (int i = takeIndex, k = 0; k < count; i = inc(i), k++)
                a[k] = items[i];
            return a;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns an array containing all of the elements in this queue, in
     * proper sequence; the runtime type of the returned array is that of
     * the specified array.  If the queue fits in the specified array, it
     * is returned therein.  Otherwise, a new array is allocated with the
     * runtime type of the specified array and the size of this queue.
     *
     * <p>If this queue fits in the specified array with room to spare
     * (i.e., the array has more elements than this queue), the element in
     * the array immediately following the end of the queue is set to
     * {@code null}.
     *
     * <p>Like the {@link #toArray()} method, this method acts as bridge between
     * array-based and collection-based APIs.  Further, this method allows
     * precise control over the runtime type of the output array, and may,
     * under certain circumstances, be used to save allocation costs.
     *
     * <p>Suppose {@code x} is a queue known to contain only strings.
     * The following code can be used to dump the queue into a newly
     * allocated array of {@code String}:
     *
     * <pre>
     *     String[] y = x.toArray(new String[0]);</pre>
     *
     * Note that {@code toArray(new Object[0])} is identical in function to
     * {@code toArray()}.
     *
     * @param a the array into which the elements of the queue are to
     *          be stored, if it is big enough; otherwise, a new array of the
     *          same runtime type is allocated for this purpose
     * @return an array containing all of the elements in this queue
     * @throws ArrayStoreException if the runtime type of the specified array
     *         is not a supertype of the runtime type of every element in
     *         this queue
     * @throws NullPointerException if the specified array is null
     */
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        final Object[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            final int count = this.count;
            final int len = a.length;
            if (len < count)
                a = (T[])java.lang.reflect.Array.newInstance(
                        a.getClass().getComponentType(), count);
            for (int i = takeIndex, k = 0; k < count; i = inc(i), k++)
                a[k] = (T) items[i];
            if (len > count)
                a[count] = null;
            return a;
        } finally {
            lock.unlock();
        }
    }

    public String toString() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int k = count;
            if (k == 0)
                return "[]";

            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (int i = takeIndex; ; i = inc(i)) {
                Object e = items[i];
                sb.append(e == this ? "(this Collection)" : e);
                if (--k == 0)
                    return sb.append(']').toString();
                sb.append(',').append(' ');
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Atomically removes all of the elements from this queue.
     * The queue will be empty after this call returns.
     */
    public void clear() {
        final Object[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            set.clear();
            for (int i = takeIndex, k = count; k > 0; i = inc(i), k--)
                items[i] = null;
            count = 0;
            putIndex = 0;
            takeIndex = 0;
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @throws IllegalArgumentException      {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public int drainTo(Collection<? super E> c) {
        checkNotNull(c);
        if (c == this)
            throw new IllegalArgumentException();
        final Object[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int i = takeIndex;
            int n = 0;
            int max = count;
            while (n < max) {
                final E x = (E)(items[i]);
                c.add(x);
                items[i] = null;
                i = inc(i);
                ++n;
            }
            if (n > 0) {
                set.clear();
                count = 0;
                putIndex = 0;
                takeIndex = 0;
                notFull.signalAll();
            }
            return n;
        } finally {
            lock.unlock();
        }
    }

    /**
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @throws IllegalArgumentException      {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public int drainTo(Collection<? super E> c, int maxElements) {
        checkNotNull(c);
        if (c == this)
            throw new IllegalArgumentException();
        if (maxElements <= 0)
            return 0;
        final Set<E> set = this.set;
        final Object[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int i = takeIndex;
            int n = 0;
            int max = (maxElements < count) ? maxElements : count;
            while (n < max) {
                final E x = (E)(items[i]);
                c.add(x);
                items[i] = null;
                set.remove(x);
                i = inc(i);
                ++n;
            }
            if (n > 0) {
                count -= n;
                takeIndex = i;
                notFull.signalAll();
            }
            return n;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns an iterator over the elements in this queue in proper sequence.
     * The elements will be returned in order from first (head) to last (tail).
     *
     * <p>The returned {@code Iterator} is a "weakly consistent" iterator that
     * will never throw {@link java.util.ConcurrentModificationException
     * ConcurrentModificationException},
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     *
     * @return an iterator over the elements in this queue in proper sequence
     */
    public Iterator<E> iterator() {
        return new Itr();
    }

    /**
     * Iterator for ArrayBlockingSetQueue. To maintain weak consistency
     * with respect to puts and takes, we (1) read ahead one slot, so
     * as to not report hasNext true but then not have an element to
     * return -- however we later recheck this slot to use the most
     * current value; (2) ensure that each array slot is traversed at
     * most once (by tracking "remaining" elements); (3) skip over
     * null slots, which can occur if takes race ahead of iterators.
     * However, for circular array-based queues, we cannot rely on any
     * well established definition of what it means to be weakly
     * consistent with respect to interior removes since these may
     * require slot overwrites in the process of sliding elements to
     * cover gaps. So we settle for resiliency, operating on
     * established apparent nexts, which may miss some elements that
     * have moved between calls to next.
     */
    private class Itr implements Iterator<E> {
        private int remaining; // Number of elements yet to be returned
        private int nextIndex; // Index of element to be returned by next
        private E nextItem;    // Element to be returned by next call to next
        private E lastItem;    // Element returned by last call to next
        private int lastRet;   // Index of last element returned, or -1 if none

        Itr() {
            final ReentrantLock lock = ArrayBlockingSetQueue.this.lock;
            lock.lock();
            try {
                lastRet = -1;
                if ((remaining = count) > 0)
                    nextItem = itemAt(nextIndex = takeIndex);
            } finally {
                lock.unlock();
            }
        }

        public boolean hasNext() {
            return remaining > 0;
        }

        public E next() {
            final ReentrantLock lock = ArrayBlockingSetQueue.this.lock;
            lock.lock();
            try {
                if (remaining <= 0)
                    throw new NoSuchElementException();
                lastRet = nextIndex;
                E x = itemAt(nextIndex);  // check for fresher value
                if (x == null) {
                    x = nextItem;         // we are forced to report old value
                    lastItem = null;      // but ensure remove fails
                }
                else
                    lastItem = x;
                while (--remaining > 0 && // skip over nulls
                        (nextItem = itemAt(nextIndex = inc(nextIndex))) == null)
                    ;
                return x;
            } finally {
                lock.unlock();
            }
        }

        public void remove() {
            final ReentrantLock lock = ArrayBlockingSetQueue.this.lock;
            final Set<E> set = ArrayBlockingSetQueue.this.set;
            lock.lock();
            try {
                int i = lastRet;
                if (i == -1)
                    throw new IllegalStateException();
                lastRet = -1;
                E x = lastItem;
                lastItem = null;
                // only remove if item still at index
                if (x != null && x == items[i]) {
                    boolean removingHead = (i == takeIndex);
                    set.remove(x);
                    removeAt(i);
                    if (!removingHead)
                        nextIndex = dec(nextIndex);
                }
            } finally {
                lock.unlock();
            }
        }
    }

}
