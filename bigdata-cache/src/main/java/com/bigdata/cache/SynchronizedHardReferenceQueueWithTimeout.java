/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Feb 9, 2009
 */

package com.bigdata.cache;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.util.DaemonThreadFactory;

/**
 * <p>
 * Thread-safe version with timeout for clearing stale references from the
 * queue. Clearing of stale entries is accomplished when a value is added to the
 * {@link SynchronizedHardReferenceQueue}. The tail of the queue is tested and
 * any entry on the tail whose age exceeds a timeout is evicted. This continues
 * until we reach the first value on the tail of the queue whose age is greater
 * than the timeout. This behavior is enabled if a non-ZERO timeout is
 * specified. Stales references are also cleared by a background thread.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: SynchronizedHardReferenceQueueWithTimeout.java 4410 2011-04-17
 *          20:11:44Z thompsonbry $
 */
public class SynchronizedHardReferenceQueueWithTimeout<T> implements
        IHardReferenceQueue<T> {

    private static final Logger log = Logger
            .getLogger(SynchronizedHardReferenceQueueWithTimeout.class);

    /**
     * Note: Synchronization for the inner {@link #queue} is realized using the
     * <strong>outer</strong> reference!
     */
    private final InnerHardReferenceQueue<ValueAge<T>> queue;
    
    /**
     * Package private access in support of the unit tests.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    final HardReferenceQueue<IRef<T>> getQueue() {
        
        return (HardReferenceQueue) queue;
        
    }
    
    /**
     * Variant with no listener and timeout.
     * 
     * @param capacity
     *            The maximum #of references that can be stored on the cache.
     *            There is no guarantee that all stored references are distinct.
     * @param timeoutNanos
     *            The timeout (in nanoseconds) for an entry in the queue. When
     *            ZERO (0L), the timeout is disabled.
     */
    public SynchronizedHardReferenceQueueWithTimeout(final int capacity,
            final long timeoutNanos) {

        this(capacity, DEFAULT_NSCAN, timeoutNanos);
        
    }

    /**
     * Optional timeout.
     * 
     * @param capacity
     *            The maximum #of references that can be stored on the cache.
     *            There is no guarantee that all stored references are distinct.
     * @param nscan
     *            The #of references to scan from the MRU position before
     *            appended a reference to the cache. Scanning is used to reduce
     *            the chance that references that are touched several times in
     *            near succession from entering the cache more than once. The
     *            #of reference tests trades off against the latency of adding a
     *            reference to the cache.
     * @param timeoutNanos
     *            The timeout (in nanoseconds) for an entry in the queue. When
     *            ZERO (0L), the timeout is disabled.
     */
    public SynchronizedHardReferenceQueueWithTimeout(final int capacity,
            final int nscan, final long timeoutNanos) {

        this(null/* listener */, capacity, nscan, timeoutNanos);
        
    }

    /*
     * package private constructor for unit tests.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    SynchronizedHardReferenceQueueWithTimeout(
            final HardReferenceQueueEvictionListener<IRef<T>> listener,
            final int capacity, final int nscan, final long timeoutNanos) {

        this.queue = new InnerHardReferenceQueue<ValueAge<T>>(
                (HardReferenceQueueEvictionListener) listener, capacity, nscan,
                timeoutNanos);

        if (queue.timeout > 0) {

            queues.add(new WeakReference<SynchronizedHardReferenceQueueWithTimeout>(
                    this));

        }

    }

    /**
     * Inner class wraps each object inserted into the queue with the nanotime
     * corresponding to that insert. A {@link Cleaner} thread runs periodically
     * and removes stale references from the tail of the queue.
     * <p>
     * Note: This deliberately DOES NOT test the tail in
     * {@link HardReferenceQueue#beforeOffer(Object)} to cut down on the
     * overhead associated with {@link #add(Object)}. Stale references will be
     * evicted regardless when the {@link Cleaner} runs.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @param <T>
     *            The generic type of the queue entries for this inner class.
     */
    static private class InnerHardReferenceQueue<T extends ValueAge<?>> extends
            HardReferenceQueue<T> {

        /**
         * The timeout (in nanoseconds) for an entry in the queue. When ZERO
         * (0L), the timeout is disabled.
         */
        private final long timeout;

        /**
         * 
         * @param listener
         * @param capacity
         * @param nscan
         * @param timeout
         *            The timeout (in nanoseconds) for an entry in the queue.
         *            When ZERO (0L), the timeout is disabled.
         */
        public InnerHardReferenceQueue(
                final HardReferenceQueueEvictionListener<T> listener,
                final int capacity, final int nscan, final long timeout) {

            super(listener, capacity, nscan);

            if (timeout < 0)
                throw new IllegalArgumentException();

            this.timeout = timeout;
            
        }

        /**
         * Examine references backwards from the tail, evicting any that have
         * become stale (too long since they were last touched).
         * 
         * @param timeout
         *            The timeout in nanoseconds.
         * 
         * @return The #of stale references which were cleared.
         */
        int evictStaleRefs(final long timeout) {

            final int size0 = size();

            if (size0 == 0)
                return 0;
            
            final long now = System.nanoTime();

            final long maxAge = now - peek().ts;

            T x;
            long age = 0;
            int ncleared = 0;
            while ((x = peek()) != null) {

                age = now - x.ts;

                if (age < timeout)
                    break;

                // evict the tail.
                evict();

                if (log.isTraceEnabled())
                    log.trace("Evicting: " + x.ref + " : timeout="
                            + TimeUnit.NANOSECONDS.toMillis(timeout)
                            + "ms, age=" + TimeUnit.NANOSECONDS.toMillis(age)
                            + "ms, size=" + size + ", ncleared=" + ncleared);

                ncleared++;

            }

            if (log.isDebugEnabled() && ncleared > 3)
                log.debug("#ncleared=" + ncleared + ", size=" + size()
                        + ", timeout=" + TimeUnit.NANOSECONDS.toMillis(timeout)
                        + ", maxAge=" + TimeUnit.NANOSECONDS.toMillis(maxAge)
                        + ", age=" + TimeUnit.NANOSECONDS.toMillis(age));

            return ncleared;
            
        }

        /**
         * Examine references backwards from the tail, evicting any that have
         * become stale (too long since they were last touched) based on the
         * timeout specified to the ctor (this is a NOP if the timeout is
         * ZERO(0)).
         */
        public int evictStaleRefs() {

            if (timeout != 0L) {

                return evictStaleRefs(timeout);

            }
            
            return 0;

        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to handle the indirection from the {@link ValueAge} object
         * to the wrapped reference. It is the wrapped references that we need
         * to test for reference equality.
         * 
         * @see <a
         *      href="https://sourceforge.net/apps/trac/bigdata/ticket/465#comment:2">
         *      Too many GRS reads</a>
         */
        @Override
        final public boolean scanHead(final int nscan, final T ref) {

            if (nscan <= 0)
                throw new IllegalArgumentException();
            
            if (ref == null)
                throw new IllegalArgumentException();
            
            /*
             * Note: This loop goes backwards from the head.  Since the head is the
             * insertion point, we decrement the head position before testing the
             * reference.  If the head is zero, then we wrap around.  This carries
             * the head back to the last index in the array (capacity-1).
             *
             * Note: This uses local variables to shadow the instance variables
             * so that we do not modify the state of the cache as a side effect.
             */

            int head = this.getHeadIndex();

            int count = this.size;

            // unwrap the caller's reference.
            final Object o1 = ref.get();
            
            for (int i = 0; i < nscan && count > 0; i++) {

                head = (head == 0 ? capacity - 1 : head - 1); // update head.

                count--; // update #of references.

                // Unwrap the reference at this position in the ring buffer.
                final Object o2 = _get(head).get();// refs[head]

                if (o1 == o2) {

                    // Found a match.

                    return true;

                }

            }

            return false;
            
        }
        
    }
    
    /*
     * Methods which DO NOT require synchronization.
     */
    
    /**
     * The timeout (in nanoseconds) for an entry in the queue. When ZERO (0L),
     * the timeout is disabled.
     */
    final public long timeout() {
        
        return queue.timeout;
        
    }

    final public int capacity() {

        return queue.capacity();
        
    }

    final public int nscan() {
        
        return queue.nscan();
        
    }

    /*
     * Methods which DO require synchronization.
     */

    /*
     * Note: I've tried coding add(ref) two different ways here. Probably this
     * is too small an issue to make a different and the runs may have been too
     * short, plus the performance could vary by hotspot compiler.
     * 
     * Another alternative, which would break encapsulation, is to have a
     * parallel array of long timestamps for the references so we can avoid
     * the allocation entirely.
     */
    
//    // allocate ValueAge outside of synchronized block: 3156/1656 on U10 query.
//    public boolean add(final T ref) {
//
//        final ValueAge<T> v = new ValueAge<T>(ref);
//        synchronized(this) {
//
//            return queue.add(v);
//            
//        }
//
//    }
    
    // allocate inside of synchronized block: 2781/406 on U10 query.
    synchronized public boolean add(final T ref) {

        if(ref == null)
            throw new IllegalArgumentException();
        
        return queue.add(new ValueAge<T>(ref));
        
    }

    synchronized public void clear(final boolean clearRefs) {
        
        queue.clear(clearRefs);
        
    }

    synchronized public boolean evict() {
        
        return queue.evict();
        
    }

    synchronized public void evictAll(final boolean clearRefs) {
        
        queue.evictAll(clearRefs);
        
    }

    synchronized public T peek() {
        
        final ValueAge<T> age = queue.peek();

        return age == null ? null : age.ref;

    }

    synchronized public boolean isEmpty() {

        return queue.isEmpty();
        
    }

    synchronized public boolean isFull() {
        
        return queue.isFull();
        
    }

    synchronized public int size() {
        
        return queue.size();
        
    }

    /*
     * Cleaner service.
     */

    /**
     * This method may be invoked by life cycle operations which need to tear
     * down the bigdata environment. Normally you do not need to do this as the
     * cleaner service uses a daemon thread and will not prevent the JVM from
     * halting. However, a servlet container running bigdata can complain that
     * some threads were not terminated if the webapp running bigdata is
     * stopped. You can invoke this method to terminate the stale reference
     * cleaner thread.
     */
    public static final void stopStaleReferenceCleaner() {
        
        cleanerService.shutdownNow();
        
    }
    
    private static final ScheduledExecutorService cleanerService;
    static {
        
        cleanerService = Executors
                .newSingleThreadScheduledExecutor(new DaemonThreadFactory(
                        "StaleReferenceCleaner"));
        
        cleanerService.scheduleWithFixedDelay(new Cleaner(),
                5000/* initialDelay */, 5000/* delay */, TimeUnit.MILLISECONDS);
   
    }
    
    /**
     * Collection of weak references to {@link SynchronizedHardReferenceQueue}s
     * with non-zero timeouts to be processed by the cleaner.
     * <p>
     * Note: a collection of {@link WeakReference}s so it does not force the
     * retention of the {@link SynchronizedHardReferenceQueue} objects by
     * itself.
     */
    private static ConcurrentLinkedQueue<WeakReference<SynchronizedHardReferenceQueueWithTimeout>> queues = new ConcurrentLinkedQueue<WeakReference<SynchronizedHardReferenceQueueWithTimeout>>();

    /**
     * Cleans stale references from all known
     * {@link SynchronizedHardReferenceQueue} instances having a non-ZERO
     * timeout.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private static class Cleaner implements Runnable {

        /**
         * Each run clears stale references from each existing
         * {@link SynchronizedHardReferenceQueue} instance. This method is also
         * responsible for removing {@link WeakReference}s which have been
         * cleared from {@link SynchronizedHardReferenceQueue#clients}.
         * <p>
         * Note: All exceptions are caught and logged since any exception thrown
         * form here would terminate the scheduled execution of this task.
         */
        public void run() {

            try {

                int ncleared = 0;
                int nqueues = 0;
                
                final Iterator<WeakReference<SynchronizedHardReferenceQueueWithTimeout>> itr = queues
                        .iterator();

                while (itr.hasNext()) {

                    final WeakReference<SynchronizedHardReferenceQueueWithTimeout> ref = itr.next();

                    final SynchronizedHardReferenceQueueWithTimeout queue = ref.get();

                    if (queue == null) {

                        itr.remove();

                        continue;

                    }

                    /*
                     * Note: Synchronization is imposed on the outer class
                     * reference.
                     */
                    synchronized (queue) {

                        ncleared += ((InnerHardReferenceQueue) queue.queue)
                                .evictStaleRefs();

                    }

                    nqueues++;

                }
                
                if (ncleared > 0 && log.isInfoEnabled())
                    log.info("Cleared " + ncleared + " stale references from "
                            + nqueues + " queues");
                
            } catch (Throwable t) {

                log.error(t, t);

            }

        }
        
    }
    
    /**
     * Interface for something wrapping a reference.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @param <T>
     */
    public static interface IRef<T> {
       T get();
    }

    /**
     * Wraps an object so that it can self-report the last timestamp when it was
     * last accessed.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @param <T>
     *            The generic type of the wrapped object.
     */
    static class ValueAge<T> implements IRef<T> {

        /**
         * The object stored in the queue.
         */
        /*private*/ final T ref;

        public T get() {
            return ref;
        }
        
        /**
         * The timestamp associated with the value. This is initialized to the
         * {@link System#nanoTime()} when the {@link ValueAge} was created and
         * is updated to the value of {@link System#nanoTime()} each time
         * {@link #touch()} is invoked.
         */
        final /*private*/ long ts = System.nanoTime();
        
        public ValueAge(final T ref) {
            
            this.ref = ref;
            
        }
        
    }
    
}
