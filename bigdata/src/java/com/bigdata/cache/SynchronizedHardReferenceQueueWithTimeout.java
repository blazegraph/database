/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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

import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * <p>
 * Thread-safe version with timeout for clearing stale references from the
 * queue. Clearing of stale entries is accomplished when a value is added to the
 * {@link SynchronizedHardReferenceQueue}. If the value implements the
 * {@link IValueAge} interface, then the tail of the queue is tested and any
 * entry on the tail whose age as reported by that interface exceeds a timeout
 * is evicted. This continues until we reach the first value on the tail of the
 * queue whose age is greater than the timeout. This behavior is enabled if a
 * non-ZERO timeout is specified and then only if the generic type of the
 * objects in the queue extends {@link IValueAge}. Stales references are also
 * cleared by a background thread.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SynchronizedHardReferenceQueueWithTimeout<T> implements
        IHardReferenceQueue<T> {

    protected static final Logger log = Logger.getLogger(SynchronizedHardReferenceQueueWithTimeout.class);
    
    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * Note: Synchronization for the inner {@link #queue} is realized using the
     * <strong>outer</strong> reference!
     */
    protected final InnerHardReferenceQueue<ValueAge<T>> queue;
    
    /**
     * Variant with no listener and timeout.
     * 
     * @param capacity
     *            The maximum #of references that can be stored on the cache.
     *            There is no guarantee that all stored references are distinct.
     * @param timeout
     *            The timeout (in nanoseconds) for an entry in the queue. When
     *            ZERO (0L), the timeout is disabled. See {@link IValueAge}.
     *            The timeout behavior is only available when the references
     *            stored in the queue implement {@link IValueAge}.
     */
    public SynchronizedHardReferenceQueueWithTimeout(final int capacity,
            final long timeout) {

        this(capacity, DEFAULT_NSCAN, timeout);
        
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
     * @param timeout
     *            The timeout (in nanoseconds) for an entry in the queue. When
     *            ZERO (0L), the timeout is disabled. See {@link IValueAge}.
     *            The timeout behavior is only available when the references
     *            stored in the queue implement {@link IValueAge}.
     */
    public SynchronizedHardReferenceQueueWithTimeout(final int capacity,
            final int nscan, final long timeout) {

        this.queue = new InnerHardReferenceQueue<ValueAge<T>>(capacity,
                DEFAULT_NSCAN, timeout);

        if (timeout > 0) {

            queues
                    .add(new WeakReference<SynchronizedHardReferenceQueueWithTimeout>(
                            this));

        }

    }

    static private class InnerHardReferenceQueue<T> extends
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
                final int capacity, final int nscan, final long timeout) {

            super(null/* listener */, capacity, nscan);

            if (timeout < 0)
                throw new IllegalArgumentException();

            this.timeout = timeout;
            
        }

        @Override
        protected final void beforeOffer(final T ref) {

            super.beforeOffer(ref);
            
            if (timeout != 0L) {

                // touch the new reference
                ((ValueAge) ref).touch();

                // evict any stale references.
                evictStaleRefs(timeout);

            }

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

            final long now = System.nanoTime();

            int ncleared = 0;

            while (!isEmpty()) {

                @SuppressWarnings("unchecked")
                final ValueAge v = (ValueAge) peek();

                final long timestamp = v.timestamp();

                final long age = now - timestamp;

                if (age < timeout) {

                    if (DEBUG)
                        log.debug("Stopping at age="
                                + TimeUnit.NANOSECONDS.toMillis(age)
                                + " : #ncleared=" + ncleared + ", size="
                                + size());

                    break;

                }

                if (DEBUG)
                    log.debug("Clearing reference: age="
                            + TimeUnit.NANOSECONDS.toMillis(age) + ", " + v);

                // evict the tail.
                evict();

                ncleared++;

            }

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

    }
    
    /*
     * Methods which DO NOT require synchronization.
     */
    
    /**
     * The timeout (in nanoseconds) for an entry in the queue. When ZERO (0L),
     * the timeout is disabled. Note that the timeout is only applied when the
     * references in the queue implement {@link IValueAge}.
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
    
    synchronized public boolean add(final T ref) {

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

        return age == null ? null : age.get();

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
     * @version $Id$
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
     * Wraps an object so that it can self-report the last timestamp when it was
     * last accessed.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <T>
     *            The generic type of the wrapped object.
     */
    private static class ValueAge<T> {

        private final T ref;
        
        public ValueAge(final T ref) {
            
            this.ref = ref;
            
        }
        
        public T get() {
            
            return ref;
            
        }
        
        /**
         * Invoked when a value is touched. The value must reset the timestamp
         * that it will report. That timestamp MUST be obtained using
         * {@link System#nanoTime()} as the age of the value will be judged by
         * comparison to the current value reported by {@link System#nanoTime()}.
         * <p>
         * Note: DO NOT invoke this method from hot code such as that will
         * impose a huge performance penalty! It is sufficient to let the
         * {@link InnerHardReferenceQueue} invoke this method itself when it
         * adds a reference.
         */
        final public void touch() {
        
            timestamp = System.nanoTime();
            
        }
        
        /**
         * Report the timestamp associated with the value. That timestamp MUST be
         * the value of {@link System#nanoTime()} when {@link #touch()} was last
         * invoked.
         */
        final public long timestamp() {
            
            return timestamp;
            
        }
        
        private long timestamp = System.nanoTime();

    }
    
}
