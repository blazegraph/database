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
 * Created on Jun 26, 2009
 */

package com.bigdata.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

/**
 * A synchronization aid that allows one or more threads to await a counter
 * becoming zero. Once the counter reaches zero, all waiting threads are
 * released. Threads may invoke {@link #await()} any time after the counter has
 * been incremented. They will not be released until the counter is zero. The
 * typical pattern is to incrementing the counter before some operation and the
 * decrement the counter after that operation. This pattern may be safely used
 * in combination with nested invocations and with concurrent threads.
 * <p>
 * Note: This class is very similar to a {@link CountDownLatch}, however the
 * counter maximum is not specified in advance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Latch {

    protected transient static final Logger log = Logger.getLogger(Latch.class);
    
    private final AtomicLong counter = new AtomicLong();
    
    private final ReentrantLock lock;
    
    private final Condition cond;
    
    private final String name;
    
    public String toString() {
        
        return getClass().getName() + "{"
                + (name == null ? "" : "name=" + name + ",") + "counter="
                + counter + "}";
        
    }
    
    public Latch() {
        
        this(null/* name */, null/* lock */);
        
    }

    /**
     * 
     * @param lock
     *            The lock to be used (optional). When none is specified, a new
     *            lock will be allocated. The caller MAY choose to specify the
     *            lock in order to avoid nested lock designs by using the same
     *            lock inside the {@link Latch} and in some outer context.
     */
    public Latch(final ReentrantLock lock) {

        this(null/* name */, lock);
        
    }

    /**
     * 
     * @param name
     *            An optional name that will be displayed by {@link #toString()}
     *            along with the current counter value.
     * @param lock
     *            The lock to be used (optional). When none is specified, a new
     *            lock will be allocated. The caller MAY choose to specify the
     *            lock in order to avoid nested lock designs by using the same
     *            lock inside the {@link Latch} and in some outer context.
     */
    public Latch(final String name, final ReentrantLock lock) {

        this.name = name; // MAY be null.

        this.lock = (lock == null ? new ReentrantLock() : lock);

        this.cond = this.lock.newCondition();

    }

    /**
     * The counter value (peek at current value without obtaining the lock).
     */
    public long get() {

        return counter.get();
        
    }

    /**
     * Increments the internal counter.
     * 
     * @return The post-increment value of the counter.
     */
    public long inc() {

        lock.lock();
        try {

            final long c = this.counter.incrementAndGet();

            if (log.isDebugEnabled())
                log.debug(toString());

            return c;

        } finally {

            lock.unlock();

        }
        
    }

    /**
     * Decrements the internal counter and releases the blocked thread(s) if the
     * counter reaches zero.
     * 
     * @return The post-decrement value.
     * 
     * @throws IllegalStateException
     *             if the counter would become negative.
     */
    public long dec() {

        lock.lock();
        try {

            if (this.counter.get() <= 0) {
                
                throw new IllegalStateException();
                
            }
            
            final long c = this.counter.decrementAndGet();

            if (log.isDebugEnabled())
                log.debug(toString());

            if (c == 0) {
               
                try {

                    // signal blocked threads.
                    _signal();

                } catch (InterruptedException ex) {

                    throw new RuntimeException(ex);

                }

            }

            return c;

        } finally {

            lock.unlock();
            
        }

    }

    /**
     * Signal any threads blocked in {@link #await(long, TimeUnit)}.
     * 
     * @throws InterruptedException
     */
    private final void _signal() throws InterruptedException {

        lock.lockInterruptibly();
        try {

            if (log.isInfoEnabled())
                log.info("signalAll()");

            // release anyone awaiting our signal.
            cond.signalAll();

        } finally {

            lock.unlock();

        }

        try {
            // allow extensions, but not while holding the lock.
            signal();
        } catch (InterruptedException t) {
            // propagate to the caller.
            throw t;
        } catch (Throwable t) {
            // log anything else thrown out.
            log.error(toString(), t);
        }
        
    }

    /**
     * Invoked when the latch reaches zero after any threads blocked at
     * {@link #await(long, TimeUnit)} have been released. This may be overridden
     * to perform additional processing, such as moving an associated object
     * onto another queue.
     * <p>
     * CAUTION: DO NOT invoke any operation from within this method which could
     * block as that would cause the thread running the asynchronous write task
     * in which this method is invoked to block. If you are transferring objects
     * to a queue, the queue MUST be unbounded.
     */
    protected void signal() throws InterruptedException {

    }

    /**
     * Await the counter to become zero unless interrupted.
     * 
     * @throws InterruptedException
     */
    public void await() throws InterruptedException {

        await(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

    }

    /**
     * Await the counter to become zero, but no longer than the timeout.
     * 
     * @param timeout
     *            The timeout.
     * @param unit
     *            The unit in which the timeout is expressed.
     * 
     * @return <code>true</code> if the counter reached zero and
     *         <code>false</code> if the timeout was exceeded before the counter
     *         reached zero.
     * 
     * @throws InterruptedException
     */
    public boolean await(final long timeout, final TimeUnit unit)
            throws InterruptedException {

        long nanos = unit.toNanos(timeout);
        long lastTime = System.nanoTime();
        
        if (lock.tryLock(nanos, TimeUnit.NANOSECONDS)) {

            try {

                // subtract out the lock waiting time.
                final long now = System.nanoTime();
                
                nanos -= now - lastTime;
                
                lastTime = now;

                if (counter.get() == 0) {

                    if (log.isInfoEnabled())
                        log.info("Done waiting (true).");

                    // don't wait.
                    return true;

                }

                if (cond.await(nanos, TimeUnit.NANOSECONDS)) {

                    if (log.isInfoEnabled())
                        log.info("Done waiting (true)");

                    return true;

                }

            } finally {

                lock.unlock();

            }
        }

        if (log.isInfoEnabled())
            log.info("Timeout (false) : counter" + counter);

        // Timeout.
        return false;
        
    }

}
