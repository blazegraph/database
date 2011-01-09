/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Apr 29, 2010
 */

package com.bigdata.counters.striped;

import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.system.SystemUtil;

import com.bigdata.counters.CounterSet;

/**
 * Abstract base class and template for striped counters designed for high
 * concurrency with eventual consistency and approximate read back without CAS
 * contention.
 * <p>
 * Each instance of this class in use by the application is backed by an array
 * of N instances protected by striped locks. You {@link #acquire()} an instance
 * and then directly manipulate the value of the counters on that instance. When
 * you {@link #release()} the instance, the counters MAY be published to the
 * outer instance visible to your application using
 * {@link #add(StripedCounters)}. This is similar to an eventually consistent
 * transactional model, except that there are no constraints on the validity of
 * the updates imposed by this scheme.
 * <p>
 * If reads will be made against the shared instance without holding a lock,
 * then the individual counters should be volatile fields, {@link AtomicLong},
 * or the like so that updates will be visible each time
 * {@link #add(StripedCounters)} is invoked by {@link #release()} will be
 * published in a timely manner.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @todo The problem with this approach is that we can not decide that some
 *       counters should be published immediately while others should be
 *       published only every M touches [this might be something which can be
 *       hacked for specific counters using an incFoo() or addFoo() method for
 *       that counter which always updates the value on the parent if invoked on
 *       a striped counter.]
 */
public class StripedCounters<T extends StripedCounters<T>> {

    /** #of releases before we post the counters to the parent. */
    private int batchSize;

    /**
     * Decremented by {@link #release()} - when ZERO (0) we post the counters to
     * the {@link #parent} and reset to the {@link #batchSize}
     * 
     */
    private int n;

    /**
     * The child counters (these are the stripes).
     */
    private final StripedCounters<?>[] a;

    private final ReentrantLock[] locks;

    private T parent;

    /**
     * Return the stripe to be used by the caller's thread.
     */
    private final int threadHash() {
        
        return Math.abs(Thread.currentThread().hashCode() % a.length);
        
    }
    /**
     * Cached value of {@link #threadHash()} as computed by the parent in
     * {@link #acquire()}.
     */
    private int threadHash;

    /**
     * Acquire a "stripe" counters object. The selected stripe is based on
     * {@link Thread#hashCode()}. The chances of a contention with a concurrent
     * thread are <code>(1.0/nstripes)</code>.
     * <p>
     * Note: When using this class you MUST be careful to avoid constructions
     * which could lead to lock ordering problems. If there are other locks
     * help, then each {@link #acquire()} MAY result in a deadlock with an
     * {@link #acquire()} by another thread depending on which stripe is
     * selected for each thread. Such deadlocks are obviously path dependent and
     * difficult to detect.
     */
    @SuppressWarnings("unchecked")
    final public T acquire() {
        if (a == null)
            return (T) this;
        final int i = threadHash();
        a[i].threadHash = i; // save for the child to use in release()
        locks[i].lock();
        return (T) a[i];
    }

    /**
     * Release the counters.
     */
    @SuppressWarnings("unchecked")
    final public void release() {
        if (parent == null)
            return;
        if (--n == 0) {
            n = batchSize;
            // lock for update on the parent.
            synchronized (parent) {
                parent.add((T) this);
            }
            // zero values on the child.
            clear();
        }
        parent.locks[threadHash].unlock();
    }

    /**
     * Required public zero argument constructor provides no concurrency
     * control. Instances created with this constructor are used (internally) in
     * a variety of contexts where striped locks are not appropriate and may be
     * used by the application as well if striped locks are not necessary.
     */
    public StripedCounters() {
        a = null;
        locks = null;
        parent = null;
        batchSize = n = 1;
    }

    /**
     * Create a striped counters object using <code>numProcessors*2</code>
     * stripes and the specified <i>batchSize</i>.
     * 
     * @param batchSize
     *            The values on the instance stripes will be published to the
     *            outer instance every batchSize {@link #release()}s.
     */
    public StripedCounters(final int batchSize) {

        this(SystemUtil.numProcessors() * 2, batchSize);
        
    }

    /**
     * Create a striped counters object.
     * 
     * @param nstripes
     *            The #of stripes. Each stripe is protected by a lock. The
     *            stripe is selected by {@link #acquire()} based on
     *            {@link Thread#hashCode()}. The chances of a contention with a
     *            concurrent thread are <code>(1.0/nstripes)</code>.
     * @param batchSize
     *            The values on the instance stripes will be published to the
     *            outer instance every batchSize {@link #release()}s.
     */
    public StripedCounters(final int nstripes, final int batchSize) {
        if (nstripes < 1)
            throw new IllegalArgumentException();
        if (batchSize < 1)
            throw new IllegalArgumentException();
        a = new StripedCounters[nstripes];
        locks = new ReentrantLock[nstripes];
        for (int i = 0; i < nstripes; i++) {
            final T t = newStripedCounters();
            t.parent = (T) this;
            t.batchSize = t.n = batchSize;
            a[i] = t;
            locks[i] = new ReentrantLock();
        }
        this.parent = null;
        this.batchSize = n = batchSize;
    }

//    /** One of a set of striped counters accessed used {@link #acquire()}. */
//    protected StripedCounters(final StripedCounters parent, final int batchSize) {
//        a = null;
//        locks = null;
//        this.parent = parent;
//        this.batchSize = n = batchSize;
//    }

    /**
     * Create an instance of the same class using the public zero-argument
     * constructor for that class.
     * 
     * @return The new instance.
     */
    @SuppressWarnings("unchecked")
    private T newStripedCounters() {

        try {

            final Constructor<?> ctor = getClass().getConstructor(
                    new Class[] {});

            return (T) ctor.newInstance(new Object[] {});

        } catch (Throwable t) {

            throw new RuntimeException(t);

        }

    }

    /*
     * Methods which must be extended by derived classes.
     */
    
    /**
     * Adds counters to the current counters.
     * 
     * @param o The other counters.
     */
    public void add(final T o) {

        // add together the given counters.
        
//        nreads += o.nreads;
//        ndiskRead += o.ndiskRead;

    }

    /**
     * Returns a new {@link StripedCounters} containing the current counter
     * values minus the given counter values.
     * 
     * @param o
     *            The other counters.
     * 
     * @return A new set of counters.
     */
    public T subtract(final T o) {

        // make a copy of the current counters.
        final T t = newStripedCounters();
        t.add((T) this);

        // subtract out the given counters.
        
//        t.nreads -= o.nreads;
//        t.ndiskRead -= o.ndiskRead;

        return t;

    }

    /**
     * Clear the counter values back to zero.
     */
    public void clear() {
        
    }
    
    /**
     * Return a new, empty {@link CounterSet}.  This method must be extended
     * to attach the various performance counters to that {@link CounterSet}.
     */
    public CounterSet getCounters() {

        return new CounterSet();
            
    }

    /**
     * Return a serialization of the performance counters.
     */
    public String toString() {
        
        return getCounters().toString();
        
    }
    
}
