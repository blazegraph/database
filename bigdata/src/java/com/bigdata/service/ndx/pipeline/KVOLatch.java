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
 * Created on Apr 22, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;


/**
 * A synchronization aid that allows one or more threads to await asynchronous
 * writes on one or more scale-out indices. Once the counter reaches zero, all
 * waiting threads are released. The counter is decemented automatically once a
 * {@link KVOC} has been successfully written onto an index by an asynchronous
 * write operation.
 * <p>
 * Since it is possible that the counter could be transiently zero as chunks are
 * being added and drained concurrently, you MUST {@link #inc() increment} the
 * counter before adding the first chunk and {@link #dec() decrement} the
 * counter after adding the last chunk. Threads may invoke {@link #await()} any
 * time after the counter has been incremented. They will not be released until
 * the counter is zero. If you have protected against transient zeros by
 * pre-incrementing the counter, the threads will not be released until the
 * asynchronous write operations are successfully completed.
 * <p>
 * The notification is based on {@link KVOCounter}, which associates an atomic
 * counter and a user-defined key with each tuple. Notices are generated when
 * the atomic counter is zero on decrement. Notices are aligned with the
 * appropriate scope by creating an instance of the {@link KVOScope} with that
 * scope and then pairing it with each {@link KVOCounter}.
 * <P>
 * For any significant workload, notification should be quite fast. However, if
 * GC is not being driven by heap churn then notification may not occur. In
 * practice, this is mainly an issue when writing unit tests since you can not
 * force the JVM to clear the weak references.
 * <p>
 * Note: This class is very similar to a {@link CountDownLatch}, however the
 * counter maximum is not specified in advance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <K>
 *            The generic type of the user-defined key.
 * 
 * @see IndexPartitionWriteTask, which is responsible for invoking
 *      {@link #dec()}.
 */
public class KVOLatch {

    protected transient static final Logger log = Logger.getLogger(KVOLatch.class);
    
    private final AtomicLong counter = new AtomicLong();
    
    private final ReentrantLock lock = new ReentrantLock();
    
    private final Condition cond = lock.newCondition();
    
    public String toString() {
        
        return getClass().getName() + "{counter=" + counter + "}";
        
    }
    
    public KVOLatch() {

    }

    /**
     * The counter value.
     */
    public long get() {

        return counter.get();
        
    }
    
    /**
     * Increments the internal counter.
     */
    public void inc() {
        
        if (this.counter.incrementAndGet() <= 0) {
            
            // counter is/was negative.
            throw new AssertionError();
            
        }
        
    }

    /**
     * Decrements the internal counter and notifies the listener if the counter
     * reaches zero. Callers do not block unless the counter is decremented to
     * zero, and then they only block long enough to obtain the internal lock
     * and release the threads in {@link #await(long, TimeUnit)}.
     */
    public void dec() {

        final long c = this.counter.decrementAndGet();

        if (c < 0) {

            // counter has become negative.
            throw new AssertionError();

        }

        if (c > 0) {

            // Return immediately.
            return;
            
        }

        try {

            lock.lockInterruptibly();
            try {

                // release anyone awaiting our signal.
                cond.signalAll();

            } finally {

                lock.unlock();
                
            }

        } catch (InterruptedException ex) {

            throw new RuntimeException(ex);

        }
        
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
     *         <code>false</code> if the timeout was exceeded before the
     *         counter reached zero.
     * 
     * @throws InterruptedException
     */
    public boolean await(long timeout, final TimeUnit unit)
            throws InterruptedException {

        if (counter.get() == 0) {

            // don't wait.
            return true;
            
        }
        
        lock.lockInterruptibly();
        try {

            if(cond.await(timeout, unit)) {
                
                return true;
                
            }
            
            return false;
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

}
