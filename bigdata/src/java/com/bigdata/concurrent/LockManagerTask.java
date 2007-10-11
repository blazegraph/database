/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 3, 2007
 */

package com.bigdata.concurrent;

import java.util.Arrays;
import java.util.concurrent.Callable;

import org.CognitiveWeb.concurrent.locking.DeadlockException;
import org.CognitiveWeb.concurrent.locking.TimeoutException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.concurrent.TestConcurrencyControl.HorridTaskDeath;

/**
 * Class encapsulates handshaking with the {@link LockManager} for an operation
 * requiring exclusive access to one or more resources and that are willing to
 * pre-declare their resource requirements.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param R
 *            The type of the object that identifies a resource for the purposes
 *            of the locking system. This is typically the name of an index.
 */
public class LockManagerTask<R extends Comparable<R>> implements
        Callable<Object> {

    protected static final Logger log = Logger.getLogger(LockManagerTask.class);
    
    private final LockManager<R> lockManager;

    private final R[] resource;

    private final Callable<Object> target;
    
    private int maxLockTries = 1;

    private long lockTimeout = 0L;

    /**
     * The {@link LockManager}.
     */
    public LockManager<R> getLockManager() {
        
        return lockManager;
        
    }
    
    /**
     * The resource(s) that are pre-declared by the task. {@link #call()} will
     * ensure that the task as a lock on these resources before it invokes
     * {@link #run()} to execution the task.
     */
    public R[] getResource() {
        
        return resource;
        
    }
    
    public int setMaxLockTries(int newValue) {

        if (newValue < 1)
            throw new IllegalArgumentException();

        int t = this.maxLockTries;

        this.maxLockTries = newValue;

        return t;

    }

    /**
     * The maximum #of times that the task will attempt to acquire its locks
     * (positive integer).
     */
    public int getMaxLockTries() {

        return maxLockTries;

    }

    public long setLockTimeout(long newValue) {

        long t = this.lockTimeout;

        this.lockTimeout = newValue;

        return t;

    }

    /**
     * The timeout (milliseconds) or ZERO (0L) for an infinite timeout.
     */
    public long getLockTimeout() {

        return lockTimeout;

    }

    /**
     * 
     * @param lockManager
     *            The lock manager.
     * 
     * @param resource
     *            The resource(s) to be locked.
     * 
     * @param target
     *            The {@link Runnable} target that will be invoked iff the locks
     *            are successfully acquired.
     */
    public LockManagerTask(LockManager<R> lockManager, R[] resource, Callable<Object> target) {

        if (lockManager == null)
            throw new NullPointerException();

        if (resource == null)
            throw new NullPointerException();

        if (resource.length == 0)
            throw new IllegalArgumentException();
        
        for (int i = 0; i < resource.length; i++) {

            if (resource[i] == null)
                throw new NullPointerException();

        }

        if(target==null) throw new NullPointerException();
        
        this.lockManager = lockManager;

        this.resource = resource;
        
        this.target = target;

    }

    /**
     * Attempt to acquire locks on resources required by the task.
     * <p>
     * Up to {@link #getMaxLockTries()} attempts will be made.
     * 
     * @exception DeadlockException
     *                if the locks could not be acquired (last exception
     *                encountered only).
     * @exception TimeoutException
     *                if the locks could not be acquired (last exception
     *                encountered only).
     */
    private void acquireLocks() throws Exception {

        for (int i = 0; i < maxLockTries; i++) {

            try {

                // Request resource lock(s).

                lockManager.lock(resource, lockTimeout);

                return;

            } catch (DeadlockException ex) {

                // Count deadlocks.

                lockManager.ndeadlock.incrementAndGet();

            } catch (TimeoutException ex) {

                // Count timeouts.

                lockManager.ntimeout.incrementAndGet();

            }

            /*
             * Release any locks granted since we did not get all of the
             * locks that we were seeking.
             */

            lockManager.releaseLocks(true/*waiting*/);

        }

    }

    /**
     * Acquires pre-declared locks and then runs the operation identified to the
     * constructor.
     * 
     * @return <code>null</code>
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    final public Object call() throws Exception {

        // start.
        lockManager.didStart(this);

        try {

            /*
             * Acquire pre-declared locks.
             * 
             * Note: in order to refactor this class so that operations do
             * NOT have to predeclare their locks you need to make sure that
             * the handshaking with the {@link LockManager} correctly
             * invokes
             * {@link LockManager#didAbort(Callable, Throwable, boolean)}
             * with the appropriate value for the "waiting" parameter
             * depending on whether or not the transaction is currently
             * waiting. This is more tricky if the operation is able to
             * request additional locks in run() since we need to either
             * carefully differentiate the context or just assume that the
             * operation is waiting unless it has completed successfully.
             */

            acquireLocks();

            log.info("Acquired locks");

        } catch (Exception ex) {

            // abort.
            lockManager.didAbort(this, ex, true /*waiting*/);

            // rethrow (do not masquerade the exception).
            throw ex;

        } catch (Throwable t) {

            // abort.
            lockManager.didAbort(this, t, true /*waiting*/);

            // rethrow (masquerade the exception).
            throw new RuntimeException(t);

        }

        /*
         * Run the task now that we have the necessary lock(s).
         */
        /*
         * Note: "running" means in "run()". 
         */
        long nrunning = lockManager.nrunning.incrementAndGet();

        // Note: not really atomic and hence only approximate.
        lockManager.maxrunning.set(Math.max(lockManager.maxrunning.get(), nrunning));

        try {

            log.info(toString() + ": run - start");

            final Object ret = target.call();

            // done "running".
            lockManager.nrunning.decrementAndGet();

            log.info(toString() + ": run - end");

            lockManager.didSucceed(this);

            return ret;

        } catch (Throwable t) {

            // done "running".
            lockManager.nrunning.decrementAndGet();

            if (t instanceof HorridTaskDeath) {

                // An expected error.

                lockManager.didAbort(this, t, false /* NOT waiting */);

                throw (HorridTaskDeath) t;

            }

            // An unexpected error.

            if(log.getLevel().isGreaterOrEqual(Level.ERROR)) {
                
                log.error("Problem running task: " + this, t);
                
            }

            lockManager.didAbort(this, t, false /* NOT waiting */);

            if (t instanceof Exception) {

                // Do not masquerade.
                throw (Exception) t;

            }

            // masquerade.
            throw new RuntimeException(t);

        }

    }

    public String toString() {

        return super.toString() + " resources=" + Arrays.toString(resource);

    }

}
