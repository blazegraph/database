package com.bigdata.concurrent;

import java.util.Arrays;
import java.util.concurrent.Callable;

import org.CognitiveWeb.concurrent.locking.DeadlockException;
import org.CognitiveWeb.concurrent.locking.TimeoutException;

import com.bigdata.concurrent.TestConcurrencyControl.HorridTaskDeath;

/**
 * Abstract base class for an operation requiring exclusive access to one or
 * more resources. This implementation requires that operations predeclare
 * their locks and coordinates with a {@link LockManager}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param R
 *            The type of the object that identifies a resource for the
 *            purposes of the locking system. This is typically the name of
 *            an index.
 */
public abstract class AbstractResourceTask<R extends Comparable<R>> implements
        Callable<Object> {

    private final LockManager<R> lockManager;

    private final R[] resource;

    private int maxLockTries = 1;

    private long lockTimeout = 0L;

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

    public int getMaxLockTries() {

        return maxLockTries;

    }

    public long setLockTimeout(long newValue) {

        long t = this.lockTimeout;

        this.lockTimeout = newValue;

        return t;

    }

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
     */
    protected AbstractResourceTask(LockManager<R> lockManager, R[] resource) {

        if (lockManager == null)
            throw new NullPointerException();

        if (resource == null)
            throw new NullPointerException();

        for (int i = 0; i < resource.length; i++) {

            if (resource[i] == null)
                throw new NullPointerException();

        }

        this.lockManager = lockManager;

        this.resource = resource;

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
     * Acquires pre-declared locks and then runs the operation.
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

            TestConcurrencyControl.log.info("Acquired locks");

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

            TestConcurrencyControl.log.info(toString() + ": run - start");

            final Object ret = run();

            // done "running".
            lockManager.nrunning.decrementAndGet();

            TestConcurrencyControl.log.info(toString() + ": run - end");

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

            TestConcurrencyControl.log
                    .error("Problem running task: " + this, t);

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

    /**
     * Run the task (resources have already been locked and will be unlocked
     * on completion).
     * 
     * @throws Exception
     */
    abstract protected Object run() throws Exception;

}
