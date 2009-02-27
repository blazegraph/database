/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 1, 2007
 */

package com.bigdata.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * basic unit tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNonBlockingLockManagerWithNewDesign extends TestCase {

    protected static final Logger log = Logger
            .getLogger(TestNonBlockingLockManagerWithNewDesign.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * 
     */
    public TestNonBlockingLockManagerWithNewDesign() {
        super();
    }

    public TestNonBlockingLockManagerWithNewDesign(String name) {
        super(name);
    }

    /**
     * Waits 10ms once it acquires its locks.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Wait10ResourceTask<T> implements Callable<T> {

        public T call() throws Exception {

//            if (INFO)
//                log.info("Executing: "+this);
            
            synchronized (this) {

                try {
                    
                    if (DEBUG)
                        log.debug("Waiting: "+this);
                    
                    wait(10/* milliseconds */);
                    
//                    if (INFO)
//                        log.info("Done waiting: "+this);
                    
                } catch (InterruptedException e) {
                    
                    throw new RuntimeException(e);
                    
                }
                
            }
            
//            if (INFO)
//                log.info("Done: "+this);
            
            return null;

        }

    }
    
    /**
     * Dies once it acquires its locks by throwing {@link HorridTaskDeath}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class DeathResourceTask<T> implements Callable<T> {
        
        public T call() throws Exception {

            if(DEBUG)
                log.debug("Arrgh!");
            
            throw new HorridTaskDeath();

        }

    }
    
    /**
     * Test startup and fast shutdown.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_shutdownNow() throws InterruptedException, ExecutionException {

        final NonBlockingLockManagerWithNewDesign<String> service = new NonBlockingLockManagerWithNewDesign<String>(
                10/* maxConcurrency */, 1/* maxLockTries */,
                true/* predeclareLocks */) {
          
            protected void ready(Runnable r) {
                
                throw new UnsupportedOperationException();
                
            }
            
        };
        
        try {
            
            assertTrue(service.isOpen());
            
        } finally {

            assertFalse(service.isShutdown());

            service.shutdownNow();

            assertTrue(service.isShutdown());
            
        }

    }

    /**
     * Test startup and normal shutdown.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_shutdown() throws InterruptedException, ExecutionException {

        final NonBlockingLockManagerWithNewDesign<String> service = new NonBlockingLockManagerWithNewDesign<String>(
                10/* maxConcurrency */, 1/* maxLockTries */,
                true/* predeclareLocks */) {
          
            protected void ready(Runnable r) {
                
                throw new UnsupportedOperationException();
                
            }
            
        };
        
        try {
            
            assertTrue(service.isOpen());
            
        } finally {

            assertFalse(service.isShutdown());

            service.shutdown();

            assertTrue(service.isShutdown());
            
        }

    }

    /**
     * Create an {@link Executor}. The caller is responsible for shutting down
     * the service.
     * 
     * @return The {@link Executor}.
     */
    private ExecutorService newExecutor() {

        final int corePoolSize = 10;
        final int maximumPoolSize = 10;
        final long keepAliveTime = 60;
        final TimeUnit unit = TimeUnit.SECONDS;
        final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
        
        return new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
                keepAliveTime, unit, workQueue, new DaemonThreadFactory(
                        getClass().getName()));
        
    }
    
    /**
     * Test ability to submit a {@link Callable} to the service and verify that
     * is reported at ready(Runnable) and that we can cancel the {@link Future}.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submitOneThenCancel() throws InterruptedException, ExecutionException {
        
        final BlockingQueue<Runnable> readyQueue = new LinkedBlockingQueue<Runnable>();

        final NonBlockingLockManagerWithNewDesign<String> service = new NonBlockingLockManagerWithNewDesign<String>(
                10/* maxConcurrency */, 1/* maxLockTries */,
                true/* predeclareLocks */) {
          
            protected void ready(Runnable r) {
                
                readyQueue.add(r);
                
            }
            
        };
        
        try {
            
            final String expected = "a";
            
            final NonBlockingLockManagerWithNewDesign<String>.LockFutureTask<String> f = (NonBlockingLockManagerWithNewDesign<String>.LockFutureTask<String>) service
                    .submit(new String[0], new Callable<String>() {
                        public String call() throws Exception {
                            return expected;
                        }
                    });

            /*
             * Note: This verifies that locks are granted synchronously when we
             * accept a task whose locks are not already held by someone else.
             */
            assertEquals(1, readyQueue.size());

            assertEquals(f, readyQueue.peek());
            
            assertFalse(f.isCancelled());

            assertFalse(f.isDone());
            
            assertEquals(
                    NonBlockingLockManagerWithNewDesign.TaskRunState.LocksReady,
                    f.getTaskRunState());
            
            f.cancel(true/* mayInterruptIfRunning */);

            assertTrue(f.isCancelled());

            assertTrue(f.isDone());
            
            assertEquals(
                    NonBlockingLockManagerWithNewDesign.TaskRunState.Halted,
                    f.getTaskRunState());
            
            try {
                f.get();
                fail("Expecting: " + CancellationException.class);
            } catch (CancellationException ex) {
                if (INFO)
                    log.info("Expected exception: " + ex);
            }
            
        } finally {

            service.shutdownNow();

        }

    }

    /**
     * Test ability to run a {@link Callable} on the service, get() the result,
     * and then shutdown the service.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public void test_runOne() throws InterruptedException, ExecutionException,
            TimeoutException {

        final ExecutorService delegate = newExecutor();

        final NonBlockingLockManagerWithNewDesign<String> service = new NonBlockingLockManagerWithNewDesign<String>(
                10/* maxConcurrency */, 1/* maxLockTries */, true/* predeclareLocks */) {

            protected void ready(Runnable r) {

                delegate.execute(r);

            }

        };

        try {

            final String expected = "a";

            final NonBlockingLockManagerWithNewDesign<String>.LockFutureTask<String> f = (NonBlockingLockManagerWithNewDesign<String>.LockFutureTask<String>) service
                    .submit(new String[0], new Callable<String>() {
                        public String call() throws Exception {
                            return expected;
                        }
                    });

            assertFalse(f.isCancelled());

            // Note: Set unit to HOURS for debugging :-)
            assertEquals(expected, f.get(10/* ms */, TimeUnit.MILLISECONDS));

            /*
             * Note: LockFutureTask can not be made to reliably release the
             * locks and update the run state before Future#get() returns.
             * Therefore this test sleeps a bit before checking the run state.
             */
            Thread.sleep(10/*ms*/);

            // verify that the run state was updated.
            assertEquals(
                    NonBlockingLockManagerWithNewDesign.TaskRunState.Halted, f
                            .getTaskRunState());

        } finally {

            service.shutdownNow();

            delegate.shutdownNow();

        }

    }

    /**
     * Test ability to run a {@link Callable} on the service which throws an
     * exception, get() the result, and then shutdown the service.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public void test_runOneThrowsException() throws InterruptedException, ExecutionException,
            TimeoutException {

        final ExecutorService delegate = newExecutor();

        final NonBlockingLockManagerWithNewDesign<String> service = new NonBlockingLockManagerWithNewDesign<String>(
                10/* maxConcurrency */, 1/* maxLockTries */, true/* predeclareLocks */) {

            protected void ready(Runnable r) {

                delegate.execute(r);

            }

        };

        try {

            final NonBlockingLockManagerWithNewDesign<String>.LockFutureTask<String> f = (NonBlockingLockManagerWithNewDesign<String>.LockFutureTask<String>) service
                    .submit(new String[0], new Callable<String>() {
                        public String call() throws Exception {
                            // task throws exception.
                            throw new HorridTaskDeath();
                        }
                    });

            assertFalse(f.isCancelled());

            try {
                // Note: Set unit to HOURS for debugging :-)
                f.get(10/* ms */, TimeUnit.MILLISECONDS);
                fail("Expecting: "+HorridTaskDeath.class);
            } catch (ExecutionException ex) {
                if (ex.getCause() instanceof HorridTaskDeath) {
                    if(INFO)
                        log.info("Ignoring expected exception: " + ex);
                } else {
                    final AssertionFailedError err = new AssertionFailedError(
                            "Expecting: " + HorridTaskDeath.class
                                    + " as the cause");
                    err.initCause(ex);
                    throw err;
                }
            }

        } finally {

            service.shutdownNow();

            delegate.shutdownNow();

        }

    }

    /**
     * Succeeds if the task holds all of its declared locks.
     * @param <R>
     * @param <T>
     * @param service
     * @param task
     */
    protected <R extends Comparable<R>, T> void assertLocksHeld(
            NonBlockingLockManagerWithNewDesign<R> service,
            NonBlockingLockManagerWithNewDesign<R>.LockFutureTask<T> task) {

        for (R r : task.getResource()) {

            if (!service.isLockHeldByTask(r, task)) {

                fail("Task does not hold lock: " + task);
                
            }
            
        }
        
    }
    
    /**
     * Succeeds if the task holds none of its declared locks.
     * @param <R>
     * @param <T>
     * @param service
     * @param task
     */
    protected <R extends Comparable<R>, T> void assertLocksNotHeld(
            NonBlockingLockManagerWithNewDesign<R> service,
            NonBlockingLockManagerWithNewDesign<R>.LockFutureTask<T> task) {
        
        for(R r : task.getResource()) {
            
            if(service.isLockHeldByTask(r, task)) {

                fail("Task holds lock: "+task);
                
            }
            
        }
        
    }
    
    /**
     * Test ability to obtain a lock, run a {@link Callable} on the service,
     * get() the result, and then shutdown the service.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public void test_runOneWithLock() throws InterruptedException,
            ExecutionException, TimeoutException {

        final ExecutorService delegate = newExecutor();

        final NonBlockingLockManagerWithNewDesign<String> service = new NonBlockingLockManagerWithNewDesign<String>(
                10/* maxConcurrency */, 1/* maxLockTries */, true/* predeclareLocks */) {

            protected void ready(Runnable r) {

                delegate.execute(r);

            }

        };
        
        try {

            final String expected = "a";

            final NonBlockingLockManagerWithNewDesign<String>.LockFutureTask<String> f = (NonBlockingLockManagerWithNewDesign<String>.LockFutureTask<String>) service
                    .submit(new String[] { "test" }, new Callable<String>() {
                        public String call() throws Exception {
                            return expected;
                        }
                    });

            assertEquals(expected, f.get(10/* ms */, TimeUnit.MILLISECONDS));

            /*
             * Note: LockFutureTask can not be made to reliably release the
             * locks and update the run state before Future#get() returns.
             * Therefore this test sleeps a bit before verifying that the locks
             * were released.
             */
            Thread.sleep(10/*ms*/);

            assertLocksNotHeld(service, f);
            
        } finally {

            service.shutdownNow();
            
            delegate.shutdownNow();

        }

    }

    /**
     * Test ability to obtain a lock, run a {@link Callable} on the service that
     * releases its locks during its computation, verify that the locks were
     * released, get() the result, and then shutdown the service.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public void test_runOneWithLockAndReleaseLockFromTask() throws InterruptedException,
            ExecutionException, TimeoutException {

        final ExecutorService delegate = newExecutor();

        final NonBlockingLockManagerWithNewDesign<String> service = new NonBlockingLockManagerWithNewDesign<String>(
                10/* maxConcurrency */, 1/* maxLockTries */, true/* predeclareLocks */) {

            protected void ready(Runnable r) {

                // verify locks are held.
                assertLocksHeld(
                        this,
                        (NonBlockingLockManagerWithNewDesign<String>.LockFutureTask<String>) r);

                delegate.execute(r);

            }

        };
        
        try {

            final String expected = "a";

            final NonBlockingLockManagerWithNewDesign<String>.LockFutureTask<String> f = (NonBlockingLockManagerWithNewDesign<String>.LockFutureTask<String>) service
                    .submit(new String[] { "test" }, new Callable<String>() {
                        public String call() throws Exception {
                            // release locks.
                            service
                                    .releaseLocksForTask(new String[] { "test" });
                            try {
                                // error expected the 2nd time.
                                service
                                        .releaseLocksForTask(new String[] { "test" });
                                fail("Expected: " + IllegalStateException.class);
                            } catch (IllegalStateException ex) {
                                if (INFO)
                                    log.info("Ignoring expected exception: "
                                            + ex);
                            }
                            return expected;
                        }
                    });

            assertEquals(expected, f.get(10/* ms */, TimeUnit.MILLISECONDS));

            /*
             * Note: LockFutureTask can not be made to reliably release the
             * locks and update the run state before Future#get() returns.
             * Therefore this test sleeps a bit before verifying that the locks
             * were released.
             */
            Thread.sleep(10/*ms*/);

            assertLocksNotHeld(service, f);
            
        } finally {

            service.shutdownNow();
            
            delegate.shutdownNow();

        }

    }

}
