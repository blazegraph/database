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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * basic unit tests.
 * 
 * FIXME Verify interaction with the writeService. The runnable target should
 * not complete until after the commit (should be ok).
 * 
 * @todo test to verify that we can cancel a running task using its
 *       {@link Future}.
 * 
 * @todo test cancellation of the task using get() w/ timeout (or get rid of the
 *       lock timeout).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNonBlockingLockManager extends TestCase {

    protected static final Logger log = Logger.getLogger(TestNonBlockingLockManager.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * 
     */
    public TestNonBlockingLockManager() {
        super();
    }

    public TestNonBlockingLockManager(String name) {
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
     * Mock executor used by a few unit tests optionally wraps a real
     * {@link Executor}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class MockExecutor implements Executor {

        final Executor delegate;
        
        public MockExecutor(Executor delegate) {
            
            this.delegate = delegate;
            
        }
        
        public void execute(final Runnable arg0) {

            if (INFO)
                log.info("Executing: " + arg0);

            if (delegate == null) {

                throw new AssertionFailedError(
                        "Not expecting task (no delegate): " + arg0);
                
            }
            
            delegate.execute(arg0);

            if (INFO)
                log.info("Executed: " + arg0);
            
        }
        
    }
    
    /**
     * Test startup and fast shutdown.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_shutdownNow() throws InterruptedException, ExecutionException {

        final NonBlockingLockManager<String> service = new NonBlockingLockManager<String>(
                10/* maxConcurrency */, true/* predeclareLocks */,
                new MockExecutor(null/*delegate*/));
        
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

        final NonBlockingLockManager<String> service = new NonBlockingLockManager<String>(
                10/* maxConcurrency */, true/* predeclareLocks */,
                new MockExecutor(null/*delegate*/));
        
        try {
            
            assertTrue(service.isOpen());
            
        } finally {

            assertFalse(service.isShutdown());

            service.shutdown();

            assertTrue(service.isShutdown());
            
        }

    }

//    /**
//     * Test startup that runs the service for a bit so you can verify that it is
//     * not sucking down the CPU.
//     * 
//     * @throws InterruptedException
//     * @throws ExecutionException
//     * 
//     * @todo this does not test anything and should be removed.
//     */
//    public void test_running() throws InterruptedException, ExecutionException {
//
//        final NonBlockingLockManager<String> service = new NonBlockingLockManager<String>(
//                10/* maxConcurrency */, true/* predeclareLocks */,
//                new MockExecutor(null/*delegate*/));
//        
//        try {
//            
//            assertTrue(service.isOpen());
//            
//            Thread.sleep(5000/* ms */);
//            
//        } finally {
//
//            assertFalse(service.isShutdown());
//
//            service.shutdown();
//
//            assertTrue(service.isShutdown());
//            
//        }
//
//    }

    /**
     * Test ability to run a {@link Callable} on the service, get() the result,
     * and then shutdown the service.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_run1() throws InterruptedException, ExecutionException {
        
        final ExecutorService delegate = Executors
                .newCachedThreadPool(new DaemonThreadFactory(getClass()
                        .getName()));

        final NonBlockingLockManager<String> service = new NonBlockingLockManager<String>(
                10/* maxConcurrency */, true/* predeclareLocks */,
                new MockExecutor(delegate));
        
        try {
            
            final String expected = "a";
            
            final Future<String> f = service.submit(new String[0], new Callable<String>(){
                public String call() throws Exception {
                    return expected;
                }
            });

            assertEquals(expected,f.get());
            
        } finally {

            service.shutdownNow();
            
            delegate.shutdownNow();

        }

    }

    /**
     * Test ability to obtain a lock, run a {@link Callable} on the service,
     * get() the result, and then shutdown the service.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_run1WithLock() throws InterruptedException, ExecutionException {
        
        final ExecutorService delegate = Executors
                .newCachedThreadPool(new DaemonThreadFactory(getClass()
                        .getName()));

        final NonBlockingLockManager<String> service = new NonBlockingLockManager<String>(
                10/* maxConcurrency */, true/* predeclareLocks */,
                new MockExecutor(delegate));
        
        try {

            final String expected = "a";

            final Future<String> f = service.submit(new String[] { "test" },
                    new Callable<String>() {
                        public String call() throws Exception {
                            return expected;
                        }
                    });

            assertEquals(expected, f.get());
            
        } finally {

            service.shutdownNow();
            
            delegate.shutdownNow();

        }

    }

//    /**
//     * Unit test verifies that {@link TxDag} can resolve a
//     * {@link LockFutureTask} in its internal "mapping" table. This is designed
//     * to detect problems with hashCode() and equals() as they relate to
//     * {@link TxDag}.
//     */
//    public void test_txDagLookup() {
//
//        final MockNonBlockingLockManager<String> service = new MockNonBlockingLockManager<String>(
//                2/* maxConcurrency */, false/* predeclareLocks */,
//                new MockExecutor(null));
//
//        try {
//
//            final TxDag txDag = service.getWaitsFor();
//
//            final NonBlockingLockManager<String>.LockFutureTask<Void> t1 = service.newFutureTask(
//                    new String[] { "a" }, new Callable<Void>() {
//                        public Void call() throws Exception {
//                            return null;
//                        }
//                    });
//
//            final NonBlockingLockManager<String>.LockFutureTask<Void> t2 = service.newFutureTask(
//                    new String[] { "a" }, new Callable<Void>() {
//                        public Void call() throws Exception {
//                            return null;
//                        }
//                    });
//
//            assertEquals(0,txDag.size());
//            txDag.addEdge(t1, t2); // t1 WAITS_FOR t2
//            assertEquals(2,txDag.size());
//            txDag.removeEdges(t1, true/*waiting*/);
////            txDag.removeEdge(t1, t2);
////            txDag.releaseVertex(t1);
////            txDag.releaseVertex(t2);
//            assertEquals(0,txDag.size());
//            
//        } finally {
//
//            service.shutdownNow();
//
//        }
//        
//    }
//    
//    /**
//     * Class exposes a factory for {@link LockFutureTask} objects.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     * @param <R>
//     */
//    static class MockNonBlockingLockManager<R extends Comparable<R>> extends
//            NonBlockingLockManager<R> {
//
//        public MockNonBlockingLockManager(int maxConcurrency,
//                boolean predeclareLocks, Executor delegate) {
//
//            super(maxConcurrency, predeclareLocks, delegate);
//            
//        }
//
//        TxDag getWaitsFor() {
//
//            return waitsFor;
//
//        }
//        
//        <T> LockFutureTask<T> newFutureTask(R[] resources,
//                final Callable<T> task) {
//
//            return new LockFutureTask<T>(resources, task,
//                    Long.MAX_VALUE/* timeout */, 3/* maxLockTries */);
//            
//        }
//        
//    }

}
