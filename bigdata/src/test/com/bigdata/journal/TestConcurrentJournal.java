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

package com.bigdata.journal;

import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import com.bigdata.service.DataService;
import com.bigdata.service.RangeQueryIterator;

/**
 * Test suite for the {@link ConcurrentJournal}.
 * 
 * FIXME write more tests!
 * 
 * @todo Verify proper partial ordering over transaction schedules by modifying
 *       {@link Journal} to extend this class and implement a correct
 *       transaction manager (runs tasks in parallel, then schedules their
 *       commits using a partial order).
 * 
 * @todo write test cases that submit various kinds of operations and verify the
 *       correctness of those individual operations. refactor the services
 *       package to do this, including things such as the
 *       {@link RangeQueryIterator}. this will help to isolate the correctness
 *       of the data service "api", including concurrency of operations, from
 *       the {@link DataService}.
 * 
 * @todo write test cases that attempt operations against a new journal (nothing
 *       committed) and verify that we see {@link NoSuchIndexException}s rather
 *       than something odder.
 * 
 * @todo do large #s of runs with a transient store where we test to verify that
 *       a random (small to modest) population of operations may be executed and
 *       the store shutdown without encountering a deadlock problem in the write
 *       service. An alternative to shutdown is to periodically let the write
 *       service become quiesent (by not submitting more tasks) and verify that
 *       the periodic group commit does not cause a deadlock. Another variant is
 *       to periodically invoke group commit from another thread at random
 *       intervals and verify that no entry timings result in deadlocks.
 * 
 * @todo run tests of transaction throughput using a number of models. E.g., a
 *       few large indices with a lot of small transactions vs a lot of small
 *       indices with small transactions vs a few large indices with moderate to
 *       large transactions. Also look out for transactions where the validation
 *       and merge on the unisolated index takes more than one group commit
 *       cycle and see how that effects the application.
 * 
 * @todo verify that unisolated reads are against the last committed state of
 *       the index(s), that they do NOT permit writes, and than concurrent
 *       writers on the same named index(s) do NOT conflict.
 * 
 * @todo explore tests in which we flood the write service and make sure that
 *       group commits are occuring in a timely basis, that we are not starving
 *       the write service thread pool (by running group commit when sufficient
 *       writers are awaiting a commit), and explore whether the write service
 *       should use a blocking queue (fixing a maximum capacity).
 * 
 * @todo Modify the RDFS database to use concurrent operations when writing the
 *       statement indices (sort and batch insert).
 * 
 * @todo add a stress/correctness test that mixes unisolated and isolated
 *       operations. Note that isolated operations (transactions) during commit
 *       simply acquire the corresponding unisolated index(s) (the transaction
 *       commit itself is just an unisolated operation on one or more indices).
 * 
 * @todo verify that lots of concurrent {@link RegisterIndexTask}s and
 *       {@link DropIndexTask}s are not problematic (proper synchronization on
 *       the {@link Name2Addr} instance).
 * 
 * @todo test writing on multiple unisolated indices and concurrency control for
 *       that.
 * 
 * @todo test writing on multiple isolated indices in the same transaction and
 *       concurrency control for that.
 * 
 * @todo test writing on multiple isolated indices in the different transactions
 *       and verify that no concurrency limits are imposed across transactions
 *       (only within transactions).
 * 
 * @todo show state-based validation for concurrent transactions on the same
 *       index that result in write-write conflicts.
 * 
 * @todo rewrite the {@link StressTestConcurrent} to use
 *       {@link #submit(AbstractIndexTask)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestConcurrentJournal extends ProxyTestCase {

    public TestConcurrentJournal() {
        super();
    }
    
    public TestConcurrentJournal(String name) {
        super(name);
    }

    /**
     * Test ability to create a {@link ConcurrentJournal} and then shut it down.
     */
    public void test_shutdown() {

        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        journal.shutdown();
        
        journal.delete();
        
    }

    public void test_shutdownNow() {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        journal.shutdownNow();
        
        journal.delete();

    }
    
    /**
     * Submits an unisolated task to the read service and verifies that it executes.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submit_readService_01() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final String resource = "foo";
        
        final AtomicBoolean ran = new AtomicBoolean(false);
        
        Future<Object> future = journal.submit(new AbstractIndexTask(journal,
                ITx.UNISOLATED, true/*readOnly*/, resource) {

            /**
             * The task just sets a boolean value and returns the name of the
             * sole resource. It does not actually read anything.
             */
            protected Object doTask() throws Exception {

                ran.compareAndSet(false, true);

                return getOnlyResource();

            }
        });

        // the test task returns the resource as its value.
        assertEquals("result",resource,future.get());
        
        /*
         * make sure that the flag was set (not reliably set until we get() the
         * future).
         */
        assertTrue("ran",ran.get());
        
        /*
         * Verify that a commit was NOT performed.
         */
        assertEquals("commit counter changed?",
                commitCounterBefore, journal.getRootBlockView()
                        .getCommitCounter());

//        Note: This does not always work since the counter is updated asynchronously.
//        
//        /*
//         * Verify that it ran on the read service.
//         */
//        assertEquals("completedTaskCount", 1,
//                ((ThreadPoolExecutor) journal.readService)
//                        .getCompletedTaskCount());
        
        journal.shutdown();

        journal.delete();

    }

    /**
     * Submits an unisolated task to the write service and verifies that it
     * executes.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submit_writeService_01() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String resource = "foo";
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final AtomicBoolean ran = new AtomicBoolean(false);
        
        Future<Object> future = journal.submit(new AbstractIndexTask(journal,
                ITx.UNISOLATED, false/*readOnly*/, resource) {

            /**
             * The task just sets a boolean value and returns the name of the
             * sole resource. It does not actually read or write on anything.
             */
            protected Object doTask() throws Exception {

                ran.compareAndSet(false, true);

                return getOnlyResource();

            }
        });

        // the test task returns the resource as its value.
        assertEquals("result",resource,future.get());
        
        /*
         * make sure that the flag was set (not reliably set until we get() the
         * future).
         */
        assertTrue("ran",ran.get());

        /*
         * Verify that the commit counter was changed.
         */
        assertEquals("commit counter unchanged?",
                commitCounterBefore+1, journal.getRootBlockView()
                        .getCommitCounter());

//      Note: This does not always work since the counter is updated asynchronously.
//      
//        /*
//         * Verify that it ran on the write service.
//         */
//        assertEquals("completedTaskCount", 1,
//                ((ThreadPoolExecutor) journal.writeService)
//                        .getCompletedTaskCount());

        journal.shutdown();

        journal.delete();

    }

    /**
     * Submits an {@link IsolationEnum#ReadOnly} task to the transaction service
     * and verifies that it executes.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submit_txService_readOnly_01() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String resource = "foo";
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final AtomicBoolean ran = new AtomicBoolean(false);
        
        final long tx = journal.newTx(IsolationEnum.ReadOnly);

        assertNotSame(ITx.UNISOLATED,tx);
        
        Future<Object> future = journal.submit(new AbstractIndexTask(journal,
                tx, true/*readOnly*/, resource) {

            /**
             * The task just sets a boolean value and returns the name of the
             * sole resource. It does not actually read or write on anything.
             */
            protected Object doTask() throws Exception {

                ran.compareAndSet(false, true);

                return getOnlyResource();

            }
        });

        // the test task returns the resource as its value.
        assertEquals("result",resource,future.get());
        
        /*
         * make sure that the flag was set (not reliably set until we get() the
         * future).
         */
        assertTrue("ran",ran.get());

        /*
         * Verify that the commit counter was NOT changed.
         */
        assertEquals("commit counter changed?",
                commitCounterBefore, journal.getRootBlockView()
                        .getCommitCounter());

//      Note: This does not always work since the counter is updated asynchronously.
//      
//        /*
//         * Verify that it ran on the transaction service.
//         */
//        assertEquals("completedTaskCount", 1,
//                ((ThreadPoolExecutor) journal.txService)
//                        .getCompletedTaskCount());

        // commit of a read-only tx returns commitTime of ZERO(0L).
        assertEquals(0L,journal.commit(tx));
        
        journal.shutdown();

        journal.delete();

    }
    

    /**
     * Submits a {@link IsolationEnum#ReadCommitted} task to the transaction
     * service and verifies that it executes.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submit_txService_readCommitted_01()
            throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String resource = "foo";
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final AtomicBoolean ran = new AtomicBoolean(false);
        
        final long tx = journal.newTx(IsolationEnum.ReadCommitted);

        assertNotSame(ITx.UNISOLATED,tx);
        
        Future<Object> future = journal.submit(new AbstractIndexTask(journal,
                tx, true/*readOnly*/, resource) {

            /**
             * The task just sets a boolean value and returns the name of the
             * sole resource. It does not actually read or write on anything.
             */
            protected Object doTask() throws Exception {

                ran.compareAndSet(false, true);

                return getOnlyResource();

            }
        });

        // the test task returns the resource as its value.
        assertEquals("result",resource,future.get());
        
        /*
         * make sure that the flag was set (not reliably set until we get() the
         * future).
         */
        assertTrue("ran",ran.get());

        /*
         * Verify that the commit counter was NOT changed.
         */
        assertEquals("commit counter changed?",
                commitCounterBefore, journal.getRootBlockView()
                        .getCommitCounter());

//      Note: This does not always work since the counter is updated asynchronously.
//      
//        /*
//         * Verify that it ran on the transaction service.
//         */
//        assertEquals("completedTaskCount", 1,
//                ((ThreadPoolExecutor) journal.txService)
//                        .getCompletedTaskCount());

        // commit of a readCommitted tx returns commitTime of ZERO(0L).
        assertEquals(0L,journal.commit(tx));
        
        journal.shutdown();

        journal.delete();

    }


    /**
     * Submits a {@link IsolationEnum#ReadWrite} task with an empty write set to
     * the transaction service and verifies that it executes.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submit_txService_readWrite_01() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String resource = "foo";
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final AtomicBoolean ran = new AtomicBoolean(false);
        
        final long tx = journal.newTx(IsolationEnum.ReadWrite);

        assertNotSame(ITx.UNISOLATED,tx);
        
        Future<Object> future = journal.submit(new AbstractIndexTask(journal,
                tx, false/*readOnly*/, resource) {

            /**
             * The task just sets a boolean value and returns the name of the
             * sole resource. It does not actually read or write on anything.
             */
            protected Object doTask() throws Exception {

                ran.compareAndSet(false, true);

                return getOnlyResource();

            }
        });

        // the test task returns the resource as its value.
        assertEquals("result",resource,future.get());
        
        /*
         * make sure that the flag was set (not reliably set until we get() the
         * future).
         */
        assertTrue("ran",ran.get());

        /*
         * Verify that the commit counter was NOT changed.
         */
        assertEquals("commit counter changed?",
                commitCounterBefore, journal.getRootBlockView()
                        .getCommitCounter());

//      Note: This does not always work since the counter is updated asynchronously.
//      
//        /*
//         * Verify that it ran on the transaction service.
//         */
//        assertEquals("completedTaskCount", 1,
//                ((ThreadPoolExecutor) journal.txService)
//                        .getCompletedTaskCount());

        // commit of a readWrite tx with an empty result set returns commitTime
        // of ZERO(0L).
        assertEquals(0L,journal.commit(tx));
        
        journal.shutdown();

        journal.delete();

    }

    /**
     * Submits an unisolated task to the read service. The task just sleeps. We
     * then verify that we can interrupt that task using
     * {@link Future#cancel(boolean)} with
     * <code>mayInterruptWhileRunning := true</code> and that an appropriate
     * exception is thrown in the main thread.
     * <p>
     * Note: In this test the task is terminated and 
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submit_interrupt01() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String[] resource = new String[]{"foo"};
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final AtomicBoolean ran = new AtomicBoolean(false);
        
        Future<Object> future = journal.submit(new AbstractIndexTask(journal,
                ITx.UNISOLATED, false/* readOnly */, resource) {

            /**
             * The task just sets a boolean value and then sleeps.
             */
            protected Object doTask() throws Exception {

                ran.compareAndSet(false, true);

                while(true) {

                    if(Thread.interrupted()) {

                        System.err.println("Interrupted.");

                        /*
                         * Note: If you simply continue processing rather than
                         * throwing an exception then the interrupt is
                         * _ignored_.
                         */

                        throw new InterruptedException("Task was interrupted");
                        
                    }

                    for(int i=0; i<10000000; i++ ) {}
                    
                }

            }

        });

        // wait until the task starts executing.
        
        while(true) {
            
            if(ran.get()) break;
            
            Thread.sleep(100);
            
        }

        // interrupts and cancels the task.
        assertTrue(future.cancel(true/*mayInterruptWhileRunning*/));

        // the task was cancelled.
        assertTrue(future.isCancelled());
        
        // verify that get() throws the expected exception.
        try {
            
            future.get();
            
            fail("Expecting: "+CancellationException.class);            
            
        } catch(CancellationException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }
        
        /*
         * make sure that the flag was set (not reliably set until we get() the
         * future).
         */
        assertTrue("ran",ran.get());

        /*
         * Verify that the commit counter was changed.
         */
        assertEquals("commit counter changed?",
                commitCounterBefore, journal.getRootBlockView()
                        .getCommitCounter());

        journal.shutdown();

        journal.delete();

    }

    /**
     * Submits an unisolated task to the read service. The task just sleeps. We
     * then verify that we can terimate that task using
     * {@link Future#cancel(boolean)} with
     * <code>mayInterruptWhileRunning := false</code> and that an appropriate
     * exception is thrown in the main thread.
     * <p>
     * Note: {@link FutureTask#cancel(boolean)} is able to return control to the
     * caller without being allowed to interrupt the task. However, it does NOT
     * terminate the task - the worker thread is still running. Once the main
     * thread reaches {@link Journal#shutdown()} it awaits the termination of
     * the {@link WriteExecutorService}. However that service does NOT
     * terminate because that worker thread is still running an infinite loop.
     * Eventually, we time out while waiting for the write service to terminate
     * and then shutdown the store. You can see what is going on in the log
     * files or using a debugger, where you can notice that the thread running
     * the task never terminates (it is a daemon thread so it does not keep the
     * JVM from terminating).
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submit_interrupt02() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String[] resource = new String[]{"foo"};
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final AtomicBoolean ran = new AtomicBoolean(false);
        
        Future<Object> future = journal.submit(new AbstractIndexTask(journal,
                ITx.UNISOLATED, false/* readOnly */, resource) {

            /**
             * The task just sets a boolean value and then runs an infinite
             * loop.
             */
            protected Object doTask() throws Exception {

                ran.compareAndSet(false, true);

                while (true) {

                    for (int i = 0; i < 10000000; i++) {

                        /*
                         * Note: This gives us a place where we can put a
                         * breakpoint.
                         */
                        
                        i++;
                        
                    }

                }

            }

        });

        // wait until the task starts executing.
        
        while(true) {
            
            if(ran.get()) break;
            
            Thread.sleep(100);
            
        }

        // this aggressively terminates the task.
        assertTrue(future.cancel(false/*mayInterruptWhileRunning*/));

        // the task was cancelled.
        assertTrue(future.isCancelled());
        
        // verify that get() throws the expected exception.
        try {
            
            future.get();
            
            fail("Expecting: "+CancellationException.class);            
            
        } catch(CancellationException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }
        
        /*
         * make sure that the flag was set (not reliably set until we get() the
         * future).
         */
        assertTrue("ran",ran.get());

        /*
         * Verify that the commit counter was NOT changed.
         */
        assertEquals("commit counter changed?",
                commitCounterBefore, journal.getRootBlockView()
                        .getCommitCounter());

        journal.shutdown();

        journal.delete();

    }

}
