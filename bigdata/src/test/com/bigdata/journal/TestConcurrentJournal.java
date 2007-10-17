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

import java.nio.channels.ClosedByInterruptException;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.journal.AbstractTask.ResubmitException;
import com.bigdata.journal.ConcurrentJournal.Options;
import com.bigdata.journal.WriteExecutorService.RetryException;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.DataService;
import com.bigdata.service.RangeQueryIterator;

/**
 * Test suite for the {@link ConcurrentJournal}.
 * 
 * @todo write test cases that submit various kinds of operations and verify the
 *       correctness of those individual operations. refactor the services
 *       package to do this, including things such as the
 *       {@link RangeQueryIterator}. this will help to isolate the correctness
 *       of the data service "api", including concurrency of operations, from
 *       the {@link DataService}.
 * 
 * @todo Verify proper partial ordering over transaction schedules (runs tasks
 *       in parallel, uses exclusive locks for access to the same isolated index
 *       within the same transaction, and schedules their commits using a
 *       partial order based on the indices that were actually written on).
 *       <p>
 *       Verify that only the indices that were written on are used to establish
 *       the partial order, not any index from which the tx might have read.
 * 
 * @todo test writing on multiple isolated indices in the different transactions
 *       and verify that no concurrency limits are imposed across transactions
 *       (only within transactions).
 * 
 * @todo show state-based validation for concurrent transactions on the same
 *       index that result in write-write conflicts.
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
        
        Future<Object> future = journal.submit(new AbstractTask(journal,
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
        
        Future<Object> future = journal.submit(new AbstractTask(journal,
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
        
        Future<Object> future = journal.submit(new AbstractTask(journal,
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
        
        Future<Object> future = journal.submit(new AbstractTask(journal,
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
        
        Future<Object> future = journal.submit(new AbstractTask(journal,
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
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submit_interrupt01() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);

        // Note:
        properties.setProperty(Options.SHUTDOWN_TIMEOUT,"500");

        final String[] resource = new String[]{"foo"};
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final AtomicBoolean ran = new AtomicBoolean(false);
        
        Future<Object> future = journal.submit(new AbstractTask(journal,
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

                    /*
                     * Note: this will notice if the Thread is interrupted.
                     */

                    Thread.sleep(Long.MAX_VALUE);
                    
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
     * then verify that we can terminate that task using
     * {@link Future#cancel(boolean)} with
     * <code>mayInterruptWhileRunning := false</code> and that an appropriate
     * exception is thrown in the main thread when we {@link Future#get()} the
     * result of the task.
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
     * <p>
     * Note: For this test we explicitly set the
     * {@link Options#SHUTDOWN_TIMEOUT} so that we do not wait forever for the
     * uninterruptable task.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submit_interrupt02() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        properties.setProperty(Options.SHUTDOWN_TIMEOUT,"500");
        
        Journal journal = new Journal(properties);
        
        final String[] resource = new String[]{"foo"};
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final AtomicBoolean ran = new AtomicBoolean(false);
        
        Future<Object> future = journal.submit(new AbstractTask(journal,
                ITx.UNISOLATED, false/* readOnly */, resource) {

            /**
             * The task just sets a boolean value and then runs an infinite
             * loop, <strong>ignoring interrupts</strong>.
             */
            protected Object doTask() throws Exception {

                ran.compareAndSet(false, true);

                while(true) {
                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    } catch(InterruptedException ex) {
                        /*ignore*/
                        System.err.println("Ignoring interrupt: "+ex);
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
         * Verify that the commit counter was NOT changed.
         */
        assertEquals("commit counter changed?",
                commitCounterBefore, journal.getRootBlockView()
                        .getCommitCounter());

        journal.shutdown();

        journal.delete();

    }

    /**
     * Submits an unisolated task to the read service. In an attempt to provoke
     * a {@link ClosedByInterruptException} the task writes continuously on the
     * {@link IRawStore}, flushing each write to the backing store, until it is
     * interrupted. It then ignores the interrupt, sleeps for a bit more, and
     * attempts to perform additional operation on the store.
     * 
     * @todo evolve to test behavior when a task does not join the abort group.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submit_interrupt03() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String[] resource = new String[]{"foo"};

        {
            journal.registerIndex(resource[0]);
            journal.commit();
        }
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        /* 0 = not running.
         * 1 - started 1st loop.
         * 2 - started 2nd loop.
         * 3 - waiting a bit before exit.
         * 4 - done.
         */
        final AtomicInteger runState = new AtomicInteger(0);
        
        final Lock lock = new ReentrantLock();

        /**
         * Used to force an abort.
         */
        class PrivateException extends RuntimeException {

            private static final long serialVersionUID = 1L;

            PrivateException(String msg) {
                super(msg);
            }
            
        }
        
        Future<Object> f1 = journal.submit(new AbstractTask(journal,
                ITx.UNISOLATED, false/* readOnly */, resource) {

            /**
             */
            protected Object doTask() throws Exception {

                runState.compareAndSet(0, 1);

                // low-level write so there is some data to be committed.
                journal.write(getRandomData());
                
                runState.compareAndSet(1, 2);
                
                runState.compareAndSet(2, 3);

                // wait more before exiting.
                {
                    final long begin = System.currentTimeMillis();
                    final long timeout = 5000; // ms
                    log.warn("Sleeping " + timeout + "ms");
                    try {
                        final long elapsed = System.currentTimeMillis() - begin;
                        Thread.sleep(timeout - elapsed/* ms */);
                    } catch (InterruptedException ex) {
                        log.warn(ex);
                    }
                }

                lock.lock();
                try {
                    runState.compareAndSet(3, 4);
                    log.warn("Done");
                } finally {
                    lock.unlock();
                }
                
                return null;
                
            }

        });

        // force async abort of the commit group.
        
        Future<Object> f2 = journal.submit(new AbstractTask(journal,
                ITx.UNISOLATED, false/* readOnly */, new String[] { "bazzar" }) {

            protected Object doTask() throws Exception {

                log.warn("Running task that will force abort of the commit group.");
                
                // low-level write.
                journal.write(getRandomData());

                throw new PrivateException("Forcing abort of the commit group.");
                
            }
            
        });
        
        // Verify that the commit counter was NOT changed.
        assertEquals("commit counter changed?",
                commitCounterBefore, journal.getRootBlockView()
                        .getCommitCounter());

        /*
         * Verify that the write service waits for the interrupted task to join
         * the abort group.
         */
        log.warn("Waiting for 1st task to finish");
        while(runState.get()<4) {
            
            lock.lock();
            try {
                if(runState.get()<4) {
                    // No abort yet.
                    assertEquals("Not expecting abort",0,journal.writeService.getAbortCount());
                }
            } finally {
                lock.unlock();
            }
            Thread.sleep(20);
            
        }
        log.warn("Reached runState="+runState);
        
        // wait for the abort or a timeout.
        {
            log.warn("Waiting for the abort.");
            final long begin = System.currentTimeMillis();
            while ((begin - System.currentTimeMillis()) < 1000) {
                if (journal.writeService.getAbortCount() > 0) {
                    log.warn("Noticed abort");
                    break;
                } else {
                    // Verify that the commit counter was NOT changed.
                    assertEquals("commit counter changed?",
                            commitCounterBefore, journal.getRootBlockView()
                                    .getCommitCounter());
                }
            }
        }

        // verify did abort.
        assertEquals("Expecting abort",1,journal.writeService.getAbortCount());

        /*
         * Verify that both futures are complete and that both throw exceptions
         * since neither task was allowed to commit.
         * 
         * Note: f1 completed successfully but the commit group was discarded
         * when f2 threw an exception. Therefore f1 sees an inner RetryException
         * while f2 sees the exception that it threw as its inner exception.
         */
        {
            
            try {f1.get();fail("Expecting exception.");}
            catch(ExecutionException ex) {
                assertTrue(isInnerCause(ex, RetryException.class));
                log.warn("Expected exception: "+ex,ex);
            }
            try {f2.get();fail("Expecting exception.");}
            catch(ExecutionException ex) {
                log.warn("Expected exception: "+ex,ex);
                assertTrue(isInnerCause(ex, PrivateException.class));
            }
        }
        
        journal.shutdown();

        journal.delete();

    }

    /**
     * Verify that an {@link AbstractTask} correctly rejects an attempt to
     * submit the same instance twice. This is important since the base class
     * has various items of state that are not thread-safe and are not designed
     * to be reusable.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_tasksAreNotThreadSafe() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String[] resource = new String[]{"foo"};
        
        /*
         * Note: this task is stateless
         */
        final AbstractTask task = new AbstractTask(journal,
                ITx.UNISOLATED, false/* readOnly */, resource) {

            protected Object doTask() throws Exception {

                return null;
                
            }

        };

        /*
         * Note: We have to request the result of the task before re-submitting
         * the task again since the duplicate instance is being silently dropped
         * otherwise - I expect that there is a hash set involved somewhere such
         * that duplicates can not exist in the queue.
         */
        journal.submit(task).get();

        /*
         * Submit the task again - it will fail.
         */
        try {

            journal.submit(task).get();
            
            fail("Expecting: "+ResubmitException.class);

        } catch(ExecutionException ex) {
            
            if(ex.getCause() instanceof ResubmitException) {
                
                System.err.println("Ignoring expected exception: "+ex);
                
            } else {
                
                fail("Expecting: "+ResubmitException.class);
                
            }
            
        }

        journal.shutdown();
        
        journal.delete();

    }

    /**
     * Correctness test of task retry when another task causes a commit group to
     * be discarded.
     * 
     * @todo implement retry and maxLatencyFromSubmit and move the tests for
     *       those features into their own test suites.
     *       
     * @todo Test retry of tasks (read only or read write) when they are part of
     *       a commit group in which some other task fails so they get
     *       interrupted and have to abort.
     * 
     * @todo also test specifically with isolated tasks to make sure that the
     *       isolated indices are being rolled back to the last commit when the
     *       task is aborted.
     */
    public void test_retry_readService() {

        fail("write this test");
        
    }
    
    public void test_retry_writeService() {

        fail("write this test");
        
    }

    public void test_retry_txService_readOnly() {

        fail("write this test");
        
    }

    /*
     * note: the difference between readOnly and readCommitted is that the
     * latter MUST read from whatever the committed state of the index is at the
     * time that it executes (or re-retries?) while the formed always reads from
     * the state of the index as of the transaction start time.
     */
    public void test_retry_txService_readCommitted() {

        fail("write this test");
        
    }

    public void test_retry_txService_readWrite() {

        fail("write this test");
        
    }
    
//     Note: I can not think of any way to write this test.
//    
//    /**
//     * This test verifies that unisolated reads are against the last committed
//     * state of the index(s), that they do NOT permit writes, and that
//     * concurrent writers on the same named index(s) do NOT conflict.
//     */
//    public void test_submit_readService_isolation01() throws InterruptedException, ExecutionException {
//        
//        Properties properties = getProperties();
//        
//        Journal journal = new Journal(properties);
//        
//        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();
//
//        final String resource = "foo";
//        
//        final AtomicBoolean ran = new AtomicBoolean(false);
//
//        final UUID indexUUID = UUID.randomUUID();
//
//        // create the index (and commit).
//        assertEquals("indexUUID", indexUUID, journal.submit(
//                new RegisterIndexTask(journal, resource, new UnisolatedBTree(
//                        journal, indexUUID))).get());
//
//        // verify commit.
//        assertEquals("commit counter unchanged?",
//                commitCounterBefore+1, journal.getRootBlockView()
//                        .getCommitCounter());
//
//        // write some data on the index (and commit).
//        final long metadataAddr = (Long) journal.submit(
//                new AbstractIndexTask(journal, ITx.UNISOLATED,
//                        false/*readOnly*/, resource) {
//
//            protected Object doTask() throws Exception {
//
//                IIndex ndx = getIndex(getOnlyResource());
//
//                // Note: the metadata address before any writes on the index.
//                final long metadataAddr = ((BTree)ndx).getMetadata().getMetadataAddr();
//
//                // write on the index.
//                ndx.insert(new byte[]{1,2,3},new byte[]{2,2,3});
//                
//                return metadataAddr;
//
//            }
//            
//        }).get();
//
//        // verify another commit.
//        assertEquals("commit counter unchanged?",
//                commitCounterBefore+2, journal.getRootBlockView()
//                        .getCommitCounter());
//
//        Future<Object> future = journal.submit(new AbstractIndexTask(journal,
//                ITx.UNISOLATED, true/*readOnly*/, resource) {
//
//            /**
//             * The task just sets a boolean value and returns the name of the
//             * sole resource. It does not actually read anything.
//             */
//            protected Object doTask() throws Exception {
//
//                ran.compareAndSet(false, true);
//
//                return getOnlyResource();
//
//            }
//        });
//
//        // the test task returns the resource as its value.
//        assertEquals("result",resource,future.get());
//        
//        /*
//         * make sure that the flag was set (not reliably set until we get() the
//         * future).
//         */
//        assertTrue("ran",ran.get());
//        
//        /*
//         * Verify that a commit was NOT performed.
//         */
//        assertEquals("commit counter changed?",
//                commitCounterBefore, journal.getRootBlockView()
//                        .getCommitCounter());
//        
//        journal.shutdown();
//
//        journal.delete();
//
//    }
    
}
