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
 * Created on Oct 3, 2007
 */

package com.bigdata.journal;

import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.journal.AbstractInterruptsTestCase.InterruptMyselfTask;
import com.bigdata.journal.AbstractTask.ResubmitException;
import com.bigdata.journal.ConcurrencyManager.Options;
import com.bigdata.journal.WriteExecutorService.RetryException;
import com.bigdata.service.DataService;
import com.bigdata.service.DataServiceRangeIterator;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Test suite for the {@link IConcurrencyManager} interface on the
 * {@link Journal}.
 * 
 * @todo write test cases that submit various kinds of operations and verify the
 *       correctness of those individual operations. refactor the services
 *       package to do this, including things such as the
 *       {@link DataServiceRangeIterator}. this will help to isolate the
 *       correctness of the data service "api", including concurrency of
 *       operations, from the {@link DataService}.
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
     * Test ability to create a {@link Journal} and then shut it down (in
     * particular this is testing shutdown of the thread pool on the
     * {@link ConcurrencyManager}).
     */
    public void test_shutdown() {

        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        journal.shutdown();
        
        journal.destroyAllResources();
        
    }

    public void test_shutdownNow() {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        journal.shutdownNow();
        
        journal.destroyAllResources();

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
                ITx.READ_COMMITTED, resource) {

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

        journal.destroyAllResources();

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
                ITx.UNISOLATED, resource) {

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

        journal.destroyAllResources();

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
                tx, resource) {

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

        journal.destroyAllResources();

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
        
        final long tx = ITx.READ_COMMITTED;
//        final long tx = journal.newTx(IsolationEnum.ReadCommitted);
//      assertNotSame(ITx.UNISOLATED, tx);
        
        Future<Object> future = journal.submit(new AbstractTask(journal, tx,
                resource) {

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

//        // commit of a readCommitted tx returns commitTime of ZERO(0L).
//        assertEquals(0L,journal.commit(tx));
        // should be illegal since this is not a full transaction.
        try {
            journal.abort(tx);
            fail("Expecting: "+IllegalStateException.class);
        } catch(IllegalStateException ex) {
            log.info("Ignoring expected exception: "+ex);
        }

        journal.shutdown();

        journal.destroyAllResources();

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
                tx, resource) {

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

        journal.destroyAllResources();

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
                ITx.UNISOLATED, resource) {

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

        journal.destroyAllResources();

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
                ITx.UNISOLATED, resource) {

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

        journal.destroyAllResources();

    }

    /**
     * This task verifies that an abort will wait until all running tasks
     * complete (ie, join the "abort group") before aborting.
     * <p>
     * If the abort occurs while task(s) are still running then actions by those
     * tasks can break the concurrency control mechanisms. For example, writes
     * on unisolated indices by tasks in the abort group could be made restart
     * safe with the next commit.  However, the lock system SHOULD still keep
     * new tasks from writing on resources locked by tasks that have not yet
     * terminated.
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

        /* 4 - done.
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
                ITx.UNISOLATED, resource) {

            /**
             */
            protected Object doTask() throws Exception {

                runState.compareAndSet(0, 1);

                // low-level write so there is some data to be committed.
                getJournal().write(getRandomData());

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
                    runState.compareAndSet(1, 4);
                    log.warn("Done");
                } finally {
                    lock.unlock();
                }
                
                return null;
                
            }

        });

        // force async abort of the commit group.
        
        Future<Object> f2 = journal.submit(new AbstractTask(journal, 
                ITx.UNISOLATED, new String[] { }) {

            protected Object doTask() throws Exception {

                log.warn("Running task that will force abort of the commit group.");
                
                // low-level write.
                getJournal().write(getRandomData());

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
                    assertEquals("Not expecting abort",0,journal.getConcurrencyManager().writeService.getAbortCount());
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
                if (journal.getConcurrencyManager().writeService.getAbortCount() > 0) {
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
        assertEquals("Expecting abort", 1,
                journal.getConcurrencyManager().writeService.getAbortCount());

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

        journal.destroyAllResources();

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
                ITx.UNISOLATED, resource) {

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
        
        journal.destroyAllResources();

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
    
    /**
     * A stress test that runs concurrent unisolated readers and writers and
     * verifies that readers able to transparently continue to read against the
     * same state of the named indices if the backing {@link FileChannel} is
     * closed by an interrupt as part of the abort protocol for the commit
     * group. In order for this test to succeed the backing {@link FileChannel}
     * must be transparently re-opened in a thread-safe manner if it is closed
     * asynchronously (i.e., closed while the buffer strategy is still open).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public void test_concurrentReadersAreOk() throws Throwable {

        // Note: clone so that we do not modify!!!
        Properties properties = new Properties(getProperties());

        // Mostly serialize the readers.
        properties.setProperty(Options.READ_SERVICE_CORE_POOL_SIZE, "2");

        // Completely serialize the writers.
        properties.setProperty(Options.WRITE_SERVICE_CORE_POOL_SIZE, "1");
        properties.setProperty(Options.WRITE_SERVICE_MAXIMUM_POOL_SIZE, "1");

        final Journal journal = new Journal(properties);

        // Note: This test requires a journal backed by stable storage.

        if (journal.isStable()) {

            // register and populate the indices and commit.
            final int NRESOURCES = 10;
            final int NWRITES = 10000;
            final String[] resource = new String[NRESOURCES];
            {
                KeyBuilder keyBuilder = new KeyBuilder(4);
                for (int i = 0; i < resource.length; i++) {
                    resource[i] = "index#" + i;
                    IIndex ndx = journal.registerIndex(resource[i]);
                    for (int j = 0; j < NWRITES; j++) {
                        byte[] val = (resource[i] + "#" + j).getBytes();
                        ndx.insert(keyBuilder.reset().append(j).getKey(), val);
                    }
                }
                journal.commit();
            }
            log.warn("Registered and populated " + resource.length
                    + " named indices with " + NWRITES + " records each");

            /**
             * Does an unisolated index scan on a named index using the last
             * committed state of the named index.
             * <p>
             * The read tasks MUST NOT throw any exceptions since we are
             * expecting transparent re-opening of the store. This means that
             * the buffer strategy implementation must notice when the store was
             * closed asynchronously, obtain a lock, re-open the store, and then
             * re-try the operation.
             * 
             * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
             *         Thompson</a>
             * @version $Id$
             */
            class ReadTask extends AbstractTask {

                protected ReadTask(IConcurrencyManager concurrencyManager,
                        String resource) {

                    super(concurrencyManager, ITx.READ_COMMITTED, resource);

                }

                protected Object doTask() throws Exception {

                    IIndex ndx = getIndex(getOnlyResource());

                    // verify writes not allowed.
                    try {
                        ndx.insert(new byte[]{}, new byte[]{});
                        fail("Expecting: "+UnsupportedOperationException.class);
                    }catch(UnsupportedOperationException ex) {
                        log.info("Ingoring expected exception: "+ex);
                    }
//                    assertTrue(ndx instanceof ReadOnlyIndex);
                    
                    IEntryIterator itr = ndx.rangeIterator(null, null);

                    int n = 0;

                    while (itr.hasNext()) {

                        itr.next();

                        n++;

                    }

                    assertEquals("#entries", n, NWRITES);

                    return null;

                }

            }

            /*
             * Runs a sequence of write operations that interrupt themselves.
             */
            final ExecutorService writerService = Executors
                    .newSingleThreadExecutor(new DaemonThreadFactory());

            /*
             * Submit tasks to the single threaded service that will in turn
             * feed them on by one to the journal's writeService. When run on
             * the journal's writeService the tasks will interrupt themselves
             * causing the commit group to be discarded and provoking the JDK to
             * close the FileChannel backing the journal.
             */
            for (int i = 0; i < 10; i++) {
                final String theResource = resource[i % resource.length];
                writerService.submit(new Callable<Object>() {
                    public Object call() throws Exception {
                        journal.submit(new InterruptMyselfTask(journal,
                                ITx.UNISOLATED, theResource));
                        // pause between submits
                        Thread.sleep(20);
                        return null;
                    }
                });

            }

            /*
             * Submit concurrent reader tasks and wait for them to run for a
             * while.
             */
            {
                Collection<AbstractTask> tasks = new LinkedList<AbstractTask>();
                for (int i = 0; i < NRESOURCES * 10; i++) {
                    
                    tasks.add(new ReadTask(journal, resource[i
                            % resource.length]));
                    
                }
                // await futures.
                List<Future<Object>> futures = journal.invokeAll(tasks, 10, TimeUnit.SECONDS);
                for(Future<Object> f : futures) {
                    if(f.isDone()) {
                        // all tasks that complete should have done so without error.
                        f.get();
                    }
                }
            }
            
            writerService.shutdownNow();
            
            log.warn("End of test");

        }

        journal.closeAndDelete();

    }
    
}
