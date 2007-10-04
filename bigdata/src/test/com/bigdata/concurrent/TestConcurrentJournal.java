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

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.CognitiveWeb.concurrent.locking.TxDag;

import junit.framework.TestCase;

import com.bigdata.btree.BTree;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.Journal;
import com.bigdata.service.DataService;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * FIXME get this working based on {@link TestConcurrencyControl}
 * 
 * @todo add a stress/correctness test that mixes unisolated and isolated
 *       operations. Note that isolated operations (transactions) during commit
 *       simply acquire the corresponding unisolated index(s) (the transaction
 *       commit itself is just an unisolated operation on one or more indices).
 *       
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestConcurrentJournal extends TestCase {

    public TestConcurrentJournal() {
        super();
    }
    
    public TestConcurrentJournal(String name) {
        super(name);
    }
    
    /**
     * A refactor of some logic from {@link DataService} into the underlying
     * journal to support greater concurrency of operations on named indices.
     * 
     * @todo move the tx service into this class. this is used for running
     *       isolated transactions (read-only, read-committed, or read-write).
     *       since transactions never write on the unisolated indices during
     *       their "active" phase, transactions may be run with arbitrary
     *       concurrency. However, the commit phase of transactions must
     *       serialized. A transaction that requests a commit is placed onto a
     *       queue. Transactions are selected to commit once they have acquired
     *       a lock on the corresponding unisolated indices, thereby enforcing
     *       serialization of their write sets both among other transactions and
     *       among unisolated writers. The commit itself consists of the
     *       standard validation and merge phrases.
     * 
     * @todo tease apart "writePoolSize" (concurrent writers) from group commit
     *       (the commit point for a set of operations is deferred until the
     *       next scheduled commit and all operations then commit together).
     * 
     * @todo modify to extend AbstractJournal so that Journal will then extend
     *       this class (rename to AbstractConcurrentJournal). the layering is
     *       mainly important in terms of isolating the logic for concurrency
     *       control into a source file (this one). in fact, this should
     *       probably just be merged into {@link AbstractJournal} so that the
     *       concurrency contraints are always imposed on the application. If I
     *       retain the distinct between a single threaded unisolated writer
     *       (Journal) and concurrent unisolated writes (ConcurrentJournal) then
     *       users of {@link AbstractJournal} or {@link Journal} should then
     *       become users of {@link AbstractConcurrentJournal} or
     *       {@link ConcurrentJournal}. Note that choosing a writerThreadPool
     *       size of (1) should use a singleThreaded executor service (fixed at
     *       one thread) so that the behavior can be identical to that of the
     *       current {@link Journal} (except that it will use
     *       {@link ResourceQueue}s and {@link TxDag}).
     * 
     * @todo move the status task into the AbstractJournal and just subclass for
     *       the DataService?
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ConcurrentJournal extends Journal {

        /**
         * Pool of threads for handling concurrent transactions on named
         * indices. Transactions are not inherently limited in their
         * concurrency. The size of the thread pool for this service governs the
         * maximum practical concurrency for transactions.
         * <p>
         * Transactions always read from historical data and buffer their writes
         * until they commit. Transactions that commit MUST acquire unisolated
         * writable indices for each index on which the transaction has written.
         * Once the transaction has acquired those writable indices it then runs
         * its commit phrase as an unisolated operation on the
         * {@link #writeService}.
         */
        final protected ExecutorService txService;

        /**
         * Pool of threads for handling concurrent unisolated read operations on
         * named indices using <strong>historical</strong> data. Unisolated
         * read operations from historical data are not inherently limited in
         * their concurrency and do not conflict with unisolated writers. The
         * size of the thread pool for this service governs the maximum
         * practical concurrency for unisolated readers.
         * <p>
         * Note that unisolated read operations on the <strong>current</strong>
         * state of an index DO conflict with unisolated writes and such tasks
         * must be run as unisolated writers.
         * <p>
         * Note: unisolated readers of historical data do require the rention of
         * historical commit records (which may span more than one logical
         * journal) until the reader terminates.
         */
        final protected ExecutorService readService;

        /**
         * Pool of threads for handling concurrent unisolated write operations
         * on named indices. Unisolated writes are always performed against the
         * current state of the named index. Unisolated writes for the same
         * named index (or index partition) conflict and must be serialized. The
         * size of this thread pool and the #of distinct named indices together
         * govern the maximum practical concurrency for unisolated writers.
         * <p>
         * Serialization is acomplished by gaining a lock on the named index.
         */
//        final protected ExecutorService writeService;
        
        /**
         * Runs a {@link StatusTask} printing out periodic service status
         * information (counters).
         */
        final protected ScheduledExecutorService statusService;

        public void shutdown() {
            
            txService.shutdown();
            
            readService.shutdown();
            
            statusService.shutdown();
            
            super.shutdown();
            
        }
        
        public void shutdownNow() {
            
            txService.shutdownNow();
            
            readService.shutdownNow();

            statusService.shutdownNow();
            
            super.shutdownNow();
            
        }
        
        public ConcurrentJournal(Properties properties) {

            super(properties);

            String val;
            
            final int writeServicePoolSize;
            final boolean groupCommit;
            final long commitPeriod;

            final int txServicePoolSize;
            final int readServicePoolSize;
            
            // txServicePoolSize
            {
            
                val = properties.getProperty(Options.TX_SERVICE_POOL_SIZE,Options.DEFAULT_TX_SERVICE_POOL_SIZE);

                txServicePoolSize = Integer.parseInt(val);

                if (txServicePoolSize < 1 ) {

                    throw new RuntimeException("The '"
                            + Options.TX_SERVICE_POOL_SIZE
                            + "' must be at least one.");

                }

                log.info(Options.TX_SERVICE_POOL_SIZE+"="+txServicePoolSize);

            }
            
            // readServicePoolSize
            {

                val = properties.getProperty(Options.READ_SERVICE_POOL_SIZE,
                        Options.DEFAULT_READ_SERVICE_POOL_SIZE);

                readServicePoolSize = Integer.parseInt(val);

                if (readServicePoolSize < 1) {

                    throw new RuntimeException("The '"
                            + Options.READ_SERVICE_POOL_SIZE
                            + "' must be at least one.");

                }

                log.info(Options.READ_SERVICE_POOL_SIZE+"="+readServicePoolSize);
                
            }

            // groupCommit
            {

                val = properties.getProperty(Options.GROUP_COMMIT,
                        Options.DEFAULT_GROUP_COMMIT);

                groupCommit = Boolean.parseBoolean(val);

                log.info(Options.GROUP_COMMIT + "=" + groupCommit);

            }

            // commitPeriod
            {

                val = properties.getProperty(Options.COMMIT_PERIOD,
                        Options.DEFAULT_COMMIT_PERIOD);

                commitPeriod = Long.parseLong(val);

                if (commitPeriod < 1) {

                    throw new RuntimeException("The '" + Options.COMMIT_PERIOD
                            + "' must be at least one.");

                }

                log.info(Options.COMMIT_PERIOD + "=" + commitPeriod);

            }

            // writeServicePoolSize
            {

                if (groupCommit) {

                    val = properties.getProperty(
                            Options.WRITE_SERVICE_POOL_SIZE,
                            Options.DEFAULT_WRITE_SERVICE_POOL_SIZE);

                    writeServicePoolSize = Integer.parseInt(val);

                    if (writeServicePoolSize < 1) {

                        throw new RuntimeException("The '"
                                + Options.WRITE_SERVICE_POOL_SIZE
                                + "' must be at least one.");

                    }

                } else {

                    // only one thread when groupCommit is disabled.

                    writeServicePoolSize = 1;

                }

                log.info(Options.WRITE_SERVICE_POOL_SIZE + "="
                        + writeServicePoolSize);

            }

            // setup thread pool for concurrent transactions.
            txService = Executors.newFixedThreadPool(txServicePoolSize,
                    DaemonThreadFactory.defaultThreadFactory());

            // setup thread pool for unisolated read operations.
            readService = Executors.newFixedThreadPool(readServicePoolSize,
                    DaemonThreadFactory.defaultThreadFactory());

            /*
             * Setup thread pool for unisolated write operations.
             */

            if (!groupCommit) {

                /*
                 * Note: If groupCommit is disabled, then we always use a single
                 * threaded ExecutorService -- one that does not allow
                 * additional threads to be created.
                 * 
                 * @todo the tasks are currently being queued on the journal's
                 * write service, but they will need to be either queued on this
                 * write service instead or we will have to encapsulate the
                 * request to queue the writes such that the correct
                 * writeService is choose.
                 */

                ExecutorService writeService = Executors
                        .newSingleThreadExecutor(DaemonThreadFactory
                                .defaultThreadFactory());

            } else {

                /*
                 * When groupCommit is enabled we use a custom
                 * ThreadPoolExecutor. This class provides handshaking to (a)
                 * insure that at most one thread is running a task for a given
                 * named index (by acquiring a lock when a thread accepts a task
                 * for a named index and releasing the lock when the task
                 * completes); and (b) allows for the synchronization of worker
                 * tasks to support groupCommit (by forcing new tasks to pause
                 * if a groupCommit is scheduled).
                 * 
                 * Synchronization is accomplished by forcing threads to pause
                 * before execution if the commitPeriod has been exceeded.
                 * 
                 * Note: Task scheduling could be modified by changing the
                 * natural order of the work queue using a PriorityBlockingQueue
                 * (by ordering by transaction start time and placing start
                 * times of zero at the end of the order (with a secondary
                 * ordering on the time at which the request was queued so that
                 * the queue remains FIFO for unisolated writes). That might be
                 * necessary in order to have transactions commit with dispatch
                 * when there are heavy unisolated writes on the data service.
                 * 
                 * @todo create the class described here and initialize the
                 * writeService using that task. We should not need to otherwise
                 * modify the code in terms of how tasks are submitted to the
                 * write service. Write a test suite, perhaps for the class in
                 * isolation.
                 */
                ScheduledExecutorService writeService = new ConcurrentWritersAndGroupCommitThreadPoolExecutor(
                        writeServicePoolSize, DaemonThreadFactory
                                .defaultThreadFactory());

                /*
                 * Schedules a repeatable task that will perform group commit.
                 * 
                 * Note: The delay between commits is estimated based on the #of
                 * times per second (1000ms) that you can sync a backing file to
                 * disk.
                 * 
                 * @todo if groupCommit is used with forceOnCommit=No or with
                 * bufferMode=Transient then the delay can be smaller and that
                 * will reduce the latency of commit without any substantial
                 * penalty.
                 * 
                 * @todo The first ballpark estimates are (without task
                 * concurrency)
                 * 
                 * Transient := 63 operations per second.
                 * 
                 * Disk := 35 operations per second (w/ forceOnCommit).
                 * 
                 * Given those data, the maximum throughput for groupCommit
                 * would be around 60 operations per second (double the
                 * performance).
                 * 
                 * Enabling task concurrency could significantly raise the #of
                 * upper bound on the operations per second depending on the
                 * available CPU resources to execute those tasks and the extent
                 * to which the tasks operation on distinct indices and may
                 * therefore be parallelized.
                 */

                final long initialDelay = 100;
                final long delay = (long) (1000 / 35d);
                final TimeUnit unit = TimeUnit.MILLISECONDS;

                writeService.scheduleWithFixedDelay(new GroupCommitTask(),
                        initialDelay, delay, unit);

            }
        
            // schedule runnable for periodic status messages.
            // @todo status delay as config parameter.
            {
                
                final long initialDelay = 100;
                final long delay = 2500;
                final TimeUnit unit = TimeUnit.MILLISECONDS;
                
                statusService = Executors
                        .newSingleThreadScheduledExecutor(DaemonThreadFactory
                                .defaultThreadFactory());
        
                statusService.scheduleWithFixedDelay(new StatusTask(), initialDelay, delay, unit );
                
            }

        }
        
        /**
         * A {@link Runnable} that executes periodically, performing a commit of all
         * pending unisolated writes.
         * <p>
         * Note: If {@link #run()} throws an exception then the task will no longer
         * be run and commit processing WILL NOT be performed.
         * 
         * @see ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        protected class GroupCommitTask implements Runnable {

            public Object call() throws Exception {

                log.info("groupCommit");
                
                return null;
                
            }

            /**
             * Performs a commit of all pending unisolated writes.
             * 
             * @todo the operations that are pending commit all need to be on a
             *       queue somewhere - perhaps just paused awaiting a
             *       {@link Condition}. if the commit succeeds, then those
             *       operations all need to return normally. if the commit fails,
             *       then the operations all need to throw an exception (set an
             *       error message and then notify the {@link Condition}?).
             * 
             * @todo The ground state from which an unisolated operation begins
             *       needs to evolve after each unisolated operation that reaches
             *       its commit point successfully. this can be acomplished by
             *       holding onto the btree reference, or even just the address at
             *       which the metadata record for the btree was last written.
             *       <p>
             *       However, if an unisolated write fails for any reason on a given
             *       index then we MUST use the last successful check point for that
             *       index.
             *       <p>
             *       Due to the way in which the {@link BTree} class is written, it
             *       "steals" child references when cloning an immutable node or
             *       leaf prior to making modifications. This means that we must
             *       reload the btree from a metadata record if we have to roll back
             *       due to an abort of some unisolated operation.
             * 
             * @todo The requirement for global serialization of transactions may be
             *       relaxed only when it is known that the transaction writes on a
             *       limited set of data services, indices, or index partitions but
             *       it must obtain a lock on the appropriate resources before it
             *       may merge its write set onto the corresponding unisolated
             *       resources. Otherwise unisolated operations could be interleaved
             *       within a transaction commit and it would no longer be atomic.
             */
            public void run() {
                // TODO Auto-generated method stub
                
            }
            
        }
        
        /**
         * Writes out periodic status information.
         * 
         * @todo make extensible (StatusTaskFactory, but note that this is an
         *       inner class and requires access to the journal).
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        protected class StatusTask implements Runnable {
            
            public void run() {

                status();
                
            }
            
            /*
             * @todo Write out the queue depths, #of operations to date, etc.
             */
            public void status() {
                
                final long commitCounter = getCommitRecord().getCommitCounter();

                System.err.println("status: commitCounter=" + commitCounter);
                
            }
            
        }

    }
    
    /**
     * @todo refactor into {@link com.bigdata.journal.Options}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Options extends com.bigdata.journal.Options {
        
        /**
         * <code>txServicePoolSize</code> - The #of threads in the pool
         * handling concurrent transactions.
         * 
         * @see #DEFAULT_TX_SERVICE_POOL_SIZE
         */
        public static final String TX_SERVICE_POOL_SIZE = "txServicePoolSize";
        
        /**
         * The default #of threads in the transaction service thread pool.
         */
        public final static String DEFAULT_TX_SERVICE_POOL_SIZE = "100";
        
        /**
         * <code>readServicePoolSize</code> - The #of threads in the pool
         * handling concurrent unisolated read requests on named indices.
         * 
         * @see #DEFAULT_READ_SERVICE_POOL_SIZE
         */
        public static final String READ_SERVICE_POOL_SIZE = "readServicePoolSize";
        
        /**
         * The default #of threads in the read service thread pool.
         */
        public final static String DEFAULT_READ_SERVICE_POOL_SIZE = "20";

        /**
         * The #of threads in the pool handling concurrent unisolated write on
         * named indices - this is forced to ONE (1) IFF {@link #GROUP_COMMIT}
         * is disabled.
         * 
         * @see #DEFAULT_WRITE_SERVICE_POOL_SIZE
         * @see #GROUP_COMMIT
         */
        public final static String WRITE_SERVICE_POOL_SIZE = "writeServicePoolSize"; 

        /**
         * The default #of threads in the write service thread pool (#of
         * concurrent index writers).
         */
        public final static String DEFAULT_WRITE_SERVICE_POOL_SIZE = "20";

        /**
         * <code>groupCommit</code> - A boolean property used to enable or
         * disable group commit. When <em>enabled</em>, concurrent unisolated
         * writes are allowed on named indices by a pool of worker threads (both
         * unisolated data service operations and transaction commit processing
         * are unisolated). Every {@link #COMMIT_PERIOD} milliseconds the writer
         * threads synchronize and unisolated writes are committed. When
         * <em>disabled</em>, only a single thread is available to process
         * writes on named indices.
         */
        public final static String GROUP_COMMIT = "groupCommit";
        
        /**
         * The default for {@link #GROUP_COMMIT}.
         */
        public final static String DEFAULT_GROUP_COMMIT = "false";
        
        /**
         * The #COMMIT_PERIOD determines the maximum latency (in milliseconds)
         * that a writer will block awaiting the next commit and is used IFF
         * {@link #GROUP_COMMIT} is enabled.
         */
        public final static String COMMIT_PERIOD = "commitPeriod";

        /**
         * The default {@link #COMMIT_PERIOD}.
         */
        public final static String DEFAULT_COMMIT_PERIOD = "100";
        
    }
    
    /**
     * A custom {@link ScheduledThreadPoolExecutor} used to support group
     * commit. This class provides handshaking to (a) insure that at most one
     * thread is running a task for a given named index (by acquiring a lock
     * when a thread accepts a task for a named index and releasing the lock
     * when the task completes); and (b) allows for the synchronization of
     * worker tasks to support groupCommit (the scheduled group commit task
     * acquires a {@link #pauseLock()} forcing new tasks to pause if a
     * groupCommit is scheduled).
     * <p>
     * Synchronization is accomplished by forcing threads to pause before
     * execution if the commitPeriod has been exceeded.
     * <p>
     * Note: Task scheduling could be modified by changing the natural order of
     * the work queue using a PriorityBlockingQueue (by ordering by transaction
     * start time and placing start times of zero at the end of the order (with
     * a secondary ordering on the time at which the request was queued so that
     * the queue remains FIFO for unisolated writes). That might be necessary in
     * order to have transactions commit with dispatch when there are heavy
     * unisolated writes on the data service.
     * 
     * @todo create the class descibed here and initialize the writeService
     *       using that task. We should not need to otherwise modify the code in
     *       terms of how tasks are submitted to the write service. Write a test
     *       suite, perhaps for the class in isolation.
     * 
     * FIXME we also need to make sure that concurrent writes are not permitted
     * on the same index.
     * 
     * @todo group commit can be realized on the {@link AbstractJournal} using
     *       (thread local?) locks on writable named indices. the lock is held
     *       until the abort or commit by the thread (or tx, which could use
     *       different threads but not more than one thread at once). write
     *       locks on indices do not conflict with read-committed or read-only
     *       transactions, but they do conflict with unisolated views of the
     *       index (both because the unisolated view is the writable view and
     *       because the index can not be read by another process concurrent
     *       with writing). adapt the test suites for the journal, esp. the
     *       StressTestConcurrent, to run on multiple indices at once.  develop
     *       a stress test that operates using all valid forms of isolation and
     *       verify that the correct locking is imposed.  group commit just adds
     *       a scheduled commit task on top of concurrent writers.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ConcurrentWritersAndGroupCommitThreadPoolExecutor extends ScheduledThreadPoolExecutor {

        public ConcurrentWritersAndGroupCommitThreadPoolExecutor(int corePoolSize) {
            super(corePoolSize);
        }

        public ConcurrentWritersAndGroupCommitThreadPoolExecutor(int corePoolSize,
                RejectedExecutionHandler handler) {
            super(corePoolSize, handler);
        }

        public ConcurrentWritersAndGroupCommitThreadPoolExecutor(int corePoolSize,
                ThreadFactory threadFactory) {
            super(corePoolSize, threadFactory);
        }

        public ConcurrentWritersAndGroupCommitThreadPoolExecutor(int corePoolSize,
                ThreadFactory threadFactory, RejectedExecutionHandler handler) {
            super(corePoolSize, threadFactory, handler);
        }

        /*
         * Support for pausing and resuming execution of new worker tasks.
         */
        private boolean isPaused;
        private ReentrantLock pauseLock = new ReentrantLock();
        private Condition unpaused = pauseLock.newCondition();

        /**
         * If task execution has been {@link #pause() paused} then
         * {@link Condition#await() awaits} someone to call {@link #resume()}.
         */
        protected void beforeExecute(Thread t, Runnable r) {
          super.beforeExecute(t, r);
          pauseLock.lock();
          try {
            while (isPaused) unpaused.await();
          } catch(InterruptedException ie) {
            t.interrupt();
          } finally {
            pauseLock.unlock();
          }
        }
      
        /**
         * Sets the flag indicating that new worker tasks must pause in
         * {@link #beforeExecute(Thread, Runnable)}.
         */
        public void pause() {
          pauseLock.lock();
          try {
            isPaused = true;
          } finally {
            pauseLock.unlock();
          }
        }

        /**
         * Notifies all paused tasks that they may now run.
         */
        public void resume() {
          pauseLock.lock();
          try {
            isPaused = false;
            unpaused.signalAll();
          } finally {
            pauseLock.unlock();
          }
        }

    }

}
