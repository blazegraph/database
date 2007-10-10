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
package com.bigdata.journal;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.concurrent.LockManager;
import com.bigdata.service.DataService;
import com.bigdata.service.RangeQueryIterator;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * A journal that supports concurrent of operations on unisolated named indices.
 * The journal uses a {@link LockManager} to administer exclusive locks on
 * unisolated named indices and thereby identify a schedule of operations such
 * that access to an unisolated named index is always single threaded while
 * access to distinct unisolated named indices MAY be concurrent.
 * <p>
 * The journal have several thread pools that facilitate concurrency. They are:
 * <dl>
 * 
 * <dt>{@link #txService}</dt>
 * <dd>This is used for the "active" phrase of transaction. Transactions read
 * from historical states of named indices during their active phase and buffer
 * the results on isolated indices. Since transactions never write on the
 * unisolated indices during their "active" phase they may be run with arbitrary
 * concurrency. A transaction that requests a commit is scheduled using the
 * {@link #writeService}. Transactions are selected to commit once they have
 * acquired a lock on the corresponding unisolated indices, thereby enforcing
 * serialization of their write sets both among other transactions and among
 * unisolated writers. The commit itself consists of the standard validation and
 * merge phrases.</dd>
 * 
 * <dt>{@link #readService}</dt>
 * <dd>Concurrent unisolated readers running against the <strong>historical</strong>
 * state of a named index.</dd>
 * 
 * <dt>{@link #writeService}</dt>
 * <dd>Concurrent unisolated writers running against the <strong>current</strong>
 * state of (or more more) named index(s). This is also used to serialize the
 * commit phrase of transactions. Writers MUST predeclare their locks, which
 * allows us to avoid deadlocks altogether. </dd>
 * 
 * <dt>{@link #commitService}</dt>
 * <dd>Schedules periodic group commits. </dd>
 * 
 * <dt>{@link #statusService}</dt>
 * <dd>Periodically logs counters.</dd>
 * </dl>
 * 
 * FIXME write more tests!
 * 
 * @todo the problem with running concurrent unisolated operations during a
 *       commit and relying on an "auto-commit" flag to indicate whether or not
 *       the index will participate is two fold. First, previous unisolated
 *       operations on the same index will not get committed if an operation is
 *       currently running, so we could wind up deferring check points of
 *       indices for quite a while. Second, if there is a problem with the
 *       commit and we have to abort, then the ongoing operations will be using
 *       unisolated indices that may include write sets that were discarded -
 *       this would make abort non-atomic.
 *       <P>
 *       The ground state from which an unisolated operation begins needs to
 *       evolve after each unisolated operation that reaches its commit point
 *       successfully. this can be acomplished by holding onto the btree
 *       reference, or even just the address at which the metadata record for
 *       the btree was last written.
 *       <p>
 *       However, if an unisolated write fails for any reason on a given index
 *       then we MUST use the last successful check point for that index.
 *       <p>
 *       Due to the way in which the {@link BTree} class is written, it "steals"
 *       child references when cloning an immutable node or leaf prior to making
 *       modifications. This means that we must reload the btree from a metadata
 *       record if we have to roll back due to an abort of some unisolated
 *       operation.
 *       <P>
 *       The requirement for global serialization of transactions may be relaxed
 *       only when it is known that the transaction writes on a limited set of
 *       data services, indices, or index partitions but it must obtain a lock
 *       on the appropriate resources before it may merge its write set onto the
 *       corresponding unisolated resources. Otherwise unisolated operations
 *       could be interleaved within a transaction commit and it would no longer
 *       be atomic.
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
 * @todo Verify that you can cancel a submitted task using the returned
 *       {@link Future} when the task is either (a) still in the queue; or (b)
 *       executing.
 * 
 * @todo Verify that operations run with the correct isolation level (including
 *       unisolated operation).
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
 * @todo test {@link SequenceTask}, e.g., by creating an index and writing on
 *       it.
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
abstract public class ConcurrentJournal extends AbstractJournal {

    /**
     * Options for the {@link ConcurrentJournal}.
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
        public final static String DEFAULT_GROUP_COMMIT = "true";

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
     * Pool of threads for handling concurrent unisolated write operations on
     * named indices. Unisolated writes are always performed against the current
     * state of the named index. Unisolated writes for the same named index (or
     * index partition) conflict and must be serialized. The size of this thread
     * pool and the #of distinct named indices together govern the maximum
     * practical concurrency for unisolated writers.
     * <p>
     * Serialization of access to unisolated named indices is acomplished by
     * gaining an exclusive lock on the unisolated named index.
     * 
     * @todo the {@link #writeService} SHOULD NOT support scheduled tasks and
     *       MUST NOT run the {@link GroupCommitRunnable} since shutdown of the
     *       {@link #writeService} will immediately disable scheduled tasks.
     *       This is especially bad news for the {@link GroupCommitRunnable}
     *       since that can lead to deadlocks where a task is awaiting a group
     *       commit but the scheduled {@link GroupCommitRunnable} has already
     *       been cancelled so the group commit is never triggered.
     */
    final protected WriteExecutorService writeService;

    /**
     * Runs the periodic {@link GroupCommitRunnable}.
     */
    final protected ScheduledExecutorService commitService;

    /**
     * Runs a {@link StatusTask} printing out periodic service status
     * information (counters).
     */
    final protected ScheduledExecutorService statusService;

    /**
     * Manages 2PL locking for writes on unisolated named indices.
     */
    final protected LockManager<String> lockManager;

    public void shutdown() {

        assertOpen();
        
        log.info("");

        final long begin = System.currentTimeMillis();

        // @todo configuration parameter (ms).
        final long willingToWait = 2000;
        
        txService.shutdown();

        readService.shutdown();

        writeService.shutdown();

        try {

            log.info("Awaiting transaction service termination");
            
            long elapsed = System.currentTimeMillis() - begin;
            
            if(!txService.awaitTermination(willingToWait-elapsed, TimeUnit.SECONDS)) {
                
                log.warn("Transaction service termination: timeout");
                
            }

        } catch(InterruptedException ex) {
            
            log.warn("Interrupted awaiting transaction service termination.", ex);
            
        }

        try {

            log.info("Awaiting read service termination");

            long elapsed = System.currentTimeMillis() - begin;
            
            if(!readService.awaitTermination(willingToWait-elapsed, TimeUnit.SECONDS)) {
                
                log.warn("Read service termination: timeout");
                
            }

        } catch(InterruptedException ex) {
            
            log.warn("Interrupted awaiting read service termination.", ex);
            
        }

        try {

//            /*
//             * Trigger a group commit to cover the edge case where we shutdown
//             * the write service (a) after a write task has executed and is
//             * awaiting the commit signal; and (b) the scheduled group commit
//             * task has not run yet. In this edge case the sheduled group commit
//             * task is NOT run since it is shutdown before it can run. Therefore
//             * we have to explicitly demand a commit before we await termination
//             * of the write service.
//             */
//            
//            log.info("Requesting final commit");
//            
//            writeService.groupCommit();

            long elapsed = System.currentTimeMillis() - begin;
            
            long timeout = willingToWait-elapsed;

            log.info("Awaiting write service termination: will wait "+timeout+"ms");

            if(!writeService.awaitTermination(timeout, TimeUnit.SECONDS)) {
                
                log.warn("Write service termination : timeout");
                
                /*
                 * Note: Do NOT request a commit from here. Tasks already on the
                 * write service must await the next commit before that service
                 * can terminate. Therefore, except in the degenerate case where
                 * no tasks are awaiting commit, it is impossible for the write
                 * service to terminate normally without having performed a
                 * commit (or abort if something went wrong).
                 */

            }
            
        } catch(InterruptedException ex) {
            
            log.warn("Interrupted awaiting write service termination.", ex);
            
        }

        /*
         * This is no longer required once the write service is properly down.
         * 
         * No more writes will be accepted once we issue the shutdown request on
         * the write service.
         * 
         * We awaited termination of the write service above. If there were
         * running write tasks then normal termination requires that those tasks
         * complete (in a timely manner) and that a group commit is performed.
         * It is possible that we will shutdown the store before normal
         * completion can be achieved, in which case some writes will not be
         * made restart safe (and their callers will get appropriate
         * exceptions).
         */
        commitService.shutdown();
        
        statusService.shutdown();

        super.shutdown();

    }

    public void shutdownNow() {

        assertOpen();
        
        log.info("");
        
        txService.shutdownNow();

        readService.shutdownNow();

        writeService.shutdownNow();

        commitService.shutdownNow();
        
        statusService.shutdownNow();

        super.shutdownNow();

    }

    /**
     * (Re-)open a journal supporting concurrent operations.
     * 
     * @param properties
     *            See {@link Options}.
     */
    public ConcurrentJournal(Properties properties) {

        super(properties);

        String val;

        final int txServicePoolSize;
        final int readServicePoolSize;

        final int writeServicePoolSize;
        final boolean groupCommit;
        final long commitPeriod;

        // txServicePoolSize
        {

            val = properties.getProperty(Options.TX_SERVICE_POOL_SIZE,
                    Options.DEFAULT_TX_SERVICE_POOL_SIZE);

            txServicePoolSize = Integer.parseInt(val);

            if (txServicePoolSize < 1) {

                throw new RuntimeException("The '"
                        + Options.TX_SERVICE_POOL_SIZE
                        + "' must be at least one.");

            }

            log.info(Options.TX_SERVICE_POOL_SIZE + "=" + txServicePoolSize);

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

            log
                    .info(Options.READ_SERVICE_POOL_SIZE + "="
                            + readServicePoolSize);

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

                val = properties.getProperty(Options.WRITE_SERVICE_POOL_SIZE,
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

//        if (!groupCommit) {
//
//            /*
//             * Note: If groupCommit is disabled, then we always use a single
//             * threaded ExecutorService -- one that does not allow multiple
//             * threads to exist at the same time.
//             */
//
//            writeService = Executors
//                    .newSingleThreadExecutor(DaemonThreadFactory
//                            .defaultThreadFactory());
//
//        } else
        {

            /*
             * When groupCommit is enabled we use a custom
             * ThreadPoolExecutor. This class allows for the synchronization
             * of worker tasks to support groupCommit by forcing new tasks
             * to pause if a groupCommit is scheduled.
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
            writeService = new WriteExecutorService(this,
                    writeServicePoolSize, DaemonThreadFactory
                            .defaultThreadFactory());
//            writeService = Executors.newFixedThreadPool(
//                    writeServicePoolSize, DaemonThreadFactory
//                            .defaultThreadFactory());
        }
        
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
            {
                final long initialDelay = 100;
//                final long delay = (long) (1000d / 35);
                final long delay = 200; // @todo config.
                final TimeUnit unit = TimeUnit.MILLISECONDS;

                commitService = Executors
                        .newSingleThreadScheduledExecutor(DaemonThreadFactory
                                .defaultThreadFactory());

                log.info("Establishing group commit: initialDelay=" + initialDelay
                    + ", periodic delay=" + delay);
                
                commitService.scheduleWithFixedDelay(new GroupCommitRunnable(writeService),
                        initialDelay, delay, unit);
                
            }
            
//        }

        // schedule runnable for periodic status messages.
        // @todo status delay as config parameter.
        {

            final long initialDelay = 100;
            final long delay = 2500;
            final TimeUnit unit = TimeUnit.MILLISECONDS;

            statusService = Executors
                    .newSingleThreadScheduledExecutor(DaemonThreadFactory
                            .defaultThreadFactory());

            statusService.scheduleWithFixedDelay(new StatusTask(),
                    initialDelay, delay, unit);

        }

        /*
         * setup the lock manager.
         * 
         * @todo we need to declare the unisolated indices to the lock
         * manager by name so that it will be willing to provide lock
         * support for those resources. we can either do that implicitly,
         * perhaps by modifying the lock manager to use a weak value hash of
         * the resources and then lazily creating an entry for each resource
         * as a lock is requested. This helps to manage the resource burden
         * of the lock manager.
         */
        {

            /*
             * Note: this means that any operation that writes on unisolated
             * indices MUST specify in advance those index(s) on which it
             * will write.
             */

            final boolean predeclareLocks = true;

            // create the lock manager.

            lockManager = new LockManager<String>(writeServicePoolSize,
                    predeclareLocks);

        }

    }

    /**
     * Run periodically to make sure that a group commit is triggered even if there
     * are no pending tasks on the write service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class GroupCommitRunnable implements Runnable {

        private final WriteExecutorService writeService;
        
        GroupCommitRunnable(WriteExecutorService writeService) {
         
            if(writeService==null) throw new NullPointerException();
            
            this.writeService = writeService;
            
        }

        /*
         * Note: Do NOT throw an exception out of here or the task will not be
         * re-scheduled!
         */
        public void run() {
            
            try {

                log.debug("Requesting periodic group commit");
                
                if(writeService.groupCommit()) {

                    log.info("Did periodic group commit");
                    
                }
                
            } catch(Throwable t) {
                
                log.warn("Group commit: "+t, t);
                
            }
            
        }
        
    }
    
    /**
     * Writes out periodic status information.
     * 
     * @todo make extensible? (StatusTaskFactory, but note that this is an
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
         * @todo write out more details on the writeService (#running, elapsed since the last commit, etc.)?
         */
        public void status() {

            final long commitCounter = getCommitRecord().getCommitCounter();

            System.err.println(
                    "status"
                    + //
                    ": commitCounter=" + commitCounter
                    + // txService (#active,#queued,#completed)
                    ", transactions=("
                    + ((ThreadPoolExecutor) txService).getActiveCount() + ","
                    + ((ThreadPoolExecutor) txService).getQueue().size() + ","
                    + ((ThreadPoolExecutor) txService).getCompletedTaskCount()+ ")"
                    + // readService (#active,#queued,#completed)
                    ", readers=("
                    + ((ThreadPoolExecutor) readService).getActiveCount() + ","
                    + ((ThreadPoolExecutor) readService).getQueue().size() + ","
                    + ((ThreadPoolExecutor) readService).getCompletedTaskCount() + ")"
                    + // writeService (#active,#queued,#completed)
                    ", writers=("
                    + ((ThreadPoolExecutor) writeService).getActiveCount() + ","
                    + ((ThreadPoolExecutor) writeService).getQueue().size() + ","
                    + ((ThreadPoolExecutor) writeService).getCompletedTaskCount() + ")"
                    
                    );

        }

    }

    /**
     * Submit a task (asynchronous). Tasks will execute asynchronously in the
     * appropriate thread pool with as much concurrency as possible.
     * <p>
     * Note: Unisolated write tasks will NOT return before the next group commit
     * (exceptions may be thrown if the task fails or the commit fails). The
     * purpose of group commits is to provide higher throughput for writes on
     * the store by only syncing the data to disk periodically rather than after
     * every write. Group commits are scheduled by the {@link #commitService}.
     * The trigger conditions for group commits may be configured using
     * {@link Options}. If you are using the store in a single threaded context
     * then you may set {@link Options#WRITE_SERVICE_POOL_SIZE} to ONE (1) which
     * has the effect of triggering commit immediately after each unisolated
     * write. However, note that you can not sync a disk more than ~ 30-40 times
     * per second so your throughput in write operations per second will never
     * exceed that for a single-threaded application writing on a hard disk.
     * (Your mileage can vary if you are writing on a transient store or using a
     * durable medium other than disk).
     * <p>
     * Note: The isolated indices used by a {@link IsolationEnum#ReadWrite}
     * transaction are NOT thread-safe. Therefore a partial order is imposed
     * over concurrent tasks for the <strong>same</strong> transaction that
     * seek to read or write on the same index(s). Full concurrency is allowed
     * when different transactions access the same index(s), but write-write
     * conflicts MAY be detected during commit processing.
     * 
     * @param task
     *            The task.
     * 
     * @return The {@link Future} that may be used to resolve the outcome of the
     *         task.
     */
    public Future<Object> submit(AbstractIndexTask task) {

        assertOpen();
        
        if(task.isolated) {

            log.info("Submitted to the transaction service");
            
            return txService.submit(task);
            
        } else if( task.readOnly ) {

            log.info("Submitted to the read service");

            return readService.submit(task);
            
        } else {
            
            log.info("Submitted to the write service");

            return writeService.submit(task);
            
        }
        
    }

    /*
     * various methods that are just submitting tasks.
     */
    
    public IIndex registerIndex(String name, IIndex btree) {
        
        try {

            // add the index iff it does not exist.
            
            if(!(Boolean) submit(new RegisterIndexTask(this,name,btree)).get()) {
                
                throw new IndexExistsException(name);
                
            }
            
            IIndex ndx = getIndex(name);
            
            if(ndx==null) {
                
                throw new NoSuchIndexException(name+" was dropped by concurrent task");
                
            }
            
            return ndx;
            
        } catch(InterruptedException ex) {

            throw new RuntimeException(ex);

        } catch (ExecutionException ex) {
            
            throw new RuntimeException(ex);
            
        }

    }

    public void dropIndex(String name) {
        
        try {

            submit(new DropIndexTask(this,name)).get();
            
        } catch(InterruptedException ex) {

            throw new RuntimeException(ex);

        } catch (ExecutionException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
    /*
     * transaction support.
     */
    
    /**
     * Abort a transaction (synchronous, low latency for read-only transactions
     * but aborts for read-write transactions are serialized since there may be
     * latency in communications with the transaction server or deletion of the
     * temporary backing store for the transaction).
     * 
     * @param ts
     *            The transaction identifier (aka start time).
     */
    public void abort(long ts) {

        ITx tx = getTx(ts);
        
        if (tx == null)
            throw new IllegalArgumentException("No such tx: " + ts);
        
        // abort is synchronous.
        tx.abort();
        
        /*
         * Note: We do not need to abort the pending group commit since nothing
         * is written by the transaction on the unisolated indices until it has
         * validated - and the validate/merge task is an unisolated write
         * operation.
         */
        
    }

    /**
     * Commit a transaction (synchronous).
     * <p>
     * Read-only transactions and transactions without write sets are processed
     * immediately and will have a commit time of zero (0L).
     * <p>
     * Transactions with non-empty write sets are placed onto the
     * {@link #writeService} and the caller will block until the transaction
     * either commits or aborts. For this reason, this method MUST be invoked
     * from within a worker thread for the transaction so that concurrent
     * transactions may continue to execute.
     * 
     * @param ts
     *            The transaction identifier (aka start time).
     * 
     * @return The transaction commit time.
     * 
     * @exception ValidationError
     *                If the transaction could not be validated. A transaction
     *                that can not be validated is automatically aborted. The
     *                caller MAY re-execute the transaction.
     * 
     * @todo support 2-phase or 3-phase commit protocol for transactions whose
     *       write sets are distributed across more than one data service.
     */
    public long commit(long ts) throws ValidationError {

        ITx tx = getTx(ts);
        
        if (tx == null)
            throw new IllegalArgumentException("No such tx: " + ts);

        /*
         * A read-only transaction can commit immediately since validation and
         * commit are basically NOPs.
         */

        if(tx.isReadOnly()) {
        
            // read-only transactions do not get a commit time.
            tx.prepare(0L);

            return tx.commit();
            
        }
        
        /*
         * A transaction with an empty write set can commit immediately since
         * validation and commit are basically NOPs (@todo the only difference
         * from the read-only case is that a commit time is being assigned, but
         * I am not sure if we need to do that.)
         */
        if(tx.isEmptyWriteSet()) {

            tx.prepare(nextTimestamp());

            return tx.commit();

        }
        
        try {

            Long commitTime = (Long) writeService.submit(
                    new TxCommitTask(this, tx)).get();
            
            if(DEBUG) {
                
                log.debug("committed: startTime="+tx.getStartTimestamp()+", commitTime="+commitTime);
                
            }
            
            return commitTime;
            
        } catch(InterruptedException ex) {
            
            // interrupted, perhaps during shutdown.
            throw new RuntimeException(ex);
            
        } catch(ExecutionException ex) {
            
            Throwable cause = ex.getCause();
            
            if(cause instanceof ValidationError) {
                
                throw (ValidationError) cause;
                
            }

            // this is an unexpected error.
            throw new RuntimeException(cause);
            
        }
        
    }
    
    /**
     * Task validates and commits a transaction when it is run by the
     * {@link Journal#writeService}.
     * <p>
     * Note: The commit protocol does not permit unisolated writes on the
     * journal once a transaction begins to prepare until the transaction has
     * either committed or aborted (if such writes were allowed then we would
     * have to re-validate any prepared transactions in order to enforce
     * serializability).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class TxCommitTask extends AbstractIndexTask {
        
        public TxCommitTask(ConcurrentJournal journal,ITx tx) {
            
            super(journal,tx.getStartTimestamp(),tx.isReadOnly(),tx.getDirtyResource());
            
        }

        /**
         * 
         * @return The commit time assgiedn to the transaction.
         */
        public Object doTask() throws Exception {
            
            /*
             * The commit time is assigned when we prepare the transaction.
             * 
             * @todo resolve this against a service in a manner that will
             * support a distributed database commit protocol.
             */
            final long commitTime = ((Tx)tx).journal.nextTimestamp();
            
            tx.prepare(commitTime);
            
            return Long.valueOf( tx.commit() );
            
        }
        
    }
    
}
