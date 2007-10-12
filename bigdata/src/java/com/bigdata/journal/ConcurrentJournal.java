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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.concurrent.LockManager;
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
 * unisolated indices during their "active" phase distinct transactions may be
 * run with arbitrary concurrency. However, concurrent tasks for the same
 * transaction must obtain an exclusive lock on the isolated index that is used
 * to buffer their writes. A transaction that requests a commit using the
 * {@link ITransactionManager} results in a task being submitted to the
 * {@link #writeService}. Transactions are selected to commit once they have
 * acquired a lock on the corresponding unisolated indices, thereby enforcing
 * serialization of their write sets both among other transactions and among
 * unisolated writers. The commit itself consists of the standard validation and
 * merge phrases.</dd>
 * 
 * <dt>{@link #readService}</dt>
 * <dd>Concurrent unisolated readers running against the <strong>historical</strong>
 * state of a named index. No locking is imposed. Concurrency is limited by the
 * size of the thread pool, but large thread pools can reduce overall
 * performance..</dd>
 * 
 * <dt>{@link #writeService}</dt>
 * <dd>Concurrent unisolated writers running against the <strong>current</strong>
 * state of (or more more) named index(s) (the "live" or "mutable" index(s)).
 * The underlying {@link BTree} is NOT thread-safe. Therefore writers MUST
 * predeclare their locks, which allows us to avoid deadlocks altogether. This
 * is also used to schedule the commit phrase of transactions. </dd>
 * 
 * <dt>{@link #commitService}</dt>
 * <dd>Schedules periodic group commits. </dd>
 * 
 * <dt>{@link #statusService}</dt>
 * <dd>Periodically {@link StatusTask#log}s counters.</dd>
 * </dl>
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
         * The maximum latency (in milliseconds) that a writer will block
         * awaiting the next commit and is used IFF {@link #GROUP_COMMIT} is
         * enabled.
         */
        public final static String COMMIT_PERIOD = "commitPeriod";

        /**
         * The default {@link #COMMIT_PERIOD}.
         * 
         * @todo tune this.
         */
        public final static String DEFAULT_COMMIT_PERIOD = "200";

        /**
         * The period (delay) between scheduled invocations of the
         * {@link StatusTask}.
         */
        public final static String STATUS_PERIOD = "statusPeriod";
        
        /**
         * The default {@link #STATUS_PERIOD}.
         */
        public final static String DEFAULT_STATUS_PERIOD = "2000";
        
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
        final TimeUnit unit = TimeUnit.MILLISECONDS;
        
        txService.shutdown();

        readService.shutdown();

        writeService.shutdown();

        try {

            log.info("Awaiting transaction service termination");
            
            long elapsed = System.currentTimeMillis() - begin;
            
            if(!txService.awaitTermination(willingToWait-elapsed, unit)) {
                
                log.warn("Transaction service termination: timeout");
                
            }

        } catch(InterruptedException ex) {
            
            log.warn("Interrupted awaiting transaction service termination.", ex);
            
        }

        try {

            log.info("Awaiting read service termination");

            long elapsed = System.currentTimeMillis() - begin;
            
            if(!readService.awaitTermination(willingToWait-elapsed, unit)) {
                
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

            if(!writeService.awaitTermination(timeout, unit)) {
                
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

        // writeServicePoolSize
        {

            val = properties.getProperty(Options.WRITE_SERVICE_POOL_SIZE,
                    Options.DEFAULT_WRITE_SERVICE_POOL_SIZE);

            writeServicePoolSize = Integer.parseInt(val);

            if (writeServicePoolSize < 1) {

                throw new RuntimeException("The '"
                        + Options.WRITE_SERVICE_POOL_SIZE
                        + "' must be at least one.");

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

        // setup thread pool for unisolated write operations.
        writeService = new WriteExecutorService(this, writeServicePoolSize,
                DaemonThreadFactory.defaultThreadFactory());

        /*
         * Schedules a repeatable task that will perform group commit.
         * 
         * Note: The delay between commits is estimated based on the #of times
         * per second (1000ms) that you can sync a backing file to disk.
         * 
         * @todo if groupCommit is used with forceOnCommit=No or with
         * bufferMode=Transient then the delay can be smaller and that will
         * reduce the latency of commit without any substantial penalty.
         * 
         * @todo The first ballpark estimates are (without task concurrency)
         * 
         * Transient := 63 operations per second.
         * 
         * Disk := 35 operations per second (w/ forceOnCommit).
         * 
         * Given those data, the maximum throughput for groupCommit would be
         * around 60 operations per second (double the performance).
         * 
         * Enabling task concurrency could significantly raise the #of upper
         * bound on the operations per second depending on the available CPU
         * resources to execute those tasks and the extent to which the tasks
         * operation on distinct indices and may therefore be parallelized.
         */
        {

            final long commitPeriod;
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

            final long initialDelay = 100;
            // final long delay = (long) (1000d / 35);
            final long delay = commitPeriod;
            final TimeUnit unit = TimeUnit.MILLISECONDS;

            commitService = Executors
                    .newSingleThreadScheduledExecutor(DaemonThreadFactory
                            .defaultThreadFactory());

            log.info("Establishing group commit: initialDelay=" + initialDelay
                    + ", periodic delay=" + delay);

            commitService.scheduleWithFixedDelay(new GroupCommitRunnable(
                    writeService), initialDelay, delay, unit);

        }

        // setup scheduled runnable for periodic status messages.
        {

            final long initialDelay = 100;
            
            final long delay = Long.parseLong(properties.getProperty(
                    Options.STATUS_PERIOD, Options.DEFAULT_STATUS_PERIOD));

            final TimeUnit unit = TimeUnit.MILLISECONDS;

            statusService = Executors
                    .newSingleThreadScheduledExecutor(DaemonThreadFactory
                            .defaultThreadFactory());

            statusService.scheduleWithFixedDelay(newStatusTask(),
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
     * Factory allows subclasses to extend the {@link StatusTask}.
     */
    protected StatusTask newStatusTask() {

        return new StatusTask();
        
    }
    
    /**
     * Writes out periodic status information.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    protected class StatusTask implements Runnable {

        protected final Logger log = Logger.getLogger(StatusTask.class);
        
        public void run() {

            status();

        }

        /**
         * Writes out the status on {@link #log}. 
         */
        protected void status() {

            final long commitCounter = getCommitRecord().getCommitCounter();

            log.info(
                    "status"
                    + // txService (#active,#queued,#completed)
                    ": transactions=("
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
                    + // commitCounter.
                    ", commitCounter=" + commitCounter
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
     *         
     * @exception RejectedExecutionException
     *                if task cannot be scheduled for execution
     * @exception NullPointerException
     *                if task null
     */
    public Future<Object> submit(AbstractTask task) {

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

    /**
     * Executes the given tasks, returning a list of Futures holding their
     * status and results when all complete.
     * 
     * @param tasks
     *            The tasks.
     * 
     * @return Their {@link Future}s.
     * 
     * @exception InterruptedException 
     *                if interrupted while waiting, in which case unfinished
     *                tasks are cancelled.
     * @exception NullPointerException 
     *                if tasks or any of its elements are null
     * @exception RejectedExecutionException 
     *                if any task cannot be scheduled for execution
     */
    public List<Future<Object>> invokeAll(Collection<AbstractTask> tasks) {
        
        Iterator<AbstractTask> itr = tasks.iterator();
        
        List<Future<Object>> futures = new LinkedList<Future<Object>>();
        
        while(itr.hasNext()) {
            
            futures.add( submit(itr.next()) );
            
        }
        
        return futures;
        
    }
    
//    /*
//     * various methods that are just submitting tasks.
//     */
//    
//    public IIndex registerIndex(String name, IIndex btree) {
//        
//        try {
//
//            // add the index iff it does not exist.
//            
//            UUID indexUUID = (UUID) submit(new RegisterIndexTask(this,name,btree)).get();
//            
//            if( indexUUID != btree.getIndexUUID() ) {
//                
//                throw new IndexExistsException(name);
//                
//            }
//            
//            IIndex ndx = getIndex(name);
//            
//            if(ndx==null) {
//                
//                throw new NoSuchIndexException(name+" was dropped by concurrent task");
//                
//            }
//            
//            return ndx;
//            
//        } catch(InterruptedException ex) {
//
//            throw new RuntimeException(ex);
//
//        } catch (ExecutionException ex) {
//            
//            throw new RuntimeException(ex);
//            
//        }
//
//    }
//
//    public void dropIndex(String name) {
//        
//        try {
//
//            submit(new DropIndexTask(this,name)).get();
//            
//        } catch(InterruptedException ex) {
//
//            throw new RuntimeException(ex);
//
//        } catch (ExecutionException ex) {
//            
//            throw new RuntimeException(ex);
//            
//        }
//        
//    }
//    
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
        
        if (tx == null) {

            throw new IllegalArgumentException("No such tx: " + ts);
            
        }

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
         * validation and commit are basically NOPs (this is the same as the
         * read-only case.)
         */

        if(tx.isEmptyWriteSet()) {

            tx.prepare(0L);

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
            throw new RuntimeException(ex);
            
        }
        
    }
    
    /**
     * Task validates and commits a transaction when it is run by the
     * {@link Journal#writeService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class TxCommitTask extends AbstractTask {
        
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
             * @todo This will be an RPC when using a distributed database. Try
             * to refactor so that we do make RPCs during the write task!
             */

            final long commitTime = nextTimestamp();
            
            tx.prepare(commitTime);
            
            return Long.valueOf( tx.commit() );
            
        }
        
    }
    
}
