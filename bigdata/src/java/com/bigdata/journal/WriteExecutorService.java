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

import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.rawstore.IRawStore;

/**
 * A custom {@link ThreadPoolExecutor} used by the {@link ConcurrentJournal} to
 * execute concurrent unisolated write tasks and perform group commits. Tasks
 * extend {@link AbstractTask}. The caller receives a {@link Future} when they
 * submit a task to the write service. That {@link Future} is NOT available
 * until the next group commit following the successful execution of the write
 * task.
 * <p>
 * Note: adding the thread name to the log messages for this class can aid
 * debugging. You can do this using the log4j configuration.
 * <p>
 * Note: the problem with running concurrent unisolated operations during a
 * commit and relying on an "auto-commit" flag to indicate whether or not the
 * index will participate is two fold. First, previous unisolated operations on
 * the same index will not get committed if an operation is currently running,
 * so we could wind up deferring check points of indices for quite a while.
 * Second, if there is a problem with the commit and we have to abort, then any
 * ongoing operations would still be using unisolated indices that could include
 * write sets that were discarded - this would make abort non-atomic.
 * <p>
 * The ground state from which an unisolated operation begins needs to evolve
 * after each unisolated operation that reaches its commit point successfully.
 * This can be acomplished by holding onto the btree reference, or even just the
 * address at which the metadata record for the btree was last written. We use
 * {@link AbstractJournal#name2Addr} for this purpose.
 * <p>
 * However, if an unisolated write fails for any reason on a given index then we
 * MUST use the last successful check point for that index. This is handled by
 * doing an abort.
 * <p>
 * Note: Due to the way in which the {@link BTree} class is written, it "steals"
 * child references when cloning an immutable node or leaf prior to making
 * modifications. This means that we must reload the btree from a metadata
 * record if we have to roll back due to an abort of some unisolated operation
 * since the state of the {@link BTree} has been changed as a side effect in a
 * non-reversable manner.
 * <p>
 * Note: Running {@link Thread}s may be interrupted at arbitrary moments for a
 * number of reasons by this class. The foremost example is a {@link Thread}
 * that is executing an {@link AbstractTask} when a concurrent decision is made
 * to discard the commit group, e.g., because another task in that commit group
 * failed. Regardless of the reason, if the {@link Thread} is performing an NIO
 * operation at the moment that the interrupt is notice, then it will close the
 * channel on which that operation was being performed. If you are using a
 * disk-based {@link BufferMode} for the journal, then the interrupt just caused
 * the backing {@link FileChannel} to be closed. In order to permit continued
 * operations on the journal, the {@link IRawStore} MUST transparently re-open
 * the channel. (The same problem can arise if you are using NIO for sockets or
 * anything else that uses the {@link Channel} abstraction.)
 * 
 * @todo The thread pool is essentially used as a queue to force tasks which
 *       have completed to await the commit. Consider placing a limit on the #of
 *       running threads when the thread pool is very large in case (a) a large
 *       #of threads would otherwise begin to execute tasks concurrently; and
 *       (b) the processor allocation strategy causes all threads to perform
 *       very slowly.
 * 
 * FIXME A task that does not declare any resources MUST NOT block in the lock
 * manager. It will, so write a test case for that in the lock manager and in
 * {@link TestConcurrentJournal}.
 * 
 * FIXME The write service is NOT the only source of reads against the store,
 * just the only source of reads against the live indices. Make sure that things
 * like {@link AbstractJournal#abort()} properly synchronize such that readers
 * can continue to execute without problems. For example, when we have to
 * re-open the backing file because it was closed when an IO was interrupted
 * concurrent readers will need the ability to re-open the channel immediately.
 * Show this with a variant of {@link TestClosedByInterruptException}.
 * 
 * FIXME Write a test that runs concurrent unisolated readers, unisolated
 * writers, and transactions and make sure that everything is Ok. There really
 * should be a correctness test here. The best way to do that may be to write a
 * correctness test for each of the three executor services (readers, writers,
 * and isolated transactions) and then to run all three tests at once against
 * the same store. As long as they use distinct indices there should be no
 * conflicts caused by the tests.
 * 
 * FIXME Finish tx support in {@link AbstractTask} and work in
 * {@link StressTestConcurrentTx}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class WriteExecutorService extends ThreadPoolExecutor {

    protected static final Logger log = Logger
            .getLogger(WriteExecutorService.class);

    /**
     * Turns on some stuff on stderr.
     * 
     * @release Set this false.
     */
    protected boolean trace = true;
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    private ConcurrentJournal journal;

    protected ConcurrentJournal getJournal() {

        return journal;

    }

    public WriteExecutorService(ConcurrentJournal journal, int corePoolSize,
            int maximumPoolSize, BlockingQueue<Runnable> queue, ThreadFactory threadFactory) {

        super( corePoolSize, maximumPoolSize, Integer.MAX_VALUE,
                TimeUnit.NANOSECONDS,
                queue, threadFactory);

        setJournal(journal);
        
    }

    private void setJournal(ConcurrentJournal journal) {

        if (journal == null)
            throw new NullPointerException();

        this.journal = journal;

        //            lastCommitTime.set( journal.getRootBlockView().getLastCommitTime() );

    }

    /*
     * Support for pausing and resuming execution of new worker tasks.
     */

    /** true iff nothing new should start. */
    private boolean isPaused;

    /** Lock used for {@link Condition}s. */
    final private ReentrantLock lock = new ReentrantLock();

    /** signaled when tasks should resume. */
    final private Condition unpaused = lock.newCondition();

    /**
     * The thread running {@link #groupCommit()} is signaled each time a task
     * has completed and will await the {@link #commit} signal.
     */
    final private Condition waiting = lock.newCondition();
    
    /**
     * Everyone awaiting this conditions is signaled when groupCommit is
     * performed.
     */
    final private Condition commit = lock.newCondition();

    /** #of tasks that are running. */
    final private AtomicInteger nrunning = new AtomicInteger(0);

    /**
     * The threads that are running our tasks (so that we can interrupt them
     * if necessary).
     */
    final private ConcurrentHashMap<Thread,AbstractTask> active = new ConcurrentHashMap<Thread,AbstractTask>();

    /** #of write tasks completed since the last commit. */
    final private AtomicInteger nwrites = new AtomicInteger(0);

    /** True iff we are executing a group commit. */
    final private AtomicBoolean groupCommit = new AtomicBoolean(false);
    
    /** True iff we are executing an abort. */
    final private AtomicBoolean abort = new AtomicBoolean(false);
    
    /**
     * The {@link Thread} that is executing the group commit and
     * <code>null</code> if group commit is not being executed.
     */
    private Thread groupCommitThread = null;
    
    /*
     * Counters
     */

    private long maxRunning = 0;
    private long maxLatencyUntilCommit = 0;
    private long maxCommitLatency = 0;
    private long ncommits = 0;
    private long naborts = 0;

    /**
     * The maximum #of tasks that are concurrently executing.
     */
    public long getMaxRunning() {
        
        return maxRunning;
        
    }
    
    /**
     * The maximum latency from when a task completes successfully until the
     * next group commit (milliseconds).
     */
    public long getMaxLatencyUntilCommit() {
        
        return maxLatencyUntilCommit;
        
    }
    
    /**
     * The maximum latency of the atomic commit operation (the maximum duration
     * of {@link AbstractJournal#commit()}) (milliseconds).
     */
    public long getMaxCommitLatency() {
        
        return maxCommitLatency;
        
    }

    /**
     * The #of commits since the {@link WriteExecutorService} was started.
     */
    public long getCommitCount() {
        
        return ncommits;
        
    }
    
    /**
     * The #of aborts since the {@link WriteExecutorService} was started.
     */
    public long getAbortCount() {
        
        return naborts;
        
    }
    
    /**
     * Sets the flag indicating that new worker tasks must pause in
     * {@link #beforeExecute(Thread, Runnable)}.
     */
    public void pause() {

        log.debug("Pausing write service");
        
        lock.lock();
        
        try {

            isPaused = true;
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

    /**
     * Notifies all paused tasks that they may now run.
     */
    public void resume() {
        
        log.debug("Resuming write service");
        
        lock.lock();
        
        try {
        
            isPaused = false;
            
            unpaused.signalAll();
            
        } finally {

            lock.unlock();
            
        }
        
    }

    /**
     * If task execution has been {@link #pause() paused} then
     * {@link Condition#await() awaits} someone to call {@link #resume()}.
     * 
     * @param t
     *            The thread that will run the task.
     * @param r
     *            The {@link Runnable} wrapping the {@link AbstractTask} - this
     *            is actually a {@link FutureTask}. See
     *            {@link AbstractExecutorService}.
     */
    protected void beforeExecute(Thread t, Runnable r) {

        // Note: [r] is the FutureTask.
        
        lock.lock();
        
        try {

            while (isPaused)
                unpaused.await();

        } catch (InterruptedException ie) {
            
            t.interrupt();
            
        } finally {
            
            lock.unlock();
            
        }
        
        super.beforeExecute(t, r);
        
    }

    /**
     * Executed before {@link AbstractTask#doTask()}
     * 
     * @param t
     *            The thread in which that task will execute.
     * @param r
     *            The {@link AbstractTask}.
     */
    protected void beforeTask(Thread t,AbstractTask r) {

        if (t == null)
            throw new NullPointerException();

        if (r == null)
            throw new NullPointerException();
        
        lock.lock();

        try {
        
            // Increment the #of running tasks.
            final int nrunning = this.nrunning.incrementAndGet();

            // Update max# of tasks concurrently running.
            maxRunning = (nrunning>maxRunning?nrunning:maxRunning);
            
            // Note the thread running the task.
            active.put(t,r);
        
            log.info("nrunning="+nrunning);
            
        } finally {
            
            lock.unlock();
            
        }
    
    }
    
    /**
     * This is executed after {@link AbstractTask#doTask()}. If the task
     * completed successfully (no exception thrown and its thread is not
     * interrupted) then we invoke {@link #groupCommit()}. If there was
     * a problem and an abort is not already in progress, then we invoke
     * {@link #abort()}.
     * 
     * @param r
     *            The {@link Callable} wrapping the {@link AbstractTask}.
     * @param t
     *            The exception thrown -or- <code>null</code> if the task
     *            completed successfully.
     * 
     * @exception RetryException
     *                if the task should be retried.
     * 
     * @todo we should do automatically re-submit the task rather than throwing
     *       a {@link RetryException}. Each try should incrementing a counter.
     *       If ntries exceeds a threashold (e.g., 3), then we should throw a
     *       "RetryCountExceeded" instead of re-submitting the task.
     *       <p>
     *       Test this by intermixing in some tasks that cause the commit group
     *       to fail with other tasks that should run to completion. Once the
     *       bad tasks are out of the way the good ones should execute fine.
     *       <p>
     *       We could also keep a maximum latency on the task and abort it if
     *       the latency is exceeded (regardless of the retry count).
     *       <p>
     *       Do NOT resubmit if the write service is being shutdown.
     */
    protected void afterTask(AbstractTask r, Throwable t) {
        
        if (r == null)
            throw new NullPointerException();
        
        lock.lock();
        
        try {
            
            /*
             * Whatever else we do, decrement the #of running writer tasks now.
             */
            
            final int nrunning = this.nrunning.decrementAndGet(); // dec. counter.

            log.info("nrunning="+nrunning);
            
            assert nrunning >= 0;
            
            /*
             * No exception and not interrupted?
             */
            
            if (t == null && ! Thread.interrupted()) {
                
                /*
                 * A write task succeeded.
                 * 
                 * Note: if the commit fails, then we need to interrupt all
                 * write tasks that are awaiting commit. This means that we can
                 * not remove the threads from [active] until after the commit.
                 */
                
                final int nwrites = this.nwrites.incrementAndGet();
                
                assert nwrites > 0;

                if (!groupCommit()) {

                    /*
                     * The task executed fine, but the commit group was aborted.
                     * This is generally due to a problem with a concurrent task
                     * causing the entire group to be discarded.
                     */
                    
                    if(journal.isOpen()) {
                        throw new RetryException();
                    } else {
                        throw new IllegalStateException("Journal is closed");
                    }
                    
                }

            } else {
                
                /*
                 * A write task failed - abort the current commit group.
                 * 
                 * Note: Since the write sets are combined we have to discard
                 * all if anyone fails.
                 * 
                 * Interrupts count as failures since we do not know whether or
                 * not the task has written any data. While we COULD check that,
                 * the expectation is that writers have already begun to write
                 * data by the time they get interrupted.
                 */

                if (t == null ) {
                    
                    /*
                     * This handles the case where the task was interrupted but
                     * it did not notice the interrupt itself.
                     */
                
                    log.warn("Task interrupted: task="+ r.getClass().getName());

                } else if (t instanceof ValidationError
                        || t.getCause() != null
                        && t.getCause() instanceof ValidationError) {

                    /*
                     * This handles the case where the task was interrupted and
                     * noticed the interrupt. Since the InterruptedException is
                     * often wrapped as a RuntimeException we also check the
                     * cause.
                     */
                    
                    log.warn("Validation failed: task=" + r.getClass().getName()
                            + " : " + t);

                } else if (t instanceof InterruptedException
                        || t.getCause() != null
                        && t.getCause() instanceof InterruptedException) {

                    /*
                     * This handles the case where the task was interrupted and
                     * noticed the interrupt. Since the InterruptedException is
                     * often wrapped as a RuntimeException we also check the
                     * cause.
                     */
                    
                    log.warn("Thread interrupted: task=" + r.getClass().getName()
                            + " : " + t);

                } else {

                    /*
                     * The task throw some other kind of exception.
                     */
                    
                    log.warn("Task failed: task=" + r.getClass().getName()
                            + " : " + t, t);

                }

                // abort the commit group.
                
                if(abort.get()) {
                    
                    log.info("Abort already in progress");
                    
                } else if(groupCommit.get()) {

                    // interrupt the thread running the group commit.
                    
                    log.info("Interrupting the thread running group commit: thread="+groupCommitThread.getName());
                    
                    groupCommitThread.interrupt();
                    
                } else {
                    
                    // no one is running group commit, so invoke abort ourselves.
                    
                    log.info("Invoking abort directly");
                    
                    abort();
                    
                }
                
            }
        
        } finally {
            
            lock.unlock();
            
        }

    }
    
    /**
     * Group commit.
     * <p>
     * This method is called by {@link #afterTask(Callable, Throwable)} for each
     * task that completes successfully. In each commit group, the group commit
     * will be executed in the {@link Thread} of the first task that calls this
     * method. At that point the thread pool is {@link #pause() paused} so that
     * no more tasks may begin to execute. Each running task that completes
     * before a timeout invoke this method in turn, causing it to await the
     * {@link #commit} signal. Once there are no more running tasks the store
     * will be committed. The set of tasks that were running when the thread
     * pool was {@link #pause() paused} will form the "commit group".
     * <p>
     * If there is a problem during the commit protocol then the write set(s)
     * are abandoned using {@link #abort()}. If there is a timeout waiting for
     * the running tasks to complete, then any {@link #active} tasks are
     * {@link Thread#interrupt() interrupted}. When they notice that they have
     * been interrupted they SHOULD throw an exception that will be propagated
     * back to the caller and made available via their {@link Future}. If the
     * thread {@link Thread#isInterrupted()} when entering
     * {@link #afterTask(Callable, Throwable)} then an exception will be thrown
     * since the task failed to notice the interrupt.
     * <p> 
     * Note: This method does NOT throw anything. All exceptions are caught and
     * handled.
     * 
     * <h4>Pre-conditions </h4>
     * <ul>
     * <li>You own the {@link #lock}</li>
     * <li>There is at least one write task that has completed.</li>
     * </ul>
     * 
     * <h4>Post-conditions (success)</h4>
     * <ul>
     * <li></li>
     * <li></li>
     * </ul>
     * 
     * <h4>Post-conditions (failure)</h4>
     * <ul>
     * <li></li>
     * <li></li>
     * </ul>
     * 
     * @return <code>true</code> IFF the commit was successful. Otherwise the
     *         commit group was aborted and the caller MUST throw a
     *         {@link RetryException} so that the abort is observable from
     *         {@link Future#get()}.
     */
    private boolean groupCommit(){

        log.debug("begin");

        lock.lock();

        /*
         * If an abort is in progress then throw an exception.
         */
        if( abort.get() ) {

            try {
            
                log.info("Abort in progress.");
            
                // abort() will no longer await this task's completion.

                waiting.signal();
                
                throw new RetryException("Abort in progress.");
                
            } finally {
                
                lock.unlock();
                
            }
            
        }
        
        /*
         * Note: This is outside of the try/finally block since the
         * [groupCommit] flag MUST NOT be cleared if it is already set and we
         * are doing that in try/finally block below.
         * 
         * If you rewrite this make sure that you do NOT cause the [groupCommit]
         * flag to be cleared except by the thread that successfully sets it on
         * entry to this method. A deadlock will arise if more than one thread
         * attempts to execute the group commit.
         */

        // attempt to atomically set the [groupCommit] flag.
        if (!groupCommit.compareAndSet(false, true)) {

            /*
             * Try/finally block ensures that we release the lock once we leave
             * this code.
             */

            try {
        
                /*
                 * This thread could not set the flag so some other thread is
                 * running the group commit and this thread will just await that
                 * commit.
                 */

                log.debug("Already executing in another thread");

                /*
                 * Notify the thread running the group commit that this thread
                 * will await that commit.
                 * 
                 * Note: We avoid the possibility of missing the [commit] signal
                 * since we currently hold the [lock].
                 */

                waiting.signal();

                try {

                    // await [commit]; releases [lock] while awaiting signal.

                    commit.await();

                    return true;
                    
                } catch (InterruptedException ex) {

                    // The task was aborted.

                    log.warn("Task interrupted awaiting group commit.");

                    return false;
                    
                }

            } finally {

                lock.unlock();

            }

        }

        log.info("This thread will run group commit");
        
        try {

            /*
             * Note: The logic above MUST NOT have released the lock if control was
             * allowed to flow down to this point.
             */
            
            assert lock.isHeldByCurrentThread();
            
            assert groupCommit.get();

            // save a reference to the thread that is running the group commit.
            groupCommitThread = Thread.currentThread();
            
            // timestamp from which we measure the latency until the commit begins.

            final long beginWait = System.currentTimeMillis();            

            /*
             * Wait a moment to let other tasks start, but if the queue is empty
             * then we make that a very small moment to keep down latency for a
             * single task that is run all by itself without anything else in
             * the queue.
             * 
             * @todo do NOT wait if the current task might exceeds its max
             * latency from submit.
             */
            
            int nwaits = 0;
            while(true) {

                final int queueSize = getQueue().size();
                final int nrunning = this.nrunning.get();
                final int nwrites = this.nwrites.get();
                final int corePoolSize = getCorePoolSize();
                final int poolSize = getPoolSize();
                final long elapsedWait = System.currentTimeMillis() - beginWait;
                                
                if ((elapsedWait > 100 && queueSize == 0) || elapsedWait > 250) {
                    
                    // Don't wait any longer.
                    
                    if(trace) System.err.println("Not waiting any longer: nwaits="
                            + nwaits + ", elapsed=" + elapsedWait
                            + "ms, queueSize=" + queueSize + ", nrunning="
                            + nrunning + ", nwrites=" + nwrites
                            + ", corePoolSize=" + corePoolSize + ", poolSize="
                            + poolSize);
                    
                    break;
                    
                }
                
                /*
                 * Note: if interrupted during sleep then the group commit will
                 * abort.
                 */
                
                waiting.await(10,TimeUnit.MICROSECONDS);
                
                nwaits++;
                
            }

            log.info("Will do group commit: nrunning="+nrunning);
            
            // notify the write service that new tasks MAY NOT run.
            
            pause();

            // wait for active tasks to complete.
            
            while (nrunning.get() > 0) {

                try {

                    /*
                     * Note: There is no way right now to back out partial
                     * writes by a task, so if a task is taking to long this
                     * would mean that we just abort the commit rather than
                     * continue to wait. However, note that abort() also must
                     * wait on the task to complete - but it has interrupted()
                     * the task, which will make polite tasks throw an
                     * exception.
                     * 
                     * Note: one could conceivably checkpoint the unisolated
                     * indices after each task and rollback to the previous
                     * checkpoint if a task is interrupted. Those check points
                     * would be maintained in the task metadata since tasks
                     * might themselves checkpoint incices and we need to know
                     * the correct checkpoint for the rollback.
                     * 
                     * The problem with index checkpoints is that they requiring
                     * flushing writes on the index to the store (or at least
                     * its write buffer), which has the side effect of making
                     * nodes and leaves in the index immutable. If task failures
                     * are rare then this could be more expensive than the
                     * occasional abort of a commit group.
                     */
                    
                    waiting.await();

//                    if(!waiting.await(250,TimeUnit.MILLISECONDS)) {
//                        
//                        elapsed = System.currentTimeMillis() - begin;
//                        
//                        /*
//                         * @todo if a timeout is supported here then add a
//                         * config parameter with option for infinite timeout iff
//                         * 0L (and write tests with that timeout value). Note
//                         * that this timeout would impose an absolute maximum on
//                         * the time that an unisolated write operation will wait
//                         * for other unisolated operation(s) to complete.
//                         * 
//                         * Another way to limit the run time of unisolated
//                         * writes is to place a timeout on [commit.await] and
//                         * interrupt all running tasks if the commit times out,
//                         * i.e., a maximum task latency timeout (measured from
//                         * the task submit).
//                         */
//                        if (elapsed > [timeout]) {
//                            
//                            log.warn("Timeout: nrunning=" + nrunning
//                                    + ", elapsed=" + elapsed);
//                            
//                            throw new TimeoutException(
//                                    "Timeout waiting for running tasks to complet.e");
//                            
//                        }
//                        
//                        log.debug("Still waiting: nrunning="+nrunning+", elapsed="+elapsed);
//                        
//                    }

                } catch (InterruptedException ex) {

                    /*
                     * The current thread was interrupted waiting for the active
                     * tasks to complete. At this point:
                     * 
                     *  - All write sets are still pending commit.
                     *  - All write tasks that are awaiting commit are still
                     * awaiting commit.
                     * 
                     * We now abort, causing all tasks to be aborted.
                     */

                    log.warn("Interrupted - discarding commit group.");
                    
                    try {

                        abort();
                        
                    } catch(Throwable t) {
                    
                        log.warn("Problem during abort?: "+t, t);
                        
                    }

                    return false;

                }

            }

            // at this point nwrites is the size of the commit group.
            log.info("Committing store: commit group size="+nwrites);

            // timestamp used to measure commit latency.
            final long beginCommit = System.currentTimeMillis();
            
            final long latencyUntilCommit = beginCommit - beginWait;
            
            if (latencyUntilCommit > maxLatencyUntilCommit) {

                maxLatencyUntilCommit = latencyUntilCommit;
                
            }

            // commit the store.
            if (!commit()) {

                return false;
                
            }

            // the commit latency.
            final long commitLatency = System.currentTimeMillis() - beginCommit;
            
            if (commitLatency > maxCommitLatency) {
                
                maxCommitLatency = commitLatency;
                
            }
            
            log.info("Commit Ok");
            
            return true;
            
        } catch(Throwable t) {
            
            log.info("Commit problem - will do abort: "+t,t);
            
            abort();
            
            return false;
            
        } finally {

            // atomically clear the [groupCommit] flag.
            groupCommit.set(false);

            // group commit is not being run.
            groupCommitThread = null;
            
            // allow new tasks to run.
            resume();

            lock.unlock();

        }

    }

    /**
     * Commit the store.
     * <p> 
     * Note: This method does NOT throw anything. All exceptions are caught and
     * handled.
     * 
     * <h4>Pre-conditions</h4>
     * <ul>
     * <li>The caller already owns the {@link #lock} - this ensures that the
     * pre-conditions are atomic since they are under the caller's control.</li>
     * <li>{@link #nrunning} is ZERO(0).</li>
     * <li>All active tasks have completed successfully.</li>
     * </ul>
     * 
     * <h4>Post-conditions (success)</h4>
     * <ul>
     * <li>nrunning, nwrites, active...</li>
     * </ul>
     * 
     * <h4>Post-conditions (failure):
     * <h4> The write sets are abandoned.
     * <ul>
     * <li>nrunning, nwrites, active...</li>
     * </ul>
     */
    private boolean commit() {

        try {

            assert lock.isHeldByCurrentThread();

            assert nrunning.get() == 0;

            // commit the store.

            /*
             * Note: if the journal was closed asynchronously then do not
             * attempt to abort the write set.
             */

            if(!journal.isOpen()) {

                return false;
                
            }

            // commit the write set.

            journal.commit();

            // #of commits that succeeded.
            
            ncommits++;
            
            if(trace) System.err.println("commit: #writes="+nwrites);
            
            return true;

        } catch (Throwable t) {

            /*
             * Abandon the write sets if something goes wrong.
             * 
             * Note: We do NOT rethrow the exception since that would cause the
             * scheduled group commit Runnable to no longer be executed!
             */

            log.error("Commit failed - abandoning write sets: " + t, t);

            abort();

            return false;

        } finally {

            resetState();

        }
        
    }

    /**
     * Abort. Interrupt all running tasks (they could be retried), await the
     * termination of those tasks, and then abandon the pending write set.
     * <p>
     * Note: This method does NOT throw anything. All exceptions are caught and
     * handled.
     * 
     * <h4>Pre-conditions</h4>
     * <ul>
     * <li>The caller already owns the {@link #lock} - this ensures that the
     * pre-conditions are atomic since they are under the caller's control.</li>
     * <li>An abort is not already in progress.</li>
     * </ul>
     * 
     * <h4>Post-conditions</h4>
     * <ul>
     * <li>nrunning, nwrites, active...</li>
     * </ul>
     */
    private void abort() {

        if(!abort.compareAndSet(false,true)) {

            // It is an error to invoke abort() if it is already in progress.
            
            throw new IllegalStateException("Abort already in progress.");
            
        }
        
        try {

            assert lock.isHeldByCurrentThread();

            /*
             * Interrupt all workers awaiting [commit] - they will throw an
             * exception that will reach the caller.
             */
            
            log.info("Interrupting tasks awaiting commit.");
            
            Iterator<Map.Entry<Thread,AbstractTask>> itr = active.entrySet().iterator();

            while (itr.hasNext()) {

                Map.Entry<Thread,AbstractTask> entry = itr.next();
                
                // set flag to deny access to resources.
                
                entry.getValue().aborted = true;
                
                // interrupt the thread running the task (do not interrupt this thread).
                
                if (Thread.currentThread() != entry.getKey()) {
                    
                    entry.getKey().interrupt();
                    
                }

            }

            // wait for active tasks to complete.

            log.info("Waiting for running tasks to complete: nrunning="+nrunning);

            while (nrunning.get() > 0) {
                
                try {

                    // Note: releases lock so that tasks may complete.

                    waiting.await();

                } catch (InterruptedException ex) {

                    /*
                     * The current thread was interrupted waiting for the active
                     * tasks to complete so that we can do the abort. At this
                     * point:
                     *  - All write tasks that are awaiting commit have been
                     * interrupted and will have thrown an exception back to the
                     * thread that submitted that task.
                     *  - All write tasks that are still running are being
                     * impolite and not noticing that they have been
                     * interrupted.
                     * 
                     * There is not a lot that we can do at this point.  For now
                     * I am just ignoring the interrupt.
                     */

                    log.warn("Interrupted awaiting running tasks - continuing.");
                    
                }
                
            }
            
            log.info("Doing abort: nrunning="+nrunning);
            
            // Nothing is running.
            assert nrunning.get() == 0;
            
            /*
             * Note: if the journal was closed asynchronously, e.g., by
             * shutdownNow(), then do not attempt to abort the write set since
             * no operations will be permitted on the journal, including an
             * abort().
             */

            if(journal.isOpen()) {

                // Abandon the write sets.
                
                journal.abort();
                
            }

            log.info("Did abort");
            
        } catch(Throwable t) {
            
            if(journal.isOpen()) {

                log.error("Problem with abort?", t);
                
            }

        } finally {

            // increment the #of aborts.
            
            naborts++;

            // clear the abort flag.
            
            abort.set(false);
            
            resetState();
            
        }
        
    }

    /**
     * Private helper resets some internal state to initial conditions following
     * either a successful commit or an abort.
     * <p>
     * Note: This method does NOT throw anything. All exceptions are caught and
     * handled.
     * 
     * <h4>Pre-conditions:</h4>
     * 
     * You MUST own the {@link #lock}.
     */
    private void resetState() {

        try {

            // no write tasks are awaiting commit.
            nwrites.set(0);

            // clear the set of active tasks.
            active.clear();

            // signal tasks awaiting [commit].
            commit.signalAll();

            // resume execution of new tasks.
            resume();
            
        } catch (Throwable t) {

            log.error("Problem witn resetState?", t);

        }

    }

    /**
     * An instance of this exception is thrown if a task successfully completed
     * but did not commit owing to a problem with some other task executing
     * concurrently in the {@link WriteExecutorService}. The task MAY be
     * retried.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class RetryException extends RuntimeException {

        /**
         * 
         */
        private static final long serialVersionUID = 2129883896957364071L;

        public RetryException() {
            super();
        }

        public RetryException(String msg) {
            super(msg);
        }
        
    }

}
