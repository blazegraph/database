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

import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;

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
 * 
 * @todo compute and make available the average latency waiting for the
 *       {@link #allquiet} and for the commit procedure itself.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class WriteExecutorService extends ScheduledThreadPoolExecutor {

    protected static final Logger log = Logger
            .getLogger(WriteExecutorService.class);

    private ConcurrentJournal journal;

    protected ConcurrentJournal getJournal() {

        return journal;

    }

    public WriteExecutorService(ConcurrentJournal journal, int corePoolSize) {

        super(corePoolSize);

        setJournal(journal);

    }

    public WriteExecutorService(ConcurrentJournal journal, int corePoolSize,
            RejectedExecutionHandler handler) {

        super(corePoolSize, handler);

        setJournal(journal);

    }

    public WriteExecutorService(ConcurrentJournal journal, int corePoolSize,
            ThreadFactory threadFactory) {

        super(corePoolSize, threadFactory);

        setJournal(journal);

    }

    public WriteExecutorService(ConcurrentJournal journal, int corePoolSize,
            ThreadFactory threadFactory, RejectedExecutionHandler handler) {

        super(corePoolSize, threadFactory, handler);

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

    /** signaled when nothing is running. */
    final private Condition allquiet = lock.newCondition();

    /**
     * Signaled when groupCommit is performed.
     */
    final private Condition commit = lock.newCondition();

    /** #of tasks that are running. */
    final private AtomicInteger nrunning = new AtomicInteger(0);

    /**
     * The threads that are running our tasks (so that we can interrupt them
     * if necessary).
     */
    final private HashSet<Thread> active = new HashSet<Thread>();

    /** #of write tasks completed since the last commit. */
    final private AtomicInteger nwrites = new AtomicInteger(0);

    /** True iff we are executing a group commit. */
    final private AtomicBoolean groupCommit = new AtomicBoolean(false);
    
    /**
     * The {@link System#currentTimeMillis()} when we began the current
     * commit group and {@link System#currentTimeMillis()} on start up of
     * the thread pool. This is used to measure how long it has been since
     * the last commit and thereby decide whether or not a commit should be
     * initiated.
     */
    private long beginCommitGroup = System.currentTimeMillis();

    /**
     * A group commit will be triggered after this many milliseconds (if
     * at least one write task has completed).  Note that group commit has
     * to wait for the active write tasks to join at a barrier.  This means
     * that is normally an additional delay once group commit is triggered.
     */
    final private long groupCommitTriggerMillis = 100;

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
     * Executed before {@link AbstractTask#doTask()}
     * 
     * @param t
     *            The thread in which that task will execute.
     * @param r
     *            The {@link Runnable} wrapping the {@link AbstractTask} -
     *            this is actually a {@link FutureTask}. See
     *            {@link AbstractExecutorService}
     */
    protected void beforeTask(Thread t,Callable r) {

        if (t == null)
            throw new NullPointerException();

        if (r == null)
            throw new NullPointerException();
        
        // Increment the #of running tasks.
        nrunning.incrementAndGet();

        // Note the thread running the task.
        active.add(t);
    
    }
    
    /**
     * This is executed after {@link AbstractTask#doTask()}. If the
     * conditions are satisified for a group commit, then one is executed in the
     * current thread. Otherwise it causes the write task to await the
     * {@link #commit} signal from the next {@link #groupCommit()}.
     * 
     * @param r
     *            The {@link Callable} wrapping the {@link AbstractTask}.
     * @param t
     *            The exception thrown -or- <code>null</code> if the task
     *            completed successfully.
     */
    protected void afterTask(Callable r, Throwable t) {
        
        if (r == null)
            throw new NullPointerException();
        
        lock.lock();
        
        try {
            
            /*
             * Whatever else we do, decrement the #of running writer tasks now.
             */
            
            final int nrunning = this.nrunning.decrementAndGet(); // dec. counter.
            
            assert nrunning >= 0;
            
            if (t == null) {
                
                /*
                 * A write task succeeded.
                 * 
                 * Note: if the commit fails, then we need to interrupt all
                 * write tasks that are awaiting commit. This means that we can
                 * not remove the threads from [active] until after the commit.
                 */
                
                final int nwrites = this.nwrites.incrementAndGet();
                
                assert nwrites > 0;

                /*
                 * FIXME Move all of this into groupCommit and then modify so
                 * that the thread awaits the commit unless it triggers the
                 * commit. More than that, commit can be immediate (or after a
                 * short interval) once the running tasks complete.
                 */
                
                if (nrunning == 0 && isPaused) {
                
                    // signal listener on [allquiet].
                    allquiet.signalAll();
                    
                }
                
                final long elapsed = System.currentTimeMillis()
                        - beginCommitGroup;
                
                /*
                 * Run a group commit: (a) if sufficient time has elapsed since
                 * the last commit; or (b) if we are in danger of starving our
                 * thread pool.
                 */

                if (elapsed > groupCommitTriggerMillis || nrunning >= getCorePoolSize()) {
                    
                    /*
                     * Do group commit then signal [commit].
                     */
                    
                    try {
                    
                        groupCommit();
                        
                    } catch(InterruptedException ex) {
                        
                        // continue processing.
                        
                        log.warn("Group commit interrupted.", ex);
                        
                        return;
                        
                    }
            
                } else {
                    
                    /*
                     * The conditions for triggering a group commit were not met
                     * so we await the [commit] signal. This causes the thread
                     * executing the task to wait until the next commit (or
                     * abort).
                     * 
                     * Note: If an abort was executed instead of a commit then
                     * the thread will have been interrupted. This is a signal
                     * to throw an exception back to the caller.
                     * 
                     * Note: The base class makes sure that the interrupted
                     * status is cleared before assigning a new task to a worker
                     * thread.
                     */
                    
                    try {
                        
                        /*
                         * Await signal indicating that the write set of the
                         * task was made restart safe.
                         */
                        
                        log.info("Awaiting commit");
                        
                        commit.await();
                        
                        log.info("Received commit signal.");
                        
                    } catch (InterruptedException ex) {
                        
                        /*
                         * The commit was not successful.
                         * 
                         * @todo pass back specific exceptions so that we can
                         * indicate if a task can be retried or if it was a
                         * problem child, timeout, etc.
                         */
                        
                        throw new RuntimeException(
                                "Interrupted awaiting commit: " + ex);
                        
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

                if (t instanceof InterruptedException || t.getCause() != null
                        && t.getCause() instanceof InterruptedException) {

                    log.warn("Task interrupted: task=" + r.getClass().getName()
                            + " : " + t);

                } else {

                    log.warn("Task failed: task=" + r.getClass().getName()
                            + " : " + t, t);

                }

                abort();
                
            }
        
        } finally {
            
            lock.unlock();
            
        }

    }
    
    /**
     * If task execution has been {@link #pause() paused} then
     * {@link Condition#await() awaits} someone to call {@link #resume()}.
     */
    protected void beforeExecute(Thread t, Runnable r) {
        
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

//    /**
//     * Decrements a counter of the #of running tasks. If the counter reaches
//     * zero and task execution is paused then signal {@link #allquiet} so
//     * that a coordinated task may execute.
//     */
//    protected void afterExecute(Runnable r, Throwable t) {
//        
//        super.afterExecute(r, t);
//                
//    }

    /**
     * Group commit.
     * <p>
     * The thread pool is {@link #pause() paused} so that no new write tasks may
     * begin to execute. Once no tasks are running we commit the store. If there
     * is a problem during the commit protocol then the write set(s) are
     * abandoned using {@link #abort()}.
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
     * @return true iff a commit was actually performed.
     * 
     * @throws InterruptedException
     *             if the group commit operation was interrupted while awaiting
     *             the "all quiet".
     */
    public boolean groupCommit() throws InterruptedException {

        log.debug("begin");

        lock.lock();
        
        if(!groupCommit.compareAndSet(false, true)) {

            /*
             * Note: This test needs to occur outside of the try/finally since
             * we do NOT want to clear the [groupCommit] flag if it is set and
             * we are doing that in finally below. If you rewrite this make sure
             * that you do NOT cause the [groupCommit] flag to be cleared when
             * it is set on entry to this method. A deadlock will arise if more
             * than one thread attempts to execute the group commit.
             */
            
            log.debug("Already executing in another thread");
            
            lock.unlock();
            
            return false;
            
        }

        try {

            if(nwrites.get() == 0) {
                
                /*
                 * Consider also testing the elapsed time since the last group
                 * commit since we don't want them to come too closely together.
                 * 
                 * Note that group commit is requested both from within the
                 * write service and from a scheduled delay task. The latter
                 * runs regardless of whether nwrites > 0 or elapsed >
                 * groupCommitTriggerMillis.
                 */
                
                log.debug("No writer tasks awaiting");
                
                return false;
                
            }
            
            log.info("Will do group commit: nrunning="+nrunning);
            
            // notify the write service that new tasks MAY NOT run.
            pause();

            // wait for active tasks to complete.
            
            final long begin = System.currentTimeMillis();
            
            while (nrunning.get() > 0) {

                try {

                    /*
                     * @todo use timeout and cancel long running tasks in the
                     * write service to guarentee the maximum latency?
                     * 
                     * Note that there is no way right now to back out partial
                     * writes by a task, so if a task is taking to long this
                     * would mean that we just abort the commit rather than
                     * continue to wait.
                     */
                    
                    if(!allquiet.await(250,TimeUnit.MILLISECONDS)) {
                        
                        final long elapsed = System.currentTimeMillis() - begin;
                        
                        /*
                         * Note: The commitService uses a delay based schedule
                         * to issue group commit requests so those request do
                         * not back up here.
                         * 
                         * @todo I had introduced a timeout here since I was
                         * seeing deadlocks during shutdown. That issue has been
                         * since resolved using a distinct commitService to
                         * issue the periodic group commit requests. Therefore I
                         * have disable the timeout again here.
                         * 
                         * If the timeout is re-introduced then add a config
                         * parameter with option for infinite timeout iff 0L
                         * (and write tests with that timeout value). Note that
                         * this timeout would impose an absolute maximum on the
                         * length of an unisolated write operation.
                         * 
                         * Another way to limit the run time of unisolated
                         * writes is to place a timeout on [commit.await] and
                         * interrupt all running tasks if the commit times out.
                         * Regardless we need to make sure that writers actually
                         * notice if they get interrupted.
                         */
//                        if (elapsed > 2000) {
//                            
//                            log.warn("Timeout: nrunning=" + nrunning
//                                    + ", elapsed=" + elapsed);
//                            
//                            throw new RuntimeException(
//                                    "Timeout waiting for running tasks to complet.e");
//                            
//                        }
                        
                        log.info("Still waiting: nrunning="+nrunning+", elapsed="+elapsed);
                        
                    }

                } catch (InterruptedException ex) {

                    /*
                     * The commit was interrupted.
                     * 
                     * All write sets are still pending commit.
                     * 
                     * All write tasks that are awaiting commit are still
                     * awaiting commit.
                     * 
                     * Reset the timeout so that group commit will be deferred
                     * for another interval.
                     * 
                     * Note: if this occurs repeatedly then all worker threads
                     * will wind up awaiting [commit] and the write service will
                     * no longer progress.
                     */

                    beginCommitGroup = System.currentTimeMillis();

                    throw ex;

                }

            }

            // at this point nwrites is the size of the commit group.
            log.info("All quiet: commit group size="+nwrites);
            
            // commit the store.
            commit();

            log.info("success");

            return true;
            
        } finally {

            if (!groupCommit.compareAndSet(true, false)) {

                throw new AssertionError();
                
            }
            
            // allow new tasks to run.
            resume();

            lock.unlock();

        }

    }

    /**
     * Commit the store.
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
    private void commit() {

        assert lock.isHeldByCurrentThread();
//        lock.lock();

        assert nrunning.get() == 0;
        
        // @todo scan workers for interrupted thread and abort if found?

        try {

            /*
             * Commit the store.
             */

            // commit the store.
            log.info("Commit");

            journal.commit();

            log.info("Success");

        } catch (Throwable t) {

            /*
             * Abandon the write sets if something goes wrong.
             * 
             * Note: We do NOT rethrow the exception since that would
             * cause the scheduled group commit Runnable to no longer be
             * executed!
             */

            log.error("Commit failed - abandoning write sets: " + t, t);

            abort();

        } finally {

            resetState();
            
//            lock.unlock();

        }

    }

    /**
     * Abort. Interrupt all running tasks (they could be retried) and abandon
     * the pending write set.
     * 
     * <h4>Pre-conditions</h4>
     * <ul>
     * <li>The caller already owns the {@link #lock} - this ensures that the
     * pre-conditions are atomic since they are under the caller's control.</li>
     * <li></li>
     * </ul>
     * 
     * <h4>Post-conditions</h4>
     * <ul>
     * <li>nrunning, nwrites, active...</li>
     * </ul>
     */
    private void abort() {

        log.info("Abandoning write sets.");

        assert lock.isHeldByCurrentThread();
//        lock.lock();

        try {

            // Abandon the write sets.
            
            journal.abort();

            /*
             * Interrupt all workers awaiting [commit] - they will throw an
             * exception that will reach the caller.
             */

            log.info("Interrupting tasks awaiting commit.");
            
            Iterator<Thread> itr = active.iterator();

            while (itr.hasNext()) {

                itr.next().interrupt();

            }

        } finally {

            resetState();
            
//            lock.unlock();
            
        }
        
    }

    /**
     * Private helper resets some internal state to initial conditions following
     * either a successful commit or an abort.
     * 
     * <h4>Pre-conditions:</h4>
     * 
     * You MUST own the {@link #lock}.
     */
    private void resetState() {

        assert lock.isHeldByCurrentThread();
        
        // no write tasks are awaiting commit.
        nwrites.set(0);
        
        // clear the set of active tasks.
        active.clear();

        // signal tasks awaiting [commit].
        commit.signalAll();

        // resume execution of new tasks.
        resume();

    }

}
