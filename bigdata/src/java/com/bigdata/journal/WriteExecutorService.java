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
package com.bigdata.journal;

import java.lang.ref.WeakReference;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.btree.BTree;
import com.bigdata.concurrent.NonBlockingLockManagerWithNewDesign;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.OverflowManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.DataService;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.WriteTaskCounters;

/**
 * A custom {@link ThreadPoolExecutor} used by the {@link ConcurrencyManager} to
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
 * This can be accomplished by holding onto the btree reference, or even just the
 * address at which the metadata record for the btree was last written. We use
 * {@link AbstractJournal#getName2Addr()} for this purpose.
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
 * non-reversible manner.
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
 * <h2>Overflow handling</h2>
 * 
 * <p>
 * The {@link WriteExecutorService} invokes {@link #overflow()} each time it
 * does a group commit. Normally the {@link WriteExecutorService} does not
 * quiesce before doing a group commit, and when it is not quiescent the
 * {@link ResourceManager} can NOT {@link #overflow()} the journal since
 * concurrent tasks are still writing on the current journal. Therefore the
 * {@link ResourceManager} monitors the {@link IBufferStrategy#getExtent()} of
 * the live journal. When it decides that the live journal is large enough it
 * {@link WriteExecutorService#pause()}s {@link WriteExecutorService} and waits
 * until {@link #overflow()} is called with a quiescent
 * {@link WriteExecutorService}. This effectively grants the
 * {@link ResourceManager} exclusive access to the journal. It can then run
 * {@link #overflow()} to setup a new journal and tell the
 * {@link WriteExecutorService} to {@link WriteExecutorService#resume()}
 * processing.
 * </p>
 * 
 * @todo There should be a clear advantage to pipelining operations for the same
 *       index partition into the same commit group. That would maximize the
 *       reuse of the index buffers and minimize the concurrent demand for
 *       distinct indices. This is basically barging in on the write service
 *       based on an affinity for active indices. The trick is to not starve out
 *       indices which are not active. If the scope is limited to a commit group
 *       or a period of time then that might do it.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class WriteExecutorService extends ThreadPoolExecutor {

    /**
     * Main log for the {@link WriteExecutorService}.
     */
    protected static final Logger log = Logger
            .getLogger(WriteExecutorService.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * Uses the {@link OverflowManager} log for things relating to synchronous
     * overflow processing.
     */
    protected static final Logger overflowLog = Logger
            .getLogger(OverflowManager.class);

    /**
     * True iff the {@link #overflowLog} level is INFO or less.
     */
    final protected static boolean OVERFLOW_INFO = overflowLog.isInfoEnabled();

    /**
     * True iff the {@link #overflowLog} level is DEBUG or less.
     */
    final protected static boolean OVERFLOW_DEBUG = overflowLog.isDebugEnabled();

    /**
     * When <code>true</code>, writes the set of {@link #active} tasks into
     * the {@link MDC} under the <code>activeTasks</code> key. This is of
     * interest if you want to know which tasks are in the same commit group.
     */
    final boolean trackActiveSetInMDC = false; // MUST be false for deploy

    private final IResourceManager resourceManager;
    
    /**
     * The name of the service if the write service is running inside of a
     * service (used for error messages).
     */
    private final String serviceName;
    
    /**
     * The object that coordinates exclusive access to the resources.
     */
    public NonBlockingLockManagerWithNewDesign<String> getLockManager() {
        
        return lockManager;
        
    }
    private final NonBlockingLockManagerWithNewDesign<String> lockManager;

    /**
     * The time in milliseconds that a group commit will await currently running
     * tasks to join the commit group.
     */
    protected final long groupCommitTimeout;

    /**
     * The time in milliseconds that a group commit will await an exclusive lock
     * on the write service in order to perform synchronous overflow processing.
     * This lock is requested IFF overflow process SHOULD be performed. The lock
     * timeout needs to be of significant duration or a lock request for a write
     * service under heavy write load will timeout, in which case an error will
     * be logged. If overflow processing is not performed the live journal
     * extent will grow without bound.
     */
    protected final long overflowLockRequestTimeout;
    
    private static class MyLockManager<R extends Comparable<R>> extends
            NonBlockingLockManagerWithNewDesign<R> {

//        private final WriteExecutorService service;
        private final WeakReference<WriteExecutorService> serviceRef;

        public MyLockManager(final int capacity, final int maxLockTries,
                final boolean predeclareLocks,
                final WriteExecutorService service) {

            super(capacity, maxLockTries, predeclareLocks);

//            this.service = service;
            this.serviceRef = new WeakReference<WriteExecutorService>(service);

        }

        protected void ready(final Runnable r) {

//            service.execute(r);
            
            final WriteExecutorService service = serviceRef.get();
            
            if(service == null)
                throw new RejectedExecutionException();
            
            service.execute(r);

        }

    }

    /**
     * Tracks the #of rejected execution exceptions on the caller's counter.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static class MyRejectedExecutionHandler implements
            RejectedExecutionHandler {

        private final AtomicLong rejectedExecutionCount;
        
        public MyRejectedExecutionHandler(final AtomicLong rejectedExecutionCount) {
            
            this.rejectedExecutionCount = rejectedExecutionCount;
            
        }
        
        public void rejectedExecution(Runnable arg0, ThreadPoolExecutor arg1) {

            rejectedExecutionCount.incrementAndGet();

            throw new RejectedExecutionException();

        }

    }
    
    /**
     * 
     * @param resourceManager
     * @param corePoolSize
     * @param maximumPoolSize
     * @param keepAliveTime
     * @param keepAliveUnit
     * @param queue
     * @param threadFactory
     * @param groupCommitTimeout
     *            The time in milliseconds that a group commit will await
     *            currently running tasks to join the commit group.
     *            @param overflowLockRequestTimeout
     */
    public WriteExecutorService(//
            final IResourceManager resourceManager,
            final int corePoolSize,
            final int maximumPoolSize,
            final long keepAliveTime,//
            final TimeUnit keepAliveUnit,//
            final BlockingQueue<Runnable> queue, 
            final ThreadFactory threadFactory,
            final long groupCommitTimeout,
            final long overflowLockRequestTimeout) {

        super(  corePoolSize, //
                maximumPoolSize,//
                keepAliveTime,//
                keepAliveUnit,//
                queue,//
                threadFactory//
                );

        if (resourceManager == null)
            throw new IllegalArgumentException();
        
        if (groupCommitTimeout < 0L) 
            throw new IllegalArgumentException();

        if (overflowLockRequestTimeout < 0L) 
            throw new IllegalArgumentException();
        
        this.groupCommitTimeout = groupCommitTimeout;
        
        this.overflowLockRequestTimeout = overflowLockRequestTimeout;
        
        // Setup the lock manager used by the write service.
        {

            /*
             * Create the lock manager. Since we pre-declare locks,
             * deadlocks are NOT possible and the capacity parameter is
             * unused.
             * 
             * Note: pre-declaring locks means that any operation that
             * writes on unisolated indices MUST specify in advance those
             * index(s) on which it will write. This is enforced by the
             * AbstractTask API.
             */

            lockManager = new MyLockManager<String>(//
                  maximumPoolSize, // capacity
                  3, // @todo config maxLockTries
                  true, // predeclareLocks
                  this);

//            lockManager = new NonBlockingLockManagerWithNewDesign<String>(//
//                    maximumPoolSize, // capacity
//                    3, // @todo config maxLockTries
//                    true // predeclareLocks
//            ) {
//                
//                protected void ready(Runnable r) {
//                    
//                    WriteExecutorService.this.execute(r);
//                    
//                }
//                
//            };
            
        }

        this.resourceManager = resourceManager;

        /*
         * Tracks rejected executions on a counter.
         */
        setRejectedExecutionHandler(new MyRejectedExecutionHandler(
                rejectedExecutionCount));

        /*
         * Extract the name of the service if we are running inside of one.
         */
        String serviceName;
        try {
            final DataService dataService = resourceManager.getDataService();
            serviceName = dataService.getServiceName();
        } catch(UnsupportedOperationException ex) {
            serviceName = "";
        }
        this.serviceName = serviceName;
        
    }

    /*
     * Support for pausing and resuming execution of new worker tasks.
     */

    /**
     * New tasks may begin to execute iff this counter is zero (0). It is
     * incremented by {@link #pause()} and decremented by {@link #resume()}.
     */
    private final AtomicInteger paused = new AtomicInteger();

    /**
     * Lock used for exclusive locks on the write service.
     */
    final private ReentrantLock exclusiveLock = new ReentrantLock();
    
    /**
     * Lock used for {@link Condition}s and to coordinate index checkpoints and
     * index rollbacks with the {@link AbstractTask}.
     * 
     * @todo we should be using {@link ReentrantLock#lockInterruptibly()} rather
     *       than lock().  This will let us notice interrupts more readily.
     */
    final private ReentrantLock lock = new ReentrantLock();

    /** signaled when tasks should resume. */
    final private Condition unpaused = lock.newCondition();

    /**
     * The thread running {@link #groupCommit()} is signaled each time a task
     * has completed processing. The task will await the {@link #commit} signal
     * before it resumes.
     * <p>
     * Note: This {@link Condition} is also used by {@link #quiesce(long, TimeUnit)}
     * so {@link Condition#signalAll()} is required rather than {@link Condition#signal()}
     * to ensure that {@link #quiesce(long, TimeUnit)} does not "steal" the signal from
     * {@link #groupCommit()} or {@link #abort()}.
     */
    final private Condition waiting = lock.newCondition();
    
    /**
     * Everyone awaiting this conditions is signaled when groupCommit is
     * performed.
     */
    final private Condition commit = lock.newCondition();

    /**
     * The #of rejected tasks.
     */
    final private AtomicLong rejectedExecutionCount = new AtomicLong();
    
    /** #of tasks that are running. */
    final private AtomicInteger nrunning = new AtomicInteger(0);

    /**
     * #of tasks that are waiting to run but are blocked on the #lock. This
     * value represents the #of tasks which have been starved from concurrent
     * execution. The main culprit for a high value here is group commit and the
     * occasional synchronous overflow or purge resources (when someone has an
     * exclusive lock on the write service).
     */
    final private AtomicInteger nready = new AtomicInteger(0);

    /**
     * The threads that are running our tasks (so that we can interrupt them
     * if necessary).
     */
    final private ConcurrentHashMap<Thread,AbstractTask> active = new ConcurrentHashMap<Thread,AbstractTask>();

    /**
     * The set of tasks that make it into the commit group (so that we can set
     * the commit time on each of them iff the group commit succeeds).
     */
    final private Map<Thread,AbstractTask> commitGroup = new LinkedHashMap<Thread, AbstractTask>();
    
    /** #of write tasks completed since the last commit. */
    final private AtomicInteger nwrites = new AtomicInteger(0);
    
    /** True iff we are executing a group commit. */
    final private AtomicBoolean groupCommit = new AtomicBoolean(false);
    
    /** True iff we are executing an abort. */
    final private AtomicBoolean abort = new AtomicBoolean(false);
    
    /*
     * Counters
     */

    private int maxPoolSize = 0;
    private long maxRunning = 0;
    private long maxCommitWaitingTime = 0;
    private long maxCommitServiceTime = 0;
    private int maxCommitGroupSize = 0;
    private int commitGroupSize = 0;
    private long byteCountPerCommit = 0L;
    private AtomicLong ngroupCommits = new AtomicLong();
    private long naborts = 0;
    private long failedTaskCount = 0;
    private long successTaskCount = 0;
    private long committedTaskCount = 0;
    private long noverflow = 0;

    protected AtomicInteger activeTaskCountWithLocksHeld = new AtomicInteger(0);

    /**
     * The #of rejected tasks.
     */
    public long getRejectedExecutionCount() {
        
        return rejectedExecutionCount.get();
        
    }
    
    /**
     * The maximum #of threads in the pool.
     */
    public int getMaxPoolSize() {
        
        return maxPoolSize;
        
    }
    
    /**
     * The maximum #of tasks that are concurrently executing without regard to
     * whether or not the tasks have acquired their locks.
     * <p>
     * Note: Since this does not reflect tasks executing concurrently with locks
     * held it is not a measure of the true concurrency of tasks executing on
     * the service.
     */
    public long getMaxRunning() {
        
        return maxRunning;
        
    }
    
    /**
     * The maximum waiting time in millseconds from when a task completes
     * successfully until the next group commit.
     */
    public long getMaxCommitWaitingTime() {
        
        return maxCommitWaitingTime;
        
    }
    
    /**
     * The maximum service time in milliseconds of the atomic commit.
     * 
     * @see AbstractJournal#commit()
     */
    public long getMaxCommitServiceTime() {
        
        return maxCommitServiceTime;
        
    }

    /**
     * The #of threads queued on the internal {@link #lock}. These are (for the
     * most part) threads waiting to start or stop during a group commit.
     * However, you can not use this measure to infer whether there are threads
     * waiting to run which are being starved during a group commit or simply
     * threads waiting to do their post-processing.
     */
    public int getInternalLockQueueLength() {

        return lock.getQueueLength();
        
    }
    
    /**
     * The #of tasks in the most recent commit group. In order to be useful
     * information this must be sampled and turned into a moving average.
     */
    public int getCommitGroupSize() {
        
        return commitGroupSize;
        
    }
    
    /**
     * The maximum #of tasks in any commit group.
     */
    public int getMaxCommitGroupSize() {
        
        return maxCommitGroupSize;
        
    }
    
    /**
     * The #of group commits since the {@link WriteExecutorService} was started
     * (all commits by this service are group commits).
     */
    public long getGroupCommitCount() {
        
        return ngroupCommits.get();
        
    }

    /**
     * The #of bytes written by the last commit. This must be sampled to turn it
     * into useful information.
     */
    public long getByteCountPerCommit() {
        
        return byteCountPerCommit;
        
    }
    
    /**
     * The #of aborts (not failed tasks) since the {@link WriteExecutorService}
     * was started. Aborts are serious events and occur IFF an
     * {@link IAtomicStore#commit()} fails.  Failed tasks do NOT result in an
     * abort.
     */
    public long getAbortCount() {
        
        return naborts;
        
    }
    
    /**
     * The #of tasks that have failed.  Task failure means that the write set(s)
     * for the task are discarded and any indices on which it has written are
     * rolled back.  Task failure does NOT cause the commit group to be discard.
     * Rather, the failed task never joins a commit group and returns control
     * immediately to the caller.
     */
    public long getTaskFailedCount() {
    
        return failedTaskCount;
        
    }
    
    /**
     * The #of tasks that have executed successfully (MIGHT NOT have been
     * committed safely).
     * 
     * @see #getTaskCommittedCount()
     */
    public long getTaskSuccessCount() {
        
        return successTaskCount;
        
    }
    
    /**
     * The #of tasks that (a) executed successfully and (b) have been committed.
     */
    public long getTaskCommittedCount() {
        
        return committedTaskCount;
        
    }
    
    /**
     * The #of times synchronous overflow processing has been performed.
     */
    public long getOverflowCount() {
    
        return noverflow;
        
    }
    
    /**
     * The instantaneous #of tasks that have <strong>acquired</strong> their
     * locks are executing concurrently on the write service. This is the real
     * measure of concurrent task execution on the write service. However, you
     * need to sample this value and compute a moving average in order to turn
     * it into useful information.
     * <p>
     * The returned value is limited by {@link #getActiveCount()}. Note that
     * {@link #getActiveCount()} reports tasks which are <strong>waiting on
     * their locks</strong> as well as those engaged in various pre- or
     * post-processing.
     */
    public int getActiveTaskCountWithLocksHeld() {

        return activeTaskCountWithLocksHeld.get();
        
    }
    
    /**
     * #of tasks that are waiting to run but are blocked on the #lock. This
     * value represents the #of tasks which have been starved from concurrent
     * execution. The main culprit for a high value here is group commit and the
     * occasional synchronous overflow or purge resources (when someone has an
     * exclusive lock on the write service).
     */
    public int getReadyCount() {
    
        return nready.get();
        
    }
    
    /**
     * <code>true</code> iff the pause flag is set such that the write service
     * will queue up new tasks without allowing them to execute.
     * <p>
     * Note: The caller MUST hold the {@link #lock} if they want this test to be
     * more than transiently valid.
     * 
     * @see #pause()
     * @see #resume()
     */
    private boolean isPaused() {
        
        return paused.get() > 0;
        
    }
    
    /**
     * Sets the flag indicating that new worker tasks must pause in
     * {@link #beforeExecute(Thread, Runnable)}.
     * <p>
     * Note: This is not a very safe thing to do and therefore the operation is
     * restricted to its use by this class. Use {@link #tryLock(long, TimeUnit)}
     * and {@link #unlock()} instead.
     */
    private void pause() {

        lock.lock();
        
        try {

            if (paused.incrementAndGet() == 0) {

                if (DEBUG)
                    log.debug("Pausing write service");

            }
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

    /**
     * Notifies all paused tasks that they may now run.
     */
    private void resume() {
        
        lock.lock();
        
        try {

            if (paused.get() == 0) {

                throw new IllegalStateException("Not paused");

            }

            if (paused.decrementAndGet() == 0) {

                if (DEBUG)
                    log.debug("Resuming write service");

                unpaused.signalAll();

            }
            
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
    protected void beforeExecute(final Thread t, final Runnable r) {

        // Note: [r] is the FutureTask.
        
        lock.lock();
        
        try {

            while (isPaused()) {

                unpaused.await();
                
            }

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
    protected void beforeTask(final Thread t, final AbstractTask r) {

        if (t == null)
            throw new NullPointerException();

        if (r == null)
            throw new NullPointerException();

        nready.incrementAndGet();
        
        lock.lock();
        
        try {
        
            // Increment the #of running tasks.
            final int nrunning = this.nrunning.incrementAndGet();

            // Update max# of tasks concurrently running.
            maxRunning = (nrunning > maxRunning ? nrunning : maxRunning);

            // Update max# of threads in the thread pool.
            final int poolSize = getPoolSize();

            maxPoolSize = (poolSize > maxPoolSize ? poolSize : maxPoolSize);
            
            // Note the thread running the task.
            active.put(t, r);
        
            if (trackActiveSetInMDC) {

                MDC.put("activeTasks", active.values().toString());
                
            }

            MDC.put("taskState", "running");

            /*
             * Note: This is the commit counter at the instant that this task
             * was starting to execute. It could be compared to the
             * commitCounter at after the task had executed to see how many
             * group commits were made while this task was running.
             */
            MDC.put("commitCounter", "commitCounter=" + ngroupCommits);

            if (INFO)
                log.info("nrunning=" + nrunning);
            
        } finally {
            
            lock.unlock();
            
            nready.decrementAndGet();
            
        }
    
    }
    
    /**
     * This is executed after {@link AbstractTask#doTask()}. If the task
     * completed successfully (no exception thrown and its thread is not
     * interrupted) then we invoke {@link #groupCommit()}. Otherwise the write
     * set of the task was already discarded by
     * {@link AbstractTask.InnerWriteServiceCallable} and we do nothing.
     * 
     * @param r
     *            The {@link Callable} wrapping the {@link AbstractTask}.
     * @param t
     *            The exception thrown -or- <code>null</code> if the task
     *            completed successfully.
     */
    protected void afterTask(final AbstractTask r, final Throwable t) {
        
        if (r == null)
            throw new NullPointerException();
        
        lock.lock();
        
        try {
            
            /*
             * Whatever else we do, decrement the #of running writer tasks now.
             */
            
            final int nrunning = this.nrunning.decrementAndGet(); // dec. counter.

            MDC.remove("taskState");

            if(INFO) log.info("nrunning="+nrunning);
            
            assert nrunning >= 0;
            
            /*
             * No exception and not interrupted?
             */
            
            if (t == null /*&& ! Thread.interrupted()*/) {
                
                /*
                 * A write task succeeded.
                 * 
                 * Note: if the commit fails, then we need to interrupt all
                 * write tasks that are awaiting commit. This means that we can
                 * not remove the threads from [active] until after the commit.
                 */
                
                final int nwrites = this.nwrites.incrementAndGet();
                
                assert nwrites > 0;

                // add to the commit group.
                commitGroup.put(Thread.currentThread(), r);
                
                // another task executed successfully.
                successTaskCount++;

                MDC.put("taskState","waitingOnCommit");

                if (!groupCommit()) {
                    
                    /*
                     * The task executed fine, but the commit group was aborted.
                     * 
                     * @todo what circumstances can cause this other than the
                     * journal being shutdown (interrupted) while tasks are
                     * running, running out of disk space or hard IO failures,
                     * etc? That is, are there any conditions from which the
                     * write service could recover or are they all terminal
                     * conditions?
                     */
                    
                    final AbstractJournal journal = resourceManager.getLiveJournal();
                    
                    if(journal.isOpen()) {

                        throw new RuntimeException("Commit failed: "+r);
                        
                    } else {
                        
                        throw new IllegalStateException("Journal is closed: "+r);
                        
                    }
                    
                }

            } else {
                
                /*
                 * A write task failed. Its write set has already been
                 * discarded. We log some messages based on the cause and then
                 * just return immediately
                 */

                failedTaskCount++;
                
                MDC.put("taskState","failure");

                if (InnerCause.isInnerCause(t, ValidationError.class)) {

                    /*
                     * ValidationError.
                     * 
                     * The task was a commit for a transaction but the
                     * transaction's write set could not be validated. Log a
                     * warning.
                     */
                    
                    if(INFO) log.info("Validation failed: task=" + r);//, t);

                } else if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                    /*
                     * InterruptedException.
                     * 
                     * The task was interrupted, noticed the interrupt, and threw
                     * out an InterruptedException.
                     */
                    
                    log.warn("Task interrupted: task=" + r);//, t);

                } else if(InnerCause.isInnerCause(t, NoSuchIndexException.class)) {

                    /*
                     * NoSuchIndexException.
                     * 
                     * The task attempted to access an index that does not
                     * exist. This is pretty common and often occurs when an
                     * application attempts to determine whether or not an index
                     * has been registered.
                     */
 
                    if(INFO) log.info("No such index: task=" + r);//, t);
                    
                } else if(InnerCause.isInnerCause(t, StaleLocatorException.class)) {

                    /*
                     * StaleLocatorException.
                     * 
                     * The task attempted to access an index partition that was
                     * split, joined or moved since the client obtain the
                     * locator for some key range. Clients should obtain a fresh
                     * locator for the key range and redirect the request to the
                     * appropriate index partition.
                     */
 
                    if(INFO) log.info("Stale locator: task=" + r);//, t);
                    
//                    log.info(this.toString(), t);
                    
                } else {

                    /*
                     * The task threw some other kind of exception.
                     */
                    
                    log.warn("Task failed: task=" + r);//, t);

                }

            }
                    
        } finally {
        
            // Remove since thread is no longer running the task.
            final ITask tmp = active.remove(Thread.currentThread());

            if(trackActiveSetInMDC) {

                MDC.put("activeTasks",active.values().toString());
                
            }

            MDC.remove("taskState");

            MDC.remove("commitCounter");
            
            lock.unlock();
            
            assert tmp == r : "Expecting "+r+", but was "+tmp;

            /*
             * Note: This reflects the total task queuing time including the
             * commit (or abort). The task service time without the commit is
             * handled by AbstractTask directly.
             */

            r.taskCounters.queuingNanoTime.addAndGet(System.nanoTime()
                    - r.nanoTime_submitTask);

        }

    }
    
    /**
     * A snapshot of the executor state.
     */
    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append("WriteExecutorService");

        sb.append("{ paused="+paused); // note: the raw counter value.
        
        sb.append(", nrunning="+nrunning);

        sb.append(", concurrentTaskCount="+activeTaskCountWithLocksHeld);

        sb.append(", activeTaskSetSize="+active.size());

        sb.append(", nwrites="+nwrites);
        
        sb.append(", groupCommitFlag="+groupCommit);

        sb.append(", abortFlag="+abort);

        sb.append(", lockHeldByCurrentThread="+lock.isHeldByCurrentThread());
        
        sb.append(", lockHoldCount="+lock.getHoldCount());

        sb.append(", lockQueueLength="+lock.getQueueLength());

        if (lock.isHeldByCurrentThread()) {

            // these all require that we are holding the lock.
            
            sb.append(", lockWaitQueueLength(unpaused)="
                    + lock.getWaitQueueLength(unpaused));

            sb.append(", lockWaitQueueLength(waiting)="
                    + lock.getWaitQueueLength(waiting));

            sb.append(", lockWaitQueueLength(commit)="
                    + lock.getWaitQueueLength(commit));

        }

        /*
         * from super class.
         */
        
        sb.append(", activeCount="+getActiveCount());

        sb.append(", queueSize="+getQueue().size());
        
        sb.append(", poolSize="+getPoolSize());
        
        sb.append(", largestPoolSize="+getLargestPoolSize());
        
        /*
         * various stats.
         */
        
        sb.append(", maxPoolSize="+maxPoolSize);
        
        sb.append(", maxRunning="+maxRunning);
        
        sb.append(", maxCommitLatency="+maxCommitServiceTime);
        
        sb.append(", maxLatencyUntilCommit="+maxCommitWaitingTime);
        
        sb.append(", groupCommitCount="+ngroupCommits);

        sb.append(", abortCount="+naborts);
        
        sb.append(", failedTaskCount="+failedTaskCount);
        
        sb.append(", successTaskCount="+successTaskCount);
        
        sb.append(", committedTaskCount="+committedTaskCount);
        
        sb.append(", overflowCount="+noverflow);
        
        sb.append("}");
        
        return sb.toString();
        
    }
    
    /**
     * Overridden to shutdown the embedded lock manager service.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        
        lockManager.shutdown();
        
        super.shutdown();
        
    }
    
    /**
     * Overridden to shutdown the embedded lock manager service.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public List<Runnable>shutdownNow() {

        lockManager.shutdownNow();
        
        return super.shutdownNow();
        
    }
    
    /**
     * Group commit.
     * <p>
     * This method is called by {@link #afterTask(Callable, Throwable)} for each
     * task that completes successfully. In each commit group, the group commit
     * will be executed in the {@link Thread} of the first task that calls this
     * method. If there are concurrent writers running, then the {@link Thread}
     * executing the {@link #groupCommit()} will wait a bit for them to complete
     * and join the commit group. Otherwise it will immediately start the commit
     * with itself as the sole member of the commit group.
     * <p>
     * After the commit and while this {@link Thread} still holds the lock, it
     * invokes {@link #overflow()} which will decide whether or not to do
     * synchronous overflow processing.
     * <p>
     * If there is a problem during the {@link #commit()} then the write set(s)
     * are abandoned using {@link #abort()}.
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
     *         commit group was aborted.
     */
    private boolean groupCommit(){

        if(DEBUG)
            log.debug("begin");

        assert lock.isHeldByCurrentThread();

        final Thread currentThread = Thread.currentThread();
        
        // the task that invoked this method.
        final ITask r = active.get(currentThread);
        
        /*
         * If an abort is in progress then throw an exception.
         */
        if( abort.get() ) {

            if(INFO) log.info("Abort in progress.");
        
            // signal so that abort() will no longer await this task's completion.
            waiting.signalAll();
            
            throw new RuntimeException("Aborted.");
            
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
             * This thread could not set the flag so some other thread is
             * running the group commit and this thread will just await that
             * commit.
             */

            if(DEBUG)
                log.debug("Already executing in another thread");

            /*
             * Notify the thread running the group commit that this thread will
             * await that commit.
             * 
             * Note: We avoid the possibility of missing the [commit] signal
             * since we currently hold the [lock].
             */

            waiting.signalAll();

            try {

                // await [commit]; releases [lock] while awaiting signal.

                commit.await();

                // did commit, so the commit counter was updated.
                MDC.put("commitCounter","commitCounter="+ngroupCommits);

                return true;

            } catch (InterruptedException ex) {

                // The task was aborted.

                log.warn("Task interrupted awaiting group commit: " + r);

                // Set the interrupt flag again.
                currentThread.interrupt();

                return false;

            }

        }

        // true iff we acquire the exclusive lock for overflow processing.
        boolean locked = false;
        try {

            /*
             * Note: The logic above MUST NOT have released the lock if control
             * was allowed to flow down to this point.
             */
            
            assert lock.isHeldByCurrentThread();
            
            assert groupCommit.get();

            // used to track the commit waiting and commit service times.
            final WriteTaskCounters taskCounters = (WriteTaskCounters) r
                    .getTaskCounters();

            assert taskCounters != null;
            
            // note: the task counters use nanos rather than millis.
            final long nanoTime_beginWait = System.nanoTime();

            if (INFO)
                log.info("This thread will run group commit: "
                        + currentThread + " : " + r);

            /*
             * Note: Synchronous overflow processing has a stronger
             * pre-condition than a normal group commit. In addition to holding
             * the lock, there MUST NOT be any running tasks (their write sets
             * would be lost when we cut over to the new journal). This flag is
             * therefore set [true] if we need to pause the write service. Also,
             * synchronous overflow is NOT performed unless we were actually
             * able to await all running tasks (nrunning == 0).
             * 
             * Note: Overflow processing is simply not permitted if it is not
             * enabled for the resource manager. However, if it is enabled then
             * overflow processing can be forced using [forceOverflow].
             */
            final boolean shouldOverflow = isShouldOverflow();

            if (shouldOverflow && overflowLog.isInfoEnabled()) {

                overflowLog
                        .info("Should overflow - will try to pause the write service.");

            }
            {
                // timestamp from which we measure the latency until the commit
                // begins.
                final long beginWait = System.currentTimeMillis();
                if (shouldOverflow) {
                    /*
                     * Try to acquire the exclusive write lock so that we can do
                     * synchronous overflow processing.
                     */
                    try {
                        if (!(locked = tryLock(overflowLockRequestTimeout,
                                TimeUnit.MILLISECONDS))) {
                            /*
                             * Note: This can cause serious problem if it
                             * persists since the service will be unable to
                             * release old resources on the disk and the live
                             * journal extent will continue to grow without
                             * bound.
                             * 
                             * The lock timeout needs to be of significant
                             * duration or a lock request for a write service
                             * under heavy write load will timeout. If overflow
                             * processing is not performed the live journal
                             * extent will grow without bound and the service
                             * will be unable to release older resources on the
                             * disk.
                             * 
                             * @todo Should probably interrupt the running tasks
                             * in order obtain the lock preemptively if the
                             * request would timeout.
                             */
                            log
                                    .error("Could not obtain exclusive lock: timeout="
                                            + overflowLockRequestTimeout
                                            + ", service=" + serviceName);
                        }
                    } catch (InterruptedException ex) {
                        log.warn("Interrupted awaiting exclusive write lock.");
                        // will not do group commit.
                        return false;
                    }
                } else {
                    /*
                     * Wait for some or all running tasks to join the commit
                     * group for greater efficiency (packs more tasks into a
                     * commit group by trading off some latency against the size
                     * of the commit group).
                     * 
                     * Note: If we do not wait (yielding the lock) at least once
                     * then the task which is performing the group will be the
                     * only task in the commit group!
                     * 
                     * Note: This will return normally unless interrupted.
                     */
                    waitForRunningTasks(groupCommitTimeout,
                            TimeUnit.MILLISECONDS);
                }
                {
                    // update [maxCommitWaitingTime]
                    final long endWait = System.currentTimeMillis();
                    final long latencyUntilCommit = endWait - beginWait;
                    if (latencyUntilCommit > maxCommitWaitingTime) {
                        maxCommitWaitingTime = latencyUntilCommit;
                    }
                }
            }
            
            /*
             * At this point [nwrites] is the size of the commit group and
             * [nrunning] is the #of concurrent tasks which are still executing.
             * Both of these values will be constant while we hold the [lock].
             * While tasks may continue to execute, [nrunning] can not be
             * decremented until a task can acquire the [lock].
             * 
             * [active] is the set of tasks concurrently executing -or-
             * participating in the group commit. [active] is exactly the commit
             * group IFF [nrunning == 0]. When [nrunning > 0] then there are
             * concurrent tasks still executing during the group commit.
             */
            final int nwrites = this.nwrites.get();

            if (INFO)
                log.info("Committing store: commitGroupSize=" + nwrites
                        + ", #running=" + nrunning + ", active="
                        + active.entrySet());

            // timestamp used to measure commit latency.
            final long beginCommit = System.currentTimeMillis();

            /*
             * Note: Only the task that actually runs the commit will note the
             * time waiting for the commit. This is always the first task to
             * join the commit group and waits for other tasks to join the same
             * commit group. The elapsed time from when this task initiates
             * commit processing until we are ready to delegate the commit to
             * the journal is the elapsed time awaiting the group commit.
             */
            taskCounters.commitWaitingNanoTime.addAndGet(System.nanoTime()
                    - nanoTime_beginWait);

            final long nanoTime_beginCommit = System.nanoTime();

            try {
                
                // commit the store (note: does NOT throw exceptions).
                if (!commit(locked)) {

                    // commit failed.
                    return false;

                }
                
            } finally {

                taskCounters.commitServiceNanoTime.addAndGet(System.nanoTime()
                        - nanoTime_beginCommit);
                
            }

            // track #of safely committed tasks.
            committedTaskCount += nwrites;

            // the commit latency.
            final long commitLatency = System.currentTimeMillis() - beginCommit;

            if (commitLatency > maxCommitServiceTime) {

                maxCommitServiceTime = commitLatency;

            }

            this.commitGroupSize = nwrites;
            
            if (nwrites > maxCommitGroupSize) {

                maxCommitGroupSize = nwrites;
                
            }
            
            if (INFO)
                log.info("Commit Ok : commitLatency=" + commitLatency
                        + ", maxCommitLatency=" + maxCommitServiceTime
                        + ", shouldOverflow=" + shouldOverflow);

            if (shouldOverflow && nrunning.get() == 0) {

                if (INFO)
                    log.info("Will do overflow now: nrunning=" + nrunning);

                // this task will do synchronous overflow processing.
                MDC.put("taskState","doSyncOverflow");
                
                overflow();

                MDC.put("taskState","didSyncOverflow");

                if (INFO)
                    log.info("Did overflow.");
                
            }
            
            return true;

        } catch (Throwable t) {

            log.error("Problem with commit? : "+serviceName+" : "+ t, t);

            /*
             * A thrown exception here indicates a failure, but not during the
             * commit() itself. One example is when the group commit is
             * interrupted during shutdown. However, there can doubtless be
             * others.
             * 
             * Since at least one task succeeded (the one executed by the thread
             * that is running the group commit) there are index checkpoints
             * that will get written by the next commit. If we do nothing then
             * those checkpoints will be made restart safe if a subsequent
             * commit succeeds, which would be pretty surprising since the task
             * will have reported a failure! So, yes, we do need to do an
             * abort() here.
             */
            
            abort();
            
            return false;
            
        } finally {

            // atomically clear the [groupCommit] flag.
            groupCommit.set(false);

            /*
             * If we obtained an exclusive lock on the write service then
             * release it now.
             */
            if(locked) {

                unlock();
                
            }

//            lock.unlock();

        }

    }

    /**
     * Wait a moment to let other tasks finish, but if the queue is empty then
     * return immediately in order to keep down latency for a single task that
     * is run all by itself without anything else in the queue.
     * <p>
     * Note: When the timeout is ZERO (0L), this methods DOES NOT yield the
     * {@link #lock}. This means that the task running the group commit will
     * not allow other tasks into the commit group and essentially disables
     * group commit.
     * 
     * @param timeout
     *            The timeout to await currently running tasks to join the
     *            commit group.
     * @param unit
     *            The unit in which that timeout is expressed.
     * 
     * @todo do NOT wait if the current task might exceeds its max latency from
     *       submit (likewise, do not start task if it has already exceeded its
     *       maximum latency from submit).
     * 
     * @todo possibly do not wait if task is part of tx?
     */
    private void waitForRunningTasks(final long timeout, final TimeUnit unit)
            throws InterruptedException {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        long lastTime;
        final long beginWait = lastTime = System.nanoTime();

        // nanoseconds remaining until timeout.
        long nanos = unit.toNanos(timeout);

        int nwaits = 0;
        
        // until timeout, while tasks are running.
        while (nanos > 0 && this.nrunning.get() > 0) {

            /*
             * Wait on condition (yields lock, allowing other tasks to enter the
             * commit group).
             * 
             * Note: throws InterruptedException
             */

            waiting.await(nanos, TimeUnit.NANOSECONDS);

            final long now = System.nanoTime();
            
            nanos -= now - lastTime;
            
            lastTime = now;

            nwaits++;
            
        }

        // Don't wait any longer.

        if (log.isInfoEnabled()) {

            /*
             * #of tasks remaining in the queue (not yet running).
             */
            final int queueSize = getQueue().size();
            
            /*
             * #of tasks that are running which did not join the commit group.
             */
            final int nrunning = this.nrunning.get();
            
            /*
             * Note: Since the caller holds the lock this will be the size of
             * the commit group.
             */
            final int commitGroupSize = this.nwrites.get();
            
            final int corePoolSize = getCorePoolSize();
            
            final int maxPoolSize = getMaximumPoolSize();
            
            final int poolSize = getPoolSize();
            
            final long elapsedWait = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - beginWait);

            log
                    .info("Not waiting any longer"//
                            + ": commitGroupSize=" + commitGroupSize//
                            + ", nwaits="+nwaits //
                            + ", elapsed(ms)=" + elapsedWait
                            + ", queueSize=" + queueSize//
                            + ", nrunning=" + nrunning//
                            + ", corePoolSize=" + corePoolSize
                            + ", poolSize="+ poolSize//
                            + ", maxPoolSize=" + maxPoolSize//
                            );
            
        }
        
    }
        
// /*
// * At this point the group commit is safe. Before we return to the
// * caller (who will release their lock on the write service), we check
// * to see if we need to do synchronous overflow processing.
// */
//        
// if (forceOverflow.get() || resourceManager.shouldOverflow()) {
//
// /*
// * Overflow iff necessary.
// */
//
// log.info("Overflow processing pre-conditions are satisfied");
//
//            assert lock.isHeldByCurrentThread();
//
//            if (nrunning.get() == 0) {
//
//                /*
//                 * Overflow processing requires that no tasks are running. Since
//                 * we are holding the lock, no new tasks can start (at least
//                 * until this thread blocks or otherwise releases the lock).
//                 * Therefore if there are no running tasks then we can
//                 * immediately begin overflow processing.
//                 */ 
//
//                log.info("No running tasks - will do overflow now.");
//                
//                overflow();
//
//                log.info("No running tasks - did overflow.");
//                
//            } else {
//    
//                /*
//                 * Otherwise we need to pause the write service (at which point
//                 * there will be no more running tasks) and then re-invoke
//                 * groupCommit() via tail recursion (at which point there will
//                 * be no uncommitted tasks). This will satisfy the
//                 * pre-conditions for synchronous overflow processing (all
//                 * writers are committed and no writers are running) and we will
//                 * take the other code path above and do overflow processing.
//                 */
//                
//                log.info("Pausing write queue so that we can do overflow processing: nrunning="
//                                + nrunning);
//                
//                assert !isPaused();
//                
//                try {
//
//                    /*
//                     * Pause the write service (no more tasks will start) and
//                     * wait until there are no more tasks running.
//                     * 
//                     * Note: If this succeeds then we need to resume() the write
//                     * service, which we do below. If it fails, then the write
//                     * service is automatically resumed by error handling within
//                     * awaitPaused().
//                     */
//
//                    awaitPaused();
//
//                    log.info("write service is paused: #running="+nrunning);
//                    
//                } catch (InterruptedException ex) {
//
//                    log.warn("Interrupted awaiting paused write service");
//
//                    // set the interrupt flag again.
//                    Thread.currentThread().interrupt();
//
//                    /*
//                     * The group commit was successful, so return true even
//                     * through we were interrupted waiting on the write service
//                     * to be paused.
//                     */
//                    
//                    return true;
//
//                }
//
//                /*
//                 * Recursive invocation of group commit now that the write
//                 * service is paused (nothing is running). When we re-enter
//                 * overflow() the write service will still be paused, any
//                 * writers will have been committed, and we can safely do
//                 * synchronous overflow processing.
//                 */
//
//                assert isPaused();
//                assert nrunning.get() == 0;
//
//                try {
//                    
//                    if (!groupCommit()) {
//                        
//                        return false;
//                        
//                    }
//                    
//                } finally {
//
//                    // resume the write service (process new tasks).
//                    resume();
//
//                }
//
//            }
//
//        }
        
//    /**
//     * Flag may be set to force overflow processing during the next group
//     * commit. The flag is cleared once an overflow has occurred.
//     */
//    public final AtomicBoolean forceOverflow = new AtomicBoolean(false);

    /**
     * Return <code>true</code> if the pre-conditions for overflow processing
     * are met.
     */
    private boolean isShouldOverflow() {

        return resourceManager.isOverflowEnabled()
//        && (forceOverflow.get() || resourceManager.shouldOverflow());
        && resourceManager.shouldOverflow();
        
    }
    
    /**
     * Once an overflow condition has been recognized and NO tasks are
     * {@link #nrunning} then {@link IResourceManager#overflow()} MAY be invoked
     * to handle synchronous overflow processing, including putting a new
     * {@link IJournal} into place and re-defining the views for all named
     * indices to include the pre-overflow view with reads being absorbed by a
     * new btree on the new journal.
     * <p>
     * Note: This method traps all of its exceptions.
     * <p>
     * Pre-conditions: You own the {@link #lock} and {@link #nrunning} is
     * zero(0).
     */
    private void overflow() {

        assert lock.isHeldByCurrentThread();

        assert nrunning.get() == 0;
        
        /*
         * This case gets run when we re-enter overflow() recursively since
         * group commit normally occurs when the write service is NOT
         * paused, which is handled below.
         */
        
        try {

//            /*
//             * @todo should the active set be empty? that is, have all tasks
//             * waiting on commit reached a state where they will neither effect
//             * or be effected by an overflow onto another journal?
//             */
//            log.info("active="+active.entrySet());
            
            /*
             * Note: This returns a Future. We could use that to cancel
             * asynchronous overflow processing if there were a reason to do so.
             */
            resourceManager.overflow();
        
            noverflow++;
            
        } catch (Throwable t) {

            log.error("Overflow error: "+serviceName+" : "+t, t);

//        } finally {
//
//            // clear force flag.
//            forceOverflow.set(false);
            
        }

    }

    /**
     * Acquires an exclusive lock on the write service.
     * <p>
     * The write service is paused for up to <i>timeout</i> units. During that
     * time no new tasks will start. The lock will be granted if all running
     * tasks complete before the <i>timeout</i> expires.
     * <p>
     * Note: The exclusive write lock is granted using the same {@link #lock}
     * that is used to coordinate all other activity of the write service. If
     * the exclusive write lock is granted then the caller's thread will hold
     * the {@link #lock} and MUST release the lock using {@link #unlock()}.
     * <p>
     * Note: When the exclusive lock is granted there will be NO running tasks
     * and the write service will be paused. This ensures that no task can run
     * on the write service and that groupCommit will not attempt to grab the
     * lock itself.
     * <p>
     * Note: If there is heavy write activity on the service then the timeout
     * may well expire before the exclusive write lock becomes available.
     * Further, the acquisition of the exclusive write lock will throttle
     * concurrent write activity and negatively impact write performance if the
     * system is heavily loaded by write tasks. Therefore, the write lock should
     * be requested only when it is necessary and a significant value should be
     * specified for the timeout (60s or more) to ensure that it is acquired.
     * 
     * @param timeout
     *            The timeout.
     * @param unit
     *            The unit in which the <i>timeout</i> is expressed.
     * 
     * @return <code>true</code> iff the exclusive lock was acquired.
     * 
     * @throws InterruptedException
     * 
     * @todo This really should not be public. It was exposed to make it easy to
     *       force overflow of the service. We should be able to achieve the
     *       same ends by setting a flag and submitting a task which writes an
     *       empty record on the raw store just in case there is no task
     *       running.
     */
    public boolean tryLock(final long timeout, final TimeUnit unit)
            throws InterruptedException {

        if (INFO)
            log.info("timeout=" + timeout + ", unit=" + unit);

        long lastTime = System.nanoTime();

        long nanos = unit.toNanos(timeout);

        boolean granted = false;

        lock.lock();

        try {

            if (!exclusiveLock.tryLock(nanos, TimeUnit.NANOSECONDS)) {

                // can't obtain the exclusive lock. @todo log @ WARN
                log.error("Exclusive write lock not granted: timeout="
                        + unit.toMillis(timeout) + "ms");

                return false;

            }

            // subtract out the elapsed time
            final long now = System.nanoTime();
            
            nanos -= now - lastTime;
            
            lastTime = now;
            
            try {

                /*
                 * Do not permit new tasks to start.
                 * 
                 * Note: New tasks can't start while we hold the [lock], but
                 * this also ensures that new tasks will not start if we have to
                 * yield the lock during quiesce() or after we return from this
                 * method (assuming that the write service was quiesced).
                 * 
                 * @todo should be a boolean if only used in tryLock() while
                 * hold the exclusiveLock.
                 */
                pause();

                /*
                 * Wait up to the remaining time for the write service to
                 * quiesce.
                 */
                granted = quiesce(nanos, TimeUnit.NANOSECONDS);

                if (!granted) {
                    // @todo log @ WARN
                    log.error("Exclusive write lock not granted: timeout="
                            + unit.toMillis(timeout) + "ms");
                }

                return granted;

            } finally {

                /*
                 * Note: If the write service quiesed during the specified
                 * timeout then the exclusiveLock is granted and we return
                 * without calling exclusiveLock.unlock().
                 */

                if (!granted) {

                    /*
                     * Since the write service did not quiesce the exclusiveLock
                     * WILL NOT be granted so we resume() the write service and
                     * release the exclusiveLock.
                     */

                    resume();

                    exclusiveLock.unlock();

                }

            }
            
        } finally {

            lock.unlock();

        }

    }
    
    /**
     * Release the exclusive write lock.
     * 
     * @throws IllegalMonitorStateException
     *             if the current thread does not own the lock.
     */
    public void unlock() {

        lock.lock();

        try {

            /*
             * Note: This ensures that the caller holds the [exclusiveLock]
             * (otherwise it will throw an IllegalMonitorStateException) before
             * allowing tasks to resume execution on the write service.
             * 
             * Note: We are already holding the [lock] so nothing can begin to
             * execute until we release the [lock], which makes this operation
             * atomic.
             */
            
            exclusiveLock.unlock();
            
            resume();
            
        } finally {
            
            lock.unlock();
            
        }
        
    }
    
    /**
     * Wait until there are no more tasks running.
     * 
     * @param nanos
     *            The maximum amount of time to wait. Use {@link Long#MAX_VALUE}
     *            to wait forever.
     * @param unit
     *            The unit for <i>timeout</i>.
     * 
     * @return true iff nothing is running.
     * 
     * @throws IllegalStateException
     *             if the write service is not paused.
     * @throws IllegalMonitorStateException
     *             if the caller does not hold the {@link #lock}.
     */
    private boolean quiesce(final long timeout, final TimeUnit unit)
            throws InterruptedException {

        if (!isPaused())
            throw new IllegalStateException();
        
        if (!lock.isHeldByCurrentThread()) {
            
            // the caller does not hold the lock.
            throw new IllegalMonitorStateException();
            
        }

        // remaining nanoseconds.
        long nanos = unit.toNanos(timeout);
        
        long lastTime = System.nanoTime();

        // used to measure total elapsed time.
        final long beginNanos = lastTime;

        // used to measure #of times that we await [waiting].
        int nwaits = 0;
        
        // #of tasks running on entry (used for log message only).
        final int beforeCount = nrunning.get();
        
        // wait for active tasks to complete, but no longer than the timeout.
        while (true) {

            final long now = System.nanoTime();
            
            nanos -= now - lastTime;
            
            lastTime = now;

            if (nanos <= 0) {

                // Timeout.
                break;

            }
            
            if (nrunning.get() == 0) {
                
                // success.
                return true;

            }

            /*
             * Each task that completes signals [waiting].
             * 
             * Note: While we specify a timeout equal to the time remaining to
             * limit the time that we will await other threads, this thread will
             * not resume until it has re-acquired the lock which it releases
             * temporarily while awaiting other threads to signal it via
             * [waiting].
             * 
             * Note: Throws InterruptedException
             */

            waiting.awaitNanos(nanos);

            nwaits++;
            
        }

        /*
         * Timeout.
         * 
         * Log the running tasks in order by their submit times, the elapsed
         * time, and the #of running tasks before/after. This is being done in
         * order to help diagnose situations in which overflow processing is not
         * triggered because we do not gain the lock. [The problem was actually
         * traced to an error in this logic where the code was not waiting long
         * enough.]
         */

        final AbstractTask[] a = active.values().toArray(new AbstractTask[0]);

        final TaskAndTime[] b = new TaskAndTime[a.length];

        long maxElapsedRunning = 0;

        for (int i = 0; i < a.length; i++) {

            final TaskAndTime t = b[i] = new TaskAndTime(a[i], lastTime);

            if (t.state == TaskAndTime.State.Running
                    && t.elapsedRunTime >= maxElapsedRunning) {

                maxElapsedRunning = t.elapsedRunTime;

            }

        }

        // sort by elapsed run time.
        Arrays.sort(b);

        log.error("Timeout! : timeout=" + unit.toMillis(timeout)
                + "ms,elapsed="
                + TimeUnit.NANOSECONDS.toMillis(lastTime - beginNanos)
                + "ms,nwaits=" + nwaits + ",runningBefore=" + beforeCount
                + ",runningNow=" + nrunning.get() + ",maxElapsedRunning="
                + maxElapsedRunning + ",::runningTasks=" + Arrays.toString(b));

        return false;

    }

    /**
     * Encapsulates a running task and its elapsed time since the task was
     * started and extends the toString() representation to give us some more
     * interesting information. The natural sort order is by the total elapsed
     * run time (descending), which is noted when the object is instantiated so
     * that the sort is stable.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static class TaskAndTime implements Comparable<TaskAndTime> {

        private final long now;
        private final AbstractTask task;
        /** The elapsed milliseconds for work performed on this task. */
        private final long elapsedRunTime;
        /** The #of milliseconds ago that work began on this task. */
        private final long startAge;
        
        private static enum State {
            Waiting,
            Running,
            Done;
        }
        private final State state;
        
        public String toString() {
            return "TaskAndTime{" + task.toString() + ",elapsedRunTime="
                    + TimeUnit.NANOSECONDS.toMillis(elapsedRunTime)
                    + ",startAge=" + TimeUnit.NANOSECONDS.toMillis(startAge)
                    + ",state=" + state + "}";
        }
        
        TaskAndTime(final AbstractTask task, final long now) {
            this.task = task;
            this.now = now;
            if (task.nanoTime_finishedWork != 0L) {
                // task is done.
                this.elapsedRunTime = (task.nanoTime_finishedWork - task.nanoTime_beginWork);
                this.state = State.Done;
            } else if (task.nanoTime_beginWork == 0L) {
                // task has not started (should not occur on the write service).
                this.elapsedRunTime = 0L;
                this.state = State.Waiting;
            } else {
                // task is running.
                this.elapsedRunTime = (now - task.nanoTime_beginWork);
                this.state = State.Running; 
            }
            this.startAge = (now - task.nanoTime_beginWork);
            
        }
        
        /**
         * Places into order by decreasing {@link #elapsedRunTime}.
         */
        public int compareTo(final TaskAndTime o) {

            if (elapsedRunTime < o.elapsedRunTime)
                return 1;

            if (elapsedRunTime > o.elapsedRunTime)
                return -1;

            return 0;
            
        }

    }
    
//    /**
//     * Orders the tasks by their submit time, which permits a stable sort and is
//     * correlated with their run time since we are only logging the running
//     * tasks.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//     *         Thompson</a>
//     * @version $Id$
//     */
//    private static class SubmitTimeComparator implements
//            Comparator<AbstractTask> {
//
//        public int compare(AbstractTask o1, AbstractTask o2) {
//
//            if (o1.nanoTime_submitTask < o2.nanoTime_submitTask)
//                return -1;
//
//            if (o1.nanoTime_submitTask > o2.nanoTime_submitTask)
//                return 1;
//
//            return 0;
//            
//        }
//        
//    }
    
    /**
     * Commit the store.
     * <p>
     * Note: This method does NOT throw anything. All exceptions are caught and
     * handled.
     * 
     * @param locked
     *            Indicates whether or not the caller has obtained an exclusive
     *            lock on the write service using
     *            {@link #tryLock(long, TimeUnit)}, e.g., when the caller
     *            intends to perform {@link #overflow()} processing after the
     *            commit.
     * 
     * <h4>Pre-conditions</h4>
     * <ul>
     * <li>The caller already owns the {@link #lock} - this ensures that the
     * pre-conditions are atomic since they are under the caller's control.</li>
     * <li>{@link #nrunning} is ZERO(0).</li>
     * <li>All active tasks have either completed successfully or are blocked
     * on {@link #lock}.</li>
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
     * 
     * @return <code>true</code> iff the commit was successful.
     */
    private boolean commit(final boolean locked) {

        assert lock.isHeldByCurrentThread();

        /*
         * Note: if the journal was closed asynchronously then do not attempt to
         * commit the write set.
         * 
         * Note: the journal MUST be open unless shutdownNow() was used on the
         * journal / data service. shutdownNow() will cause the journal to be
         * immediately closed, even while there are existing tasks running on
         * the various concurrency services, including this write service.
         * 
         * Note: the resource manager and the journal can really be closed at
         * any time, so you can see an exception thrown even though we check
         * that they are open as a pre-condition here.
         * 
         * @todo why not an abort() here? (because the journal is not accessible
         * so there is nothing to rollback).
         */

        if(!resourceManager.isOpen()) {
            
            log.warn("ResourceManager not open?");

            resetState();
            
            return false;
            
        }
        
        // note: throws IllegalStateException if resource manager is not open.
        final AbstractJournal journal = resourceManager.getLiveJournal();

        if(!journal.isOpen()) {

            log.warn("Journal not open?");

            resetState();
            
            return false;
            
        }

        try {

            /*
             * Atomic commit for the store.
             * 
             * Note: Since everything has already been checkpointed, the commit
             * is essentially a checkpoint of the Name2Addr map, writing an
             * updated commit record, and updating the root block. The tasks
             * themselves do NOT play any role in the commit. Things can still
             * go wrong at this level, in which case the commit will fail and we
             * will do an abort which will discard the commit list and our own
             * hard references to the rollback checkpoint records.
             * 
             * Note: [timestamp] will be 0L if there were no writes on the
             * journal to commit.
             */

            if (locked) {
                
                /*
                 * Note: an exclusive lock is obtained before overflow
                 * processing so this is the last commit before we overflow the
                 * journal.
                 */
                if (OVERFLOW_DEBUG)
                    overflowLog.debug("before: " + journal.getRootBlockView());
                
            }

            // #of bytes on the journal as of the previous commit point.
            final long byteCountBefore = journal.getRootBlockView().getNextOffset();
            
            final long timestamp = journal.commit();
            
            // #of bytes on the journal after the commit.
            final long byteCountAfter = journal.getRootBlockView().getNextOffset();
            
            if (timestamp == 0L) {

                if (INFO)
                    log.info("Nothing to commit");

                return true;
                
            }

            // #of bytes written since the last commit.
            this.byteCountPerCommit = (byteCountAfter - byteCountBefore);
            
            /*
             * Set the commitTime on each of the tasks in the commitGroup.
             * 
             * Note: [commitGroup] is cleared by resetState() in finally{}.
             */
            {
                
                assert nwrites.get() == commitGroup.size();
                
                for (AbstractTask task : commitGroup.values()) {
                
                    task.commitTime = timestamp;
                    
                }
                
            }
            
            // #of commits that succeeded.
            ngroupCommits.incrementAndGet();
            
            // did commit, so the commit counter was updated.
            MDC.put("commitCounter","commitCounter="+ngroupCommits);

            // this task did the commit.
            MDC.put("taskState", "didCommit");
            
            if (INFO) {
            
                log.info("commit: #writes=" + nwrites + ", timestamp="
                        + timestamp);
                
            }
            
            if (locked) {
                
                /*
                 * Note: an exclusive lock is obtained before overflow
                 * processing so this is the last commit before we overflow the
                 * journal.
                 */

                if (OVERFLOW_INFO)
                    overflowLog.info("commit: #writes=" + nwrites
                            + ", timestamp=" + timestamp + ", paused!");

                if (OVERFLOW_DEBUG)
                    overflowLog.debug("after : " + journal.getRootBlockView());
                
            }

            return true;

        } catch (Throwable t) {

            /*
             * Something went wrong in the commit itself.
             */

            log.error("Commit failed - will abort: "+serviceName+" : "+ t, t);

            abort();

            return false;

        } finally {

            resetState();

        }

    }

    /**
     * Abort. Interrupt all running tasks, await the termination of those tasks,
     * and then abandon the pending write sets.
     * <p>
     * Note: This discards the set of rollback checkpoint records for the
     * unisolated indices such that tasks which start after this abort will
     * re-load the index from the last commit point, not the last checkpoint.
     * This is accomplished by the {@link AbstractJournal#abort()} protocol. It
     * discards its committers (including {@link Name2Addr}) and the
     * canonicalizing mapping for indices from their checkpoint addresses,
     * forcing the reload on demand of indices from the store.
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

        assert lock.isHeldByCurrentThread();

        // Note: set true iff this thread gets interrupted.
        boolean interrupted = false;
        
        final Thread thread = Thread.currentThread();
        
        try {

            /*
             * Interrupt all active tasks - they will throw an exception that
             * will reach the caller.
             */
            
            if (INFO)
                log.info("Interrupting tasks awaiting commit.");
            
            final Iterator<Map.Entry<Thread, AbstractTask>> itr = active
                    .entrySet().iterator();

            int ninterrupted = 0;
            
            while (itr.hasNext()) {

                final Map.Entry<Thread,AbstractTask> entry = itr.next();
                
                // set flag to deny access to resources.
                
                entry.getValue().aborted = true;
                
                // interrupt the thread running the task (do not interrupt this thread).
                
                if (thread != entry.getKey()) {
                    
                    entry.getKey().interrupt();
                    
                    ninterrupted++;
                    
                }
                
            }

            if (INFO)
                log.info("Interrupted " + ninterrupted + " tasks.");
            
            // wait for active tasks to complete.

            if (INFO)
                log.info("Waiting for running tasks to complete: nrunning="
                        + nrunning);

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

                    interrupted = true;
                    
                }
                
            }
            
            if (INFO)
                log.info("Doing abort: nrunning=" + nrunning);
            
            // Nothing is running.
            assert nrunning.get() == 0;
            
            /*
             * Note: if the journal was closed asynchronously, e.g., by
             * shutdownNow(), then do not attempt to abort the write set since
             * no operations will be permitted on the journal, including an
             * abort().
             */

            final AbstractJournal journal = resourceManager.getLiveJournal();

            if(journal.isOpen()) {

                // Abandon the write sets.
                
                journal.abort();
                
            }

            if (INFO)
                log.info("Did abort");
            
        } catch(Throwable t) {
            
            AbstractJournal journal = resourceManager.getLiveJournal();

            if(journal.isOpen()) {

                log.error("Problem with abort? : "+serviceName+" : "+t, t);
                
            }

        } finally {

            // increment the #of aborts.
            
            naborts++;

            // clear the abort flag.
            
            abort.set(false);
            
            // includes resume()
            
            resetState();
            
        }
        
        if(interrupted) {
            
            // Set the interrupt flag now that we are done with the abort.
            thread.interrupt();
            
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

            commitGroup.clear();
            
//            // clear the set of active tasks.
//            active.clear();

            // signal tasks awaiting [commit].
            commit.signalAll();

//            // resume execution of new tasks.
//            resume();
            
        } catch (Throwable t) {

            log.error("Problem with resetState? : "+serviceName+" : "+t, t);

        }

    }

}
