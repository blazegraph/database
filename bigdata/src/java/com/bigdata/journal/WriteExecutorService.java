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

import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
import org.apache.log4j.MDC;

import com.bigdata.btree.BTree;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.OverflowManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.util.InnerCause;

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
 * This can be acomplished by holding onto the btree reference, or even just the
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
 * <h2>Overflow handling</h2>
 * 
 * <p>
 * The {@link WriteExecutorService} invokes {@link #overflow()} each time it
 * does a group commit. Normally the {@link WriteExecutorService} does not
 * quiese before doing a group commit, and when it is not quiesent the
 * {@link ResourceManager} can NOT {@link #overflow()} the journal since
 * concurrent tasks are still writing on the current journal. Therefore the
 * {@link ResourceManager} monitors the {@link IBufferStrategy#getExtent()} of
 * the live journal. When it decides that the live journal is large enough it
 * {@link WriteExecutorService#pause()}s {@link WriteExecutorService} and waits
 * until {@link #overflow()} is called with a quiesent
 * {@link WriteExecutorService}. This effectively grants the
 * {@link ResourceManager} exclusive access to the journal. It can then run
 * {@link #overflow()} to setup a new journal and tell the
 * {@link WriteExecutorService} to {@link WriteExecutorService#resume()}
 * processing.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class WriteExecutorService extends ThreadPoolExecutor {

    protected static final Logger log = Logger
            .getLogger(WriteExecutorService.class);

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

    private final IResourceManager resourceManager;
    
    public WriteExecutorService(IResourceManager resourceManager,
            int corePoolSize, int maximumPoolSize,
            BlockingQueue<Runnable> queue, ThreadFactory threadFactory) {

        super(corePoolSize, maximumPoolSize, Integer.MAX_VALUE,
                TimeUnit.NANOSECONDS, queue, threadFactory);

        if (resourceManager == null)
            throw new IllegalArgumentException();
        
        this.resourceManager = resourceManager;
        
    }

    /*
     * Support for pausing and resuming execution of new worker tasks.
     */

    /** true iff nothing new should start. */
    private boolean paused;

    /**
     * Lock used for {@link Condition}s and to coordinate index checkpoints and
     * index rollbacks with the {@link AbstractTask}.
     */
    final protected ReentrantLock lock = new ReentrantLock();

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
    
    /**
     * A list of the {@link ITx#UNISOLATED} AbstractTask}s executed by this
     * {@link WriteExecutorService} in the order in which their commits are
     * serialized.
     * <p>
     * {@link AbstractTask}s are already executed in a partial ordered
     * determined by their exclusive resource locks. This partial order is
     * sufficient to guarentee that concurrent tasks do not read or write on the
     * same unisolated index. In addition, the {@link AbstractTask} itself
     * coordinates a post-task checkpoint using the {@link #lock}, which
     * imposes a total serialization order over the {@link AbstractTask}s.
     * <p>
     * Tasks are added to this list after they have successfully checkpointed
     * themselves and are removed during the group commit protocol. If a task
     * fails during its normal execution, then it is never entered into this
     * list. If a task succeeds and writes its checkpoint record(s), then other
     * tasks MAY begin to execute that depend on the same resources and hence on
     * the checkpoint(s) written by that task. If a task fails or fails to write
     * its checkpoint record(s) then its write set is rolled back and it is NOT
     * entered onto this list since its write set will not be visible and can
     * not become a precondition for other tasks.
     * <p>
     * During the group commit, there is a per-task commit protocol that
     * atomically updates the {@link Name2Addr} object on the live journal to
     * reflect (a) named indices registered by the task; (b) named indices
     * dropped by the task; and (c) checkpoint records for named indices written
     * by the task.
     * <p>
     * If this per-task commit protocol fails for a given task, then the
     * transitive closure of downstream tasks in the {@link #serializationOrder}
     * having a dependency (a lock on a resource that was accessed by the
     * pre-condition task) MUST fail since its preconditions have not been made
     * restart safe. Note that this transitive closure expands the set of
     * resources as well as the set of downstream tasks since each resource lock
     * can cause new preconditions to exists. Task whose preconditions have not
     * been made restart safe are made to fail by interrupting the thread in
     * which they are executing.
     * <p>
     * During the group commit, tasks are removed from the
     * {@link #serializationOrder} as their post-commit processing is performed.
     * After a group commit, the serialization order will always be empty. This
     * is because tasks require the {@link #lock} in order to add themselves to
     * the {@link #serializationOrder} but the group commit will hold the lock
     * once the commit group is stabilized thereby preventing any tasks not in
     * the commit group from entering the {@link #serializationOrder}.
     * <p>
     * Note: This list is not thread-safe. In order to read or write on this
     * list you MUST own the {@link #lock}.
     * 
     * @todo does anything strictly prevent a task that is executed after
     *       another task and with overlapping resource locks from becoming the
     *       task to run the group commit? Probably not, in which case modify
     *       the logic that chooses the group commit thread to require that the
     *       thread is the one whose task is first in the serialization order!
     */
    private final List<AbstractTask> serializationOrder = new LinkedList<AbstractTask>();

    /**
     * Appends the task to the {@link #serializationOrder}.
     * 
     * @param task
     */
    void addTaskToSerializationOrder(AbstractTask task) {
        
        if (task == null)
            throw new IllegalArgumentException();

        log.info("#tasks=" + serializationOrder.size() + ", task="
                + task.toString());
       
        // the lock MUST be held by the thread before you can touch this list.
        assert lock.isHeldByCurrentThread();
        
        // add to the end of the list.
        serializationOrder.add(task);
        
    }
    
    /*
     * Counters
     */

    private int maxPoolSize = 0;
    private long maxRunning = 0;
    private long maxLatencyUntilCommit = 0;
    private long maxCommitLatency = 0;
    private long ngroupCommits = 0;
    private long naborts = 0;
    private long failedTaskCount = 0;
    private long successTaskCount = 0;
    private long committedTaskCount = 0;
    private long noverflow = 0;

    protected AtomicInteger concurrentTaskCount = new AtomicInteger(0);

    /**
     * The maximum #of threads in the pool.
     */
    public int getMaxPoolSize() {
        
        return maxPoolSize;
        
    }
    
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
     * The #of group commits since the {@link WriteExecutorService} was started
     * (all commits by this service are group commits).
     */
    public long getGroupCommitCount() {
        
        return ngroupCommits;
        
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
    public int getConcurrentTaskCount() {

        return concurrentTaskCount.get();
        
    }
    
    /**
     * <code>true</code> iff the pause flag is set such that the write service
     * will queue up new tasks without allowing them to execute.
     * 
     * @see #pause()
     * @see #resume()
     */
    public boolean isPaused() {
        
        return paused;
        
    }
    
    /**
     * Sets the flag indicating that new worker tasks must pause in
     * {@link #beforeExecute(Thread, Runnable)}.
     * <p>
     * Note: This is not a very safe thing to do and therefore the operation is
     * restricted to its use by this class. However, {@link #resume()} is
     * exposed so that the {@link OverflowManager} can direct the
     * {@link WriteExecutorService} to resume processing as soon as synchronous
     * overflow is complete (concurrent to the startup of asynchronous
     * processing).
     */
    protected void pause() {

        log.debug("Pausing write service");
        
        lock.lock();
        
        try {

            paused = true;
            
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
        
            paused = false;
            
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

            while (paused)
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

            // Update max# of threads in the thread pool.
            final int poolSize = getPoolSize();
            
            maxPoolSize = (poolSize>maxPoolSize?poolSize:maxPoolSize);
            
            // Note the thread running the task.
            active.put(t,r);
        
            if (trackActiveSetInMDC) {

                MDC.put("activeTasks", active.values().toString());
                
            }

            MDC.put("taskState", "running");

            MDC.put("commitCounter",Long.toString(ngroupCommits));

            if(INFO)
                log.info("nrunning="+nrunning);
            
        } finally {
            
            lock.unlock();
            
        }
    
    }
    
    /**
     * When <code>true</code>, writes the set of {@link #active} tasks into
     * the {@link MDC} under the <code>activeTasks</code> key. This is of
     * interest if you want to know which tasks are in the same commit group.
     * 
     * @todo the task should be decorated with information reflecting its state
     *       in the {@link WriteExecutorService}, e.g., whether it is running,
     *       waiting on a commit, etc.
     */
    final boolean trackActiveSetInMDC = true;
    
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
    protected void afterTask(ITask r, Throwable t) {
        
        if (r == null)
            throw new NullPointerException();
        
        lock.lock();
        
        try {
            
            /*
             * Whatever else we do, decrement the #of running writer tasks now.
             */
            
            final int nrunning = this.nrunning.decrementAndGet(); // dec. counter.

            MDC.remove("taskState");

            log.info("nrunning="+nrunning);
            
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
                    
                    log.info("Validation failed: task=" + r);//, t);

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
 
                    log.info("No such index: task=" + r);//, t);
                    

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
 
                    log.info("Stale locator: task=" + r);//, t);
                    
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
            
        }

    }
    
    /**
     * A snapshot of the executor state.
     */
    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("WriteExecutorService");

        sb.append("{ paused="+paused);
        
        sb.append(", nrunning="+nrunning);

        sb.append(", concurrentTaskCount="+concurrentTaskCount);

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
        
        sb.append(", maxCommitLatency="+maxCommitLatency);
        
        sb.append(", maxLatencyUntilCommit="+maxLatencyUntilCommit);
        
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
     * Group commit.
     * <p>
     * This method is called by {@link #afterTask(Callable, Throwable)} for each
     * task that completes successfully. In each commit group, the group commit
     * will be executed in the {@link Thread} of the first task that calls this
     * method. If there are concurrent writers running, then the {@link Thread}
     * executing the {@link #groupCommit()} will wait a bit for them to complete
     * and join the commit group. Otherwise it will immediately start the commit
     * with itself as the role member of the commit group.
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

        log.debug("begin");

        assert lock.isHeldByCurrentThread();

        // the task that invoked this method.
        final ITask r = active.get(Thread.currentThread());
        
        /*
         * If an abort is in progress then throw an exception.
         */
        if( abort.get() ) {

            log.info("Abort in progress.");
        
            // signal so that abort() will no longer await this task's completion.
            waiting.signal();
            
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

            log.debug("Already executing in another thread");

            /*
             * Notify the thread running the group commit that this thread will
             * await that commit.
             * 
             * Note: We avoid the possibility of missing the [commit] signal
             * since we currently hold the [lock].
             */

            waiting.signal();

            try {

                // await [commit]; releases [lock] while awaiting signal.

                commit.await();

                // did commit, so the commit counter was updated.
                MDC.put("commitCounter",Long.toString(ngroupCommits));

                return true;

            } catch (InterruptedException ex) {

                // The task was aborted.

                log.warn("Task interrupted awaiting group commit: " + r);

                // Set the interrupt flag again.
                Thread.currentThread().interrupt();

                return false;

            }

        }

        try {

            /*
             * Note: The logic above MUST NOT have released the lock if control
             * was allowed to flow down to this point.
             */
            
            assert lock.isHeldByCurrentThread();
            
            assert groupCommit.get();

            // save a reference to the thread that is running the group commit.
            groupCommitThread = Thread.currentThread();
            
            log.info("This thread will run group commit: "+Thread.currentThread()+" : "+r);

            /*
             * Note: Synchronous overflow processing has a stronger
             * pre-condition than a normal group commit. In addition to holding
             * the lock, there MUST NOT be any running tasks (their write sets
             * would be lost when we cut over to the new journal). This flag is
             * therefore set [true] if we need to pause the write service. Also,
             * synchronous overflow is NOT performed unless we were actually
             * able to await all running tasks (nrunning == 0).
             */
            final boolean shouldOverflow = (forceOverflow.get() || resourceManager.shouldOverflow());

            if (shouldOverflow)
                log.warn("Should overflow - will try to pause the write service.");

            /*
             * Wait for some or all running tasks to join the commit group.
             */
            waitForRunningTasks(shouldOverflow /* pauseWriteService */);

            // Note: this clears the interrupt flag!
            if(Thread.currentThread().isInterrupted()) {
                
                log.warn("Interrupted awaiting other tasks to join the group commit.");
                
                // will not do group commit.
                return false;
                
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
                log.info("Committing store: commit group size=" + nwrites
                        + ", #running=" + nrunning + ", active="
                        + active.entrySet());

            // timestamp used to measure commit latency.
            final long beginCommit = System.currentTimeMillis();

            // commit the store (note: does NOT throw exceptions).
            if (!commit()) {

                // commit failed.
                return false;

            }

            // track #of safely committed tasks.
            committedTaskCount += nwrites;

            // the commit latency.
            final long commitLatency = System.currentTimeMillis() - beginCommit;

            if (commitLatency > maxCommitLatency) {

                maxCommitLatency = commitLatency;

            }

            if (INFO)
                log.info("Commit Ok : commitLatency=" + commitLatency
                        + ", maxCommitLatency=" + maxCommitLatency
                        + ", shouldOverflow=" + shouldOverflow);

            if (shouldOverflow && nrunning.get() == 0) {

                log.info("Will do overflow now: nrunning="+nrunning);

                // this task will do synchronous overflow processing.
                MDC.put("taskState","doSyncOverflow");
                
                overflow();

                MDC.put("taskState","didSyncOverflow");

                log.info("Did overflow.");
                
            }
            
            return true;

        } catch (Throwable t) {

            log.error("Problem with commit? : " + t, t);

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
             * commit succeeds, which would be pretty suprising since the task
             * will have reported a failure! So, yes, we do need to do an
             * abort() here.
             */
            
            abort();
            
            return false;
            
        } finally {

            // atomically clear the [groupCommit] flag.
            groupCommit.set(false);

            // group commit is not being run.
            groupCommitThread = null;
            
            /*
             * Allow new tasks to run.
             */
            if(isPaused()) {

                resume();
                
            }

//            lock.unlock();

        }

    }

    /**
     * Wait a moment to let other tasks start, but if the queue is empty then we
     * make that a very small moment to keep down latency for a single task that
     * is run all by itself without anything else in the queue.
     * <p>
     * Note: This updates {@link #maxLatencyUntilCommit} as a side-effect.
     * 
     * @param pauseWriteService
     *            When <code>true</code>, an attempt will be made to pause
     *            the write service such that there are no running tasks.
     *            However, this is NOT a guarentee and the caller MUST test
     *            {@link #nrunning} in order to determine whether or not any
     *            tasks are still running.
     * 
     * @todo do NOT wait if the current task might exceeds its max latency from
     *       submit (likewise, do not start task if it has already execeeded its
     *       maximum latency from submit).
     */
    private void waitForRunningTasks(boolean pauseWriteService)
            throws InterruptedException {

        assert lock.isHeldByCurrentThread();
        
        assert !isPaused();

        // timestamp from which we measure the latency until the commit begins.
        final long beginWait = System.currentTimeMillis();

        if (pauseWriteService) {

            if (INFO)
                log.info("Should overflow - will pause the write service.");

            /*
             * Pause the write service (no more tasks will start) and wait
             * until there are no more tasks running.
             * 
             * Note: If this succeeds then the caller MUST resume() the
             * write service. If it fails, then the write service is
             * automatically resumed by error handling within awaitPaused().
             */

            final long timeout = 2000; // ms

            if (awaitPaused(timeout)) {

                log.info("write service is paused: #running=" + nrunning);

            }

        } else {

            int nwaits = 0;
            while (true) {

                final int queueSize = getQueue().size();
                final int nrunning = this.nrunning.get();
                final int nwrites = this.nwrites.get();
                final int corePoolSize = getCorePoolSize();
                final int maxPoolSize = getMaximumPoolSize();
                final int poolSize = getPoolSize();
                final long elapsedWait = System.currentTimeMillis() - beginWait;

                if ((elapsedWait > 100 && queueSize == 0) || elapsedWait > 250) {

                    // Don't wait any longer.

                    if (INFO)
                        log.info("Not waiting any longer: nwaits=" + nwaits
                                + ", elapsed=" + elapsedWait + "ms, queueSize="
                                + queueSize + ", nrunning=" + nrunning
                                + ", nwrites=" + nwrites + ", corePoolSize="
                                + corePoolSize + ", poolSize=" + poolSize
                                + ", maxPoolSize=" + maxPoolSize);

                    break;

                }

                /*
                 * Note: if interrupted during sleep then the group commit will
                 * abort.
                 */

                waiting.await(10, TimeUnit.MICROSECONDS);

                nwaits++;

            }

        }

        final long endWait = System.currentTimeMillis();

        final long latencyUntilCommit = endWait - beginWait;

        if (latencyUntilCommit > maxLatencyUntilCommit) {

            maxLatencyUntilCommit = latencyUntilCommit;

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
        
    /**
     * Flag may be set to force overflow processing during the next group
     * commit. The flag is cleared once an overflow has occurred.
     */
    public final AtomicBoolean forceOverflow = new AtomicBoolean(false);

    /**
     * Once an overflow condition has been recognized and NO tasks are
     * {@link #nrunning running} then {@link IResourceManager#overflow()} MAY be
     * invoked to handle synchronous overflow processing, including putting a
     * new {@link IJournal} into place and re-defining the views for all named
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

            log.info("Doing overflow");
        
            /*
             * @todo should the active set be empty? that is, have all tasks
             * waiting on commit reached a state where they will neither effect
             * or be effected by an overflow onto another journal?
             */
            log.warn("active="+active.entrySet());
            
            resourceManager.overflow();
        
            noverflow++;

            log.info("Did overflow");

        } catch (Throwable t) {

            log.error("Overflow error", t);

        } finally {

            // clear force flag.
            forceOverflow.set(false);
            
        }

    }
    
    /**
     * Pause the {@link WriteExecutorService} and wait until there are no more
     * tasks running (eg, an exclusive lock on the write service as a whole).
     * <p>
     * On a successful return (returns <code>true</code>) the write service
     * will be paused and no tasks will be running. After a successful return
     * the caller MUST ensure that the processing is {@link #resume() resumed}
     * before relinquishing control.
     * <p>
     * On an unsuccessful return (returns <code>false</code>) the write
     * service will NOT be paused and there will be at least one task still
     * running.
     * 
     * @param timeout
     *            The maximum amount of time to wait. Use {@link Long#MAX_VALUE}
     *            to wait forever.
     * 
     * @return true iff nothing is running.
     */
    private boolean awaitPaused(final long timeout) throws InterruptedException {

        assert timeout >= 0L;
        
        assert lock.isHeldByCurrentThread();

        assert ! isPaused(); 
        
        // notify the write service that new tasks MAY NOT run.
        pause();

        final long begin = System.currentTimeMillis();
        
        // wait for active tasks to complete.
        int n;
        while ((n = nrunning.get()) > 0) {

            final long elapsed = begin - System.currentTimeMillis();
            
            final long remaining = timeout - elapsed;
            
            if (remaining <= 0L) {

                log.warn("timeout: elapsed=" + elapsed + ", nrunning=" + n);
                
                resume();
                
                return false;
                
            }
            
            if (INFO)
                log.info("There are " + n + " tasks running after " + elapsed
                        + "ms.");
            
            try {

                /*
                 * Each task that completes signals [waiting].
                 * 
                 * Note: While we specify a timeout equal to the time remaining
                 * to limit the time that we will await other threads, this
                 * thread will not resume until it has re-acquired the lock
                 * which it releases temporarily while awaiting other threads to
                 * signal it via [waiting].
                 */

                waiting.await(remaining, TimeUnit.MILLISECONDS);

            } catch (InterruptedException ex) {

                log.warn("Interrupted awaiting pause.");

                // resume processing.
                resume();

                throw ex;
                
            }

        }
        
        log.info("Write service is paused: #running="+nrunning);
        
        return true;

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
    private boolean commit() {

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
         * @todo why not an abort() here?
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

            final long timestamp = journal.commit();

            // #of commits that succeeded.
            ngroupCommits++;
            
            // did commit, so the commit counter was updated.
            MDC.put("commitCounter",Long.toString(ngroupCommits));

            // this task did the commit.
            MDC.put("taskState", "didCommit");
            
            if (INFO)
                log.info("commit: #writes=" + nwrites + ", timestamp="
                        + timestamp);
            
            return true;

        } catch (Throwable t) {

            /*
             * Something went wrong in the commit itself.
             */

            log.error("Commit failed - will abort: " + t, t);

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
        
        try {

            /*
             * Interrupt all active tasks - they will throw an exception that
             * will reach the caller.
             */
            
            log.info("Interrupting tasks awaiting commit.");
            
            final Iterator<Map.Entry<Thread, AbstractTask>> itr = active
                    .entrySet().iterator();

            int ninterrupted = 0;
            
            while (itr.hasNext()) {

                final Map.Entry<Thread,AbstractTask> entry = itr.next();
                
                // set flag to deny access to resources.
                
                entry.getValue().aborted = true;
                
                // interrupt the thread running the task (do not interrupt this thread).
                
                if (Thread.currentThread() != entry.getKey()) {
                    
                    entry.getKey().interrupt();
                    
                    ninterrupted++;
                    
                }
                
            }

            log.info("Interrupted "+ninterrupted+" tasks.");
            
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

                    interrupted = true;
                    
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

            final AbstractJournal journal = resourceManager.getLiveJournal();

            if(journal.isOpen()) {

                // Abandon the write sets.
                
                journal.abort();
                
            }

            log.info("Did abort");
            
        } catch(Throwable t) {
            
            AbstractJournal journal = resourceManager.getLiveJournal();

            if(journal.isOpen()) {

                log.error("Problem with abort?", t);
                
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
            Thread.currentThread().interrupt();
            
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

//            // clear the set of active tasks.
//            active.clear();

            // signal tasks awaiting [commit].
            commit.signalAll();

            // resume execution of new tasks.
            resume();
            
        } catch (Throwable t) {

            log.error("Problem witn resetState?", t);

        }

    }

}
