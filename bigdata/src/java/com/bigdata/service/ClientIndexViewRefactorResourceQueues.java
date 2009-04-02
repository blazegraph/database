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
 * Created on Apr 22, 2007
 */

package com.bigdata.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.proc.AbstractIndexProcedureConstructor;
import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.StaleLocatorException;

/**
 * <p>
 * A client-side view of a scale-out index as of some <i>timestamp</i>.
 * </p>
 * <p>
 * This variant is a refactor which automatically aggregates writes for an index
 * partition using resource queues. This gives better throughput when writes are
 * scattered across more than a handfull of index partitions. Throughput is
 * improved for several reasons: (a) fewer threads are required since the client
 * will never issue more than one concurrent write request per index partition;
 * (b) writes are automatically aggregated, resulting in larger writes as the
 * system becomes more heavily loaded; (c) the clients effectively self-throttle
 * the write requests so the data services never have more than one concurrent
 * write request per client per index partition on the data service; and (d) if
 * an index partition is split / moved / joined, then the client must re-issue
 * at most ONE write request since it only submits a single request per index
 * partition at a time.
 * </p>
 * <p>
 * This view automatically handles the split, join, or move of index partitions
 * within the federation. The {@link IDataService} throws back a (sometimes
 * wrapped) {@link StaleLocatorException} when it does not have a registered
 * index as of some timestamp. If this exception is observed when the client
 * makes a request using a cached {@link PartitionLocator} record then the
 * locator record is stale. The client automatically fetches the locator
 * record(s) covering the same key range as the stale locator record and the
 * re-issues the request against the index partitions identified in those
 * locator record(s). This behavior correctly handles index partition split,
 * merge, and move scenarios. The implementation of this policy is limited to
 * exactly three places in the code: {@link AbstractDataServiceProcedureTask},
 * {@link PartitionedTupleIterator}, and {@link DataServiceTupleIterator}.
 * </p>
 * <p>
 * Note that only {@link ITx#UNISOLATED} and {@link ITx#READ_COMMITTED}
 * operations are subject to stale locators since they are not based on a
 * historical committed state of the database. Historical read and
 * fully-isolated operations both read from historical committed states and the
 * locators are never updated for historical states (only the current state of
 * an index partition is split, joined, or moved - the historical states always
 * remain behind).
 * </p>
 * 
 * @todo add resource queues.
 *       <p>
 *       There is going to be an interaction with how stale locators are
 *       handled.
 *       <p>
 *       Read-only operations could be sent with greater concurrency, but if
 *       they are read-only operations for the unisolated index then we could
 *       have stale locators so maybe greater concurrency should be limited to
 *       read-only index views.
 *       <p>
 *       Some tasks are not parallelizable. For those cases the caller needs to
 *       wait until the previous subtask is complete before a new task can be
 *       submitted.
 *       <p>
 *       If the {@link AbstractDataServiceProcedureTask} is in the base class
 *       then I can just override {@link #runTasks(boolean, ArrayList)} and be
 *       done with it. However, the concrete implementations of the tasks will
 *       need to differ in how they handle resubmits of the tasks.
 *       <p>
 *       In fact, I can leave in {@link #getRecursionDepth()} but simply ignore
 *       it in {@link #runTasks(boolean, ArrayList)}.
 *       <p>
 *       The right way to abstract this is by defining an {@link Executor}, but
 *       {@link #runTasks(boolean, ArrayList)} is basically just that. What it
 *       needs to do for this varient is place the tasks onto the appropriate
 *       resource queues, which needs to be an atomic operation. Inside of that
 *       operation, a task is submitted immediately if it is at the head of the
 *       queue (in which case the queue was empty and will now contain only the
 *       one task which is being executed). Otherwise the task will wait in the
 *       queue until the task which is currently at the head of the queue
 *       returns. That suggests that we need to examine the queues atomically
 *       when each task completes. We remove the task at the head of the queue
 *       (the one use {@link Future} is now complete) and examine the queue. if
 *       it is empty, then we go onto the next future. Otherwise we aggregate
 *       the tasks in the queue into a single task and send it along to the data
 *       service.
 *       <p>
 *       So, separate out the aggregation logic from the logic to submit the
 *       tasks so I can try to "intrinsically" aggregate tasks where the same
 *       operation was requested? No, that seems way too difficult. Just place
 *       them into a "combined" task and send that along. But how do the futures
 *       get back for the individual tasks? Ungh. And what about having
 *       read-only operations mixed in there? And a read-only index view should
 *       not bother with this logic.
 *       <p>
 *       Why not use an unlimited thread pool on the client and scatter tasks on
 *       split? This is a pretty rare event.
 *       <p>
 *       If all of this is limited to {@link #runTasks(boolean, ArrayList)} then
 *       that can be an interface and I can just provide different
 *       implementations of that interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClientIndexViewRefactorResourceQueues extends AbstractScaleOutClientIndexView2 {

    /**
     * A collection of resource queues for writes on the active index
     * partitions. There will be one resource queue per index partition for
     * which there is a write operation.
     * <p>
     * Unlike the use of the lock service for the {@link ConcurrencyManager},
     * we aggregate the writes before tasking them. That happens when they are
     * submitted for execution.
     * <p>
     * Note: This is a concurrent collection since new resources may be added
     * while concurrent operations resolve resources to their queues. Stale
     * {@link Queue}s are purged after they become only weakly reachable.
     * <p>
     * Note: The timeout is set to one LBS reporting period so you can see which
     * resource queues had activity in the last reporting period.
     */
    final private ConcurrentWeakValueCacheWithTimeout<Integer, Queue<AbstractDataServiceProcedureTask>> resourceQueues;

    /**
     * Return the task queue {@link Queue} for the index partition, creating one
     * atomically if there is none for that index partition.
     * 
     * @param partitionId
     *            The index partition identifier.
     * 
     * @return The task {@link Queue}.
     */
    private Queue<AbstractDataServiceProcedureTask> declareResource(
            final Integer partitionId) {

        if (partitionId == null)
            throw new IllegalArgumentException();
        
        // test 1st to avoid creating a new ResourceQueue if it already exists.
        Queue<AbstractDataServiceProcedureTask> resourceQueue = resourceQueues
                .get(partitionId);

        if (resourceQueue != null) {

            // already exists.
            return resourceQueue;

        }

        // not found, so create a new ResourceQueue for that resource.
        resourceQueue = new LinkedBlockingQueue<AbstractDataServiceProcedureTask>(/* unboundedCapacity */);

        // put if absent.
        final Queue<AbstractDataServiceProcedureTask> oldval = resourceQueues
                .putIfAbsent(partitionId, resourceQueue);

        if (oldval != null) {

            // concurrent insert, so use the winner's resource queue.
            return oldval;

        }

        // we were the winner, so return the our new resource queue.
        return resourceQueue;

    }

    /**
     * Create a view on a scale-out index.
     * 
     * @param fed
     *            The federation containing the index.
     * @param name
     *            The index name.
     * @param timestamp
     *            A transaction identifier, {@link ITx#UNISOLATED} for the
     *            unisolated index view, {@link ITx#READ_COMMITTED}, or
     *            <code>timestamp</code> for a historical view no later than
     *            the specified timestamp.
     * @param metadataIndex
     *            The {@link IMetadataIndex} for the named scale-out index as of
     *            that timestamp. Note that the {@link IndexMetadata} on this
     *            object contains the template {@link IndexMetadata} for the
     *            scale-out index partitions.
     */
    public ClientIndexViewRefactorResourceQueues(final AbstractScaleOutFederation fed,
            final String name, final long timestamp,
            final IMetadataIndex metadataIndex) {

        super(fed,name,timestamp,metadataIndex);

        resourceQueues = new ConcurrentWeakValueCacheWithTimeout<Integer, Queue<AbstractDataServiceProcedureTask>>(
                1000/* nresources */, TimeUnit.SECONDS.toNanos(60));

    }

    /**
     * Runs a set of tasks.
     * 
     * @param parallel
     *            <code>true</code> iff the tasks MAY be run in parallel.
     * @param tasks
     *            The tasks to be executed.
     */
    protected void runTasks(final boolean parallel,
            final ArrayList<AbstractDataServiceProcedureTask> tasks) {

        if (tasks.isEmpty()) {

            log.warn("No tasks to run?", new RuntimeException(
                    "No tasks to run?"));

            return;

        }

// if (getRecursionDepth().get() > 0) {
//
//            /*
//             * Force sequential execution of the tasks in the caller's thread.
//             */
//
//            runInCallersThread(tasks);
//
//        } else
        if (tasks.size() == 1) {

            runOne(tasks.get(0));

        } else if (parallel) {

            /*
             * Map procedure across the index partitions in parallel.
             */

            runParallel(tasks);

        } else {

            /*
             * Sequential execution against of each split in turn.
             */

            runSequence(tasks);

        }

    }

    private final ReentrantLock lock = new ReentrantLock(/* fair */);
    
    /**
     * Maps a set of {@link AbstractDataServiceProcedureTask} tasks across the
     * index partitions in strict sequence. The tasks are run on the
     * {@link #getThreadPool()} so that sequential tasks never increase the
     * total burden placed by the client above the size of that thread pool.
     * 
     * @param tasks
     *            The tasks.
     */
    protected void runOne(final AbstractDataServiceProcedureTask task) {

        if (INFO)
            log.info("Running one task (#active="
                    + getThreadPool().getActiveCount() + ", queueSize="
                    + getThreadPool().getQueue().size() + ") : "
                    + task.toString());

        try {

            /*
             * @todo with this approach we wind up not having a Future which is
             * a non-starter.
             * 
             * @todo the lock service handles half of the problem (it gives us a
             * Future and it will delay execution of the task until the task
             * reaches the head of its resource queue).
             * 
             * @todo the lock service handles something which is NOT a problem
             * (it waits for a task to hold all of its locks when these tasks
             * are independent; each task sits in a single resource queue and
             * can execute as soon as it reaches the head of that queue).
             * 
             * @todo the lock services DOES NOT handle aggregation of tasks (the
             * rolling up of other tasks into the queue into a single task
             * either by wrapping them in a meta-task or by literally re-writing
             * the task as one with the same task ctor but having more data).
             * 
             * @todo So, clone the lock service and produce a purpose specific
             * class that we will use here.
             * 
             * @todo The point (byte[] key) and key-range (byte[] fromKey,
             * byte[] toKey) tasks can be "aggregated" by combining them within
             * in meta-task which is executed against the remote data service,
             * or they could just be submitted individually or using invokeAll()
             * (which might not be as good since the futures would otherwise
             * arrive incrementally). There is some opportunity for eliminating
             * identical requests, but how common is that?
             * 
             * @todo The AbstractIndexProcedureConstructor is only used for the
             * (byte[][] key, byte[][] val) variants. Those are precisely the
             * ones that we want to aggregate. See the
             * KeyArrayDataServiceProcedureTask. The ctors would have to be
             * "equals" to be combined, not just instances of the same ctor
             * class.
             * 
             * @todo The KeyArray procedures are the one that can be aggregated.
             * That can be accomplished by a merge sort of the data for the
             * individual procedures.
             * 
             * @todo The tasks in the queue would still need to have their
             * futures complete and the data would have to be reverse mapped
             * onto the original tasks which raises some hairy questions about
             * the IResultHandler. Arrgh! I can't see any way to handle the
             * reverse credit assignment for tasks such as SPOWriteProc which
             * return the #of tuples which were modified.
             * 
             * @todo If I can't rewrite the tasks using a merge sort then the
             * only benefits of this approach are going to be: (1) the use of
             * fewer threads on the client (no more than one per index partition
             * for writes); (2) clients will self-throttle; (3) fewer tasks will
             * be invalidated when a stale locator exception is thrown.
             * 
             * @todo I was really looking for the impact on the request size
             * (more data in the request) and the consequent improvement in the
             * caching when the request executes.
             * 
             * Maybe there is another way to refactor the data write paradigm
             * for better throughput. Perhaps in terms of the "relation" on
             * which the write is takening place.
             * 
             * This would be easier to think about as pure data. It is the
             * requirement to know how many tuples were modified that is killing
             * me. I believe that this requirement was driven by the fix point
             * closure logic, so maybe there is a way to handle that
             * differently. At that point, a write is a write and could be
             * aggregated.
             * 
             * @see AbstractSolutionBuffer, which notes a 20:1 performance win
             *      for combining chunks together for scale-out JOINs.
             * 
             * @see IMutableRelation; if it returned a boolean then the fixed
             *      point logic would still be Ok.
             * 
             * So this seems workable for both raw writes and custom writes
             * which are only returning a mutation count, even if it does have a
             * lot of consequences for the API in various places.
             * 
             * @todo If I can work this out, then I could also apply the
             * aggregation to requests from different clients on the service,
             * which might be a good time to refactor the AbstractTask and the
             * group commit logic and get distributed commits working.
             */
            
//            final Queue<AbstractDataServiceProcedureTask> queue = declareResource(task.split.pmd
//                    .getPartitionId());
//
//            lock.lock();
//            try {
//            
//                final boolean runNow = queue.isEmpty();
//                
//                queue.add(task);
//                
//            } finally {
//                
//                lock.unlock();
//                
//            }
        
            final Future<Void> f = getThreadPool().submit(task);

            // await completion of the task.
            f.get(taskTimeout, TimeUnit.MILLISECONDS);

        } catch (Exception e) {

            if (INFO)
                log.info("Execution failed: task=" + task, e);

            throw new ClientException("Execution failed: " + task,e);

        }

    }
    
    /**
     * Maps a set of {@link DataServiceProcedureTask} tasks across the index
     * partitions in parallel.
     * 
     * @param tasks
     *            The tasks.
     */
    protected void runParallel(
            final ArrayList<AbstractDataServiceProcedureTask> tasks) {

        final long begin = System.currentTimeMillis();
        
        if(INFO)
        log.info("Running " + tasks.size() + " tasks in parallel (#active="
                + getThreadPool().getActiveCount() + ", queueSize="
                + getThreadPool().getQueue().size() + ") : "
                + tasks.get(0).toString());
        
        int nfailed = 0;
        
        final LinkedList<Throwable> causes = new LinkedList<Throwable>();
        
        try {

            final List<Future<Void>> futures = getThreadPool().invokeAll(tasks,
                    taskTimeout, TimeUnit.MILLISECONDS);
            
            final Iterator<Future<Void>> itr = futures.iterator();
           
            int i = 0;
            
            while(itr.hasNext()) {
                
                final Future<Void> f = itr.next();
                
                try {
                    
                    f.get();
                    
                } catch (ExecutionException e) {
                    
                    final AbstractDataServiceProcedureTask task = tasks.get(i);

                    // log w/ stack trace so that we can see where this came
                    // from.
                    log.error("Execution failed: task=" + task, e);

                    if (task.causes != null) {

                        causes.addAll(task.causes);

                    } else {

                        causes.add(e);

                    }

                    nfailed++;
                    
                }
                
            }
            
        } catch (InterruptedException e) {

            throw new RuntimeException("Interrupted: "+e);

        }
        
        if (nfailed > 0) {
            
            throw new ClientException("Execution failed: ntasks="
                    + tasks.size() + ", nfailed=" + nfailed, causes);
            
        }

        if (INFO)
            log.info("Ran " + tasks.size() + " tasks in parallel: elapsed="
                + (System.currentTimeMillis() - begin));

    }

    /**
     * Maps a set of {@link DataServiceProcedureTask} tasks across the index
     * partitions in strict sequence. The tasks are run on the
     * {@link #getThreadPool()} so that sequential tasks never increase the
     * total burden placed by the client above the size of that thread pool.
     * 
     * @param tasks
     *            The tasks.
     */
    protected void runSequence(final ArrayList<AbstractDataServiceProcedureTask> tasks) {

        if (INFO)
            log.info("Running " + tasks.size() + " tasks in sequence (#active="
                    + getThreadPool().getActiveCount() + ", queueSize="
                    + getThreadPool().getQueue().size() + ") : "
                    + tasks.get(0).toString());

        final Iterator<AbstractDataServiceProcedureTask> itr = tasks.iterator();

        while (itr.hasNext()) {

            final AbstractDataServiceProcedureTask task = itr.next();

            try {

                final Future<Void> f = getThreadPool().submit(task);
                
                // await completion of the task.
                f.get(taskTimeout, TimeUnit.MILLISECONDS);

            } catch (Exception e) {
        
                if(INFO) log.info("Execution failed: task=" + task, e);

                throw new ClientException("Execution failed: " + task, e, task.causes);

            }

        }

    }
    
//    /**
//     * Executes the tasks in the caller's thread.
//     * 
//     * @param tasks
//     *            The tasks.
//     */
//    protected void runInCallersThread(
//            final ArrayList<AbstractDataServiceProcedureTask> tasks) {
//        
//        final int ntasks = tasks.size();
//        
//        if (WARN && ntasks > 1)
//            log.warn("Running " + ntasks
//                + " tasks in caller's thread: recursionDepth="
//                + getRecursionDepth().get() + "(#active="
//                + getThreadPool().getActiveCount() + ", queueSize="
//                + getThreadPool().getQueue().size() + ") : "
//                + tasks.get(0).toString());
//
//        final Iterator<AbstractDataServiceProcedureTask> itr = tasks.iterator();
//
//        while (itr.hasNext()) {
//
//            final AbstractDataServiceProcedureTask task = itr.next();
//
//            try {
//
//                task.call();
//                
//            } catch (Exception e) {
//
////                if (INFO)
////                    log.info("Execution failed: task=" + task, e);
//
//                throw new ClientException("Execution failed: recursionDepth="
//                        + getRecursionDepth() + ", task=" + task, e,
//                        task.causes);
//
//            }
//            
//        }
//
//    }
    
}
