/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Aug 31, 2010
 */
package com.bigdata.bop.engine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NoSuchBOpException;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.fed.FederatedRunningQuery;
import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.DelegateBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IMultiSourceCloseableIterator;
import com.bigdata.relation.accesspath.MultiSourceSequentialCloseableIterator;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.util.concurrent.Memoizer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * {@link IRunningQuery} implementation based on the assignment of
 * {@link IChunkMessage}(s) to an operator task. Operators (other than those
 * with "operator-at-once" evaluation semantics) will typically executed
 * multiple times, consuming at least one {@link IChunkMessage} each time they
 * are evaluated. {@link IChunkMessage}s target a specific operator (bopId) and
 * shard (shardId). In scale-out, binding sets will be mapped across the target
 * access path and may be replicated to one or more nodes depending on the
 * distribution of the shards. This evaluation strategy is compatible with both
 * the {@link Journal} (aka standalone) and the {@link IBigdataFederation} (aka
 * clustered or scale-out).
 * <p>
 * Note: The challenge with this implementation is managing the amount of data
 * buffered on the JVM heap without introducing control structures which can
 * result in deadlock or starvation. This has been addressed to a large extent
 * by sharing a lock between this class and the per-operator input work queues
 * using modified version of the JSR 166 classes. For high volume operator at
 * once evaluation, we need to buffer the data on the native process heap using
 * the {@link IMemoryManager}.
 */
public class ChunkedRunningQuery extends AbstractRunningQuery {

    private final static transient Logger log = Logger
            .getLogger(ChunkedRunningQuery.class);

    private final static boolean INFO = log.isInfoEnabled();
    
    /**
     * Logger for the {@link ChunkTask}.
     */
    private final static Logger chunkTaskLog = Logger
            .getLogger(ChunkTask.class);
    
    /**
     * Used to map {@link IBindingSet}s across the federation or to migrate
     * solutions from the managed object heap onto the native heap.
     * 
     * @see BLZG-533 Vector query engine on native heap.
     */
    private final IChunkHandler chunkHandler;

    /**
     * A collection of (bopId,partitionId) keys mapped onto a collection of
     * operator task evaluation contexts for currently executing operators for
     * this query.
     */
    private final ConcurrentHashMap<BSBundle, ConcurrentHashMap<ChunkFutureTask,ChunkFutureTask>> operatorFutures;

    /**
     * A map of unbounded work queues for each (bopId,partitionId). Empty queues
     * are removed from the map.
     * <p>
     * The map is guarded by the {@link #lock}.
     * <p>
     * Note: Using a hash map here means that {@link #consumeChunks()} will draw
     * from operator queues more or less at random. Using an ordered map will
     * impose a bias, depending on the natural ordering of the map keys.
     * 
     * @see BSBundle#compareTo(BSBundle)
     * 
     *      FIXME Either this and/or {@link #operatorFutures} must be a weak
     *      value map in order to ensure that entries are eventually cleared in
     *      scale-out where the #of entries can potentially be very large since
     *      they are per (bopId,shardId). While these maps were initially
     *      declared as {@link ConcurrentHashMap} instances, if we remove
     *      entries once the map/queue entry is empty, this appears to open a
     *      concurrency hole which does not exist if we leave entries with empty
     *      map/queue values in the map. Changing to a weak value map should
     *      provide the necessary pruning of unused entries without opening up
     *      this concurrency hole.
     */
    private final ConcurrentMap<BSBundle, BlockingQueue<IChunkMessage<IBindingSet>>> operatorQueues;

    /**
     * Set to <code>true</code> to make {@link #operatorQueues} and ordered map.
     * When <code>true</code>, {@link #consumeChunk()} will have an ordered bias
     * in how it schedules work. [The historical behavior is present when this
     * is <code>false</code>.]
     */
    private static final boolean orderedOperatorQueueMap = false;
    
    /**
     * FIXME It appears that this is Ok based on a single unit test known to
     * fail when {@link #removeMapOperatorQueueEntries} is <code>true</code>,
     * but I expect that a similar concurrency problem could also exist for the
     * {@link #operatorFutures} even through it does not produce a deadlock.
     */
    static private final boolean removeMapOperatorFutureEntries = false;
    
    /**
     * FIXME See operatorQueues for why removing the map entries appears to
     * cause problems. This is problem is demonstrated by
     * TestQueryEngine#test_query_slice_noLimit() when
     * {@link PipelineOp.Annotations#PIPELINE_QUEUE_CAPACITY} is ONE (1).
     */
    static private final boolean removeMapOperatorQueueEntries = false;

    /**
     * When <code>true</code> the {@link IHaltOpMessage} is queued on an
     * {@link Executor} and will be delivered asynchronously to the query
     * controller. When <code>false</code> the message is delivered in the same
     * thread that ran the {@link ChunkTask}. This has implications for the
     * relative timing of changes to the {@link RunState} of the
     * {@link IRunningQuery}.
     * <p>
     * Note: it appears to be valid to use either synchronous or asynchronous
     * messaging for the {@link IHaltOpMessage}s. However, this can always be
     * changes to <code>true</code> in order to rule out the asynchronous
     * arrival of the {@link IHaltOpMessage} as a source of concurrency issues.
     * 
     * TODO Test the performance impact of this option, e.g., on BSBM.
     */
    static private final boolean asynchronousHaltMessage = true;
    
//    /**
//     * The chunks available for immediate processing (they must have been
//     * materialized).
//     * <p>
//     * Note: This is package private so it will be visible to the
//     * {@link QueryEngine}.
//     */
//    final/* private */BlockingQueue<IChunkMessage<IBindingSet>> chunksIn = new LinkedBlockingDeque<IChunkMessage<IBindingSet>>();
    
    /**
     * @param queryEngine
     *            The {@link QueryEngine} on which the query is running. In
     *            scale-out, a query is typically instantiated on many
     *            {@link QueryEngine}s.
     * @param queryId
     *            The identifier for that query.
     * @param controller
     *            <code>true</code> iff the {@link QueryEngine} is the query
     *            controller for this query (the {@link QueryEngine} which will
     *            coordinate the query evaluation).
     * @param clientProxy
     *            The query controller. In standalone, this is the same as the
     *            <i>queryEngine</i>. In scale-out, this is a proxy for the
     *            query controller whenever the query is instantiated on a node
     *            other than the query controller itself.
     * @param query
     *            The query.
     * @param saStats
     *            Statistics object containing static analysis statistics
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the <i>readTimestamp</i> is {@link ITx#UNISOLATED}
     *             (queries may not read on the unisolated indices).
     * @throws IllegalArgumentException
     *             if the <i>writeTimestamp</i> is neither
     *             {@link ITx#UNISOLATED} nor a read-write transaction
     *             identifier.
     */
    public ChunkedRunningQuery(final QueryEngine queryEngine, final UUID queryId,
            final boolean controller, final IQueryClient clientProxy,
            final PipelineOp query, final IChunkMessage<IBindingSet> realSource) {

        super(queryEngine, queryId, controller, clientProxy, query, realSource);

        this.chunkHandler = getChunkHandler(queryEngine, query);
        
        this.operatorFutures = new ConcurrentHashMap<BSBundle, ConcurrentHashMap<ChunkFutureTask, ChunkFutureTask>>();

        if (orderedOperatorQueueMap) {

            this.operatorQueues = new ConcurrentSkipListMap<BSBundle, BlockingQueue<IChunkMessage<IBindingSet>>>();
            
        } else {
            
            this.operatorQueues = new ConcurrentHashMap<BSBundle, BlockingQueue<IChunkMessage<IBindingSet>>>();

        }

    }

    /**
     * Make a chunk of binding sets available for consumption by the query.
     * <p>
     * Note: this is invoked by {@link QueryEngine#acceptChunk(IChunkMessage)}
     * 
     * @param msg
     *            The chunk.
     * 
     * @return <code>true</code> if the message was accepted.
     */
    @Override
    protected boolean acceptChunk(final IChunkMessage<IBindingSet> msg) {

        if (msg == null)
            throw new IllegalArgumentException();

        if (!msg.isMaterialized())
            throw new IllegalStateException();

//        if (SolutionsLog.solutionsLog.isInfoEnabled())
//            SolutionsLog.log(msg);
        
        final BSBundle bundle = new BSBundle(msg.getBOpId(), msg
                .getPartitionId());

        lock.lock();

        try {

            if (isDone()) {
                // The query is no longer running.
                msg.release();
                return false;
                //throw new RuntimeException(ERR_QUERY_DONE, future.getCause());
            }

            BlockingQueue<IChunkMessage<IBindingSet>> queue = operatorQueues
                    .get(bundle);

            if (queue == null) {

                /*
                 * There is no input queue for this operator, so we create one
                 * now while we are holding the lock. If the target is a
                 * pipelined operator, then we impose a limit on the #of
                 * messages which may be buffered for that operator. If the
                 * operator is NOT pipelined, e.g., ORDER_BY, then we use an
                 * unbounded queue.
                 * 
                 * TODO Unit/stress tests with capacity set to 1.
                 */
                
                // The target operator for this message.
                final PipelineOp bop = (PipelineOp) getBOp(msg.getBOpId());
                
                final int capacity = bop.isPipelinedEvaluation() ? bop.getProperty(
                        PipelineOp.Annotations.PIPELINE_QUEUE_CAPACITY,
                        PipelineOp.Annotations.DEFAULT_PIPELINE_QUEUE_CAPACITY)
                        : Integer.MAX_VALUE;

                // Create a new queue using [lock].
                queue = new com.bigdata.jsr166.LinkedBlockingDeque<IChunkMessage<IBindingSet>>(//
                        capacity,
                        lock);

                // Add to the collection of operator input queues.
                if (operatorQueues.put(bundle, queue) != null) {

                    /*
                     * There must not be an entry for this operator. We checked
                     * for this above. Nobody else should be adding entries into
                     * the [operatorQueues] map.
                     */

                    throw new AssertionError(bundle.toString());

                }
             
            }

            // offer (non-blocking)
            if (!queue.offer(msg)) {
                // Offer failed.  Work queue will block.
                getQueryEngine().counters.blockedWorkQueueCount.increment();
                getQueryEngine().counters.blockedWorkQueueRunningTotal.increment();
                try {
                    // blocking put()
                    queue.put(msg);
                    getQueryEngine().counters.bufferedChunkMessageCount.increment();
                    if (msg instanceof LocalNativeChunkMessage) {
                        getQueryEngine().counters.bufferedChunkMessageBytesOnNativeHeap
                                .add(((LocalNativeChunkMessage) msg).getByteCount());
                    }
                } finally {
                    // Queue unblocked
                    getQueryEngine().counters.blockedWorkQueueCount.decrement();
                }
            }

            // and see if the target operator can run now.
            scheduleNext(bundle);
            
            return true;
            
        } catch(InterruptedException ex) {
            
            // wrap interrupt thrown out of queue.put(msg);
            
            msg.release();
            
            throw new RuntimeException(ex);
            
        } finally {

            lock.unlock();

        }

    }

    /**
     * {@inheritDoc}.
     * <p>
     * Examines the input queue for each (bopId,partitionId). If there is work
     * available and no task is currently running, then drain the work queue and
     * submit a task to consume that work.
     */
    @Override
    protected void consumeChunk() {
        lock.lock();
        try {
            for (BSBundle bundle : operatorQueues.keySet()) {
                scheduleNext(bundle);
            }
        } catch (RuntimeException ex) {
            halt(ex);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Overridden to attempt to consume another chunk each time an operator
     * reports that it has halted evaluation. This is necessary because the
     * haltOp() message can arrive asynchronously, so we need to test the work
     * queues in case there are "at-once" operators awaiting the termination of
     * their predecessor(s) in the pipeline.
     */
    @Override
    protected void haltOp(final IHaltOpMessage msg) {
        lock.lock();
        try {
            super.haltOp(msg);
            consumeChunk();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Examine the input queue for the (bopId,partitionId). If there is work
     * available, then drain the work queue and submit a task to consume that
     * work. This handles {@link PipelineOp.Annotations#MAX_PARALLEL},
     * {@link PipelineOp.Annotations#PIPELINED}, and
     * {@link PipelineOp.Annotations#MAX_MESSAGES_PER_TASK} as special cases.
     * 
     * @param bundle
     *            The (bopId,partitionId).
     * 
     * @return <code>true</code> if a new task was started.
     * 
     * @todo Also handle {@link PipelineOp.Annotations#MAX_MEMORY} here by
     *       handshaking with the {@link FederatedRunningQuery}.
     */
    private boolean scheduleNext(final BSBundle bundle) {
        if (bundle == null)
            throw new IllegalArgumentException();
        final PipelineOp bop = (PipelineOp) getBOp(bundle.bopId);
        final int maxParallel = bop.getMaxParallel();
        final boolean pipelined = bop.isPipelinedEvaluation();
        final int maxMessagesPerTask = bop.getProperty(
                PipelineOp.Annotations.MAX_MESSAGES_PER_TASK,
                PipelineOp.Annotations.DEFAULT_MAX_MESSAGES_PER_TASK);
//      final long maxMemory = bop.getProperty(
//              PipelineOp.Annotations.MAX_MEMORY,
//              PipelineOp.Annotations.DEFAULT_MAX_MEMORY);
        lock.lock();
        try {
            // Make sure the query is still running.
            if(isDone())
                return false;
            // Is there a Future for this (bopId,partitionId)?
            ConcurrentHashMap<ChunkFutureTask, ChunkFutureTask> map = operatorFutures
                    .get(bundle);
            // #of instances of the operator already running.
            int nrunning = 0;
            if (map != null) {
                for (ChunkFutureTask cft : map.keySet()) {
                    if (cft.isDone()) {
                        // Remove tasks which have already terminated.
                        map.remove(cft);
                    }
                    nrunning++;
                }
                if (nrunning == 0) {
                    // No tasks running for this operator.
                    if(removeMapOperatorFutureEntries)
                        if (map != operatorFutures.remove(bundle)) throw new AssertionError();
                }
            }
            if (nrunning >= maxParallel) {
                /*
                 * Defer concurrent execution for the same (bopId,shardId) since
                 * there are already at lease [maxParallel] instances of this
                 * operator running for that (bopId,shardId).
                 */
                if (log.isDebugEnabled())
                    log.debug("Deferring next execution: " + bundle
                            + ", #running=" + nrunning + ", maxParallel="
                            + maxParallel + ", runState=" + runStateString());
                return false;
            }
//          {
//              /*
//               * Verify that we can acquire sufficient permits to do some
//               * work.
//               */
//              final BlockingQueue<IChunkMessage<IBindingSet>> queue = operatorQueues
//                      .get(bundle);
//              if (queue == null || queue.isEmpty()) {
//                  // No work.
//                  return false;
//              }
//              // The queue could be increased, but this will be its minimum size.
//              final int minQueueSize = queue.size();
//              if(!outstandingMessageSemaphore.tryAcquire(minQueueSize)) {
//                  // Not enough permits.
//                  System.err.println("Permits: required=" + minQueueSize
//                          + ", available="
//                          + outstandingMessageSemaphore.availablePermits()
//                          + ", bundle=" + bundle);
//                  return false;
//              }
//          
//          }
            // Get the work queue for that (bopId,partitionId).
            final BlockingQueue<IChunkMessage<IBindingSet>> queue = operatorQueues
                    .get(bundle);
            if (queue == null) {
                // no work
                return false;
            }
//          if (false && pipelined && !getQueryEngine().isScaleOut()) {
//              /*
//               * For pipelined operators, examine the sink and altSink (if
//               * different). For each of the sink and altSink, if the
//               * (alt)sink operator is also pipelined and there are more than
//               * maxMessagesPerTask messages on its work queue, then this
//               * operator needs to block, which we handle by deferring its
//               * next evaluation task. This allows us to bound the amount of
//               * data placed onto the JVM heap.
//               * 
//               * FIXME In order to do this for scale-out, we need to examine
//               * each of the shardIds for the (alt)sink, for example, by
//               * organizing the operatorQueues as a nested map: (bopId =>
//               * (shardId => queue)).
//               */
//
//              // parent (null if this is the root of the operator tree).
//              final BOp p = BOpUtility.getParent(getQuery(), bop);
//              /*
//               * The sink is the parent. The parent MUST have an id so we can
//               * target it with a message. (The sink will be null iff there is
//               * no parent for this operator.)
//               */
//              final Integer sinkId = BOpUtility.getEffectiveDefaultSink(bop,
//                      p);
//              // altSink (null when not specified).
//              final Integer altSinkId = (Integer) bop
//                      .getProperty(PipelineOp.Annotations.ALT_SINK_REF);
//
//              if (sinkId != null) {
//
//                  // Examine the work queue for the sink.
//                  
//                  final BlockingQueue<IChunkMessage<IBindingSet>> sinkQueue = operatorQueues
//                          .get(new BSBundle(sinkId, -1));
//
//                  final int sinkQueueSize = sinkQueue == null ? 0 : sinkQueue
//                          .size();
//
//                  final int sinkMaxMessagesPerTask = getBOp(sinkId).getProperty(
//                          PipelineOp.Annotations.MAX_MESSAGES_PER_TASK,
//                          PipelineOp.Annotations.DEFAULT_MAX_MESSAGES_PER_TASK);
//
//                  if (sinkQueueSize > sinkMaxMessagesPerTask) {
//                      log.warn("Waiting on consumer(s): bopId="
//                              + bundle.bopId + ", nrunning=" + nrunning
//                              + ", maxParallel=" + maxParallel + ", sink="
//                              + sinkId + ", sinkQueueSize=" + sinkQueueSize
//                              + ", sinkMaxMessagesPerTask="
//                              + sinkMaxMessagesPerTask+", runState="+runStateString());
//                      return false;
//                  }
//
//              }
//
//              if (altSinkId != null && !altSinkId.equals(sinkId)) {
//
//                  // Examine the work queue for the altSink.
//
//                  final BlockingQueue<IChunkMessage<IBindingSet>> sinkQueue = operatorQueues
//                          .get(new BSBundle(altSinkId, -1));
//
//                  final int sinkQueueSize = sinkQueue == null ? 0 : sinkQueue
//                          .size();
//
//                  final int sinkMaxMessagesPerTask = getBOp(altSinkId).getProperty(
//                          PipelineOp.Annotations.MAX_MESSAGES_PER_TASK,
//                          PipelineOp.Annotations.DEFAULT_MAX_MESSAGES_PER_TASK);
//
//                  if (sinkQueueSize > sinkMaxMessagesPerTask) {
//                      log.warn("Waiting on consumer(s): bopId="
//                              + bundle.bopId + ", nrunning=" + nrunning
//                              + ", maxParallel=" + maxParallel + ", altSink="
//                              + altSinkId + ", altSinkQueueSize="
//                              + sinkQueueSize + ", sinkMaxMessagesPerTask="
//                              + sinkMaxMessagesPerTask+", runState="+runStateString());
//                      return false;
//                  }
//
//              }
//
//            }
            if (queue.isEmpty()) {
                // No work, so remove work queue for (bopId,partitionId).
                if(removeMapOperatorQueueEntries)
                    if (queue != operatorQueues.remove(bundle)) throw new AssertionError();
                return false;
            }
            /*
             * true iff operator requires at once evaluation and all solutions
             * are now available for that operator.
             */
            boolean atOnceReady = false;
            if (!pipelined) {
                if (!isAtOnceReady(bundle.bopId)) {
                    /*
                     * This operator is not pipelined, so we need to wait until
                     * all of its input solutions have been materialized (no
                     * prior operator in the pipeline is running or has inputs
                     * available which could cause it to run).
                     * 
                     * TODO This is where we should examine MAX_MEMORY and the
                     * buffered data to see whether or not to trigger an
                     * evaluation pass for the operator based on the data
                     * already materialized for that operator.
                     */
                    if (log.isDebugEnabled())
                        log.debug("Waiting on producer(s): bopId="
                                + bundle.bopId);
                    return false;
                }
                atOnceReady = true;
            }
            /*
             * Drain the work queue for that (bopId,partitionId).
             * 
             * Note: If the operator is pipelined, then we do not drain more
             * than [maxMessagesPerTask] messages at a time. The remainder are
             * left on the work queue for the next task instance which we start
             * for this operator.
             */
            final List<IChunkMessage<IBindingSet>> accepted = new LinkedList<IChunkMessage<IBindingSet>>();
            try {
                /*
                 * Note: Once we drain these messages from the work queue we are
                 * responsible for calling release() on them.
                 */
                queue.drainTo(accepted, pipelined ? maxMessagesPerTask
                        : Integer.MAX_VALUE);
                // #of messages accepted from the work queue.
                final int naccepted = accepted.size();
                getQueryEngine().counters.bufferedChunkMessageCount.add(-naccepted);
                {
                    long byteCount = 0;
                    for(IChunkMessage<?> msg : accepted) {
                        if (msg instanceof LocalNativeChunkMessage) {
                            byteCount += ((LocalNativeChunkMessage) msg).getByteCount();
                        }
                    }
                    if(byteCount!=0) {
                        getQueryEngine().counters.bufferedChunkMessageBytesOnNativeHeap.add(-byteCount);
                    }
                }
                // #of messages remaining on the work queue.
                final int nremaining = queue.size();
                if (nremaining == 0) {
                    // Remove the work queue for that (bopId,partitionId).
                    if(removeMapOperatorQueueEntries)
                        if(queue != operatorQueues.remove(bundle)) throw new AssertionError();
                } else if (pipelined) {
                    /*
                     * After removing the maximum amount from a pipelined operator,
                     * the work queue is still not empty.
                     */
                    if (INFO)
                        log.info("Work queue is over capacity: bundle=" + bundle
                                + ", naccepted=" + naccepted + ", nremaining="
                                + nremaining + ", maxMessagesPerTask="
                                + maxMessagesPerTask + ", runState="
                                + runStateString());
                }
                /*
                 * Combine the messages into a single source to be consumed by a
                 * task.
                 */
                int nassigned = 1;
                final Iterator<IChunkMessage<IBindingSet>> mitr = accepted.iterator();
                final IChunkMessage<IBindingSet> firstChunk = mitr.next();
                // See BOpContext#isLastInvocation()
                final boolean isLastInvocation = pipelined
    //                    && nremaining == 0
    //                    && maxParallel == 1
    //                    && isOperatorDone(bundle.bopId)
                        && firstChunk.isLastInvocation()
                        ;
                /*
                 * Note: There is no longer any reliance on the IAsynchronous
                 * Iterator API here. It is perfectly sufficient to only
                 * implement ICloseableIterator. Query operator and chunk
                 * message implementations should be revisited with this
                 * simplifying assumption in mind.
                 * 
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/475
                 */
                final IMultiSourceCloseableIterator<IBindingSet[]> source = new MultiSourceSequentialCloseableIterator<IBindingSet[]>(//
    //                  accepted.remove(0).getChunkAccessor().iterator()//
                        firstChunk.getChunkAccessor().iterator()//
                        );
    //            for (IChunkMessage<IBindingSet> msg : accepted) {
    //          source.add(msg.getChunkAccessor().iterator());
                // #of solutions accepted across those chunk messages.
                final long solutionsAccepted;
                {
                    long na = firstChunk.getSolutionCount();
                    while (mitr.hasNext()) {
                        final IChunkMessage<IBindingSet> msg = mitr.next();
                        na += msg.getSolutionCount();
                        source.add(msg.getChunkAccessor().iterator());
                        nassigned++;
                    }
                    solutionsAccepted = na;
                }
                if (nassigned != naccepted)
                    throw new AssertionError();
                /*
                 * Create task to consume that source.
                 */
                final ChunkFutureTask cft;
                try {
                    cft = new ChunkFutureTask(
                            new ChunkTask(bundle.bopId, bundle.shardId,
                                    naccepted, isLastInvocation, source));
                } catch (Throwable t2) {
                    // Ensure accepted messages are released();
                    safeRelease(accepted);
                    halt(t2); // ensure query halts.
                    if (getCause() != null) {
                        // Abnormal termination - wrap and rethrow.
                        throw new RuntimeException(t2);
                    }
                    // normal termination - swallow the exception.
                    return false;
                }
                /*
                 * Save the Future for this task. Together with the logic above this
                 * may be used to limit the #of concurrent tasks per (bopId,shardId)
                 * to one for a given query.
                 */
                if (map == null) {
                    map = new ConcurrentHashMap<ChunkFutureTask, ChunkFutureTask>();
                    operatorFutures.put(bundle, map);
                }
                map.put(cft, cft);
                /*
                 * Submit task for execution (asynchronous).
                 */
                if (INFO)
                    log.info("Running task: bop=" + bundle.bopId
                            + (pipelined?"":", atOnceReady=" + atOnceReady) + ", bop="
                            + bop.toShortString() + ", messages=" + naccepted
                            + ", solutions=" + solutionsAccepted
                            + (log.isDebugEnabled()?", runState=" + runStateString():""));
                getQueryEngine().execute(cft);
                return true;
            } catch(Throwable t) {
                // Ensure accepted messages are released();
                safeRelease(accepted);
                // wrap and rethrow cause.
                throw new RuntimeException(t);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Ensure messages are {@link IChunkMessage#release() released()}. Nothing
     * is thrown unless the {@link Throwable} has a root cause which indicates
     * an interrupt.
     * 
     * @param accepted
     *            The messages.
     * 
     * @throws RuntimeException
     *             wrapping an {@link InterruptedException} cause.
     */
    private void safeRelease(final List<IChunkMessage<IBindingSet>> accepted) {
        if (accepted == null)
            return;
        try {
            for (IChunkMessage<IBindingSet> msg : accepted) {
                try {
                    msg.release();
                } catch (Throwable t2) {
                    if (isRootCauseInterrupt(t2)) {
                        throw t2;
                    }
                    log.error("Could not release message: " + msg, t2);
                }
            }
        } catch (Throwable t2) {
            if (isRootCauseInterrupt(t2)) {
                // Wrap and throw out.
                throw new RuntimeException(t2);
            }
            log.error(t2, t2);
        }
    }

    /**
     * A {@link FutureTask} which conditionally schedules another task for the
     * same (bopId, partitionId) once this the wrapped {@link ChunkTask} is
     * done. This is similar to the {@link Memoizer} pattern. This class
     * coordinates with the {@link #operatorFutures}, which maintains a map of
     * all currently running tasks which are consuming chunks.
     * <p>
     * The {@link ChunkTask} is wrapped by a {@link ChunkTaskWrapper} which is
     * responsible for communicating the changes in the query's running state
     * back to the {@link RunState} object on the query controller.
     */
    private class ChunkFutureTask extends FutureTaskMon<Void> {

        private final ChunkTask t;

        public ChunkFutureTask(final ChunkTask chunkTask) {

            /*
             * Note: wraps chunk task to communicate run state changes back to
             * the query controller.
             */
            super(new ChunkTaskWrapper(chunkTask), null/* result */);

            this.t = chunkTask;

        }

        @Override
        public void run() {

            try {

                super.run();
                
            } catch(Throwable t) {

                // ensure query halts.
                halt(t);
                
                if (getCause() != null) {

                    // abnormal termination. wrap and rethrow.
                    throw new RuntimeException(t);
                    
                }
                
                // otherwise ignore exception (normal termination).
                
            } finally {

                /*
                 * This task is done executing so remove its Future before we
                 * attempt to schedule another task for the same
                 * (bopId,partitionId).
                 */

                final ConcurrentHashMap<ChunkFutureTask, ChunkFutureTask> map = operatorFutures
                        .get(new BSBundle(t.bopId, t.partitionId));

                if (map != null) {

                    map.remove(this, this);

                }

            }

            // Schedule another task if any messages are waiting.
            ChunkedRunningQuery.this.scheduleNext(new BSBundle(t.bopId,
                    t.partitionId));

        }
        
    }

    /**
     * Wraps the {@link ChunkTask} and handles various handshaking with the
     * {@link ChunkedRunningQuery} and the {@link RunState} on the query controller.
     * Since starting and stopping a {@link ChunkTask} requires handshaking with
     * the query controller (and thus can require RMI), it is important that
     * these actions take place once the task has been submitted - otherwise
     * they would be synchronous in the loop which consumes available chunks and
     * generates new {@link ChunkTask}s.
     */
    private class ChunkTaskWrapper implements Runnable {

        private final ChunkTask t;

        public ChunkTaskWrapper(final ChunkTask chunkTask) {

            if (chunkTask == null)
                throw new IllegalArgumentException();
            
            this.t = chunkTask;
            
        }
        
        @Override
        public void run() {

            final QueryEngine queryEngine = getQueryEngine();
            final UUID serviceId = queryEngine.getServiceUUID();
            final QueryEngineCounters c = queryEngine.counters;
            try {

                /*
                 * Notify query controller that operator task will start (sync
                 * notification).
                 * 
                 * Note: This is potentially an RMI back to the controller. It
                 * is invoked from within the running task in order to remove
                 * the latency for that RMI from the thread which submits tasks
                 * to consume chunks.
                 * 
                 * FIXME This is a protocol that should be optimized to provide
                 * better throughput for scale-out. E.g., a single socket on
                 * which we transmit and receive notice about operator
                 * start/stop metadata using some non-blocking service.
                 */

//                final boolean lastPassRequested = ((PipelineOp) (t.bop))
//                        .isLastPassRequested();

                getQueryController().startOp(
                        new StartOpMessage(getQueryId(), t.bopId,
                                t.partitionId, serviceId, t.messagesIn 
//                                ,t.bop.getEvaluationContext(),
//                                lastPassRequested
                                ));

                /*
                 * Run the operator task.
                 */
                final long begin = System.currentTimeMillis();
                try {
                    c.operatorStartCount.increment();
                    c.operatorActiveCount.increment();
                    t.call();
                } catch(Throwable t2) {
                    halt(t2); // ensure query halts.
                    if (getCause() != null) {
                        // Abnormal termination - wrap and rethrow.
                        throw new RuntimeException(t2);
                    }
                    // normal termination - swallow the exception.
                } finally {
                    c.operatorHaltCount.increment();
                    c.operatorActiveCount.decrement();
                    t.context.getStats().elapsed.add(System.currentTimeMillis()
                            - begin);
                }

                // Notify query controller that operator task did run.
                sendHaltMessage(serviceId, t, null/* firstCause */);
                
            } catch (Throwable ex1) {

                /*
                 * Mark the query as halted on this node regardless of whether
                 * we are able to communicate with the query controller.
                 * 
                 * Note: Invoking halt(t) here will log an error. This logged
                 * error message is necessary in order to catch errors in
                 * clientProxy.haltOp() (above and below).
                 */

                // ensure halted.
                halt(ex1);

                if (getCause() != null) {

                    // Log an error (abnormal termination only).
                    log.error("queryId=" + getQueryId() + ", bopId=" + t.bopId
                            + ", bop=" + t.bop, ex1);

                }
                
                // Notify query controller that operator task did run.
                sendHaltMessage(serviceId, t, getCause()/* firstCauseIfError */);

            }
        
        } // runOnce()
        
    }

    /**
     * Send a {@link IHaltOpMessage}. Whether the delivery is synchronous or not
     * depends on {@link #asynchronousHaltMessage}. If this is the controller
     * and we are using synchronous message delivery, then the message is
     * delivered directly to {@link AbstractRunningQuery#haltOp(IHaltOpMessage)}.
     * Otherwise it will pass through {@link IQueryClient#haltOp(IHaltOpMessage)}
     * .
     * 
     * @param serviceId
     *            The service to be notified.
     * @param t
     *            The {@link ChunkTask}.
     * @param cause
     *            The first cause (or <code>null</code> if the task did not end
     *            with an error).
     */
    private final void sendHaltMessage(final UUID serviceId, final ChunkTask t,
            final Throwable cause) {

        final IHaltOpMessage msg = new HaltOpMessage(getQueryId(), t.bopId,
                t.partitionId, serviceId, cause, //t.sinkId,
                t.sinkMessagesOut.get(), //t.altSinkId,
                t.altSinkMessagesOut.get(),//
                t.context.getStats());

        if (asynchronousHaltMessage) {
            try {
                /*
                 * Queue a task to send the halt message to the query controller
                 * (asynchronous notification).
                 */
                final SendHaltMessageTask sendTask = new SendHaltMessageTask(
                        getQueryController(), msg, ChunkedRunningQuery.this);
                // submit task (asynchronous execution).
                t.context.getExecutorService().execute(sendTask);
            } catch (RejectedExecutionException ex) {
                // e.g., service is shutting down.
                if (INFO)
                    log.info("Could not send message: " + msg, ex);
            } catch (Throwable ex) {
                log.error("Could not send message: " + msg + " : " + ex, ex);
            }
        } else {
            /*
             * Synchronous delivery (local).
             */
            if (isController()) {
                haltOp(msg);
            } else {
                new SendHaltMessageTask(getQueryController(), msg,
                        ChunkedRunningQuery.this).run();
            }
        }

    }
    
    /**
     * Runnable evaluates an operator for some chunk of inputs. In scale-out,
     * the operator may be evaluated against some partition of a scale-out
     * index.
     */
    private class ChunkTask implements Callable<Void> {

        /** Alias for the {@link ChunkTask}'s logger. */
        private final Logger log = chunkTaskLog;

        /** The index of the bop which is being evaluated. */
        private final int bopId;

        /**
         * The index partition against which the operator is being evaluated and
         * <code>-1</code> if the operator is not being evaluated against a
         * shard.
         */
        private final int partitionId;

        /** The operator which is being evaluated. */
        private final BOp bop;

        /**
         * The index of the operator which is the default sink for outputs
         * generated by this evaluation. This is the
         * {@link BOp.Annotations#BOP_ID} of the parent of this operator. This
         * will be <code>null</code> if the operator does not have a parent and
         * is not a query since no outputs will be generated in that case.
         */
        private final Integer sinkId;

        /**
         * The index of the operator which is the alternative sink for outputs
         * generated by this evaluation. This is <code>null</code> unless the
         * operator explicitly specifies an alternative sink using either
         * {@link PipelineOp.Annotations#ALT_SINK_REF} or
         * {@link PipelineOp.Annotations#ALT_SINK_GROUP}.  
         */
        private final Integer altSinkId;

        /**
         * The sink on which outputs destined for the {@link #sinkId} operator
         * will be written and <code>null</code> if {@link #sinkId} is
         * <code>null</code>.
         */
        private final IBlockingBuffer<IBindingSet[]> sink;

        /**
         * The sink on which outputs destined for the {@link #altSinkId}
         * operator will be written and <code>null</code> if {@link #altSinkId}
         * is <code>null</code>.
         */
        private final IBlockingBuffer<IBindingSet[]> altSink;

        /**
         * The evaluation context for this operator.
         */
        private final BOpContext<IBindingSet> context;

        /**
         * {@link FutureTask} which evaluates the operator (evaluation is
         * delegated to this {@link FutureTask}).
         */
        private final FutureTask<Void> ft;

        /**
         * The #of input messages.
         */
        final int messagesIn;
        
        /** #of chunk messages out to sink. */
        final AtomicInteger sinkMessagesOut = new AtomicInteger(0);

        /** #of chunk messages out to altSink. */
        final AtomicInteger altSinkMessagesOut = new AtomicInteger(0);

        /**
         * A human readable representation of the {@link ChunkTask}'s state.
         */
        @Override
        public String toString() {
            return "ChunkTask" + //
                    "{query=" + getQueryId() + //
                    ",bopId=" + bopId + //
                    ",partitionId=" + partitionId + //
                    ",sinkId=" + sinkId + //
                    ",altSinkId=" + altSinkId + //
                    "}";
        }

        /**
         * Core implementation.
         * <p>
         * This looks up the {@link BOp} which is the target for the message in
         * the {@link IRunningQuery#getBOpIndex() BOp Index}, creates the
         * sink(s) for the {@link BOp}, creates the {@link BOpContext} for that
         * {@link BOp}, and wraps the value returned by
         * {@link PipelineOp#eval(BOpContext)} in order to handle the outputs
         * written on those sinks.
         * 
         * @param bopId
         *            The operator to which the message was addressed.
         * @param partitionId
         *            The partition identifier to which the message was
         *            addressed.
         * @param messagesIn
         *            The number of {@link IChunkMessage} to be consumed by this
         *            task.
         * @param isLastInvocation
         *            iff the caller knows that no additional messages can be
         *            produced which target this operator (regardless of the
         *            shard).
         * @param source
         *            Where the task will read its inputs.
         */
        public ChunkTask(final int bopId, final int partitionId,
                final int messagesIn, boolean isLastInvocation,
                final ICloseableIterator<IBindingSet[]> src) {

            this.bopId = bopId;
            
            this.partitionId = partitionId;
         
            this.messagesIn = messagesIn;
            
            bop = getBOpIndex().get(bopId);
            
            if (bop == null)
                throw new NoSuchBOpException(bopId);
            
            if (!(bop instanceof PipelineOp))
                throw new UnsupportedOperationException(bop.getClass()
                        .getName());

            // self
            final PipelineOp op = ((PipelineOp) bop);

            // parent (null if this is the root of the operator tree).
            final BOp p = BOpUtility.getParent(getQuery(), op);

            /*
             * The sink is the parent. The parent MUST have an id so we can
             * target it with a message. (The sink will be null iff there is no
             * parent for this operator.)
             */
            sinkId = BOpUtility.getEffectiveDefaultSink(bop, p);

            // altSink (null when not specified).
            altSinkId = (Integer) op
                    .getProperty(PipelineOp.Annotations.ALT_SINK_REF);
//            {
//                // altSink (null when not specified).
//                final Integer altSinkId = (Integer) op
//                        .getProperty(PipelineOp.Annotations.ALT_SINK_REF);
//                final Integer altSinkGroup = (Integer) op
//                        .getProperty(PipelineOp.Annotations.ALT_SINK_GROUP);
//                if (altSinkId != null && altSinkGroup != null)
//                    throw new RuntimeException(
//                            "Annotations are mutually exclusive: "
//                                    + PipelineOp.Annotations.ALT_SINK_REF
//                                    + " and "
//                                    + PipelineOp.Annotations.ALT_SINK_GROUP);
//                if (altSinkGroup != null) {
//                    /*
//                     * Lookup the first pipeline op in the conditional binding
//                     * group and use its bopId as the altSinkId.
//                     */
//                    this.altSinkId = BOpUtility.getFirstBOpIdForConditionalGroup(
//                            getQuery(), altSinkGroup);
//                } else {
//                    // MAY be null.
//                    this.altSinkId = altSinkId;
//                }
//            }

            if (altSinkId != null && !getBOpIndex().containsKey(altSinkId))
                throw new NoSuchBOpException(altSinkId);

            if (altSinkId != null && sinkId == null) {
                throw new RuntimeException(
                        "The primary sink must be defined if the altSink is defined: "
                                + bop);
            }

            /*
             * Setup the BOpStats object. For some operators, e.g., SliceOp,
             * this MUST be the same object across all invocations of that
             * instance of that operator for this query. This is marked by the
             * PipelineOp#isSharedState() method and is handled by a
             * putIfAbsent() pattern when that method returns true.
             * 
             * Note: RunState#haltOp() avoids adding a BOpStats object to itself
             * since that would cause double counting when the same object is
             * used for each invocation of the operator.
             * 
             * Note: It tends to be more useful to have distinct BOpStats
             * objects for each operator task instance that we run as this makes
             * it possible to see how much work was performed by that task
             * instance. The data are aggregated in the [statsMap] across the
             * entire run of the query.
             */
            final BOpStats stats;
            if (((PipelineOp) bop).isSharedState()) {//|| statsMap != null) {
                // shared stats object.
                stats = getStats(bopId);
            } else {
                // distinct stats objects, aggregated as each task finishes.
                stats = op.newStats();
//        		log.warn("bopId=" + bopId + ", stats=" + stats);
            }
            if (stats == null) {
                throw new AssertionError("No stats: op=" + op);
            }

//            // The groupId (if any) for this operator.
//            final Integer fromGroupId = (Integer) op
//                    .getProperty(PipelineOp.Annotations.CONDITIONAL_GROUP);

            if (p == null) {
                /*
                 * The top-most operator in the query plan is the last operator
                 * in evaluation order. It MUST run on the query controller.
                 * This is required in order for it to add solutions to the
                 * query buffer. The QueryEngine verifies this before it
                 * executes a query plan.
                 * 
                 * Note: Operators MUST NOT close the query buffer since they do
                 * not have enough context to decide when the query is done. The
                 * query buffer will be closed when the query is cancelled.
                 * Closing it prematurely will cause the query to fail if any
                 * other operator evaluation pass attempts to write on the query
                 * buffer.
                 */
                sink = new NoCloseBuffer<IBindingSet[]>(getQueryId(), bop, bopId,
                        partitionId, getQueryBuffer());
            } else {
//                final BOp targetOp = getBOpIndex().get(sinkId);
//                final Integer toGroupId = (Integer) targetOp
//                        .getProperty(PipelineOp.Annotations.CONDITIONAL_GROUP);
//                final SinkTransitionMetadata stm = new SinkTransitionMetadata(
//                        fromGroupId, toGroupId, true/* isSink */);
                sink = newBuffer(op, sinkId, //null/*stm*/, 
                        sinkMessagesOut, stats);
            }

            if (altSinkId == null) {
                altSink = null;
                // } else if(altSinkId.equals(sinkId)){
                /*
                 * @todo Note: The optimization when altSink:=sink is now only
                 * possible when the groupId is not changing during the
                 * transition.
                 */ 
                // altSink = sink;
            } else {
//                final BOp targetOp = getBOpIndex().get(altSinkId);
//                final Integer toGroupId = (Integer) targetOp
//                        .getProperty(PipelineOp.Annotations.CONDITIONAL_GROUP);
//                final SinkTransitionMetadata stm = new SinkTransitionMetadata(
//                        fromGroupId, toGroupId, false/* isSink */);
                altSink = newBuffer(op, altSinkId, //null/* stm */,
                        altSinkMessagesOut, stats);
            }

            // context : @todo pass in IChunkMessage or IChunkAccessor
            context = new BOpContext<IBindingSet>(ChunkedRunningQuery.this,
                    partitionId, stats, op, isLastInvocation, src, sink,
                    altSink);

//            if(isLastInvocation)
//                context.setLastInvocation();
            
            // FutureTask for operator execution (not running yet).
            if ((ft = op.eval(context)) == null)
                throw new RuntimeException("No future: " + op);

        }

        /**
         * Factory returns the {@link IBlockingBuffer} on which the operator
         * should write its outputs which target the specified <i>sinkId</i>.
         * 
         * @param op
         *            The operator whose evaluation task is being constructed.
         * @param sinkId
         *            The identifier for an operator which which the task will
         *            write its solutions (either the primary or alternative
         *            sink).
         * @param stats
         *            The statistics object for the evaluation of the operator.
         *            
         * @return The buffer on which the operator should write outputs which
         *         target that sink.
         */
        private IBlockingBuffer<IBindingSet[]> newBuffer(final PipelineOp op,
                final int sinkId,//
                final AtomicInteger sinkMessagesOut, //
                final BOpStats stats//
                ) {

            return new HandleChunkBuffer(ChunkedRunningQuery.this, bopId,
                    partitionId, sinkId, op.getChunkCapacity(),
                    op.isReorderSolutions(), sinkMessagesOut, stats);

        }

        /**
         * Evaluate the {@link IChunkMessage}.
         */
        @Override
        public Void call() throws Exception {
            if (log.isDebugEnabled())
                log.debug("Running chunk: " + this);
            try {
                ft.run(); // run
                ft.get(); // verify success
                /*
                 * If the operator ran successfully, then it should have flushed
                 * its sink(s) and closed them.
                 */
                if (sink != null) {
                    sink.flush();
                    sink.close();
                }
                if (altSink != null) {
                    altSink.flush();
                    altSink.close();
                }
            } catch (Throwable t) {
                /*
                 * Ensure query halts.
                 * 
                 * Note: This is where we attach the metadata about the operator
                 * and query for which the error was observed.
                 */
                halt(new Exception("task=" + toString() + ", cause=" + t, t));
                if (getCause() != null) {
                    // Abnormal termination - wrap and rethrow.
                    // TODO Why is this line empty? (I think that it is handled by the ChunkTaskWrapper.)
                }
                // otherwise ignore exception (normal completion).
            } finally {
                /*
                 * Ensure that the source is closed.
                 * 
                 * TODO This is not being guarded by a lock so we might not
                 * safely publish the state change to the source iterator when
                 * it is closed.
                 */
                context.getSource().close();
                /**
                 * Ensure that the task is cancelled.
                 * 
                 * Note: This does not appear to be necessary. I am observing
                 * the interrupt of the operator evaluation task regardless.
                 * 
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/707
                 *      (BlockingBuffer.close() does not unblock threads)
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/716
                 *      (Verify that IRunningQuery instances (and nested
                 *      queries) are correctly cancelled when interrupted)
                 */
//                ft.cancel(true/*mayInterruptIfRunning*/);
            }
            // Done.
            return null;
        } // call()

    } // class ChunkTask
    
    /**
     * A delegation pattern which does not pass on the {@link #close()} method.
     * This is used to prevent a {@link PipelineOp} from accidentally closing
     * the query buffer.
     */
    private class NoCloseBuffer<E> extends DelegateBuffer<E> {

        private final UUID queryId;
        private final BOp bop;
        private final int bopId;
        final int partitionId;
        
        public NoCloseBuffer(final UUID queryId, final BOp bop, final int bopId,
                final int partitionId, final IBlockingBuffer<E> delegate) {
        
            super(delegate);
            
            this.queryId = queryId;
            this.bop = bop;
            this.bopId = bopId;
            this.partitionId = partitionId;

        }

        @Override
        public void add(final E e) {
            super.add(e);
            if (SolutionsLog.INFO) {
                SolutionsLog
                        .log(queryId, bop, bopId, partitionId, (IBindingSet[]) e);
            }
        }

        @Override
        public void close() {
            // NOP - This makes sure that the query buffer is not closed.
        }

    } // class NoCloseBuffer

    /**
     * Class traps {@link #add(IBindingSet[])} to handle the {@link IBindingSet}
     * [] chunks as they are generated by the running operator task, invoking
     * {@link ChunkedRunningQuery#handleOutputChunk(BOp, int, IBlockingBuffer)}
     * for each generated chunk to synchronously emit {@link IChunkMessage}s.
     * <p>
     * This use of this class significantly increases the parallelism and
     * throughput of selective queries. If output chunks are not "handled" until
     * the {@link ChunkTask} is complete then the total latency of selective
     * queries is increased dramatically.
     */
    static private class HandleChunkBuffer implements
            IBlockingBuffer<IBindingSet[]> {

        private final ChunkedRunningQuery q;

        private final int bopId;
        
        private final int partitionId;

        private final int sinkId;

//        /**
//         * The desired chunk size. 
//         */
//        private final int chunkCapacity;

        /** The minimum desired chunk size (50% of the {@link #chunkCapacity}). */
        private final int minChunkSize;

        /** The maximum desired chunk size (150% of the {@link #chunkCapacity}) */
        private final int maxChunkSize;
        /**
         * When <code>true</code>, the buffer MAY reorder solutions. When
         * <code>false</code>, it MUST NOT.
         */
        private final boolean reorderSolutions;
        
        private final AtomicInteger sinkMessagesOut;

        private final BOpStats stats;

        private volatile boolean open = true;

        /**
         * A list of small chunks that will be combined into a single chunk. The
         * solutions in this list are always evicted by {@link #flush()}.
         */
        private List<IBindingSet[]> smallChunks = null;
        
        /**
         * The #of elements in the {@link #smallChunks} buffer. Each element is
         * an {@link IBindingSet}, so this is the number of solutions that have
         * not yet been flushed through because we have not yet made up a single
         * decent sized chunk.
         */
        private int chunkSize = 0;

        /**
         * 
         * @param q
         * @param bopId
         * @param sinkId
         * @param chunkCapacity
         *            The target capacity for each chunk.
         * @param reorderSolutions
         *            When <code>true</code>, the buffer MAY reorder solutions.
         *            When <code>false</code>, it MUST NOT.
         * @param sinkMessagesOut
         * @param stats
         */
        public HandleChunkBuffer(final ChunkedRunningQuery q, final int bopId,
                final int partitionId,
                final int sinkId, final int chunkCapacity,
                final boolean reorderSolutions,
                final AtomicInteger sinkMessagesOut, final BOpStats stats) {
            this.q = q;
            this.bopId = bopId;
            this.partitionId = partitionId;
            this.sinkId = sinkId;
//            this.chunkCapacity = chunkCapacity;
            this.reorderSolutions = reorderSolutions;
            this.sinkMessagesOut = sinkMessagesOut;
            this.stats = stats;
            this.minChunkSize = (chunkCapacity >> 1); // 50%
            this.maxChunkSize = chunkCapacity + (chunkCapacity >> 1); // 150%
        }

        /**
         * Handle sink output, sending appropriate chunk message(s). This method
         * MUST NOT block since that will deadlock the caller.
         * <p>
         * Note: This maps output over shards/nodes in s/o.
         * <p>
         * Note: This must be synchronized in case the caller is multi-threaded
         * since it has a possible side effect on the internal buffer.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/798" >
         *      Solution order not always preserved. </a>
         */
        @Override
        public void add(final IBindingSet[] e) {
            
            if(!open)
                throw new BufferClosedException();

            if (SolutionsLog.INFO) {
                SolutionsLog.log(q.getQueryId(), q.getBOp(bopId), bopId,
                        partitionId, (IBindingSet[]) e);
            }

            if (false) {

                /*
                 * Note: Do this INSTEAD if you want to complete disable both
                 * reordering and chunk combination. This should ONLY be used
                 * for debugging. Chunk combination is an important throughput
                 * enhancer.
                 */

                // outputChunk(e);

            } else {
                
                if (reorderSolutions) {

                    // Solutions MAY be reordered.
                    addReorderAllowed(e);

                } else {

                    // Solutions MUST NOT be reordered.
                    addReorderNotAllowed(e);

                }
                
            }
            
        } // add()

        /**
         * We are allowed to reorder the solutions.
         * <p>
         * This will reorder solutions by outputting the current chunk
         * immediately if it is GTE 50% of the target chunkCapacity. This is
         * also a non-blocking code path (no lock is taken in this method).
         * <p>
         * Otherwise, the chunk is added {@link #smallChunks} list. If the #of
         * solutions on the {@link #smallChunks} reaches a threshold, then the
         * {@link #smallChunks} list is converted into a single chunk an
         * evicted. 
         */
        private void addReorderAllowed(final IBindingSet[] e) {
            
            if (e.length < minChunkSize) {

                /*
                 * The caller's array is significantly smaller than the target
                 * chunk size. Append the caller's array to the internal list
                 * and return immediately. The buffered chunks will be copied
                 * through either in a subsequent add() or in flush().
                 */
                
                synchronized (this) {

                    if (chunkSize + e.length > maxChunkSize) {

                        // flush the buffer first.
                        outputBufferedChunk();

                    }

                    if (smallChunks == null)
                        smallChunks = new LinkedList<IBindingSet[]>();

                    // Add to the buffer.
                    smallChunks.add(e);
                    chunkSize += e.length;

                    return;

                }
            
            }

            // output the caller's chunk immediately.
            outputChunk(e);

        }
        
        /**
         * We are not allowed to reorder the solutions.
         * <p>
         * This always outputs solutions in the same order that they are added.
         * In order to avoid pushing through small chunks, it allows the output
         * chunk to be over the target capacity (by 50%).
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/798" >
         *      Solution order not always preserved. </a>
         */
        private void addReorderNotAllowed(final IBindingSet[] e) {

            synchronized (this) {

                if (chunkSize + e.length > maxChunkSize) {

                    /*
                     * The combined chunk would be too large for the buffer.
                     */

                    // Flush the buffer.
                    outputBufferedChunk();

                    if (e.length > minChunkSize) {

                        /*
                         * The internal buffer is empty. The chunk is big
                         * enough. Sent it through immediately.
                         */
                       
                        outputChunk(e);
                        
                        return;
                        
                    }
                    
                }

                /*
                 * Add the chunk to the internal buffer.
                 */

                if (smallChunks == null)
                    smallChunks = new LinkedList<IBindingSet[]>();

                // Add to the buffer.
                smallChunks.add(e);
                chunkSize += e.length;

            } // synchronized(this)

        }
        
        /**
         * Output a chunk, updating the counters.
         * 
         * @param e
         *            The chunk.
         */
        private void outputChunk(final IBindingSet[] e) {

            final int chunkSize = e.length;
            
            stats.unitsOut.add(chunkSize);
            
            stats.chunksOut.increment();
            
            final int messagesOut = q.getChunkHandler().handleChunk(q, bopId,
                    sinkId, e);
            
            sinkMessagesOut.addAndGet(messagesOut);
            
        }
        
        /**
         * Output the internal buffer.
         */
        synchronized // Note: has side-effect on internal buffer. 
        private void outputBufferedChunk() {
            if (smallChunks == null || chunkSize == 0) {
                return;
            }
            if (smallChunks.size() == 1) {
                // directly output a single small chunk.
                outputChunk(smallChunks.get(0));
                chunkSize = 0;
                smallChunks = null;
                return;
            }
            // exact fit buffer.
            final IBindingSet[] chunk = new IBindingSet[chunkSize];
            // copy the small chunks into the buffer.
            int destPos = 0;
            for (IBindingSet[] e : smallChunks) {
                System.arraycopy(e/* src */, 0/* srcPos */, chunk/* dest */,
                        destPos, e.length/* length */);
                destPos += e.length;
            }
            outputChunk(chunk);
            chunkSize = 0;
            smallChunks = null;
        }

        @Override
        synchronized // Note: possible side-effect on internal buffer.
        public long flush() {
            if (open)
                outputBufferedChunk();
            return 0L;
//          return sink.flush();
      }

        @Override
        public void abort(final Throwable cause) {
            open = false;
            q.halt(cause);
//            sink.abort(cause);
        }

        @Override
        public void close() {
//            sink.close();
            open = false;
        }

        @Override
        public Future getFuture() {
//            return sink.getFuture();
            return null;
        }

        @Override
        public boolean isEmpty() {
            return true;
//            return sink.isEmpty();
        }

        @Override
        public boolean isOpen() {
            return open && !q.isDone();
//            return sink.isOpen();
        }

        @Override
        public IAsynchronousIterator<IBindingSet[]> iterator() {
            throw new UnsupportedOperationException();
//            return sink.iterator();
        }

        @Override
        public void reset() {
//            sink.reset();
        }

        @Override
        public void setFuture(Future future) {
            throw new UnsupportedOperationException();
//            sink.setFuture(future);
        }

        @Override
        public int size() {
            return 0;
//            return sink.size();
        }

    } // class HandleChunkBuffer
    
    /**
     * {@link Runnable} sends the {@link IQueryClient} a message indicating that
     * some query has halted on some node. This is used to send such messages
     * asynchronously.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static class SendHaltMessageTask implements Runnable {

        private final IQueryClient clientProxy;

        private final IHaltOpMessage msg;

        private final ChunkedRunningQuery q;

        public SendHaltMessageTask(final IQueryClient clientProxy,
                final IHaltOpMessage msg, final ChunkedRunningQuery q) {

            if (clientProxy == null)
                throw new IllegalArgumentException();

            if (msg == null)
                throw new IllegalArgumentException();

            if (q == null)
                throw new IllegalArgumentException();

            this.clientProxy = clientProxy;

            this.msg = msg;

            this.q = q;

        }

        @Override
        public void run() {
			try {
				if (q.isController()) {
					/*
					 * Local method call.
					 * 
					 * Note: This MUST NOT be done using RMI when the operator
					 * is using shared state to provide live statistics update
					 * for operators which evaluate on the query controller.
					 * Using RMI here results in double-counting with each new
					 * haltOp and can cause queries to terminate too soon
					 * because they have false #of chunks and units out
					 * reported.
					 * 
					 * @see https://sourceforge.net/apps/trac/bigdata/ticket/464
					 */
					q.haltOp(msg);
				} else {
					// RMI.
					clientProxy.haltOp(msg);
				}
            } catch (Throwable e) {
                if (!isRootCauseInterrupt(e)) {
                	/*
                	 * @see https://sourceforge.net/apps/trac/bigdata/ticket/479
                	 */
                    log.error("Could not notify query controller: " + e, e);
                }
                q.cancel(true/* mayInterruptIfRunning */);
            }
        }

    }

    @Override
    protected boolean cancelRunningOperators(final boolean mayInterruptIfRunning) {
        
        boolean cancelled = false;
        
        final Iterator<ConcurrentHashMap<ChunkFutureTask,ChunkFutureTask>> fitr = operatorFutures.values().iterator();
        
        while (fitr.hasNext()) {
        
            final ConcurrentHashMap<ChunkFutureTask,ChunkFutureTask> set = fitr.next();

            for(ChunkFutureTask f : set.keySet()) {

                /*
                 * Note: This can wind up setting the interrupt status on the
                 * thread in which it is called. For example, SLICE will call
                 * halt(), which calls AbstractRunningQuery#cancel(), which
                 * calls this method. Since the SliceOp() is still running it's
                 * future will be cancelled here. The code in
                 * AbstractRunningQuery#cancel() needs to be robust in the face
                 * of that interrupt.
                 */

                if (f.cancel(mayInterruptIfRunning))
                    cancelled = true;
                
            }
            
        }
     
        return cancelled;
        
    }

    @Override
    protected void releaseAcceptedMessages() {

        for (Map.Entry<BSBundle, BlockingQueue<IChunkMessage<IBindingSet>>> e : operatorQueues
                .entrySet()) {

            final BlockingQueue<IChunkMessage<IBindingSet>> queue = e.getValue();

            if (queue.isEmpty())
                continue;
            
            final LinkedList<IChunkMessage<IBindingSet>> c = new LinkedList<IChunkMessage<IBindingSet>>();

            queue.drainTo(c);

            for (IChunkMessage<IBindingSet> msg : c) {

                msg.release();
                
            }
            
        }
     
    }

    /**
     * Return a summary of the work queue for the operators in this query
     * (non-blocking).
     * 
     * @return A map whose keys are the operator identifiers and whose values
     *         provide summary statistics for the work queue(s) for those
     *         operators.
     *         <p>
     *         Note: For a cluster, there is one work queue per (operator,shard)
     *         pair.
     */
    protected Map<Integer/* bopId */, QueueStats> getQueueStats() {
        
        final Map<Integer, QueueStats> map = new HashMap<Integer, QueueStats>();

        for (Map.Entry<BSBundle, BlockingQueue<IChunkMessage<IBindingSet>>> e : operatorQueues
                .entrySet()) {

            final BSBundle bundle = e.getKey();

            final BlockingQueue<IChunkMessage<IBindingSet>> queue = e
                    .getValue();

            @SuppressWarnings("unchecked")
            final IChunkMessage<IBindingSet>[] chunks = queue
                    .toArray(new IChunkMessage[0]);

            if (chunks.length == 0)
                continue;

            final Integer bopId = Integer.valueOf(bundle.bopId);

            QueueStats stats = map.get(bopId);

            if (stats == null) {

                map.put(bopId, stats = new QueueStats());

            }
            
            stats.shardSet.add(bundle.shardId);
            
            for (IChunkMessage<IBindingSet> msg : chunks) {

                stats.chunkCount++;

                stats.solutionCount += msg.getSolutionCount();

            }

        }

        return map;
        
    }
    
    /**
     * Factory returns the effective {@link IChunkHandler} for this query.
     * 
     * @param queryEngine
     * @param query
     * 
     * @return The effective {@link IChunkHandler} policy.
     * 
     * @see QueryHints#QUERY_ENGINE_CHUNK_HANDLER
     * @see BLZG-533 Vector query engine on native heap.
     */
    protected IChunkHandler getChunkHandler(final QueryEngine queryEngine, final PipelineOp query) {

        /**
         * Use whatever is configured by the query hints.
         */
        return query.getProperty(
                QueryEngine.Annotations.CHUNK_HANDLER,
                QueryHints.DEFAULT_QUERY_ENGINE_CHUNK_HANDLER);
        
    }

    /**
     * Return the effective {@link IChunkHandler} for this query.
     * 
     * @see BLZG-533 Vector query engine on native heap.
     */
    final protected IChunkHandler getChunkHandler() {
        return chunkHandler;
    }

}
