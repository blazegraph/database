/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 31, 2010
 */
package com.bigdata.bop.engine;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NoSuchBOpException;
import com.bigdata.bop.PipelineOp;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IMultiSourceAsynchronousIterator;
import com.bigdata.relation.accesspath.MultiSourceSequentialAsynchronousIterator;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.Memoizer;

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
 * 
 * @todo The challenge with this implementation is managing the amount of data
 *       buffered on the JVM heap without introducing control structures which
 *       can result in deadlock or starvation. One way to manage this is to move
 *       the data off of the JVM heap onto direct ByteBuffers and then
 *       potentially spilling blocks to disk, e.g., using an RWStore based cache
 *       pattern.
 */
public class ChunkedRunningQuery extends AbstractRunningQuery {

    private final static transient Logger log = Logger
            .getLogger(ChunkedRunningQuery.class);

    /**
     * Logger for the {@link ChunkTask}.
     */
    private final static Logger chunkTaskLog = Logger
            .getLogger(ChunkTask.class);

//	/**
//	 * The maximum number of operator tasks which may be concurrently executed
//	 * for a given (bopId,shardId).
//	 * 
//	 * @see QueryEngineTestAnnotations#MAX_CONCURRENT_TASKS_PER_OPERATOR_AND_SHARD
//	 */
//    final private int maxConcurrentTasksPerOperatorAndShard;

//	/**
//	 * The maximum #of concurrent tasks for this query across all operators and
//	 * shards.
//	 * 
//	 * Note: This is not a safe option and MUST be removed. It is possible for
//	 * N-1 tasks to backup with the Nth task not running due to concurrent
//	 * execution of some of the N-t tasks.
//	 */
//	final private int maxConcurrentTasks = 10;

	/*
	 * FIXME Explore the use of this semaphore to limit the maximum #of messages
	 * further. (Note that placing a limit on messages would allow us to buffer
	 * potentially many chunks. That could be solved by making LocalChunkMessage
	 * transparent in terms of the #of chunks or _binding_sets_ which it is
	 * carrying, but let's take this one step at a time).
	 * 
	 * The first issue is ensuring that the query continue to make progress when
	 * a semaphore with a limited #of permits is introduced. This is because the
	 * ChunkFutureTask only attempts to schedule the next task for a given
	 * (bopId,shardId) but we could have failed to accept outstanding work for
	 * any of a number of operator/shard combinations. Likewise, the QueryEngine
	 * tells the RunningQuery to schedule work each time a message is dropped
	 * onto the QueryEngine, but the signal to execute more work is lost if the
	 * permits were not available immediately.
	 * 
	 * One possibility would be to have a delayed retry. Another would be to
	 * have ChunkTaskFuture try to run *any* messages, not just messages for the
	 * same (bopId,shardId).
	 * 
	 * Also, when scheduling work, there needs to be some bias towards the
	 * downstream operators in the query plan in order to ensure that they get a
	 * chance to clear work from upstream operators. This suggests that we might
	 * carry an order[] and use it to scan the work queue -- or make the work
	 * queue a priority heap using the order[] to place a primary sort over the
	 * bopIds in terms of the evaluation order and letting the shardIds fall in
	 * increasing shard order so we have a total order for the priority heap (a
	 * total order may also require a tie breaker, but I think that the priority
	 * heap allows ties).
	 * 
	 * This concept of memory overhead and permits would be associated with the
	 * workload waiting on a given node for processing. (In scale-out, we do not
	 * care how much data is moving in the cluster, only how much data is
	 * challenging an individual machine).
	 * 
	 * This emphasize again why we need to get the data off of the Java heap.
	 * 
	 * The same concept should apply for chained buffers.  Maybe one way to do
	 * this is to allocate a fixed budget to each query for the Java heap and
	 * the C heap and then the query blocks or goes to disk.
	 */
//	/**
//	 * The maximum number of binding sets which may be outstanding before a task
//	 * which is producing binding sets will block. This value may be used to
//	 * limit the memory demand of a query in which some operators produce
//	 * binding sets faster than other operators can consume them.
//	 * 
//	 * @todo This could be generalized to consider the Java heap separately from
//	 *       the native heap as we get into the use of native ByteBuffers to
//	 *       buffer intermediate results.
//	 * 
//	 * @todo This is expressed in terms of messages and not {@link IBindingSet}s
//	 *       because the {@link LocalChunkMessage} does not self-report the #of
//	 *       {@link IBindingSet}s (or chunks).  [It should really be bytes on the
//   *       heap even if we can count binding sets and #s of bindings, but we
//   *       do not serialize all binding sets so we have to have one measure 
//   *       for serialized and one measure for live objects.]
//	 */
//	final private int maxOutstandingMessageCount = 100;
//
//	/**
//	 * A counting semaphore used to limit the #of outstanding binding set chunks
//	 * which may be buffered before a producer will block when trying to emit
//	 * another chunk.
//	 * 
//	 * @see HandleChunkBuffer#outputChunk(IBindingSet[])
//	 * @see #scheduleNext(BSBundle)
//	 * 
//	 * @see #maxOutstandingMessageCount
//	 */
//	final private Semaphore outstandingMessageSemaphore = new Semaphore(maxOutstandingMessageCount);
    
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
     */
    private final Map<BSBundle, BlockingQueue<IChunkMessage<IBindingSet>>> operatorQueues;
    
//    /**
//     * When running in stand alone, we can chain together the operators and have
//     * much higher throughput. Each operator has an {@link BlockingBuffer} which
//     * is essentially its input queue. The operator will drain its input queue
//     * using {@link BlockingBuffer#iterator()}.
//     * <p>
//     * Each operator closes its {@link IBlockingBuffer} sink(s) once its own
//     * source has been closed and it has finished processing that source. Since
//     * multiple producers can target the same operator, we need a means to
//     * ensure that the source for the target operator is not closed until each
//     * producer which targets that operator has closed its corresponding sink.
//     * <p>
//     * In order to support this many-to-one producer/consumer pattern, we wrap
//     * the input queue (a {@link BlockingBuffer}) for each operator having
//     * multiple sources with a {@link MultiplexBlockingBuffer}. This class gives
//     * each producer their own view on the underlying {@link BlockingBuffer}.
//     * The underlying {@link BlockingBuffer} will not be closed until all
//     * source(s) have closed their view of that buffer. This collection keeps
//     * track of the {@link MultiplexBlockingBuffer} wrapping the
//     * {@link BlockingBuffer} which is the input queue for each operator.
//     * <p>
//     * The input queues themselves are {@link BlockingBuffer} objects. Those
//     * objects are available from this map using
//     * {@link MultiplexBlockingBuffer#getBackingBuffer()}. These buffers are
//     * pre-allocated by {@link #populateInputBufferMap(BOp)}.
//     * {@link #startTasks(BOp)} is responsible for starting the operator tasks
//     * in a "back-to-front" order. {@link #startQuery(IChunkMessage)} kicks off
//     * the query and invokes {@link #startTasks(BOp)} to chain the input queues
//     * and output queues together (when so chained, the output queues are skins
//     * over the input queues obtained from {@link MultiplexBlockingBuffer}).
//     * 
//     * FIXME The inputBufferMap will let us construct consumer producer chains
//     * where the consumer _waits_ for all producer(s) which target the consumer
//     * to close the sink associated with that consumer. Unlike when attaching an
//     * {@link IChunkMessage} to an already running operator, the consumer will
//     * NOT terminate (due to lack up input) until each running producer
//     * terminating that consumer terminates. This will improve concurrency,
//     * result in fewer task instances, and have better throughput than attaching
//     * a chunk to an already running task. However, in scale-out we will have
//     * tasks running on different nodes so we can not always chain together the
//     * producer and consumer in this tightly integrated manner.
//     */
//    final private ConcurrentHashMap<Integer/*operator*/, MultiplexBlockingBuffer<IBindingSet[]>/*inputQueue*/> inputBufferMap;


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
            final PipelineOp query) {

        super(queryEngine, queryId, controller, clientProxy, query);
        
////        combineReceivedChunks = query.getProperty(
////                QueryEngineTestAnnotations.COMBINE_RECEIVED_CHUNKS,
////                QueryEngineTestAnnotations.DEFAULT_COMBINE_RECEIVED_CHUNKS);

//		this.maxConcurrentTasksPerOperatorAndShard = 300;
//		this.maxConcurrentTasksPerOperatorAndShard = query
//				.getProperty(
//						QueryEngineTestAnnotations.MAX_CONCURRENT_TASKS_PER_OPERATOR_AND_SHARD,
//						QueryEngineTestAnnotations.DEFAULT_MAX_CONCURRENT_TASKS_PER_OPERATOR_AND_SHARD);
        
        this.operatorFutures = new ConcurrentHashMap<BSBundle, ConcurrentHashMap<ChunkFutureTask,ChunkFutureTask>>();
        
        this.operatorQueues = new ConcurrentHashMap<BSBundle, BlockingQueue<IChunkMessage<IBindingSet>>>();
        
//        /*
//         * Setup the BOpStats object for each pipeline operator in the query.
//         */
//        if (controller) {
//            
////            runState = new RunState(this);
//
////            statsMap = new ConcurrentHashMap<Integer, BOpStats>();
////
////            populateStatsMap(query);
//
////			/*
////			 * FIXME Review the concept of mutation queries. It used to be that
////			 * queries could only either read or write. Now we have access paths
////			 * which either read or write and each query could use zero or more
////			 * such access paths.
////			 */
////            if (true/*!query.isMutation()*/) {
////
////            	// read-only query.
////            	
////                final BOpStats queryStats = statsMap.get(query.getId());
//
////                queryBuffer = new BlockingBufferWithStats<IBindingSet[]>(query,
////                        queryStats);
////
////                queryIterator = new QueryResultIterator<IBindingSet[]>(this,
////                        queryBuffer.iterator());
//
////            } else {
////
////                // Note: Not used for mutation queries.
////                queryBuffer = null;
////                queryIterator = null;
//
//            }
//
//        } else {
//
////            runState = null; // Note: only on the query controller.
////            statsMap = null; // Note: only on the query controller.
////            queryBuffer = null; // Note: only on the query controller.
////            queryIterator = null; // Note: only when queryBuffer is defined.
//            
//        }

    }

//    /**
//     * Take a chunk generated by some pass over an operator and make it
//     * available to the target operator. How this is done depends on whether the
//     * query is running against a standalone database or the scale-out database.
//     * <p>
//     * Note: The return value is used as part of the termination criteria for
//     * the query.
//     * <p>
//     * The default implementation supports a standalone database. The generated
//     * chunk is left on the Java heap and handed off synchronously using
//     * {@link QueryEngine#acceptChunk(IChunkMessage)}. That method will queue
//     * the chunk for asynchronous processing.
//     * 
//     * @param bop
//     *            The operator which wrote on the sink.
//     * @param sinkId
//     *            The identifier of the target operator.
//     * @param sink
//     *            The intermediate results to be passed to that target operator.
//     * 
//     * @return The #of {@link IChunkMessage} sent. This will always be ONE (1)
//     *         for scale-up. For scale-out, there will be at least one
//     *         {@link IChunkMessage} per index partition over which the
//     *         intermediate results were mapped.
//     */
//    protected <E> int handleOutputChunk(final BOp bop, final int sinkId,
//            final IBlockingBuffer<IBindingSet[]> sink) {
//
//        if (bop == null)
//            throw new IllegalArgumentException();
//
//        if (sink == null)
//            throw new IllegalArgumentException();
//
//        if (inputBufferMap != null && inputBufferMap.get(sinkId) != null) {
//            /*
//             * FIXME The sink is just a wrapper for the input buffer so we do
//             * not need to do anything to propagate the data from one operator
//             * to the next.
//             */
//            return 0;
//        }
//        
//        /*
//         * Note: The partitionId will always be -1 in scale-up.
//         */
//        final int partitionId = -1;
//
//        final boolean oneMessagePerChunk = bop.getProperty(
//                QueryEngineTestAnnotations.ONE_MESSAGE_PER_CHUNK,
//                QueryEngineTestAnnotations.DEFAULT_ONE_MESSAGE_PER_CHUNK);
//
//        if (oneMessagePerChunk) {
//
//            final IAsynchronousIterator<IBindingSet[]> itr = sink.iterator();
//
//            int nchunks = 0;
//
//            while (itr.hasNext()) {
//
//                final IBlockingBuffer<IBindingSet[]> tmp = new BlockingBuffer<IBindingSet[]>(
//                        1);
//
//                tmp.add(itr.next());
//
//                tmp.close();
//
//                final LocalChunkMessage<IBindingSet> chunk = new LocalChunkMessage<IBindingSet>(
//                        clientProxy, queryId, sinkId, partitionId, tmp
//                                .iterator());
//
//                queryEngine.acceptChunk(chunk);
//
//                nchunks++;
//
//            }
//
//            return nchunks;
//
//        }
//
//        final LocalChunkMessage<IBindingSet> chunk = new LocalChunkMessage<IBindingSet>(
//                clientProxy, queryId, sinkId, partitionId, sink.iterator());
//
//        queryEngine.acceptChunk(chunk);
//
//        return 1;
//
//    }

//    /**
//     * Consume zero or more chunks in the input queue for this query. The
//     * chunk(s) will either be assigned to an already running task for the
//     * target operator or they will be assigned to new tasks.
//     * 
//     * FIXME Drain the input queue, assigning any chunk waiting to a task. If
//     * the task is already running, then add the chunk to that task. Otherwise
//     * start a new task.
//     */
//    protected void consumeChunk() {
//        final IChunkMessage<IBindingSet> msg = chunksIn.poll();
//        if (msg == null)
//            return;
//        try {
//            if (!msg.isMaterialized())
//                throw new IllegalStateException();
//            if (log.isTraceEnabled())
//                log.trace("Accepted chunk: " + msg);
//            final BSBundle bundle = new BSBundle(msg.getBOpId(), msg
//                    .getPartitionId());
////            /*
////             * Look for instance of this task which is already running.
////             */
////            final ChunkFutureTask chunkFutureTask = operatorFutures.get(bundle);
////            if (!queryEngine.isScaleOut() && chunkFutureTask != null) {
////                /*
////                 * Attempt to atomically attach the message as another src.
////                 */
////                if (chunkFutureTask.chunkTask.context.addSource(msg
////                        .getChunkAccessor().iterator())) {
////                    /*
////                     * @todo I've commented this out for now. I am not convinced
////                     * that we need to update the RunState when accepting
////                     * another message into a running task. This would only
////                     * matter if haltOp() reported the #of consumed messages,
////                     * but RunState.haltOp() just decrements the #of available
////                     * messages by one which balances startOp(). Just because we
////                     * attach more messages dynamically does not mean that we
////                     * need to report that back to the query controller as long
////                     * as haltOp() balances startOp().
////                     */
//////                    lock.lock();
//////                    try {
//////                        /*
//////                         * message was added to a running task.
//////                         * 
//////                         * FIXME This needs to be an RMI in scale-out back to
//////                         * the query controller so it can update the #of
//////                         * messages which are being consumed by this task.
//////                         * However, doing RMI here will add latency into the
//////                         * thread submitting tasks for evaluation and the
//////                         * coordination overhead of addSource() in scale-out may
//////                         * be too high. However, if we do not combine sources in
//////                         * scale-out then we may have too much overhead in terms
//////                         * of the #of running tasks with few tuples per task.
//////                         * Another approach is the remote async iterator with
//////                         * multiple sources (parallel multi source iterator).
//////                         * 
//////                         * FIXME This code path is NOT being taken in scale-out
//////                         * right now since it would not get the message to the
//////                         * query controller. We will need to add addSource() to
//////                         * IQueryClient parallel to startOp() and haltOp() for
//////                         * this to work.
//////                         */
//////                        runState.addSource(msg, queryEngine.getServiceUUID());
//////                        return;
//////                    } finally {
//////                        lock.unlock();
//////                    }
////                }
////            }
//            // wrap runnable.
//            final ChunkFutureTask ft = new ChunkFutureTask(new ChunkTask(msg));
//            /*
//             * FIXME Rather than queue up a bunch of operator tasks for the same
//             * (bopId,partitionId), this blocks until the current operator task
//             * is done and then submits the new one. This prevents us from
//             * allocating 100s of threads for complex queries and prevents us
//             * from losing track of the Futures of those tasks. However, since
//             * this is happening in the caller's thread the QueryEngine is not
//             * making any progress while we are blocked. A pattern which hooks
//             * the Future and then submits the next task (such as the
//             * LatchedExecutor) would fix this. This might have to be one
//             * LatchedExecutor per pipeline operator.
//             */
//            FutureTask<Void> existing = operatorFutures.putIfAbsent(bundle, ft);
//            if (existing != null) {
//                existing.get();
//                if (!operatorFutures.remove(bundle, existing))
//                    throw new AssertionError();
//                if (operatorFutures.put(bundle, ft) != null)
//                    throw new AssertionError();
//            }
////            // add to list of active futures for this query.
////            if (operatorFutures.put(bundle, ft) != null) {
////                /*
////                 * Note: This can cause the FutureTask to be accessible (above)
////                 * before startOp() has been called for that ChunkTask (the
////                 * latter occurs when the chunk task actually runs.) This a race
////                 * condition has been resolved in RunState by allowing
////                 * addSource() even when there is no registered task running for
////                 * that [bopId].
////                 * 
////                 * FIXME This indicates that we have more than one future for
////                 * the same (bopId,shardId). When this is true we are losing
////                 * track of Futures with the consequence that we can not
////                 * properly cancel them. Instead of losing track like this, we
////                 * should be targeting the running operator instance with the
////                 * new chunk. This needs to be done atomically, e.g., using the
////                 * [lock].
////                 * 
////                 * Even if we only have one task per operator in standalone and
////                 * we attach chunks to an already running task in scale-out,
////                 * there is still the possibility in scale-out that a task may
////                 * have closed its source but still be running, in which case we
////                 * would lose the Future for the already running task when we
////                 * start a new task for the new chunk for the target operator.
////                 */
////                // throw new AssertionError();
////            }
//            // submit task for execution (asynchronous).
//            queryEngine.execute(ft);
//        } catch (Throwable ex) {
//            // halt query.
//            throw new RuntimeException(halt(ex));
//        }
//    }

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

        final BSBundle bundle = new BSBundle(msg.getBOpId(), msg
                .getPartitionId());

        lock.lock();

        try {

            if (isDone()) {
            	// The query is no longer running.
            	return false;
                //throw new RuntimeException(ERR_QUERY_DONE, future.getCause());
            }

            BlockingQueue<IChunkMessage<IBindingSet>> queue = operatorQueues
                    .get(bundle);

            if (queue == null) {

                queue = new LinkedBlockingQueue<IChunkMessage<IBindingSet>>(/* unbounded */);

                operatorQueues.put(bundle, queue);

            }

            queue.add(msg);

            return true;
            
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
    protected void consumeChunk() {
        lock.lock();
        try {
			for (BSBundle bundle : operatorQueues.keySet()) {
				try {
					scheduleNext(bundle);
				} catch (RuntimeException ex) {
					halt(ex);
				}
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Examine the input queue for the (bopId,partitionId). If there is work
     * available and no task is currently running, then drain the work queue and
     * submit a task to consume that work.
     * 
     * @param bundle
     *            The (bopId,partitionId).
     *            
     * @return <code>true</code> if a new task was started.
     */
    private boolean scheduleNext(final BSBundle bundle) {
        if (bundle == null)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            // Make sure the query is still running.
			if(isDone())
				return false;
			// Is there a Future for this (bopId,partitionId)?
			ConcurrentHashMap<ChunkFutureTask, ChunkFutureTask> map = operatorFutures
					.get(bundle);
			if (map != null) {
				int nrunning = 0;
				for (ChunkFutureTask cft : map.keySet()) {
					if (cft.isDone())
						map.remove(cft);
					nrunning++;
				}
				if (map.isEmpty())
					operatorFutures.remove(bundle);
                /*
                 * FIXME If we allow a limit on the concurrency then we need to
                 * manage things in order to guarantee that deadlock can not
                 * arise.
                 */
//				if (nrunning > maxConcurrentTasksPerOperatorAndShard) {
//					// Too many already running.
//					return false;
//				}
			}
//			if (runState.getTotalRunningCount() > maxConcurrentTasks) {
//				// Too many already running.
//				return false;
//			}
//			{
//				/*
//				 * Verify that we can acquire sufficient permits to do some
//				 * work.
//				 */
//				final BlockingQueue<IChunkMessage<IBindingSet>> queue = operatorQueues
//						.get(bundle);
//				if (queue == null || queue.isEmpty()) {
//					// No work.
//					return false;
//				}
//				// The queue could be increased, but this will be its minimum size.
//				final int minQueueSize = queue.size();
//				if(!outstandingMessageSemaphore.tryAcquire(minQueueSize)) {
//					// Not enough permits.
//					System.err.println("Permits: required=" + minQueueSize
//							+ ", available="
//							+ outstandingMessageSemaphore.availablePermits()
//							+ ", bundle=" + bundle);
//					return false;
//				}
//			
//			}
			// Remove the work queue for that (bopId,partitionId).
            final BlockingQueue<IChunkMessage<IBindingSet>> queue = operatorQueues
                    .remove(bundle);
            if (queue == null || queue.isEmpty()) {
                // no work
                return false;
            }
            // Drain the work queue for that (bopId,partitionId).
            final List<IChunkMessage<IBindingSet>> messages = new LinkedList<IChunkMessage<IBindingSet>>();
            queue.drainTo(messages);
            final int nmessages = messages.size();
            /*
             * Combine the messages into a single source to be consumed by a
             * task.
             */
            int nchunks = 1;
            final IMultiSourceAsynchronousIterator<IBindingSet[]> source = new MultiSourceSequentialAsynchronousIterator<IBindingSet[]>(messages.remove(0).getChunkAccessor().iterator());
            for (IChunkMessage<IBindingSet> msg : messages) {
                source.add(msg.getChunkAccessor().iterator());
                nchunks++;
            }
			/*
			 * Create task to consume that source.
			 */
			final ChunkFutureTask cft = new ChunkFutureTask(new ChunkTask(
					bundle.bopId, bundle.shardId, nmessages, source));
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
			getQueryEngine().execute(cft);
			return true;
        } finally {
            lock.unlock();
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
    private class ChunkFutureTask extends FutureTask<Void> {

        private final ChunkTask t;

        public ChunkFutureTask(final ChunkTask chunkTask) {

            /*
             * Note: wraps chunk task to communicate run state changes back to
             * the query controller.
             */
            super(new ChunkTaskWrapper(chunkTask), null/* result */);

            this.t = chunkTask;

        }

        public void run() {

            super.run();

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

            // Schedule another task if any messages are waiting.
            ChunkedRunningQuery.this.scheduleNext(new BSBundle(
                    t.bopId, t.partitionId));
        
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
        
        public void run() {

            final UUID serviceId = getQueryEngine().getServiceUUID();
            try {

                /*
                 * Notify query controller that operator task will start (sync
                 * notification).
                 * 
                 * Note: This is potentially an RMI back to the controller. It
                 * is invoked from within the running task in order to remove
                 * the latency for that RMI from the thread which submits tasks
                 * to consume chunks.
                 */
                getQueryController().startOp(new StartOpMessage(getQueryId(), t.bopId,
                        t.partitionId, serviceId, t.messagesIn));

				/*
				 * Run the operator task.
				 */
				final long begin = System.currentTimeMillis();
				try {
					t.call();
				} finally {
					t.context.getStats().elapsed.add(System.currentTimeMillis()
							- begin);
				}

                /*
                 * Queue task to notify the query controller that operator task
                 * did run (async notification).
                 */
                final HaltOpMessage msg = new HaltOpMessage(getQueryId(), t.bopId,
                        t.partitionId, serviceId, null/* cause */, t.sinkId,
                        t.sinkMessagesOut.get(), t.altSinkId,
                        t.altSinkMessagesOut.get(), t.context.getStats());
                try {
                    t.context.getExecutorService().execute(
                            new SendHaltMessageTask(getQueryController(), msg,
                                    ChunkedRunningQuery.this));
                } catch (RejectedExecutionException ex) {
                    // e.g., service is shutting down.
                    log.error("Could not send message: " + msg, ex);
                }
                
            } catch (Throwable ex1) {

				/*
				 * Note: SliceOp will cause other operators to be interrupted
				 * during normal evaluation so it is not useful to log an
				 * InterruptedException @ ERROR.
				 */
				if (!InnerCause.isInnerCause(ex1, InterruptedException.class)
		 		 && !InnerCause.isInnerCause(ex1, BufferClosedException.class)
		 		 ) {
					// Log an error.
                    log.error("queryId=" + getQueryId() + ", bopId=" + t.bopId
                            + ", bop=" + t.bop, ex1);
                }

                /*
                 * Mark the query as halted on this node regardless of whether
                 * we are able to communicate with the query controller.
                 * 
                 * Note: Invoking halt(t) here will log an error. This logged
                 * error message is necessary in order to catch errors in
                 * clientProxy.haltOp() (above and below).
                 */
                final Throwable firstCause = halt(ex1);

                final HaltOpMessage msg = new HaltOpMessage(getQueryId(), t.bopId,
                        t.partitionId, serviceId, firstCause, t.sinkId,
                        t.sinkMessagesOut.get(), t.altSinkId,
                        t.altSinkMessagesOut.get(), t.context.getStats());
                try {
                    /*
                     * Queue a task to send the halt message to the query
                     * controller (async notification).
                     */
                    t.context.getExecutorService().execute(
                            new SendHaltMessageTask(getQueryController(), msg,
                                    ChunkedRunningQuery.this));
                } catch (RejectedExecutionException ex) {
					// e.g., service is shutting down.
					if (log.isInfoEnabled())
						log.info("Could not send message: " + msg, ex);
				} catch (Throwable ex) {
					log
							.error("Could not send message: " + msg + " : "
									+ ex, ex);
				}

            }
        
        } // runOnce()
        
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
         * @param source
         *            Where the task will read its inputs.
         */
        public ChunkTask(final int bopId, final int partitionId,
                final int messagesIn,
                final IAsynchronousIterator<IBindingSet[]> src) {

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

            {
                // altSink (null when not specified).
                final Integer altSinkId = (Integer) op
                        .getProperty(PipelineOp.Annotations.ALT_SINK_REF);
                final Integer altSinkGroup = (Integer) op
                        .getProperty(PipelineOp.Annotations.ALT_SINK_GROUP);
                if (altSinkId != null && altSinkGroup != null)
                    throw new RuntimeException(
                            "Annotations are mutually exclusive: "
                                    + PipelineOp.Annotations.ALT_SINK_REF
                                    + " and "
                                    + PipelineOp.Annotations.ALT_SINK_GROUP);
                if (altSinkGroup != null) {
                    /*
                     * Lookup the first pipeline op in the conditional binding
                     * group and use its bopId as the altSinkId.
                     */
                    this.altSinkId = BOpUtility.getFirstBOpIdForConditionalGroup(
                            getQuery(), altSinkGroup);
                } else {
                    // MAY be null.
                    this.altSinkId = altSinkId;
                }
            }

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
            }
            assert stats != null;

            // The groupId (if any) for this operator.
            final Integer fromGroupId = (Integer) op
                    .getProperty(PipelineOp.Annotations.CONDITIONAL_GROUP);

            if (p == null) {
                sink = getQueryBuffer();
            } else {
                final BOp targetOp = getBOpIndex().get(sinkId);
                final Integer toGroupId = (Integer) targetOp
                        .getProperty(PipelineOp.Annotations.CONDITIONAL_GROUP);
                sink = newBuffer(op, sinkId, new SinkTransitionMetadata(
                        fromGroupId, toGroupId, true/* isSink */),
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
                final BOp targetOp = getBOpIndex().get(altSinkId);
                final Integer toGroupId = (Integer) targetOp
                        .getProperty(PipelineOp.Annotations.CONDITIONAL_GROUP);
                altSink = newBuffer(op, altSinkId, new SinkTransitionMetadata(
                        fromGroupId, toGroupId, false/* isSink */),
                        altSinkMessagesOut, stats);
            }

            // context : @todo pass in IChunkMessage or IChunkAccessor
            context = new BOpContext<IBindingSet>(ChunkedRunningQuery.this,
                    partitionId, stats, src, sink, altSink);

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
                final int sinkId,
                final SinkTransitionMetadata sinkTransitionMetadata,
                final AtomicInteger sinkMessagesOut, final BOpStats stats) {

//            final MultiplexBlockingBuffer<IBindingSet[]> factory = inputBufferMap == null ? null
//                    : inputBufferMap.get(sinkId);
//
//            if (factory != null) {
//
//                return factory.newInstance();
//
//            }

//            return new HandleChunkBuffer(sinkId, sinkMessagesOut, op
//                    .newBuffer(stats));

            /*
             * FIXME The buffer allocated here is useless unless we play games
             * in HandleChunkBuffer to combine chunks or run a thread which
             * drains chunks from all operator tasks (but the task can not
             * complete until it is fully drained).
             */
//            final IBlockingBuffer<IBindingSet[]> b = new BlockingBuffer<IBindingSet[]>(
//                    op.getChunkOfChunksCapacity(), op.getChunkCapacity(), op
//                            .getChunkTimeout(),
//                    BufferAnnotations.chunkTimeoutUnit);

            return new SinkTransitionBuffer(new HandleChunkBuffer(
                    ChunkedRunningQuery.this, bopId, sinkId, op
                            .getChunkCapacity(), sinkMessagesOut, stats),
                    sinkTransitionMetadata);

        }

        /**
         * Evaluate the {@link IChunkMessage}.
         */
        public Void call() throws Exception {
            if (log.isDebugEnabled())
                log.debug("Running chunk: " + this);
            ft.run(); // run
            ft.get(); // verify success
            // Done.
            return null;
        } // call()

    } // class ChunkTask
    
    /**
     * Class traps {@link #add(IBindingSet[])} to handle the IBindingSet[]
     * chunks as they are generated by the running operator task, invoking
     * {@link ChunkedRunningQuery#handleOutputChunk(BOp, int, IBlockingBuffer)} for
     * each generated chunk to synchronously emit {@link IChunkMessage}s.
     * <p>
     * This use of this class significantly increases the parallelism and
     * throughput of selective queries. If output chunks are not "handled"
     * until the {@link ChunkTask} is complete then the total latency of
     * selective queries is increased dramatically.
     */
    static private class HandleChunkBuffer implements
            IBlockingBuffer<IBindingSet[]> {

        private final ChunkedRunningQuery q;

        private final int bopId;

        private final int sinkId;

        /**
         * The target chunk size. When ZERO (0) chunks are output immediately as
         * they are received (the internal buffer is not used).
         */
        private final int chunkCapacity;
        
//        private final SinkTransitionMetadata sinkTransitionMetadata;
        
        private final AtomicInteger sinkMessagesOut;

        private final BOpStats stats;

        private volatile boolean open = true;

//        /**
//         * An internal buffer which is used if chunkCapacity != ZERO.
//         */
//        private IBindingSet[] chunk = null;
        private List<IBindingSet[]> smallChunks = null;
        
        /**
         * The #of elements in the internal {@link #chunk} buffer.
         */
        private int chunkSize = 0;

        /**
         * 
         * @param q
         * @param bopId
         * @param sinkId
         * @param chunkCapacity
         * @param sinkMessagesOut
         * @param stats
         */
        public HandleChunkBuffer(final ChunkedRunningQuery q, final int bopId,
                final int sinkId, final int chunkCapacity,
//                final SinkTransitionMetadata sinkTransitionMetadata,
                final AtomicInteger sinkMessagesOut, final BOpStats stats) {
            this.q = q;
            this.bopId = bopId;
            this.sinkId = sinkId;
            this.chunkCapacity = chunkCapacity;
//            this.sinkTransitionMetadata = sinkTransitionMetadata;
            this.sinkMessagesOut = sinkMessagesOut;
            this.stats = stats;
        }

        /**
         * Handle sink output, sending appropriate chunk message(s). This method
         * MUST NOT block since that will deadlock the caller.
         * <p>
         * Note: This maps output over shards/nodes in s/o.
         * <p>
         * Note: This must be synchronized in case the caller is multi-threaded
         * since it has a possible side effect on the internal buffer.
         */
        public void add(final IBindingSet[] e) {
            
            if(!open)
                throw new BufferClosedException();

//            for (IBindingSet bset : e) {
//                sinkTransitionMetadata.handleBindingSet(bset);
//            }
            
//            if (chunkCapacity != 0 && e.length < (chunkCapacity >> 1)) {
//                /*
//                 * The caller's array is significantly smaller than the target
//                 * chunk size. Append the caller's array to the internal buffer
//                 * and return immediately. The internal buffer will be copied
//                 * through either in a subsequent add() or in flush().
//                 */
//                synchronized (this) {
//
//                    if (chunk == null)
//                        chunk = new IBindingSet[chunkCapacity];
//
//                    if (chunkSize + e.length > chunkCapacity) {
//
//                        // flush the buffer first.
//                        outputBufferedChunk();
//
//                    }
//
//                    // copy the chunk into the buffer.
//                    System.arraycopy(e/* src */, 0/* srcPos */,
//                                    chunk/* dest */, chunkSize/* destPos */,
//                                    e.length/* length */);
//
//                    chunkSize += e.length;
//
//                    return;
//
//                }
//
//            }

			if (chunkCapacity != 0 && e.length < (chunkCapacity >> 1)) {
				/*
				 * The caller's array is significantly smaller than the target
				 * chunk size. Append the caller's array to the internal list
				 * and return immediately. The buffered chunks will be copied
				 * through either in a subsequent add() or in flush().
				 */
				synchronized (this) {

					if (smallChunks == null)
						smallChunks = new LinkedList<IBindingSet[]>();

					if (chunkSize + e.length > chunkCapacity) {

						// flush the buffer first.
						outputBufferedChunk();

					}
					
					smallChunks.add(e);

					chunkSize += e.length;

					return;

				}
			}

            // output the caller's chunk immediately.
            outputChunk(e);

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
            
//        	try {
//				q.outstandingMessageSemaphore.acquire();
//			} catch (InterruptedException e1) {
//				throw new RuntimeException(e1);
//			}
        	
        }
        
        /**
         * Output the internal buffer.
         */
        synchronized // Note: has side-effect on internal buffer. 
        private void outputBufferedChunk() {
//            if (chunk == null || chunkSize == 0)
//                return;
//            if (chunkSize != chunk.length) {
//                // truncate the array.
//                chunk = Arrays.copyOf(chunk, chunkSize);
//            }
//            outputChunk(chunk);
//            chunkSize = 0;
//            chunk = null;
            if (smallChunks == null || chunkSize == 0)
                return;
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
        
        synchronized // Note: possible side-effect on internal buffer.
        public long flush() {
            if (open)
                outputBufferedChunk();
            return 0L;
//          return sink.flush();
      }

        public void abort(Throwable cause) {
            open = false;
            q.halt(cause);
//            sink.abort(cause);
        }

        public void close() {
//            sink.close();
            open = false;
        }

        public Future getFuture() {
//            return sink.getFuture();
            return null;
        }

        public boolean isEmpty() {
            return true;
//            return sink.isEmpty();
        }

        public boolean isOpen() {
            return open && !q.isDone();
//            return sink.isOpen();
        }

        public IAsynchronousIterator<IBindingSet[]> iterator() {
            throw new UnsupportedOperationException();
//            return sink.iterator();
        }

        public void reset() {
//            sink.reset();
        }

        public void setFuture(Future future) {
            throw new UnsupportedOperationException();
//            sink.setFuture(future);
        }

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

        private final HaltOpMessage msg;

        private final ChunkedRunningQuery q;

        public SendHaltMessageTask(final IQueryClient clientProxy,
                final HaltOpMessage msg, final ChunkedRunningQuery q) {

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

        public void run() {
            try {
                clientProxy.haltOp(msg);
            } catch (Throwable e) {
				if (!InnerCause.isInnerCause(e, InterruptedException.class)) {
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

        		if (f.cancel(mayInterruptIfRunning))
        			cancelled = true;
        		
        	}
        	
        }
     
        return cancelled;
        
    }

//    @Override
    protected IChunkHandler getChunkHandler() {
        
        return StandaloneChunkHandler.INSTANCE;
        
    }

}
