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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NoSuchBOpException;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IMultiSourceAsynchronousIterator;
import com.bigdata.relation.accesspath.MultiSourceSequentialAsynchronousIterator;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.util.concurrent.Haltable;
import com.bigdata.util.concurrent.Memoizer;

/**
 * Metadata about running queries.
 */
public class RunningQuery implements Future<Void>, IRunningQuery {

    private final static transient Logger log = Logger
            .getLogger(RunningQuery.class);

    /**
     * Logger for the {@link ChunkTask}.
     */
    private final static Logger chunkTaskLog = Logger
            .getLogger(ChunkTask.class);

    /**
     * Error message used when an operation which must be performed on the query
     * controller is attempted on some other {@link IQueryPeer}.
     */
    static protected final String ERR_NOT_CONTROLLER = "Operator only permitted on the query controller";

    /**
     * Error message used when a request is made after the query has stopped
     * executing.
     */
    static protected final String ERR_QUERY_DONE = "Query is no longer running";
    
    /**
     * The class executing the query on this node.
     */
    final private QueryEngine queryEngine;

    /** The unique identifier for this query. */
    final private UUID queryId;

    /**
     * The query deadline. The value is the system clock time in milliseconds
     * when the query is due and {@link Long#MAX_VALUE} if there is no deadline.
     * In order to have a guarantee of a consistent clock, the deadline is
     * interpreted by the query controller.
     */
    final private AtomicLong deadline = new AtomicLong(Long.MAX_VALUE);

	/**
	 * The timestamp(ms) when the query begins to execute.
	 */
	final private AtomicLong startTime = new AtomicLong(System
			.currentTimeMillis());

	/**
	 * The timestamp (ms) when the query is done executing and ZERO (0L) if the
	 * query is not done.
	 */
	final private AtomicLong doneTime = new AtomicLong(0L);

    /**
     * <code>true</code> iff the outer {@link QueryEngine} is the controller for
     * this query.
     */
    final private boolean controller;

    /**
     * The client executing this query (aka the query controller).
     * <p>
     * Note: The proxy is primarily for light weight RMI messages used to
     * coordinate the distributed query evaluation. Ideally, all large objects
     * will be transfered among the nodes of the cluster using NIO buffers.
     */
    final private IQueryClient clientProxy;

    /** The query. */
    final private PipelineOp query;

//    /**
//     * @see QueryEngineTestAnnotations#COMBINE_RECEIVED_CHUNKS
//     */
//    final protected boolean combineReceivedChunks;
    
    /**
     * An index from the {@link BOp.Annotations#BOP_ID} to the {@link BOp}. This
     * index is generated by the constructor. It is immutable and thread-safe.
     */
    private final Map<Integer, BOp> bopIndex;

    /**
     * The run state of the query and the result of the computation iff it
     * completes execution normally (without being interrupted, cancelled, etc).
     */
    final private Haltable<Void> future = new Haltable<Void>();

	/**
	 * The maximum number of operator tasks which may be concurrently executor
	 * for a given (bopId,shardId).
	 */
    final private int maxConcurrentTasksPerOperatorAndShard;
    
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
    
    /**
     * The runtime statistics for each {@link BOp} in the query and
     * <code>null</code> unless this is the query controller.
     */
    final private ConcurrentHashMap<Integer/* bopId */, BOpStats> statsMap;

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

    /**
     * The buffer used for the overall output of the query pipeline.
     * <p>
     * Note: This only exists on the query controller, and then only when the
     * top-level operator is not a mutation. In order to ensure that the results
     * are transferred to the query controller in scale-out, the top-level
     * operator in the query plan must specify
     * {@link BOpEvaluationContext#CONTROLLER}. For example, {@link SliceOp}
     * uses this {@link BOpEvaluationContext}.
     */
    final private IBlockingBuffer<IBindingSet[]> queryBuffer;

    /**
     * The iterator draining the {@link #queryBuffer} and <code>null</code> iff
     * the {@link #queryBuffer} is <code>null</code>.
     */
    final private IAsynchronousIterator<IBindingSet[]> queryIterator;

    /**
     * A lock guarding various state changes. This guards changes to the
     * internal state of the {@link #runState} object. It is also used to
     * serialize requests to {@link #acceptChunk(IChunkMessage)} and
     * {@link #cancel(boolean)} and make atomic decision concerning whether to
     * attach a new {@link IChunkMessage} to an operator task which is already
     * running or to start a new task for that message.
     * 
     * @see RunState
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * The run state of this query and <code>null</code> unless this is the
     * query controller.
     */
    final private RunState runState;

    /**
     * Flag used to prevent retriggering of {@link #lifeCycleTearDownQuery()}.
     */
    private final AtomicBoolean didQueryTearDown = new AtomicBoolean(false);

//    /**
//     * The chunks available for immediate processing (they must have been
//     * materialized).
//     * <p>
//     * Note: This is package private so it will be visible to the
//     * {@link QueryEngine}.
//     */
//    final/* private */BlockingQueue<IChunkMessage<IBindingSet>> chunksIn = new LinkedBlockingDeque<IChunkMessage<IBindingSet>>();

    /**
     * Set the query deadline. The query will be cancelled when the deadline is
     * passed. If the deadline is passed, the query is immediately cancelled.
     * 
     * @param deadline
     *            The deadline.
     * @throws IllegalArgumentException
     *             if the deadline is non-positive.
     * @throws IllegalStateException
     *             if the deadline was already set.
     * @throws UnsupportedOperationException
     *             unless node is the query controller.
     */
    public void setDeadline(final long deadline) {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (deadline <= 0)
            throw new IllegalArgumentException();

        // set the deadline.
        if (!this.deadline
                .compareAndSet(Long.MAX_VALUE/* expect */, deadline/* update */)) {

            // the deadline is already set.
            throw new IllegalStateException();

        }

        if (deadline < System.currentTimeMillis()) {

            // deadline has already expired.
            halt(new TimeoutException());

        }

    }

    public long getDeadline() {
        return deadline.get();
    }

    public long getStartTime() {
    	return startTime.get();
    }

    public long getDoneTime() {
    	return doneTime.get();
    }

    public long getElapsed() {
		long mark = doneTime.get();
		if (mark == 0L)
			mark = System.currentTimeMillis();
    	return mark - startTime.get();
    }

    /**
     * The class executing the query on this node.
     */
    public QueryEngine getQueryEngine() {

        return queryEngine;

    }

    /**
     * The client executing this query (aka the query controller).
     * <p>
     * Note: The proxy is primarily for light weight RMI messages used to
     * coordinate the distributed query evaluation. Ideally, all large objects
     * will be transfered among the nodes of the cluster using NIO buffers.
     */
    public IQueryClient getQueryController() {

        return clientProxy;

    }

    /**
     * The unique identifier for this query.
     */
    public UUID getQueryId() {

        return queryId;

    }

    /**
     * Return the operator tree for this query.
     */
    public PipelineOp getQuery() {

        return query;

    }

    /**
     * Return <code>true</code> iff this is the query controller.
     */
    public boolean isController() {

        return controller;

    }

    public Map<Integer/* bopId */, BOpStats> getStats() {

        return Collections.unmodifiableMap(statsMap);

    }

    public Map<Integer,BOp> getBOpIndex() {
        
        return bopIndex;
        
    }
    
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
    public RunningQuery(final QueryEngine queryEngine, final UUID queryId,
            final boolean controller, final IQueryClient clientProxy,
            final PipelineOp query) {

        if (queryEngine == null)
            throw new IllegalArgumentException();

        if (queryId == null)
            throw new IllegalArgumentException();

        if (clientProxy == null)
            throw new IllegalArgumentException();

        if (query == null)
            throw new IllegalArgumentException();

        this.queryEngine = queryEngine;

        this.queryId = queryId;

        this.controller = controller;

        this.clientProxy = clientProxy;

        this.query = query;

//        combineReceivedChunks = query.getProperty(
//                QueryEngineTestAnnotations.COMBINE_RECEIVED_CHUNKS,
//                QueryEngineTestAnnotations.DEFAULT_COMBINE_RECEIVED_CHUNKS);

        this.bopIndex = BOpUtility.getIndex(query);

		this.maxConcurrentTasksPerOperatorAndShard = query
				.getProperty(
						QueryEngineTestAnnotations.MAX_CONCURRENT_TASKS_PER_OPERATOR_AND_SHARD,
						QueryEngineTestAnnotations.DEFAULT_MAX_CONCURRENT_TASKS_PER_OPERATOR_AND_SHARD);
        
        this.operatorFutures = new ConcurrentHashMap<BSBundle, ConcurrentHashMap<ChunkFutureTask,ChunkFutureTask>>();
        
        this.operatorQueues = new ConcurrentHashMap<BSBundle, BlockingQueue<IChunkMessage<IBindingSet>>>();
        
        /*
         * Setup the BOpStats object for each pipeline operator in the query.
         */
        if (controller) {
            
            runState = new RunState(this);

            statsMap = new ConcurrentHashMap<Integer, BOpStats>();

            populateStatsMap(query);

            if (!query.isMutation()) {

                final BOpStats queryStats = statsMap.get(query.getId());

                queryBuffer = new BlockingBufferWithStats<IBindingSet[]>(query,
                        queryStats);

                queryIterator = new QueryResultIterator<IBindingSet[]>(this,
                        queryBuffer.iterator());

            } else {

                // Note: Not used for mutation queries.
                queryBuffer = null;
                queryIterator = null;

            }

        } else {

            runState = null; // Note: only on the query controller.
            statsMap = null; // Note: only on the query controller.
            queryBuffer = null; // Note: only on the query controller.
            queryIterator = null; // Note: only when queryBuffer is defined.
            
        }

//        if(!queryEngine.isScaleOut()) {
//            /*
//             * Since the query engine is using the stand alone database mode we
//             * will now setup the input queues for each operator. Those queues
//             * will be used by each operator which targets a given operator.
//             * Each operator will start once and will run until all of its
//             * source(s) are closed.
//             * 
//             * This allocates the buffers in a top-down manner (this is the
//             * reverse of the pipeline evaluation order). Allocation halts at if
//             * we reach an operator without children (e.g., StartOp) or an
//             * operator which is a CONTROLLER (Union). (If allocation does not
//             * halt at those boundaries then we can allocate buffers which will
//             * not be used. On the one hand, the StartOp receives a message
//             * containing the chunk to be evaluated. On the other hand, the
//             * buffers are not shared between the parent and a subquery so
//             * allocation within the subquery is wasted. This is also true for
//             * the [statsMap].)
//             */
//            inputBufferMap = null;
////            inputBufferMap = new ConcurrentHashMap<Integer, MultiplexBlockingBuffer<IBindingSet[]>>();
////            populateInputBufferMap(query);
//        } else {
//            inputBufferMap = null;
//        }
        
    }

	/**
	 * Pre-populate a map with {@link BOpStats} objects for the query. Only the
	 * child operands are visited. Operators in subqueries are not visited since
	 * they will be assigned {@link BOpStats} objects when they are run as a
	 * subquery.
	 */
    private void populateStatsMap(final BOp op) {

        if(!(op instanceof PipelineOp))
            return;
        
        final PipelineOp bop = (PipelineOp) op;

        final int bopId = bop.getId();
        
        statsMap.put(bopId, bop.newStats());

        if (!op.getProperty(BOp.Annotations.CONTROLLER,
                BOp.Annotations.DEFAULT_CONTROLLER)) {
            /*
             * Visit children, but not if this is a CONTROLLER operator since
             * its children belong to a subquery.
             */
            for (BOp t : op.args()) {
                // visit children (recursion)
                populateStatsMap(t);
            }
        }
        
    }
    
//    /**
//     * Pre-populate a map with {@link MultiplexBlockingBuffer} objects for the
//     * query. Operators in subqueries are not visited since they will be
//     * assigned buffer objects when they are run as a subquery. Operators
//     * without children are not visited since they can not be the targets of
//     * some other operator and hence do not need to have an assigned input
//     * buffer.
//     */
//    private void populateInputBufferMap(final BOp op) {
//
//        if(!(op instanceof PipelineOp))
//            return;
//
//        if (op.arity() == 0)
//            return;
//        
//        final PipelineOp bop = (PipelineOp) op;
//
//        final int bopId = bop.getId();
//        
//        inputBufferMap.put(bopId, new MultiplexBlockingBuffer<IBindingSet[]>(
//                bop.newBuffer(statsMap.get(bopId))));
//
//        if (!op.getProperty(BOp.Annotations.CONTROLLER,
//                BOp.Annotations.DEFAULT_CONTROLLER)) {
//            /*
//             * Visit children, but not if this is a CONTROLLER operator since
//             * its children belong to a subquery.
//             */
//            for (BOp t : op.args()) {
//                // visit children (recursion)
//                populateInputBufferMap(t);
//            }
//        }
//
//    }

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

    /**
     * Invoked once by the query controller with the initial
     * {@link IChunkMessage} which gets the query moving.
     */
    void startQuery(final IChunkMessage<IBindingSet> msg) {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (msg == null)
            throw new IllegalArgumentException();

        if (!queryId.equals(msg.getQueryId()))
            throw new IllegalArgumentException();

        lock.lock();

        try {

            lifeCycleSetUpQuery();

            runState.startQuery(msg);

//            if (inputBufferMap != null) {
//                // Prestart a task for each operator.
//                startTasks(query);
//            }

        } catch (TimeoutException ex) {

            halt(ex);

        } finally {

            lock.unlock();

        }

    }

//    /**
//     * Prestart a task for each operator. The operators are started in
//     * back-to-front order (reverse pipeline evaluation order). The input queues
//     * for the operators were created in by {@link #populateInputBufferMap(BOp)}
//     * and are found in {@link #inputBufferMap}. The output queues for the
//     * operators are skins over the output queues obtained from
//     * {@link MultiplexBlockingBuffer}.
//     * 
//     * @param op
//     *            The
//     * 
//     * @see #inputBufferMap
//     */
//    private void startTasks(final BOp op) {
//
//        if(!(op instanceof PipelineOp))
//            return;
//
//        if (op.arity() == 0)
//            return;
//        
//        final PipelineOp bop = (PipelineOp) op;
//
//        final int bopId = bop.getId();
//
//        final MultiplexBlockingBuffer<IBindingSet[]> inputBuffer = inputBufferMap
//                .get(bopId);
//
//        if (inputBuffer == null)
//            throw new AssertionError("No input buffer? " + op);
//
//        final IAsynchronousIterator<IBindingSet[]> src = inputBuffer
//                .getBackingBuffer().iterator();
//        
//        final ChunkTask chunkTask = new ChunkTask(bopId, -1/* partitionId */,
//                src);
//
//        final FutureTask<Void> futureTask = wrapChunkTask(chunkTask);
//        
//        queryEngine.execute(futureTask);
//
//        if (!op.getProperty(BOp.Annotations.CONTROLLER,
//                BOp.Annotations.DEFAULT_CONTROLLER)) {
//            /*
//             * Visit children, but not if this is a CONTROLLER operator since
//             * its children belong to a subquery.
//             */
//            for (BOp t : op.args()) {
//                // visit children (recursion)
//                startTasks(t);
//            }
//        }
//        
//    }
    
    /**
     * Message provides notice that the operator has started execution and will
     * consume some specific number of binding set chunks.
     * 
     * @param msg
     *            The {@link StartOpMessage}.
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    public void startOp(final StartOpMessage msg) {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (msg == null)
            throw new IllegalArgumentException();
        
        if (!queryId.equals(msg.queryId))
            throw new IllegalArgumentException();

        lock.lock();

        try {

            if (runState.startOp(msg))
                lifeCycleSetUpOperator(msg.bopId);

        } catch (TimeoutException ex) {

            halt(ex);

        } finally {

            lock.unlock();

        }

    }

    /**
     * Message provides notice that the operator has ended execution. The
     * termination conditions for the query are checked. (For scale-out, the
     * node node controlling the query needs to be involved for each operator
     * start/stop in order to make the termination decision atomic).
     * 
     * @param msg
     *            The {@link HaltOpMessage}
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    public void haltOp(final HaltOpMessage msg) {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (msg == null)
            throw new IllegalArgumentException();
        
        if (!queryId.equals(msg.queryId))
            throw new IllegalArgumentException();

        lock.lock();

        try {

            // update per-operator statistics.
            final BOpStats tmp = statsMap.putIfAbsent(msg.bopId, msg.taskStats);

            // combine stats, but do not combine a stats object with itself.
            if (tmp != null && tmp != msg.taskStats) {
                tmp.add(msg.taskStats);
            }

            if (runState.haltOp(msg)) {

                /*
                 * No more chunks can appear for this operator so invoke its end
                 * of life cycle hook.
                 */

                lifeCycleTearDownOperator(msg.bopId);

                if (runState.isAllDone()) {

                    // Normal termination.
                    halt();

                }

            }

        } catch (Throwable t) {

            halt(t);

        } finally {

            lock.unlock();

        }

    }

    /**
     * Hook invoked the first time the given operator is evaluated for the
     * query. This may be used to set up life cycle resources for the operator,
     * such as a distributed hash table on a set of nodes identified by
     * annotations of the operator.
     * 
     * @param bopId
     *            The operator identifier.
     */
    protected void lifeCycleSetUpOperator(final int bopId) {

        if (log.isTraceEnabled())
            log.trace("queryId=" + queryId + ", bopId=" + bopId);

    }

    /**
     * Hook invoked the after the given operator has been evaluated for the
     * query for what is known to be the last time. This may be used to tear
     * down life cycle resources for the operator, such as a distributed hash
     * table on a set of nodes identified by annotations of the operator.
     * 
     * @param bopId
     *            The operator identifier.
     */
    protected void lifeCycleTearDownOperator(final int bopId) {

        if (log.isTraceEnabled())
            log.trace("queryId=" + queryId + ", bopId=" + bopId);

    }

    /**
     * Hook invoked the before any operator is evaluated for the query. This may
     * be used to set up life cycle resources for the query.
     */
    protected void lifeCycleSetUpQuery() {

        if (log.isTraceEnabled())
            log.trace("queryId=" + queryId);

    }

    /**
     * Hook invoked when the query terminates. This may be used to tear down
     * life cycle resources for the query.
     */
    protected void lifeCycleTearDownQuery() {

        if (log.isTraceEnabled())
            log.trace("queryId=" + queryId);

    }

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
     */
    protected void acceptChunk(final IChunkMessage<IBindingSet> msg) {

        if (msg == null)
            throw new IllegalArgumentException();

        if (!msg.isMaterialized())
            throw new IllegalStateException();

        final BSBundle bundle = new BSBundle(msg.getBOpId(), msg
                .getPartitionId());

        lock.lock();

        try {

            // verify still running.
            if (future.isDone())
                throw new RuntimeException(ERR_QUERY_DONE, future.getCause());

            BlockingQueue<IChunkMessage<IBindingSet>> queue = operatorQueues
                    .get(bundle);

            if (queue == null) {

                queue = new LinkedBlockingQueue<IChunkMessage<IBindingSet>>(/* unbounded */);

                operatorQueues.put(bundle, queue);

            }

            queue.add(msg);

        } finally {

            lock.unlock();

        }

    }

    /**
     * Examines the input queue for each (bopId,partitionId). If there is work
     * available and no task is currently running, then drain the work queue and
     * submit a task to consume that work.
     */
    protected void consumeChunk() {
        lock.lock();
        try {
            for(BSBundle bundle : operatorQueues.keySet()) {
                scheduleNext(bundle);
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
			if(future.isDone())
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
				if (nrunning > maxConcurrentTasksPerOperatorAndShard) {
					// Too many already running.
					return false;
				}
			}
			// Remove the work queue for that (bopId,partitionId).
            final BlockingQueue<IChunkMessage<IBindingSet>> queue = operatorQueues
                    .remove(bundle);
            if (queue == null || queue.isEmpty()) {
                // no work
                return false;
            }
            // Drain the work queue.
            final List<IChunkMessage<IBindingSet>> messages = new LinkedList<IChunkMessage<IBindingSet>>();
            queue.drainTo(messages);
            final int nmessages = messages.size();
            /*
             * Combine the messages into a single source to be consumed by a
             * task.
             */
            final IMultiSourceAsynchronousIterator<IBindingSet[]> source = new MultiSourceSequentialAsynchronousIterator<IBindingSet[]>(messages.remove(0).getChunkAccessor().iterator());
            for (IChunkMessage<IBindingSet> msg : messages) {
                source.add(msg.getChunkAccessor().iterator());
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
			queryEngine.execute(cft);
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
            RunningQuery.this.scheduleNext(new BSBundle(
                    t.bopId, t.partitionId));
        
        }
        
    }

    /**
     * Wraps the {@link ChunkTask} and handles various handshaking with the
     * {@link RunningQuery} and the {@link RunState} on the query controller.
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

            final UUID serviceId = queryEngine.getServiceUUID();
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
                clientProxy.startOp(new StartOpMessage(queryId, t.bopId,
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
                final HaltOpMessage msg = new HaltOpMessage(queryId, t.bopId,
                        t.partitionId, serviceId, null/* cause */, t.sinkId,
                        t.sinkMessagesOut.get(), t.altSinkId,
                        t.altSinkMessagesOut.get(), t.context.getStats());
                try {
                    t.context.getExecutorService().execute(
                            new SendHaltMessageTask(clientProxy, msg,
                                    RunningQuery.this));
                } catch (RejectedExecutionException ex) {
                    // e.g., service is shutting down.
                    log.error("Could not send message: " + msg, ex);
                }
                
            } catch (Throwable ex1) {

                // Log an error.
                log.error("queryId=" + queryId + ", bopId=" + t.bopId, ex1);

                /*
                 * Mark the query as halted on this node regardless of whether
                 * we are able to communicate with the query controller.
                 * 
                 * Note: Invoking halt(t) here will log an error. This logged
                 * error message is necessary in order to catch errors in
                 * clientProxy.haltOp() (above and below).
                 */
                final Throwable firstCause = halt(ex1);

                final HaltOpMessage msg = new HaltOpMessage(queryId, t.bopId,
                        t.partitionId, serviceId, firstCause, t.sinkId,
                        t.sinkMessagesOut.get(), t.altSinkId,
                        t.altSinkMessagesOut.get(), t.context.getStats());
                try {
                    /*
                     * Queue a task to send the halt message to the query
                     * controller (async notification).
                     */
                    t.context.getExecutorService().execute(
                            new SendHaltMessageTask(clientProxy, msg,
                                    RunningQuery.this));
                } catch (RejectedExecutionException ex) {
                    // e.g., service is shutting down.
                    log.warn("Could not send message: " + msg, ex);
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
         * operator explicitly specifies an alternative sink using
         * {@link PipelineOp.Annotations#ALT_SINK_REF}.
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
                    "{query=" + queryId + //
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
         * the {@link RunningQuery#bopIndex}, creates the sink(s) for the
         * {@link BOp}, creates the {@link BOpContext} for that {@link BOp}, and
         * wraps the value returned by {@link PipelineOp#eval(BOpContext)} in
         * order to handle the outputs written on those sinks.
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
            
            bop = bopIndex.get(bopId);
            
            if (bop == null)
                throw new NoSuchBOpException(bopId);
            
            if (!(bop instanceof PipelineOp))
                throw new UnsupportedOperationException(bop.getClass()
                        .getName());

            // self
            final PipelineOp op = ((PipelineOp) bop);

            // parent (null if this is the root of the operator tree).
            final BOp p = BOpUtility.getParent(query, op);

            /*
             * The sink is the parent. The parent MUST have an id so we can
             * target it with a message. (The sink will be null iff there is no
             * parent for this operator.)
             */
            sinkId = BOpUtility.getEffectiveDefaultSink(bop, p);

            // altSink (null when not specified).
            altSinkId = (Integer) op
                    .getProperty(PipelineOp.Annotations.ALT_SINK_REF);

            if (altSinkId != null && !bopIndex.containsKey(altSinkId))
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
                stats = statsMap.get(bopId);
            } else {
                // distinct stats objects, aggregated as each task finishes.
                stats = op.newStats();
            }
            assert stats != null;

            sink = (p == null ? queryBuffer : newBuffer(op, sinkId,
                    sinkMessagesOut, stats));

            altSink = altSinkId == null ? null
                    : altSinkId.equals(sinkId) ? sink : newBuffer(op,
                            altSinkId, altSinkMessagesOut, stats);

            // context : @todo pass in IChunkMessage or IChunkAccessor
            context = new BOpContext<IBindingSet>(RunningQuery.this,
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
                final int sinkId, final AtomicInteger sinkMessagesOut, final BOpStats stats) {

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
             * FIXME THe buffer allocated here is useless unless we play games
             * in HandleChunkBuffer to combine chunks or run a thread which
             * drains chunks from all operator tasks (but the task can not
             * complete until it is fully drained).
             */
//            final IBlockingBuffer<IBindingSet[]> b = new BlockingBuffer<IBindingSet[]>(
//                    op.getChunkOfChunksCapacity(), op.getChunkCapacity(), op
//                            .getChunkTimeout(),
//                    BufferAnnotations.chunkTimeoutUnit);
  
            return new HandleChunkBuffer(RunningQuery.this, bopId, sinkId, op
                    .getChunkCapacity(), sinkMessagesOut, stats);
            
        }

        /**
         * Evaluate the {@link IChunkMessage}.
         */
        public Void call() throws Exception {
            if (log.isDebugEnabled())
                log.debug("Running chunk: " + this);
            ft.run(); // run
            ft.get(); // verify success
//            if (sink != null && sink != queryBuffer && !sink.isEmpty()) {
//                if (sinkId == null)
//                    throw new RuntimeException("sinkId not defined: bopId="
//                            + bopId + ", query=" + BOpUtility.toString(query));
//                /*
//                 * Handle sink output, sending appropriate chunk message(s).
//                 * 
//                 * Note: This maps output over shards/nodes in s/o.
//                 */
//                sinkMessagesOut.addAndGet(handleOutputChunk(bop, sinkId, sink));
//            }
//            if (altSink != null && altSink != queryBuffer && !altSink.isEmpty()) {
//                if (altSinkId == null)
//                    throw new RuntimeException("altSinkId not defined: bopId="
//                            + bopId + ", query=" + BOpUtility.toString(query));
//                /*
//                 * Handle alt sink output, sending appropriate chunk message(s).
//                 * 
//                 * Note: This maps output over shards/nodes in s/o.
//                 */
//                altSinkMessagesOut.addAndGet(handleOutputChunk(bop, altSinkId,
//                        altSink));
//            }
            // Done.
            return null;
        } // call()

    } // class ChunkTask
    
    /**
     * Class traps {@link #add(IBindingSet[])} to handle the IBindingSet[]
     * chunks as they are generated by the running operator task, invoking
     * {@link RunningQuery#handleOutputChunk(BOp, int, IBlockingBuffer)} for
     * each generated chunk to synchronously emit {@link IChunkMessage}s.
     * <p>
     * This use of this class significantly increases the parallelism and
     * throughput of selective queries. If output chunks are not "handled"
     * until the {@link ChunkTask} is complete then the total latency of
     * selective queries is increased dramatically.
     */
    static private class HandleChunkBuffer implements
            IBlockingBuffer<IBindingSet[]> {

        private final RunningQuery q;

        private final int bopId;

        private final int sinkId;

        /**
         * The target chunk size. When ZERO (0) chunks are output immediately as
         * they are received (the internal buffer is not used).
         */
        private final int chunkCapacity;
        
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
        public HandleChunkBuffer(final RunningQuery q, final int bopId,
                final int sinkId, final int chunkCapacity,
                final AtomicInteger sinkMessagesOut, final BOpStats stats) {
            this.q = q;
            this.bopId = bopId;
            this.sinkId = sinkId;
            this.chunkCapacity = chunkCapacity;
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

            stats.unitsOut.add(((Object[]) e).length);
            
            stats.chunksOut.increment();
            
            sinkMessagesOut.addAndGet(q.getChunkHandler().handleChunk(q, bopId,
                    sinkId, e));
            
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
    
//    private static class BlockingBufferWithStats<E> extends BlockingBuffer<E> {
//
//        private final BOpStats stats;
//
//        /**
//         * @param chunkOfChunksCapacity
//         * @param chunkCapacity
//         * @param chunkTimeout
//         * @param chunkTimeoutUnit
//         * @param stats
//         */
//        public BlockingBufferWithStats(int chunkOfChunksCapacity,
//                int chunkCapacity, long chunkTimeout,
//                TimeUnit chunkTimeoutUnit, final BOpStats stats) {
//
//            super(chunkOfChunksCapacity, chunkCapacity, chunkTimeout,
//                    chunkTimeoutUnit);
//            
//            this.stats = stats;
//            
//        }
//
//        /**
//         * Overridden to track {@link BOpStats#unitsOut} and
//         * {@link BOpStats#chunksOut}.
//         * <p>
//         * Note: {@link BOpStats#chunksOut} will report the #of chunks added to
//         * this buffer. However, the buffer MAY combine chunks either on add()
//         * or when drained by the iterator so the actual #of chunks read back
//         * from the iterator MAY differ.
//         * <p>
//         * {@inheritDoc}
//         */
//        @Override
//        public boolean add(final E e, final long timeout, final TimeUnit unit)
//                throws InterruptedException {
//
//            final boolean ret = super.add(e, timeout, unit);
//
//            if (e.getClass().getComponentType() != null) {
//
//                stats.unitsOut.add(((Object[]) e).length);
//
//            } else {
//
//                stats.unitsOut.increment();
//
//            }
//
//            stats.chunksOut.increment();
//
//            return ret;
//
//        }
//
//        /**
//         * You can uncomment a line in this method to see who is closing the
//         * buffer.
//         * <p>
//         * {@inheritDoc}
//         */
//        @Override
//        public void close() {
//
////            if (isOpen())
////                log.error(toString(), new RuntimeException("STACK TRACE"));
//
//            super.close();
//            
//        }
//        
//    }

    /**
     * {@link Runnable} sends the {@link IQueryClient} a message indicating that
     * some query has halted on some node. This is used to send such messages
     * asynchronously
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static class SendHaltMessageTask implements Runnable {

        private final IQueryClient clientProxy;

        private final HaltOpMessage msg;

        private final RunningQuery q;

        public SendHaltMessageTask(final IQueryClient clientProxy,
                final HaltOpMessage msg, final RunningQuery q) {

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
                log.error("Could not notify query controller: " + e, e);
                q.cancel(true/* mayInterruptIfRunning */);
            }
        }

    }

    /**
     * Return an iterator which will drain the solutions from the query. The
     * query will be cancelled if the iterator is
     * {@link ICloseableIterator#close() closed}.
     * 
     * @throws UnsupportedOperationException
     *             if this is not the query controller.
     */
    public IAsynchronousIterator<IBindingSet[]> iterator() {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (queryIterator == null)
            throw new UnsupportedOperationException();

        return queryIterator;

    }

    public void halt() {

        lock.lock();

        try {

            // signal normal completion.
            future.halt((Void) null);

            // interrupt anything which is running.
            cancel(true/* mayInterruptIfRunning */);

        } finally {

            lock.unlock();

        }

    }

    public Throwable halt(final Throwable t) {

        if (t == null)
            throw new IllegalArgumentException();

        lock.lock();

        try {

            log.error(toString(), t);

            try {

                // signal error condition.
                return future.halt(t);

            } finally {
                
                // interrupt anything which is running.
                cancel(true/* mayInterruptIfRunning */);

            }

        } finally {

            lock.unlock();

        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * Cancelled queries :
     * <ul>
     * <li>must reject new chunks</li>
     * <li>must cancel any running operators</li>
     * <li>must not begin to evaluate operators</li>
     * <li>must release all of their resources</li>
     * <li>must not cause the solutions to be discarded before the client can
     * consume them.</li>
     * </ul>
     */
    final public boolean cancel(final boolean mayInterruptIfRunning) {
        lock.lock();
        try {
            // halt the query.
            boolean cancelled = future.cancel(mayInterruptIfRunning);
            if (didQueryTearDown
                    .compareAndSet(false/* expect */, true/* update */)) {
                /*
                 * Do additional cleanup exactly once.
                 */
                // cancel any running operators for this query on this node.
                cancelled |= cancelRunningOperators(mayInterruptIfRunning);
                if (controller) {
                    // cancel query on other peers.
                    cancelled |= cancelQueryOnPeers(future.getCause());
                }
                if (queryBuffer != null) {
                    /*
                     * Close the query buffer so the iterator draining the query
                     * results will recognize that no new results will become
                     * available.
                     */
                    queryBuffer.close();
                }
                // life cycle hook for the end of the query.
                lifeCycleTearDownQuery();
                // mark done time.
                doneTime.set(System.currentTimeMillis());
                // log summary statistics for the query.
				if (isController())
					QueryLog.log(this);
            }
            // remove from the collection of running queries.
            queryEngine.halt(this);
            // true iff we cancelled something.
            return cancelled;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Cancel any running operators for this query on this node.
     * <p>
     * Note: This will wind up invoking the tear down methods for each operator
     * which was running or which could have been re-triggered.
     * 
     * @return <code>true</code> if any operators were cancelled.
     */
    private boolean cancelRunningOperators(final boolean mayInterruptIfRunning) {
        
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

    /**
     * Cancel the query on each node where it is known to be running.
     * <p>
     * Note: The default implementation verifies that the caller is holding the
     * {@link #lock} but is otherwise a NOP. This is overridden for scale-out.
     * 
     * @param cause
     *            When non-<code>null</code>, the cause.
     * 
     * @return <code>true</code> iff something was cancelled.
     * 
     * @throws IllegalMonitorStateException
     *             unless the {@link #lock} is held by the current thread.
     * @throws UnsupportedOperationException
     *             unless this is the query controller.
     */
    protected boolean cancelQueryOnPeers(final Throwable cause) {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);
        
        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return false;

    }

    final public Void get() throws InterruptedException, ExecutionException {

        return future.get();

    }

    final public Void get(long arg0, TimeUnit arg1)
            throws InterruptedException, ExecutionException, TimeoutException {

        return future.get(arg0, arg1);

    }

    final public boolean isCancelled() {

        return future.isCancelled();

    }

    final public boolean isDone() {

        return future.isDone();

    }

    final public Throwable getCause() {
    	
    	return future.getCause();
    	
    }
    
    public IBigdataFederation<?> getFederation() {

        return queryEngine.getFederation();

    }

    public IIndexManager getIndexManager() {

        return queryEngine.getIndexManager();

    }

    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getName());
        sb.append("{queryId=" + queryId);
        sb.append(",deadline=" + deadline.get());
        sb.append(",isDone=" + isDone());
        sb.append(",isCancelled=" + isCancelled());
        sb.append(",runState=" + runState);
        sb.append(",controller=" + controller);
        sb.append(",clientProxy=" + clientProxy);
        sb.append(",query=" + query);
        sb.append("}");
        return sb.toString();
    }

    public IChunkHandler getChunkHandler() {
        
        return StandaloneChunkHandler.INSTANCE;
        
    }

}
