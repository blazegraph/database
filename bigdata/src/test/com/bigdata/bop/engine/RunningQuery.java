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

import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.NoSuchBOpException;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.resources.ResourceManager;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Metadata about running queries.
 * 
 * @todo HA aspects of running queries?  Checkpoints for long running queries?
 */
public class RunningQuery implements Future<Map<Integer,BOpStats>> {

    private final static transient Logger log = Logger
            .getLogger(RunningQuery.class);

    /**
     * The run state of the query and the result of the computation iff it
     * completes execution normally (without being interrupted, cancelled, etc).
     */
    final private Haltable<Map<Integer,BOpStats>> future = new Haltable<Map<Integer,BOpStats>>();

    /**
     * The runtime statistics for each {@link BOp} in the query and
     * <code>null</code> unless this is the query controller.
     */
    final private ConcurrentHashMap<Integer/* bopId */, BOpStats> statsMap;

    /**
     * The class executing the query on this node.
     */
    final QueryEngine queryEngine;

    /** The unique identifier for this query. */
    final long queryId;

    /**
     * The timestamp or transaction identifier against which the query is
     * reading.
     */
    final long readTimestamp;

    /**
     * The timestamp or transaction identifier against which the query is
     * writing.
     */
    final long writeTimestamp;

    /**
     * The timestamp when the query was accepted by this node (ms).
     */
    final long begin;

    /**
     * How long the query is allowed to run (elapsed milliseconds) -or-
     * {@link Long#MAX_VALUE} if there is no deadline.
     */
    final long timeout;

    /**
     * <code>true</code> iff the outer {@link QueryEngine} is the controller for
     * this query.
     */
    final boolean controller;

    /**
     * The client executing this query.
     */
    final IQueryClient clientProxy;

    /** The query iff materialized on this node. */
    final AtomicReference<BOp> queryRef;

    /**
     * The buffer used for the overall output of the query pipeline.
     * 
     * @todo How does the pipeline get attached to this buffer? Via a special
     *       operator? Or do we just target the coordinating {@link QueryEngine}
     *       as the sink of the last operator so we can use NIO transfers?
     */
    final IBlockingBuffer<IBindingSet[]> queryBuffer;

    /**
     * An index from the {@link BOp.Annotations#BOP_ID} to the {@link BOp}.
     */
    private final Map<Integer, BOp> bopIndex;

    /**
     * A collection of the currently executing future for operators for this
     * query.
     */
    private final ConcurrentHashMap<BOpShard, Future<?>> operatorFutures = new ConcurrentHashMap<BOpShard, Future<?>>();

    /**
     * A lock guarding {@link #runningTaskCount}, {@link #availableChunkCount},
     * {@link #availableChunkCountMap}.
     */
    private final ReentrantLock runStateLock = new ReentrantLock();

    /**
     * The #of tasks for this query which have started but not yet halted and
     * ZERO (0) if this is not the query coordinator.
     * <p>
     * This is guarded by the {@link #runningStateLock}.
     */
    private long runningTaskCount = 0;

    /**
     * The #of chunks for this query of which a running task has made available
     * but which have not yet been accepted for processing by another task and
     * ZERO (0) if this is not the query coordinator.
     * <p>
     * This is guarded by the {@link #runningStateLock}.
     */
    private long availableChunkCount = 0;

    /**
     * A map reporting the #of chunks available for each operator in the
     * pipeline (we only report chunks for pipeline operators). The total #of
     * chunks available for any given operator in the pipeline is reported by
     * {@link #availableChunkCount}.
     * <p>
     * The movement of the intermediate binding set chunks forms an acyclic
     * directed graph. This map is used to track the #of chunks available for
     * each bop in the pipeline. When a bop has no more incoming chunks, we send
     * an asynchronous message to all nodes on which that bop had executed
     * informing the {@link QueryEngine} on that node that it should immediately
     * release all resources associated with that bop.
     * <p>
     * This is guarded by the {@link #runningStateLock}.
     * 
     * FIXME {@link IConstraint}s for {@link PipelineJoin}, distinct elements
     * and other filters for {@link IPredicate}s, conditional routing for
     * binding sets in the pipeline (to route around an optional join group
     * based on an {@link IConstraint}), and then buffer management for s/o.
     * 
     * @todo SCALEOUT: Life cycle management of the operators and the query
     *       implies both a per-query bop:NodeList map on the query coordinator
     *       identifying the nodes on which the query has been executed and a
     *       per-query bop:ResourceList map identifying the resources associated
     *       with the execution of that bop on that node. In fact, this could be
     *       the same {@link #resourceMap} except that we would lose type
     *       information about the nature of the resource so it is better to
     *       have distinct maps for this purpose.
     */
    private final Map<Integer/* bopId */, AtomicLong/* availableChunkCount */> availableChunkCountMap = new LinkedHashMap<Integer, AtomicLong>();

    /**
     * A collection reporting on the #of instances of a given {@link BOp} which
     * are concurrently executing.
     * <p>
     * This is guarded by the {@link #runningStateLock}.
     */
    private final Map<Integer/*bopId*/, AtomicLong/*runningCount*/> runningCountMap = new LinkedHashMap<Integer, AtomicLong>();

    /**
     * A collection of the operators which have executed at least once.
     * <p>
     * This is guarded by the {@link #runningStateLock}.
     */
    private final Set<Integer/*bopId*/> startedSet = new LinkedHashSet<Integer>();
    
    /**
     * A map associating resources with running queries. When a query halts, the
     * resources listed in its resource map are released. Resources can include
     * {@link ByteBuffer}s backing either incoming or outgoing
     * {@link BindingSetChunk}s, temporary files associated with the query, hash
     * tables, etc.
     * 
     * @todo Cache any resources materialized for the query on this node (e.g.,
     *       temporary graphs materialized from a peer or the client). A bop
     *       should be able to demand those data from the cache and otherwise
     *       have them be materialized.
     * 
     * @todo only use the values in the map for transient objects, such as a
     *       hash table which is not backed by the disk. For {@link ByteBuffer}s
     *       we want to make the references go through the {@link BufferService}
     *       . For files, through the {@link ResourceManager}.
     * 
     * @todo We need to track the resources in use by the query so they can be
     *       released when the query terminates. This includes: buffers; joins
     *       for which there is a chunk of binding sets that are currently being
     *       executed; downstream joins (they depend on the source joins to
     *       notify them when they are complete in order to decide their own
     *       termination condition); local hash tables which are part of a DHT
     *       (especially when they are persistent); buffers and disk resources
     *       allocated to N-way merge sorts, etc.
     * 
     * @todo The set of buffers having data which has been accepted for this
     *       query.
     * 
     * @todo The set of buffers having data which has been generated for this
     *       query.
     */
    private final ConcurrentHashMap<UUID, Object> resourceMap = new ConcurrentHashMap<UUID, Object>();

    /**
     * The chunks available for immediate processing.
     * 
     * @todo SCALEOUT: We need to model the chunks available before they are
     *       materialized locally such that (a) they can be materialized on
     *       demand (flow control); and (b) we can run the operator when there
     *       are sufficient chunks available without taking on too much data.
     */
    final BlockingQueue<BindingSetChunk> chunksIn = new LinkedBlockingDeque<BindingSetChunk>();

    /**
     * Return <code>true</code> iff this is the query controller.
     */
    public boolean isController() {
    
        return controller;
        
    }

    /**
     * Return the current statistics for the query and <code>null</code> unless
     * this is the query controller.
     * 
     * @todo When the query is done, there will be one entry in this map for
     *       each operator in the pipeline. Non-pipeline operators such as
     *       {@link Predicate}s do not currently make it into this map.
     */
    public Map<Integer/*bopId*/,BOpStats> getStats() {
        
        return statsMap;
        
    }

    /**
     * 
     * @param queryId
     * @param begin
     * @param clientProxy
     * @param query
     *            The query (optional).
     */
    public RunningQuery(final QueryEngine queryEngine, final long queryId,
            final long readTimestamp, final long writeTimestamp,
            final long begin, final long timeout, final boolean controller,
            final IQueryClient clientProxy, final BOp query,
            final IBlockingBuffer<IBindingSet[]> queryBuffer) {
        this.queryEngine = queryEngine;
        this.queryId = queryId;
        this.readTimestamp = readTimestamp;
        this.writeTimestamp = writeTimestamp;
        this.begin = begin;
        this.timeout = timeout;
        this.controller = controller;
        this.clientProxy = clientProxy;
        this.queryRef = new AtomicReference<BOp>(query);
        if (controller && query == null)
            throw new IllegalArgumentException();
        this.queryBuffer = queryBuffer;
        this.bopIndex = BOpUtility.getIndex(query);
        this.statsMap = controller ? new ConcurrentHashMap<Integer, BOpStats>()
                : null;
    }

    /**
     * Create a {@link BindingSetChunk} from a sink and add it to the queue.
     * 
     * @param sinkId
     * @param sink
     * 
     * @return The #of chunks made available for consumption by the sink. This
     *         will always be ONE (1) for scale-up. For scale-out, there will be
     *         one chunk per index partition over which the intermediate results
     *         were mapped.
     * 
     * @todo SCALEOUT: This is where we need to map the binding sets over the
     *       shards for the target operator. Once they are mapped, write the
     *       binding sets onto an NIO buffer for the target node and then send
     *       an RMI message to the node telling it that there is a chunk
     *       available for the given (queryId,bopId,partitionId).
     * 
     * @todo If we are running standalone, then do not format the data into a
     *       {@link ByteBuffer}.
     *       <p>
     *       For selective queries in s/o, first format the data onto a list of
     *       byte[]s, one per target shard/node. Then, using a lock, obtain a
     *       ByteBuffer if there is none associated with the query yet.
     *       Otherwise, using the same lock, obtain a slice onto that ByteBuffer
     *       and put as much of the byte[] as will fit, continuing onto a newly
     *       recruited ByteBuffer if necessary. Release the lock and notify the
     *       target of the ByteBuffer slice (buffer#, off, len). Consider
     *       pushing the data proactively for selective queries.
     *       <p>
     *       For unselective queries in s/o, proceed as above but we need to get
     *       the data off the heap and onto the {@link ByteBuffer}s quickly
     *       (incrementally) and we want the consumers to impose flow control on
     *       the producers to bound the memory demand (this needs to be
     *       coordinated carefully to avoid deadlocks). Typically, large result
     *       sets should result in multiple passes over the consumer's shard
     *       rather than writing the intermediate results onto the disk.
     */
    private int add(final int sinkId, final IBlockingBuffer<IBindingSet[]> sink) {

        /*
         * Note: The partitionId will always be -1 in scale-up.
         */
        final BindingSetChunk chunk = new BindingSetChunk(queryId, sinkId,
                -1/* partitionId */, sink.iterator());

        queryEngine.add(chunk);
        
        return 1;
        
    }

    /**
     * Make a chunk of binding sets available for consumption by the query.
     * <p>
     * Note: this is invoked by {@link QueryEngine#add(BindingSetChunk)}.
     * 
     * @param chunk
     *            The chunk.
     */
    void add(final BindingSetChunk chunk) {

        if (chunk == null)
            throw new IllegalArgumentException();

        // verify still running.
        future.halted();

        // add chunk to be consumed.
        chunksIn.add(chunk);

        if (log.isDebugEnabled())
            log.debug("queryId=" + queryId + ", chunksIn.size()="
                    + chunksIn.size());

    }

    /**
     * Invoked once by the query controller with the initial
     * {@link BindingSetChunk} which gets the query moving.
     * 
     * @todo this should reject multiple invocations for a given query instance.
     */
    public void startQuery(final BindingSetChunk chunk) {
        if (!controller)
            throw new UnsupportedOperationException();
        if (chunk == null)
            throw new IllegalArgumentException();
        if (chunk.queryId != queryId) // @todo equals() if queryId is UUID.
            throw new IllegalArgumentException();
        runStateLock.lock();
        try {
            lifeCycleSetUpQuery();
            availableChunkCount++;
            {
                AtomicLong n = availableChunkCountMap.get(chunk.bopId);
                if (n == null)
                    availableChunkCountMap.put(chunk.bopId, n = new AtomicLong());
                n.incrementAndGet();
            }
            if (log.isInfoEnabled())
                log.info("queryId=" + queryId + ",runningTaskCount="
                        + runningTaskCount + ",availableChunks="
                        + availableChunkCount);
            System.err.println("startQ : bopId=" + chunk.bopId + ",running="
                    + runningTaskCount + ",available=" + availableChunkCount);
            queryEngine.add(chunk);
        } finally {
            runStateLock.unlock();
        }
    }

    /**
     * Message provides notice that the operator has started execution and will
     * consume some specific number of binding set chunks.
     * 
     * @param bopId
     *            The identifier of the operator.
     * @param partitionId
     *            The index partition identifier against which the operator is
     *            executing.
     * @param serviceId
     *            The identifier of the service on which the operator is
     *            executing.
     * @param fanIn
     *            The #of chunks that will be consumed by the operator
     *            execution.
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    public void startOp(final StartOpMessage msg) {
        if (!controller)
            throw new UnsupportedOperationException();
        final Integer bopId = Integer.valueOf(msg.bopId);
        runStateLock.lock();
        try {
            runningTaskCount++;
            {
                AtomicLong n = runningCountMap.get(bopId);
                if (n == null)
                    runningCountMap.put(bopId, n = new AtomicLong());
                n.incrementAndGet();
                if(startedSet.add(bopId)) {
                    // first evaluation pass for this operator.
                    lifeCycleSetUpOperator(msg.bopId);
                }
            }
            availableChunkCount -= msg.nchunks;
            {
                AtomicLong n = availableChunkCountMap.get(bopId);
                if (n == null)
                    throw new AssertionError();
                n.addAndGet(-msg.nchunks);
            }
            System.err.println("startOp: bopId=" + msg.bopId + ",running="
                    + runningTaskCount + ",available=" + availableChunkCount
                    + ",fanIn=" + msg.nchunks);
            final long elapsed = System.currentTimeMillis() - begin;
            if (log.isTraceEnabled())
                log.trace("bopId=" + msg.bopId + ",partitionId=" + msg.partitionId
                        + ",serviceId=" + msg.serviceId + " : runningTaskCount="
                        + runningTaskCount + ", availableChunkCount="
                        + availableChunkCount + ", elapsed=" + elapsed);
            if (elapsed > timeout) {
                future.halt(new TimeoutException());
                cancel(true/* mayInterruptIfRunning */);
            }
        } finally {
            runStateLock.unlock();
        }
    }

    /**
     * Message provides notice that the operator has ended execution. The
     * termination conditions for the query are checked. (For scale-out, the
     * node node controlling the query needs to be involved for each operator
     * start/stop in order to make the termination decision atomic).
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     * 
     * @todo Clone the {@link BOpStats} before reporting to avoid concurrent
     *       modification?
     * 
     * @todo SCALEOUT: Do not release buffers backing the binding set chunks
     *       generated by an operator or the outputs of the final operator (the
     *       query results) until the sink has accepted those outputs. This
     *       means that we must not release the output buffers when the bop
     *       finishes but when its consumer finishes draining the {@link BOp}s
     *       outputs.
     */
    public void haltOp(final HaltOpMessage msg) {
        if (!controller)
            throw new UnsupportedOperationException();
        runStateLock.lock();
        try {
            // update per-operator statistics.
            {
                final BOpStats stats = statsMap.get(msg.bopId);
                if (stats == null) {
                    statsMap.put(msg.bopId, msg.taskStats);
                } else {
                    stats.add(msg.taskStats);
                }
            }
            /*
             * Update termination criteria counters.
             */
            // chunks generated by this task.
            final int fanOut = msg.sinkChunksOut + msg.altSinkChunksOut;
            availableChunkCount += fanOut;
            if (msg.sinkId != null) {
                AtomicLong n = availableChunkCountMap.get(msg.sinkId);
                if (n == null)
                    availableChunkCountMap.put(msg.sinkId, n = new AtomicLong());
                n.addAndGet(msg.sinkChunksOut);
            }
            if (msg.altSinkId != null) {
                AtomicLong n = availableChunkCountMap.get(msg.altSinkId);
                if (n == null)
                    availableChunkCountMap.put(msg.altSinkId, n = new AtomicLong());
                n.addAndGet(msg.altSinkChunksOut);
            }
            // one less task is running.
            runningTaskCount--;
            {
                final AtomicLong n = runningCountMap.get(msg.bopId);
                if (n == null)
                    throw new AssertionError();
                n.decrementAndGet();
            }
            // Figure out if this operator is done.
            if (isOperatorDone(msg.bopId)) {
                /*
                 * No more chunks can appear for this operator so invoke its end
                 * of life cycle hook.
                 */
                lifeCycleTearDownOperator(msg.bopId);
            }
            System.err.println("haltOp : bopId=" + msg.bopId + ",running="
                    + runningTaskCount + ",available=" + availableChunkCount
                    + ",fanOut=" + fanOut);
            assert runningTaskCount >= 0 : "runningTaskCount="
                    + runningTaskCount;
            assert availableChunkCount >= 0 : "availableChunkCount="
                    + availableChunkCount;
            final long elapsed = System.currentTimeMillis() - begin;
            if (log.isTraceEnabled())
                log.trace("bopId=" + msg.bopId + ",partitionId=" + msg.partitionId
                        + ",serviceId=" + queryEngine.getServiceId()
                        + ", nchunks=" + fanOut + " : runningTaskCount="
                        + runningTaskCount + ", availableChunkCount="
                        + availableChunkCount + ", elapsed=" + elapsed);
            // test termination criteria
            if (msg.cause != null) {
                // operator failed on this chunk.
                log.error("Error: Canceling query: queryId=" + queryId
                        + ",bopId=" + msg.bopId + ",partitionId="
                        + msg.partitionId, msg.cause);
                future.halt(msg.cause);
                cancel(true/* mayInterruptIfRunning */);
            } else if (runningTaskCount == 0 && availableChunkCount == 0) {
                // success (all done).
                future.halt(getStats());
                cancel(true/* mayInterruptIfRunning */);
            } else if (elapsed > timeout) {
                // timeout
                future.halt(new TimeoutException());
                cancel(true/* mayInterruptIfRunning */);
            }
        } finally {
            runStateLock.unlock();
        }
    }

    /**
     * Return <code>true</code> the specified operator can no longer be
     * triggered by the query. The specific criteria are that no operators which
     * are descendants of the specified operator are running or have chunks
     * available against which they could run. Under those conditions it is not
     * possible for a chunk to show up which would cause the operator to be
     * executed.
     * 
     * @param bopId
     *            Some operator identifier.
     * 
     * @return <code>true</code> if the operator can not be triggered given the
     *         current query activity.
     * 
     * @throws IllegalMonitorStateException
     *             unless the {@link #runStateLock} is held by the caller.
     */
    protected boolean isOperatorDone(final int bopId) {

        if (!runStateLock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return PipelineUtility.isDone(bopId, queryRef.get(), bopIndex,
                runningCountMap, availableChunkCountMap);

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
     
        System.err.println("lifeCycleSetUpOperator: queryId=" + queryId
                + ", bopId=" + bopId);

    }

    /**
     * Hook invoked the after the given operator has been evaluated for the
     * query for what is known to be the last time. This may be used to tear
     * down life cycle resources for the operator, such as a distributed hash
     * table on a set of nodes identified by annotations of the operator.
     * 
     * @param bopId
     *            The operator identifier.
     * 
     * @todo Prevent retrigger?  See {@link #cancel(boolean)}.
     */
    protected void lifeCycleTearDownOperator(final int bopId) {

        System.err.println("lifeCycleTearDownOperator: queryId=" + queryId
                + ", bopId=" + bopId);

    }

    /**
     * Hook invoked the before any operator is evaluated for the query. This may
     * be used to set up life cycle resources for the query.
     */
    protected void lifeCycleSetUpQuery() {

        System.err.println("lifeCycleSetUpQuery: queryId=" + queryId);

    }

    /**
     * Hook invoked when the query terminates. This may be used to tear down
     * life cycle resources for the query.
     */
    protected void lifeCycleTearDownQuery() {

        System.err.println("lifeCycleTearDownQuery: queryId=" + queryId);

    }
    
    /**
     * Return a {@link FutureTask} which will consume the binding set chunk. The
     * caller must run the {@link FutureTask}.
     * 
     * @param chunk
     *            A chunk to be consumed.
     */
    @SuppressWarnings("unchecked")
    protected FutureTask<Void> newChunkTask(final BindingSetChunk chunk) {
        /*
         * Look up the BOp in the index, create the BOpContext for that BOp, and
         * return the value returned by BOp.eval(context).
         */
        final BOp bop = bopIndex.get(chunk.bopId);
        if (bop == null) {
            throw new NoSuchBOpException(chunk.bopId);
        }
        if (!(bop instanceof BindingSetPipelineOp)) {
            /*
             * @todo evaluation of element[] pipelines needs to use pretty much
             * the same code, but it needs to be typed for E[] rather than
             * IBindingSet[].
             */
            throw new UnsupportedOperationException(bop.getClass().getName());
        }
        // self
        final BindingSetPipelineOp op = ((BindingSetPipelineOp) bop);
        // parent (null if this is the root of the operator tree).
        final BOp p = BOpUtility.getParent(queryRef.get(), op);
        // sink (null unless parent is defined)
        final Integer sinkId = p == null ? null : (Integer) p
                .getProperty(BindingSetPipelineOp.Annotations.BOP_ID);
        final IBlockingBuffer<IBindingSet[]> sink = (p == null ? queryBuffer
                : op.newBuffer());
        // altSink [@todo altSink=null or sink when not specified?]
        final Integer altSinkId = (Integer) op
                .getProperty(BindingSetPipelineOp.Annotations.ALT_SINK_REF);
        if (altSinkId != null && !bopIndex.containsKey(altSinkId)) {
            throw new NoSuchBOpException(altSinkId);
        }
        final IBlockingBuffer<IBindingSet[]> altSink = altSinkId == null ? null
                : op.newBuffer();
        // context
        final BOpContext context = new BOpContext(queryEngine.getFederation(),
                queryEngine.getLocalIndexManager(), readTimestamp,
                writeTimestamp, chunk.partitionId, op.newStats(), chunk.source,
                sink, altSink);
        // FutureTask for operator execution (not running yet).
        final FutureTask<Void> f = op.eval(context);
        // Hook the FutureTask.
        final Runnable r = new Runnable() {
            public void run() {
                final UUID serviceId = queryEngine.getServiceId();
                /*
                 * @todo SCALEOUT: Combine chunks available on the queue for the
                 * current bop. This is not exactly "fan in" since multiple
                 * chunks could be available in scaleup as well.
                 */
                int fanIn = 1;
                int sinkChunksOut = 0;
                int altSinkChunksOut = 0;
                try {
                    clientProxy.startOp(new StartOpMessage(queryId,
                            chunk.bopId, chunk.partitionId, serviceId, fanIn));
                    if (log.isDebugEnabled())
                        log.debug("Running chunk: queryId=" + queryId
                                + ", bopId=" + chunk.bopId + ", bop=" + bop);
                    f.run(); // run
                    f.get(); // verify success
                    if (sink != queryBuffer && !sink.isEmpty()) {
                        // handle output chunk.
                        sinkChunksOut += add(sinkId, sink);
                    }
                    if (altSink != queryBuffer && altSink != null
                            && !altSink.isEmpty()) {
                        // handle alt sink output chunk.
                        altSinkChunksOut += add(altSinkId, altSink);
                    }
                    clientProxy.haltOp(new HaltOpMessage(queryId, chunk.bopId,
                            chunk.partitionId, serviceId, null/* cause */,
                            sinkId, sinkChunksOut, altSinkId,
                            altSinkChunksOut, context.getStats()));
                } catch (Throwable t) {
                    try {
                        clientProxy.haltOp(new HaltOpMessage(queryId,
                                chunk.bopId, chunk.partitionId, serviceId,
                                t/* cause */, sinkId, sinkChunksOut, altSinkId,
                                altSinkChunksOut, context.getStats()));
                    } catch (RemoteException e) {
                        cancel(true/* mayInterruptIfRunning */);
                        log.error("queryId=" + queryId, e);
                    }
                }
            }
        };
        // wrap runnable.
        final FutureTask<Void> f2 = new FutureTask(r, null/* result */);
        // add to list of active futures for this query.
        operatorFutures.put(new BOpShard(chunk.bopId, chunk.partitionId), f2);
        // return : caller will execute.
        return f2;
    }

    /**
     * Return an iterator which will drain the solutions from the query. The
     * query will be cancelled if the iterator is
     * {@link ICloseableIterator#close() closed}.
     * 
     * @return
     * 
     * @todo Not all queries produce binding sets. For example, mutation
     *       operations. We could return the mutation count for mutation
     *       operators, which could be reported by {@link BOpStats} for that
     *       operator (unitsOut).
     * 
     * @todo SCALEOUT: Track chunks consumed by the client so we do not release
     *       the backing {@link ByteBuffer} before the client is done draining
     *       the iterator.
     */
    public IAsynchronousIterator<IBindingSet[]> iterator() {

        return queryBuffer.iterator();
        
    }

    /*
     * Future
     * 
     * Note: This is implemented using delegation to the Haltable so we can hook
     * various methods in order to clean up the state of a completed query.
     */

    /**
     * @todo Cancelled queries must reject or drop new chunks, etc. Queries must
     *       release all of their resources when they are done().
     */
    final public boolean cancel(final boolean mayInterruptIfRunning) {
        // halt the query.
        boolean cancelled = future.cancel(mayInterruptIfRunning);
        // cancel any running operators for this query.
        for (Future<?> f : operatorFutures.values()) {
            if (f.cancel(mayInterruptIfRunning))
                cancelled = true;
        }
        if (queryBuffer != null) {
            // close the output sink.
            queryBuffer.close();
        }
        // remove from the set of running queries.
        queryEngine.runningQueries.remove(queryId, this);
        // release any resources for this query.
        queryEngine.releaseResources(this);
        // life cycle hook for the end of the query.
        lifeCycleTearDownQuery();
        // true iff we cancelled something.
        return cancelled;
    }

    final public Map<Integer, BOpStats> get() throws InterruptedException,
            ExecutionException {

        return future.get();
        
    }

    final public Map<Integer, BOpStats> get(long arg0, TimeUnit arg1)
            throws InterruptedException, ExecutionException, TimeoutException {
        
        return future.get(arg0, arg1);
        
    }

    final public boolean isCancelled() {
        
        return future.isCancelled();
        
    }

    final public boolean isDone() {

        return future.isDone();
        
    }

}
