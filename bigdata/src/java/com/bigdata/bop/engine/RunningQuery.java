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
import com.bigdata.bop.NoSuchBOpException;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.util.concurrent.Haltable;

/**
 * Metadata about running queries.
 */
public class RunningQuery implements Future<Map<Integer,BOpStats>>, IRunningQuery {

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
    final private QueryEngine queryEngine;

    /** The unique identifier for this query. */
    final private long queryId;

    /**
     * The timestamp or transaction identifier against which the query is
     * reading.
     */
    final private long readTimestamp;

    /**
     * The timestamp or transaction identifier against which the query is
     * writing.
     */
    final private long writeTimestamp;

    /**
     * The timestamp when the query was accepted by this node (ms).
     */
    final private long begin;

    /**
     * How long the query is allowed to run (elapsed milliseconds) -or-
     * {@link Long#MAX_VALUE} if there is no deadline.
     */
    final private long timeout;

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

    /** The query iff materialized on this node. */
    final private AtomicReference<BOp> queryRef;

    /**
     * The buffer used for the overall output of the query pipeline.
     * 
     * @todo How does the pipeline get attached to this buffer? Via a special
     *       operator? Or do we just target the coordinating {@link QueryEngine}
     *       as the sink of the last operator so we can use NIO transfers?
     */
    final private IBlockingBuffer<IBindingSet[]> queryBuffer;

    /**
     * An index from the {@link BOp.Annotations#BOP_ID} to the {@link BOp}.
     */
    protected final Map<Integer, BOp> bopIndex;

    /**
     * A collection of {@link Future}s for currently executing operators for
     * this query.
     */
    private final ConcurrentHashMap<BSBundle, Future<?>> operatorFutures = new ConcurrentHashMap<BSBundle, Future<?>>();

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
     * The chunks available for immediate processing.
     * <p>
     * Note: This is package private so it will be visible to the
     * {@link QueryEngine}.
     * 
     * @todo It is likely that we can convert to the use of
     *       {@link BlockingQueue} instead of {@link BlockingBuffer} in the
     *       operators and then handle the logic for combining chunks inside of
     *       the {@link QueryEngine}. E.g., by scanning this list for chunks for
     *       the same bopId and combining them logically into a single chunk.
     *       <p>
     *       For scale-out, chunk combination will naturally occur when the node
     *       on which the operator will run requests the {@link ByteBuffer}s
     *       from the source nodes. Those will get wrapped up logically into a
     *       source for processing. For selective operators, those chunks can be
     *       combined before we execute the operator. For unselective operators,
     *       we are going to run over all the data anyway.
     */
    final /*private*/ BlockingQueue<BindingSetChunk> chunksIn = new LinkedBlockingDeque<BindingSetChunk>();

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
    public long getQueryId() {
        
        return queryId;
        
    }

    /**
     * Return the operator tree for this query. If query processing is
     * distributed and the query has not been materialized on this node, then it
     * is materialized now.
     * 
     * @return The query.
     */
    public BOp getQuery() {

        if (queryRef.get() == null) {

            synchronized (queryRef) {

                if (queryRef.get() == null) {

                    try {

                        queryRef.set(clientProxy.getQuery(queryId));

                    } catch (RemoteException e) {

                        throw new RuntimeException(e);

                    }

                }

            }

        }
        
        return queryRef.get();

   }

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
     * <p>
     * Note: If we are running standalone, then we leave the data on the heap
     * rather than formatting it onto a {@link ByteBuffer}.
     * 
     * @param sinkId
     *            The identifier of the target operator.
     * @param sink
     *            The intermediate results to be passed to that target operator.
     * 
     * @return The #of chunks made available for consumption by the sink. This
     *         will always be ONE (1) for scale-up. For scale-out, there will be
     *         one chunk per index partition over which the intermediate results
     *         were mapped.
     */
    protected <E> int add(final int sinkId,
            final IBlockingBuffer<IBindingSet[]> sink) {

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
                        + ",serviceId=" + queryEngine.getServiceUUID()
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
        // altSink (null when not specified).
        final Integer altSinkId = (Integer) op
                .getProperty(BindingSetPipelineOp.Annotations.ALT_SINK_REF);
        if (altSinkId != null && !bopIndex.containsKey(altSinkId)) {
            throw new NoSuchBOpException(altSinkId);
        }
        final IBlockingBuffer<IBindingSet[]> altSink = altSinkId == null ? null
                : op.newBuffer();
        // context
        final BOpContext context = new BOpContext(this, chunk.partitionId, op
                .newStats(), chunk.source, sink, altSink);
        // FutureTask for operator execution (not running yet).
        final FutureTask<Void> f = op.eval(context);
        // Hook the FutureTask.
        final Runnable r = new Runnable() {
            public void run() {
                final UUID serviceId = queryEngine.getServiceUUID();
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
        operatorFutures.put(new BSBundle(chunk.bopId, chunk.partitionId), f2);
        // return : caller will execute.
        return f2;
    }

    /**
     * Return an iterator which will drain the solutions from the query. The
     * query will be cancelled if the iterator is
     * {@link ICloseableIterator#close() closed}.
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

    public void halt() {

        cancel(true/* mayInterruptIfRunning */);

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
        // life cycle hook for the end of the query.
        lifeCycleTearDownQuery();
        // remove from the collection of running queries.
        queryEngine.runningQueries.remove(queryId, this);
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

    public IBigdataFederation<?> getFederation() {
        
        return queryEngine.getFederation();
        
    }

    public IIndexManager getIndexManager() {
        
        return queryEngine.getIndexManager();
        
    }

    public long getReadTimestamp() {
     
        return readTimestamp;
        
    }

    public long getWriteTimestamp() {
        
        return writeTimestamp;
        
    }

}
