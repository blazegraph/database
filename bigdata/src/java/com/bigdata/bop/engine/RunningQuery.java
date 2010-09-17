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

import java.rmi.RemoteException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NoSuchBOpException;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.util.concurrent.Haltable;

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
    final private BindingSetPipelineOp query;

    /**
     * An index from the {@link BOp.Annotations#BOP_ID} to the {@link BOp}.
     */
    protected final Map<Integer, BOp> bopIndex;

    /**
     * The run state of the query and the result of the computation iff it
     * completes execution normally (without being interrupted, cancelled, etc).
     */
    final private Haltable<Void> future = new Haltable<Void>();

    /**
     * A collection of {@link Future}s for currently executing operators for
     * this query.
     */
    private final ConcurrentHashMap<BSBundle, Future<?>> operatorFutures = new ConcurrentHashMap<BSBundle, Future<?>>();

    /**
     * The runtime statistics for each {@link BOp} in the query and
     * <code>null</code> unless this is the query controller.
     */
    final private ConcurrentHashMap<Integer/* bopId */, BOpStats> statsMap;

    /**
     * The buffer used for the overall output of the query pipeline.
     * <p>
     * Note: This only exists on the query controller. In order to ensure that
     * the results are transferred to the query controller in scale-out, the
     * top-level operator in the query plan must specify
     * {@link BOpEvaluationContext#CONTROLLER}. For example, {@link SliceOp}
     * uses this {@link BOpEvaluationContext}.
     */
    final private IBlockingBuffer<IBindingSet[]> queryBuffer;

    /**
     * A lock guarding {@link RunState#totalRunningTaskCount},
     * {@link RunState#totalAvailableChunkCount},
     * {@link RunState#availableChunkCountMap}. This is <code>null</code> unless
     * this is the query controller.
     * 
     * @see RunState
     */
    private final ReentrantLock runStateLock;

    /**
     * The run state of this query and <code>null</code> unless this is the
     * query controller.
     */
    final private RunState runState;

    /**
     * Flag used to prevent retriggering of {@link #lifeCycleTearDownQuery()}.
     */
    final AtomicBoolean didQueryTearDown = new AtomicBoolean(false);
    
    /**
     * The chunks available for immediate processing (they must have been
     * materialized).
     * <p>
     * Note: This is package private so it will be visible to the
     * {@link QueryEngine}.
     */
    final/* private */BlockingQueue<IChunkMessage<IBindingSet>> chunksIn = new LinkedBlockingDeque<IChunkMessage<IBindingSet>>();

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

        if(!controller)
            throw new UnsupportedOperationException();
        
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
            future.halt(new TimeoutException());
            cancel(true/* mayInterruptIfRunning */);
            
        }

    }

    /**
     * Return the query deadline (the time at which it will terminate regardless
     * of its run state).
     * 
     * @return The query deadline (milliseconds since the epoch) and
     *         {@link Long#MAX_VALUE} if no explicit deadline was specified.
     */
    public long getDeadline() {
        
        return deadline.get();
        
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
    public BindingSetPipelineOp getQuery() {

        return query;
        
    }
    
    /**
     * Return <code>true</code> iff this is the query controller.
     */
    public boolean isController() {
    
        return controller;
        
    }

    /**
     * Return the current statistics for the query and <code>null</code> unless
     * this is the query controller. For {@link BindingSetPipelineOp} operator
     * which is evaluated there will be a single entry in this map.
     */
    public Map<Integer/*bopId*/,BOpStats> getStats() {
        
        return statsMap;
        
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
            final BindingSetPipelineOp query) {

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
        
        bopIndex = BOpUtility.getIndex(query);
        
        statsMap = controller ? new ConcurrentHashMap<Integer, BOpStats>()
                : null;

        runStateLock = controller ? new ReentrantLock() : null;
        
        runState = controller ? new RunState(this) : null;
        
        if (controller) {

            final BOpStats queryStats = query.newStats();
            
            statsMap.put((Integer) query
                    .getRequiredProperty(BOp.Annotations.BOP_ID), queryStats);
            
            if (!query.isMutation()) {

                queryBuffer = query.newBuffer(queryStats);
                
            } else {
                
                // Note: Not used for mutation queries.
                queryBuffer = null;
                
            }
            
        } else {
            
            // Note: only exists on the query controller.
            queryBuffer = null;
            
        }

        // System.err
        // .println("new RunningQuery:: queryId=" + queryId
//                        + ", isController=" + controller + ", queryController="
//                        + clientProxy + ", queryEngine="
//                        + queryEngine.getServiceUUID());
        
    }

    /**
     * Take a chunk generated by some pass over an operator and make it
     * available to the target operator. How this is done depends on whether the
     * query is running against a standalone database or the scale-out database.
     * <p>
     * Note: The return value is used as part of the termination criteria for
     * the query.
     * <p>
     * The default implementation supports a standalone database. The generated
     * chunk is left on the Java heap and handed off synchronously using
     * {@link QueryEngine#acceptChunk(IChunkMessage)}. That method will queue
     * the chunk for asynchronous processing.
     * 
     * @param sinkId
     *            The identifier of the target operator.
     * @param sink
     *            The intermediate results to be passed to that target operator.
     * 
     * @return The #of {@link IChunkMessage} sent. This will always be ONE (1)
     *         for scale-up. For scale-out, there will be at least one
     *         {@link IChunkMessage} per index partition over which the
     *         intermediate results were mapped.
     */
    protected <E> int handleOutputChunk(final int sinkId,
            final IBlockingBuffer<IBindingSet[]> sink) {

        /*
         * Note: The partitionId will always be -1 in scale-up.
         */
        final int partitionId = -1;
        
        /*
         * FIXME Raise this into an annotation that we can tweak from the unit
         * tests and then debug the problem.
         */
        final boolean oneMessagePerChunk = false;
        
        if (oneMessagePerChunk) {

            final IAsynchronousIterator<IBindingSet[]> itr = sink.iterator();

            int nchunks = 0;
            
            while (itr.hasNext()) {

                final IBlockingBuffer<IBindingSet[]> tmp = new BlockingBuffer<IBindingSet[]>(
                        1);

                tmp.add(itr.next());
                
                tmp.close();
                
                final LocalChunkMessage<IBindingSet> chunk = new LocalChunkMessage<IBindingSet>(
                        clientProxy, queryId, sinkId, partitionId, tmp.iterator());

                queryEngine.acceptChunk(chunk);

                nchunks++;
                
            }
            
            return nchunks;
            
        }
        
        final LocalChunkMessage<IBindingSet> chunk = new LocalChunkMessage<IBindingSet>(
                clientProxy, queryId, sinkId, partitionId, sink.iterator());

        queryEngine.acceptChunk(chunk);

        return 1;

    }   
    
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

        if (isCancelled())
            throw new IllegalStateException("Cancelled");

        if (isDone())
            throw new IllegalStateException("Done");

        // verify still running.
        future.halted();

        // add chunk to be consumed.
        chunksIn.add(msg);

        if (log.isDebugEnabled())
            log.debug("chunksIn.size()=" + chunksIn.size() + ", msg=" + msg);

    }

    /**
     * Invoked once by the query controller with the initial
     * {@link IChunkMessage} which gets the query moving.
     */
    void startQuery(final IChunkMessage<IBindingSet> msg) {

        if (!controller)
            throw new UnsupportedOperationException();
        
        if (msg == null)
            throw new IllegalArgumentException();
        
        if (!queryId.equals(msg.getQueryId()))
            throw new IllegalArgumentException();
        
        runStateLock.lock();
        
        try {

            lifeCycleSetUpQuery();        
            
            runState.startQuery(msg);
            
        } finally {
            
            runStateLock.unlock();
            
        }

    }

    /**
     * Message provides notice that the operator has started execution and will
     * consume some specific number of binding set chunks.
     * 
     * @param msg The {@link StartOpMessage}.
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    public void startOp(final StartOpMessage msg) {

        if (!controller)
            throw new UnsupportedOperationException();
        
        runStateLock.lock();
        
        try {
        
            if (runState.startOp(msg))
                lifeCycleSetUpOperator(msg.bopId);
            
        } catch(TimeoutException ex) {

            future.halt(ex);
            cancel(true/* mayInterruptIfRunning */);

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
     * @param msg The {@link HaltOpMessage}
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    public void haltOp(final HaltOpMessage msg) {
        
        if (!controller)
            throw new UnsupportedOperationException();

        // update per-operator statistics.
        final BOpStats tmp = statsMap.putIfAbsent(msg.bopId, msg.taskStats);

        if (tmp != null && tmp != msg.taskStats) {
            // combine, but do not add to self.
            tmp.add(msg.taskStats);
        }

        Throwable cause = null;
        boolean allDone = false;
        runStateLock.lock();
        
        try {
        
            if (runState.haltOp(msg)) {

                /*
                 * No more chunks can appear for this operator so invoke its end
                 * of life cycle hook.
                 */
                
                lifeCycleTearDownOperator(msg.bopId);

                if(runState.allDone.get()) {

                    allDone = true;
                    
                }
                
            }

        } catch(Throwable ex) {

            cause = ex;

        } finally {
            
            runStateLock.unlock();
            
        }

        /*
         * Handle query termination once we have released the runStateLock.
         * 
         * Note: In scale-out, query termination can involve RMI to the nodes on
         * which query operators are known to be running and to nodes on which
         * resources were allocated which were scoped to the query or an
         * operator's evaluation. Those RMI messages should not go out while we
         * are holding the runStateLock since that could cause deadlock with
         * call backs on haltOp() from the query peers for that query.
         */

        if (cause != null) {
            
            /*
             * Timeout, interrupted, operator error, or internal error in
             * RunState.
             */   
            
            future.halt(cause);
            cancel(true/* mayInterruptIfRunning */);
            
        } else if (allDone) {
            
            // Normal termination.
            future.halt((Void) null);
            cancel(true/* mayInterruptIfRunning */);
            
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
     * 
     * @todo Prevent retrigger?  See {@link #cancel(boolean)}.
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
    
    /**
     * Return a {@link FutureTask} which will consume the binding set chunk. The
     * caller must run the {@link FutureTask}.
     * 
     * @param chunk
     *            A chunk to be consumed.
     */
    @SuppressWarnings("unchecked")
    protected FutureTask<Void> newChunkTask(
            final IChunkMessage<IBindingSet> chunk) {

        // create runnable to evaluate a chunk for an operator and partition.
        final Runnable r = new ChunkTask(chunk);

        // wrap runnable.
        final FutureTask<Void> f2 = new FutureTask(r, null/* result */);

        // add to list of active futures for this query.
        operatorFutures.put(new BSBundle(chunk.getBOpId(), chunk
                .getPartitionId()), f2);

        // return : caller will execute.
        return f2;

    }

    /**
     * Runnable evaluates an operator for some chunk of inputs. In scale-out,
     * the operator may be evaluated against some partition of a scale-out
     * index.
     */
    private class ChunkTask implements Runnable {

        /** Alias for the {@link ChunkTask}'s logger. */
        private final Logger log = chunkTaskLog;

        /**
         * The message with the materialized chunk to be consumed by the
         * operator.
         */
        final IChunkMessage<IBindingSet> msg;
        
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
         * {@link BindingSetPipelineOp.Annotations#ALT_SINK_REF}.
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
         * Create a task to consume a chunk. This looks up the {@link BOp} which
         * is the target for the message in the {@link RunningQuery#bopIndex},
         * creates the sink(s) for the {@link BOp}, creates the
         * {@link BOpContext} for that {@link BOp}, and wraps the value returned
         * by {@link PipelineOp#eval(BOpContext)} in order to handle the outputs
         * written on those sinks.
         * 
         * @param msg
         *            A message containing the materialized chunk and metadata
         *            about the operator which will consume that chunk.
         * 
         * @throws IllegalStateException
         *             unless {@link IChunkMessage#isMaterialized()} is
         *             <code>true</code>.
         */
        public ChunkTask(final IChunkMessage<IBindingSet> msg) {
            if(!msg.isMaterialized())
                throw new IllegalStateException();
            this.msg = msg;
            bopId = msg.getBOpId();
            partitionId = msg.getPartitionId();
            bop = bopIndex.get(bopId);
            if (bop == null) {
                throw new NoSuchBOpException(bopId);
            }
            if (!(bop instanceof BindingSetPipelineOp)) {
                /*
                 * @todo evaluation of element[] pipelines needs to use pretty
                 * much the same code, but it needs to be typed for E[] rather
                 * than IBindingSet[].
                 * 
                 * @todo evaluation of Monet style BATs would also operate under
                 * different assumptions, closer to those of an element[].
                 */
                throw new UnsupportedOperationException(bop.getClass()
                        .getName());
            }
            
            // self
            final BindingSetPipelineOp op = ((BindingSetPipelineOp) bop);
            
            // parent (null if this is the root of the operator tree).
            final BOp p = BOpUtility.getParent(query, op);
            
            // sink (null unless parent is defined)
            sinkId = p == null ? null : (Integer) p
                    .getProperty(BindingSetPipelineOp.Annotations.BOP_ID);
            
            // altSink (null when not specified).
            altSinkId = (Integer) op
                    .getProperty(BindingSetPipelineOp.Annotations.ALT_SINK_REF);
            
            if (altSinkId != null && !bopIndex.containsKey(altSinkId))
                throw new NoSuchBOpException(altSinkId);
            
            if (altSinkId != null && sinkId == null) {
                throw new RuntimeException(
                        "The primary sink must be defined if the altSink is defined: "
                                + bop);
            }

            if (sinkId != null && altSinkId != null
                    && sinkId.intValue() == altSinkId.intValue()) {
                throw new RuntimeException(
                        "The primary and alternative sink may not be the same operator: "
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
             */
            final BOpStats stats;
            if (((PipelineOp<?>) bop).isSharedState()) {
                final BOpStats foo = op.newStats();
                final BOpStats bar = statsMap.putIfAbsent(bopId, foo);
                stats = (bar == null ? foo : bar);
            } else {
                stats = op.newStats();
            }
            
            sink = (p == null ? queryBuffer : op.newBuffer(stats));

            altSink = altSinkId == null ? null : op.newBuffer(stats);
            
            // context : @todo pass in IChunkMessage or IChunkAccessor
            context = new BOpContext<IBindingSet>(RunningQuery.this,
                    partitionId, stats, msg.getChunkAccessor()
                            .iterator(), sink, altSink);

            // FutureTask for operator execution (not running yet).
            ft = op.eval(context);
            
        }

        /**
         * Evaluate the {@link IChunkMessage}.
         */
        public void run() {
            final UUID serviceId = queryEngine.getServiceUUID();
            int fanIn = 1;
            int sinkChunksOut = 0;
            int altSinkChunksOut = 0;
            try {
                clientProxy.startOp(new StartOpMessage(queryId, bopId,
                        partitionId, serviceId, fanIn));
                if (log.isDebugEnabled())
                    log.debug("Running chunk: " + msg);
                ft.run(); // run
                ft.get(); // verify success
                if (sink != null && sink != queryBuffer && !sink.isEmpty()) {
                    /*
                     * Handle sink output, sending appropriate chunk
                     * message(s).
                     * 
                     * Note: This maps output over shards/nodes in s/o.
                     */
                    sinkChunksOut += handleOutputChunk(sinkId, sink);
                }
                if (altSink != null && altSink != queryBuffer
                        && !altSink.isEmpty()) {
                    /*
                     * Handle alt sink output, sending appropriate chunk
                     * message(s).
                     * 
                     * Note: This maps output over shards/nodes in s/o.
                     */
                    altSinkChunksOut += handleOutputChunk(altSinkId,
                            altSink);
                }
                final HaltOpMessage msg = new HaltOpMessage(queryId, bopId,
                        partitionId, serviceId, null/* cause */, sinkId,
                        sinkChunksOut, altSinkId, altSinkChunksOut, context
                                .getStats());
                clientProxy.haltOp(msg);
            } catch (Throwable t) {
                /*
                 * Mark the query as halted on this node regardless of whether
                 * we are able to communicate with the query controller.
                 * 
                 * Note: Invoking halt(t) here will log an error. This logged
                 * error message is necessary in order to catch errors in
                 * clientProxy.haltOp() (above and below).
                 */
                // Note: uncomment if paranoid about masked errors after the 1st reported error.
//                log.error("queryId=" + queryId + ", bopId=" + bopId, t);

                if (t == future.halt(t)) {
                    /*
                     * Send the halt message to the query controller.
                     * 
                     * Note: Since the exception return from halt(t) is our
                     * exception, we are responsible for communicating this
                     * exception to the query controller. If that message does
                     * not arrive then the query controller will not know that
                     * we have terminated the query. This can result in a long
                     * running query which must be explicitly cancelled on the
                     * query controller.
                     * 
                     * @todo if we are unable to send the message to the query
                     * controller then we could retry each time an error is
                     * thrown for this query.
                     */
                    final HaltOpMessage msg = new HaltOpMessage(queryId, bopId,
                            partitionId, serviceId, t/* cause */, sinkId,
                            sinkChunksOut, altSinkId, altSinkChunksOut, context
                                    .getStats());
                    try {
                        clientProxy.haltOp(msg);
                    } catch (RemoteException e) {
                        cancel(true/* mayInterruptIfRunning */);
                        log.error("queryId=" + queryId + ", bopId=" + bopId, e);
                    }
                }
            }

        } // run()
        
    } // class ChunkTask

    /**
     * Return an iterator which will drain the solutions from the query. The
     * query will be cancelled if the iterator is
     * {@link ICloseableIterator#close() closed}.
     * 
     * @throws UnsupportedOperationException
     *             if this is not the query controller.
     */
    public IAsynchronousIterator<IBindingSet[]> iterator() {

        if(!controller)
            throw new UnsupportedOperationException();
        
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
     * 
     * FIXME SCALEOUT: Each query engine peer touched by the running query (or
     * known to have an operator task running at the time that the query was
     * halted) must be notified that the query has been terminated and the
     * receiving query engines must interrupt any running tasks which they have
     * locally for that query.
     * <p>
     * Since this involves RMI to the nodes, we should not issue those RMIs
     * while holding the {@link #runStateLock} (and this could even deadlock
     * with call back from those nodes).
     * <p>
     * When the controller sends a node a terminate signal for an operator, it
     * should not bother to RMI back to the controller (unless this is done for
     * the purposes of confirmation, which is available from the RMI return in
     * any case).
     * 
     * FIXME SCALEOUT: Life cycle methods for operators must have hooks for the
     * operator implementations which are evaluated on the query controller
     * (here) but also on the nodes on which the query will run (for hash
     * partitioned operators).
     */
    final public boolean cancel(final boolean mayInterruptIfRunning) {
        // halt the query.
        boolean cancelled = future.cancel(mayInterruptIfRunning);
        // cancel any running operators for this query on this node.
        for (Future<?> f : operatorFutures.values()) {
            if (f.cancel(mayInterruptIfRunning))
                cancelled = true;
        }
        if (queryBuffer != null) {
            // close the output sink.
            queryBuffer.close();
        }
        if(didQueryTearDown.compareAndSet(false/*expect*/, true/*update*/)) {
            // life cycle hook for the end of the query.
            lifeCycleTearDownQuery();
        }
        // remove from the collection of running queries.
        queryEngine.runningQueries.remove(queryId, this);
        // true iff we cancelled something.
        return cancelled;
    }

    final public Void get() throws InterruptedException,
            ExecutionException {

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
    
}
