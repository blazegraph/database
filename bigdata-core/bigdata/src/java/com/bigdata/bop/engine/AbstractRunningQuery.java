/**

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
 * Created on Dec 30, 2010
 */

package com.bigdata.bop.engine;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.DefaultQueryAttributes;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bset.EndOp;
import com.bigdata.bop.engine.RunState.RunStateEnum;
import com.bigdata.bop.fed.EmptyChunkMessage;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.DirectBufferPoolAllocator;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.rwstore.sector.MemoryManager;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.Haltable;
import com.bigdata.util.concurrent.IHaltable;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Abstract base class for various {@link IRunningQuery} implementations. The
 * purpose of this class is to isolate aspects common to different designs for
 * managing resources for a running query and make it easier to realize
 * different strategies for managing the resources allocated to a running query.
 * <p>
 * There are common requirements for the {@link IRunningQuery}, but a variety of
 * ways in which those requirements can be met. Among the common requirements
 * are a means to manage tradeoffs in the allocation of various resources to the
 * operators in each query. Some of the more important tradeoffs are the #of
 * threads to allocate to each operator (threads bounds IO for Java 6 since we
 * are using a synchronous IO model) and the amount of RAM allocated to each
 * operator (including RAM on the JVM heap and RAM on the native Java process
 * heap). If the #of threads is too restrictive, then queries will progress
 * slowly due to insufficient IO level parallelism. If the query buffers too
 * much data on the JVM heap, then it can cause GC overhead problems that can
 * drastically reduce the responsiveness and throughput of the JVM. Data can be
 * moved off of the JVM heap onto the Java process heap by serializing it into
 * <em>direct</em> {@link ByteBuffer}s. This can be very efficient in
 * combination with hash joins at the expense of increasing the latency to the
 * first result when compared with pipelined evaluation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class AbstractRunningQuery implements IRunningQuery {

    /**
     * Error message used when an operation which must be performed on the query
     * controller is attempted on some other {@link IQueryPeer}.
     */
    protected static final String ERR_NOT_CONTROLLER = "Operator only permitted on the query controller";

    /**
     * Error message used when a request is made after the query has stopped
     * executing.
     */
    protected static final String ERR_QUERY_DONE = "Query is no longer running";

    /**
     * Error message used when a request is addressed to an operator other than
     * the head of the pipeline in a context where the request must be addressed
     * to the operator at the head of the pipeline (e.g., when presenting the
     * initial binding sets to get the query moving.)
     */
    protected static final String ERR_NOT_PIPELINE_START = "Not pipeline start";

    /**
     * Error message used when no operator can be found for a given
     * {@link BOp.Annotations#BOP_ID}.
     */
    protected static final String ERR_NO_SUCH_BOP = "No such bop: id=";

    /**
     * Error message used when two operators have the same
     * {@link BOp.Annotations#BOP_ID}.
     */
    protected static final String ERR_DUPLICATE_IDENTIFIER = "Duplicate identifier: id=";

    private final static transient Logger log = Logger
            .getLogger(AbstractRunningQuery.class);

    /**
     * The class executing the query on this node.
     */
    final private QueryEngine queryEngine;

    /** The unique identifier for this query. */
    final private UUID queryId;
    
    /**
     * Stats associated with static analysis
     */
    private StaticAnalysisStats saStats = null;

//    /**
//     * The query deadline. The value is the system clock time in milliseconds
//     * when the query is due and {@link Long#MAX_VALUE} if there is no deadline.
//     * In order to have a guarantee of a consistent clock, the deadline is
//     * interpreted by the query controller.
//     */
//    final private AtomicLong deadline = new AtomicLong(Long.MAX_VALUE);

    /**
     * The timestamp (ms) when the query begins to execute.
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

    /**
     * The original message which kicked off this query on the query controller.
     * This is NOT required when the query is materialized on another node and
     * MAY be <code>null</code>, but the original message used to kick off the
     * query on the query controller MUST be provided so we can ensure that the
     * source iteration is always closed when the query is cancelled.
     */
    final private IChunkMessage<IBindingSet> realSource;
    
    /** The query. */
    final private PipelineOp query;

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
     * The {@link Future} of this query.
     * <p>
     * Note: This is exposed to the {@link QueryEngine} to let it cache the
     * {@link Future} for recently finished queries.
     */
    final protected IHaltable<Void> getFuture() {

        return future;
        
    }

    /**
     * The runtime statistics for each {@link BOp} in the query and
     * <code>null</code> unless this is the query controller.
     */
    final private ConcurrentHashMap<Integer/* bopId */, BOpStats> statsMap;

    /**
     * The buffer used for the overall output of the query pipeline.
     * <p>
     * Note: This only exists on the query controller, and then only when the
     * top-level operator is not a mutation. In order to ensure that the results
     * are transferred to the query controller in scale-out, the top-level
     * operator in the query plan must specify
     * {@link BOpEvaluationContext#CONTROLLER}. For example, {@link SliceOp} or
     * {@link EndOp} both require this {@link BOpEvaluationContext}.
     */
    final private IBlockingBuffer<IBindingSet[]> queryBuffer;

    /**
     * The iterator draining the {@link #queryBuffer} and <code>null</code> iff
     * the {@link #queryBuffer} is <code>null</code>.
     */
    final private ICloseableIterator<IBindingSet[]> queryIterator;

//    /**
//     * The #of solutions delivered to the {@link #queryBuffer}.
//     */
//    public long getSolutionCount() {
//
//        if (queryBuffer != null) {
//            
//            ((BlockingBufferWithStats<?>) queryBuffer).getElementsAddedCount();
//            
//        }
//
//        return 0L;
//        
//    }
//
//    /**
//     * The #of solution chunks delivered to the {@link #queryBuffer}.
//     */
//    public long getSolutionChunkCount() {
//
//        if (queryBuffer != null) {
//            
//            ((BlockingBufferWithStats<?>) queryBuffer).getChunksAddedCount();
//            
//        }
//
//        return 0L;
//        
//    }
    
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
    protected final ReentrantLock lock = new ReentrantLock();

    /**
     * The run state of this query and <code>null</code> unless this is the
     * query controller.
     */
    final private RunState runState;
    
    /**
     * Flag used to prevent retriggering of query tear down activities in
     * {@link #cancel(boolean)}.
     */
    private final AtomicBoolean didQueryTearDown = new AtomicBoolean(false);

//    /**
//     * A collection reporting on whether or not a given operator has been torn
//     * down. This collection is used to provide the guarantee that an operator
//     * is torn down exactly once, regardless of the #of invocations of the
//     * operator or the #of errors which might occur during query processing.
//     * 
//     * @see PipelineOp#tearDown()
//     */
//    private final Map<Integer/* bopId */, AtomicBoolean> tornDown = new LinkedHashMap<Integer, AtomicBoolean>();

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
    final public void setDeadline(final long deadline) {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        try {

            /*
             * Attempt to set the deadline.
             */
            
            runState.setDeadline(deadline);
            
            queryEngine.addQueryToDeadlineQueue(this);
            
        } catch (QueryTimeoutException e) {

            /*
             * Deadline is expired, so halt the query.
             */

            halt(e);
            
        }
        
    }

    /**
     * If the query deadline has expired, then halt the query.
     * 
     * @throws QueryTimeoutException
     *             if the query deadline has expired.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/772">
     *      Query timeout only checked at operator start/stop. </a>
     */
    final protected void checkDeadline() {

        if (isDone()) {

            // already terminated.
            return;
            
        }

        try {
        
//            if (log.isTraceEnabled())
//                log.trace("Checking " + deadline);

            runState.checkDeadline();
            
        } catch (QueryTimeoutException ex) {

            halt(ex);

            /*
             * Note: The exception is not rethrown when the query halts for a
             * deadline. See startOp() and haltOp() for the standard behavior.
             */

        }
        
    }

    @Override
    final public long getDeadline() {

        return runState.getDeadline();
        
    }

    @Override
    final public long getStartTime() {
        
        return startTime.get();
        
    }

    @Override
    final public long getDoneTime() {
        
        return doneTime.get();
        
    }

    @Override
    final public long getElapsed() {
        
        long mark = doneTime.get();
        
        if (mark == 0L)
            mark = System.currentTimeMillis();
        
        return mark - startTime.get();
        
    }

    /**
     * Return the buffer used for the overall output of the query pipeline and
     * <code>null</code> if this is not the query controller.
     */
    final protected IBlockingBuffer<IBindingSet[]> getQueryBuffer() {
        
        return queryBuffer;
        
    }

    @Override
    public QueryEngine getQueryEngine() {

        return queryEngine;

    }

    @Override
    final public IQueryClient getQueryController() {

        return clientProxy;

    }

    @Override
    final public UUID getQueryId() {

        return queryId;

    }

    @Override
    final public PipelineOp getQuery() {

        return query;

    }

    /**
     * Return <code>true</code> iff this is the query controller.
     */
    final public boolean isController() {

        return controller;

    }

    @Override
    final public Map<Integer/* bopId */, BOpStats> getStats() {

        return Collections.unmodifiableMap(statsMap);

    }

    /**
     * Return the {@link BOpStats} instance associated with the given
     * {@link BOp} identifier.
     * 
     * @param bopId
     *            The {@link BOp} identifier.
     * 
     * @return The associated {@link BOpStats} object -or- <code>null</code> if
     *         there is no entry for that {@link BOp} identifier.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     */
    final public BOpStats getStats(final Integer bopId) {

        if (bopId == null)
            throw new IllegalArgumentException();

        if (statsMap == null)
            throw new IllegalStateException("bopId=" + bopId + ", query="
                    + BOpUtility.toString(query));

        return statsMap.get(bopId);
        
    }
    
    @Override
    final public Map<Integer, BOp> getBOpIndex() {

        return bopIndex;

    }

    /**
     * Return the {@link BOp} having the specified id.
     * 
     * @param bopId
     *            The {@link BOp} identifier.
     * 
     * @return The {@link BOp}.
     * 
     * @throws IllegalArgumentException
     *             if there is no {@link BOp} with that identifier declared in
     *             this query.
     */
    final public BOp getBOp(final int bopId) {

        final BOp bop = getBOpIndex().get(bopId);

        if (bop == null) {

            throw new IllegalArgumentException("Not found: id=" + bopId
                    + ", query=" + query);

        }

        return bop;
        
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
     *            <i>queryEngine</i>. In scale-out, this is an RMI proxy for the
     *            query controller whenever the query is instantiated on a node
     *            other than the query controller itself.
     * @param query
     *            The query.
     * @param realSource
     *            The original message which kicked off this query on the query
     *            controller. This is NOT required when the query is
     *            materialized on another node and MAY be <code>null</code>, but
     *            the original message used to kick off the query on the query
     *            controller MUST be provided so we can ensure that the source
     *            iteration is always closed when the query is cancelled.
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
    public AbstractRunningQuery(final QueryEngine queryEngine,
            final UUID queryId, final boolean controller,
            final IQueryClient clientProxy, final PipelineOp query,
            final IChunkMessage<IBindingSet> realSource) {

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

        this.realSource = realSource;
        
        this.bopIndex = BOpUtility.getIndex(query);

        /*
         * Setup the BOpStats object for each pipeline operator in the query.
         */
        if (controller) {

            runState = new RunState(this);

            statsMap = new ConcurrentHashMap<Integer, BOpStats>();

            populateStatsMap(query);

            /*
             * FIXME Review the concept of mutation queries. It used to be that
             * queries could only either read or write. Now we have access paths
             * which either read or write and each query could use zero or more
             * such access paths.
             */
            if (true/* !query.isMutation() */) {

                // read-only query.

                final BOpStats queryStats = statsMap.get(query.getId());

                queryBuffer = newQueryBuffer(query, queryStats);

                queryIterator = new QueryResultIterator<IBindingSet[]>(this,
                        queryBuffer.iterator());

                // } else {
                //
                // // Note: Not used for mutation queries.
                // queryBuffer = null;
                // queryIterator = null;

            }

        } else {

            runState = null; // Note: only on the query controller.
            statsMap = null; // Note: only on the query controller.
            queryBuffer = null; // Note: only on the query controller.
            queryIterator = null; // Note: only when queryBuffer is defined.

        }

    }

    /**
     * Return the buffer that will be used to absorb solutions. The solutions
     * will be drained from the buffer using its iterator.
     * 
     * @param query
     *            The root of the query plan.
     * @param queryStats
     *            Used to track statistics on the solutions to the query (#of
     *            chunks, #of units).
     *            
     * @return The buffer.
     */
    final protected IBlockingBuffer<IBindingSet[]> newQueryBuffer(
            final PipelineOp query, final BOpStats queryStats) {

        return new BlockingBufferWithStats<IBindingSet[]>(query, queryStats);
        
    }
    
    /**
     * Pre-populate a map with {@link BOpStats} objects for the query. Only the
     * child operands are visited. Operators in subqueries are not visited since
     * they will be assigned {@link BOpStats} objects when they are run as a
     * subquery.
     * 
     * @see BOp.Annotations#CONTROLLER
     */
    private void populateStatsMap(final BOp op) {

        if (!(op instanceof PipelineOp))
            return;

        final PipelineOp bop = (PipelineOp) op;

        final int bopId = bop.getId();

		final BOpStats stats = bop.newStats();
		statsMap.put(bopId, stats);
//		log.warn("bopId=" + bopId + ", stats=" + stats);

        /*
         * Visit children.
         * 
         * Note: The CONTROLLER concept has its subquery expressed through an
         * annotation, not through its arguments. We always want to visit the
         * child arguments of a pipeline operator. We just do not want to visit
         * the operators in its sub-query plan.
         */
        final Iterator<BOp> itr = op.argIterator();

        while (itr.hasNext()) {

            final BOp t = itr.next();

            // visit children (recursion)
            populateStatsMap(t);

        }
            
    }

    /**
     * Message provides notice that the query has started execution and will
     * consume some specific number of binding set chunks.
     * 
     * @param msg
     *            The initial message presented to the query. The message is
     *            used to update the query {@link RunState}. However, the
     *            message will not be consumed until it is presented to
     *            {@link #acceptChunk(IChunkMessage)} by the {@link QueryEngine}
     *            .
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    final protected void startQuery(final IChunkMessage<IBindingSet> msg) {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (msg == null)
            throw new IllegalArgumentException();

        if (!queryId.equals(msg.getQueryId()))
            throw new IllegalArgumentException();

        lock.lock();

        try {

            runState.startQuery(msg);

//            lifeCycleSetUpQuery();

        } catch (TimeoutException ex) {

            halt(ex);

        } finally {

            lock.unlock();

        }

    }

    /**
     * Message provides notice that the operator has started execution and will
     * consume some specific number of binding set chunks.
     * 
     * @param msg
     *            The {@link IStartOpMessage}.
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    final protected void startOp(final IStartOpMessage msg) {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (msg == null)
            throw new IllegalArgumentException();

        if (!queryId.equals(msg.getQueryId()))
            throw new IllegalArgumentException();

        lock.lock();

        try {

            if(log.isTraceEnabled())
                log.trace(msg.toString());
            
            if (future.isDone()) // BLZG-1418
                throw new RuntimeException("Query is done");
            
            runState.startOp(msg);

        } catch (TimeoutException ex) {

            halt(ex);

            /*
             * Note: The exception is not rethrown when the query halts for a
             * deadline.
             */
            
        } finally {

            lock.unlock();

        }

    }

    /**
     * Message provides notice that the operator has ended execution. The
     * termination conditions for the query are checked. (For scale-out, the
     * node controlling the query needs to be involved for each operator
     * start/stop in order to make the termination decision atomic).
     * 
     * @param msg
     *            The {@link IHaltOpMessage}
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    protected void haltOp(final IHaltOpMessage msg) {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (msg == null)
            throw new IllegalArgumentException();

        if (!queryId.equals(msg.getQueryId()))
            throw new IllegalArgumentException();

        lock.lock();

        try {

            if(log.isTraceEnabled())
                log.trace(msg.toString());

            // update per-operator statistics.
            {
                // Data race on insert into CHM.
                BOpStats tmp = statsMap.putIfAbsent(msg.getBOpId(),
                        msg.getStats());

                /**
                 * Combine stats, but do not combine a stats object with itself.
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/464">
                 *      Query Statistics do not update correctly on cluster</a>
                 */
                if (tmp == null) {
                    // won the data race.
                    tmp = msg.getStats();
                } else {
                    // lost the data race.
                    if (tmp != msg.getStats()) {
                        tmp.add(msg.getStats());
                    }
                }
                /**
                 * Post-increment now that we know who one the data race.
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/793">
                 *      Explain reports incorrect value for opCount</a>
                 */
                tmp.opCount.increment();
                // log.warn("bop=" + getBOp(msg.getBOpId()).toShortString()
                // + " : stats=" + tmp);
            }

            switch (runState.haltOp(msg)) {
            case Running:
            case RunningLastPass:
                return;
            case StartLastPass: {
                @SuppressWarnings("rawtypes")
                final Set doneOn = runState.getDoneOn(msg.getBOpId());
                doLastPass(msg.getBOpId(), doneOn);
                return;
            }
            case AllDone:
                /*
                 * Operator is all done.
                 */
                triggerOperatorsAwaitingLastPass();
                // Release any native buffers.
                releaseNativeMemoryForOperator(msg.getBOpId());
                // Check to see if the query is also all done.
                if (runState.isAllDone()) {
                    if (log.isInfoEnabled())
                        log.info("Query reports all done: bopId=" + msg.getBOpId()
                                + ", msg=" + msg + ", runState=" + runState);
                    // Normal termination.
                    halt((Void) null);
                }
                return;
            default:
                throw new AssertionError();
            }
            
        } catch (Throwable t) {

            halt(t);
            
            /*
             * Note: The exception is not rethrown when the query halts for a
             * deadline.
             */

        } finally {

            lock.unlock();

        }

    }

    /**
     * Method handles the case where there are downstream operators awaiting
     * last pass evaluation or at-once evaluation is not re-triggered by the last
     * {@link IChunkMessage} output from an upstream operator. If this situation
     * arises the query will just sit there waiting for a trigger to kick of
     * last pass evaluation. This method works around that by sending an empty
     * {@link IChunkMessage} if the operator would not otherwise have been
     * triggered.
     * 
     * @param msg
     *
     * @see <a href="http://trac.blazegraph.com/ticket/868"> COUNT(DISTINCT) returns no rows rather than ZERO. </a>
     */
    private void triggerOperatorsAwaitingLastPass() {

        /*
         * Examine all downstream operators. Find any at-once operators that
         * can no longer be triggered and which have not yet executed. Then
         * trigger them with an empty chunk message so they will run once
         * and only once.
         */

        // Consider the operators which require at-once evaluation.
        for (Integer bopId : runState.getAtOnceRequired()) {

            if (runState.getOperatorRunState(bopId) == RunStateEnum.StartLastPass) {

                if (log.isInfoEnabled())
                    log.info("Triggering at-once (no solutions in): " + bopId);

                /*
                 * Since evaluation is purely local, we specify -1 as the shardId.
                 */
                final IChunkMessage<IBindingSet> emptyMessage = new EmptyChunkMessage<IBindingSet>(
                        getQueryController(), queryId, bopId, -1/* shardId */, true/* lastInvocation */);

                acceptChunk(emptyMessage);

            }

        }
    	
        if (runState.getTotalLastPassRemainingCount() == 0) {

            return;
            
        }
        
        // Consider the operators which require last pass evaluation.
        for (Integer bopId : runState.getLastPassRequested()) {

            if (runState.getOperatorRunState(bopId) == RunStateEnum.StartLastPass) {

                @SuppressWarnings("rawtypes")
                final Set doneOn = runState.getDoneOn(bopId);

                if (log.isInfoEnabled())
                    log.info("Triggering last pass: " + bopId);

                doLastPass(bopId, doneOn);

            }

        }

    }

    /**
     * Queue empty {@link IChunkMessage}s to trigger the last evaluation pass
     * for an operator which can not be re-triggered by any upstream operator or
     * by {@link IChunkMessage}s which have already been buffered.
     * <p>
     * Note: If the queue for accepting new chunks could block then this could
     * deadlock. We work around that by using the same lock for the
     * AbstractRunningQuery and the queue of accepted messages. If the queue
     * blocks, this thread will be yield the lock and another thread may make
     * progress.
     * 
     * @param msg
     * @param doneOn
     *            The collection of shards or services on which the operator
     *            need to receive a last evaluation pass message.
     */
    @SuppressWarnings("rawtypes")
    protected void doLastPass(final int bopId, final Set doneOn) {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (doneOn == null) {
            /*
             * This operator was never started on anything and we do not need to
             * generate any last pass messages.
             */
            throw new AssertionError("doneOn is null? : bopId=" + bopId
                    + ", runState=" + runState);
        }

        if (doneOn.isEmpty()) {
            /*
             * The operator has received all last evaluation pass notices so
             * this method should not have been called (RunStateEnum should be
             * AllDone).
             */
            throw new AssertionError("doneOn is empty? : bopId=" + bopId
                    + ", runState=" + runState);
        }

        if (doneOn.size() != 1) {
            /*
             * This base class can only handle purely local queries for which
             * there will only be a single element in the doneOn set (either the
             * shardId -1 or the serviceId for the query controller). This
             * method needs to be overridden to handle doneOn in a cluster.
             */
            throw new AssertionError("doneOn set not single element? : bopId="
                    + bopId + ", runState=" + runState + ", doneOn=" + doneOn);
        }

        if (log.isInfoEnabled())
            log.info("Triggering last pass: " + bopId);

        /*
         * Since evaluation is purely local, we specify -1 as the shardId.
         */
        final IChunkMessage<IBindingSet> emptyMessage = new EmptyChunkMessage<IBindingSet>(
                getQueryController(), queryId, bopId, -1/* shardId */, true/* lastInvocation */);

        acceptChunk(emptyMessage);

    }
    
    /**
     * Return <code>true</code> iff the preconditions have been satisfied for
     * the "at-once" invocation of the specified operator (no predecessors are
     * running or could be triggered and the operator has not been evaluated).
     * 
     * @param bopId
     *            Some operator identifier.
     * 
     * @return <code>true</code> iff the "at-once" evaluation of the operator
     *         may proceed.
     */
    protected boolean isAtOnceReady(final int bopId) {
        
        lock.lock();
        
        try {

//          if (isDone()) {
//              // The query has already halted.
//              throw new InterruptedException();
//          }
            
            return runState.isAtOnceReady(bopId);
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

    /**
     * Return the {@link RunStateEnum} for an operator.
     * 
     * @param bopId
     *            The operator.
     *            
     * @return It's {@link RunStateEnum}.
     */
    protected RunStateEnum getRunState(final int bopId) {
        
        lock.lock();
        
        try {

//          if (isDone()) {
//              // The query has already halted.
//              throw new InterruptedException();
//          }
            
            return runState.getOperatorRunState(bopId);
            
        } finally {
            
            lock.unlock();
            
        }
        
    }
    
    /**
     * Attempt to return the {@link RunStateEnum} for an operator
     * (non-blocking).
     * <p>
     * Note: This method is intended for use in contexts where it is desirable,
     * but not critical, to have the {@link RunStateEnum} for the operator. For
     * example, in log messages. The implementation is non-blocking and will
     * barge in if the lock is available and return the {@link RunStateEnum} of
     * the operator. If the lock is not available, it will return
     * <code>null</code>.
     * 
     * @param bopId
     *            The operator.
     * 
     * @return It's {@link RunStateEnum} and <code>null</code> if the lock could
     *         not be acquired.
     */
    protected RunStateEnum tryGetRunState(final int bopId) {

        if (lock.tryLock()) {

            try {

                // if (isDone()) {
                // // The query has already halted.
                // throw new InterruptedException();
                // }

                return runState.getOperatorRunState(bopId);

            } finally {

                lock.unlock();

            }

        } else {
        
            return null;
            
        }

    }
    
    /**
     * Release native memory associated with this operator, if any (NOP, but
     * overridden in scale-out to release NIO buffers used to move solutions
     * around in the cluster).
     * <p>
     * Note: Operators are responsible for releasing their child
     * {@link IMemoryManager} context, if any, when they terminate and should
     * specify the {@link PipelineOp.Annotations#LAST_PASS} annotation to
     * receive notice in the form of a final evaluation pass over an empty
     * {@link IChunkMessage}. If they do NOT release an {@link IMemoryManager}
     * context which is a child of the {{@link #getMemoryManager() query's
     * context}, then their child {@link IMemoryManager} context will be
     * retained until the termination of the query, at which point the query's
     * {@link IMemoryManager} context will be release, and all child contexts
     * will be released automatically along with it.
     * 
     * @param bopId
     * 
     * @see #releaseNativeMemoryForQuery()
     */
    protected void releaseNativeMemoryForOperator(final int bopId) {
        // NOP
    }
    
    /**
     * Release native memory associated with this query, if any.
     * 
     * FIXME This could cause direct buffers to be released back to the pool
     * before the operator tasks have terminated. That is NOT safe as the
     * buffers could then be reissued to other threads while existing threads
     * still have references to the buffers. Really, the same problem exists
     * with the allocation contexts used for NIO transfers of IBindingSet[]s.
     * <p>
     * We will have to be very careful to wait until each operator's Future
     * isDone() before calling clear() on the IMemoryManager to release the
     * native buffers back to the pool. If we release a buffer while an operator
     * is still running, then we will get data corruption arising from the
     * recycling of the buffer to another native buffer user.
     * <p>
     * AbstractRunningQuery.cancel(...) is where we need to handle this, more
     * specifically cancelRunningOperators(). Right now it is not waiting for
     * those operators to terminate.
     * <p>
     * Making this work is tricky. AbstractRunningQuery is holding a lock. The
     * operator tasks do not actually require that lock to terminate, but they
     * are wrapped by a ChunkWrapperTask, which handles reporting back to the
     * AbstractRunningQuery and *does* need the lock, and also by a
     * ChunkFutureTask. Since we actually do do ChunkFutureTask.get(), we are
     * going to deadlock if we invoke that while holding the
     * AbstractRunningQuery's lock.
     * <p>
     * The alternative is to handle the tear down of the native buffers for a
     * query asynchronously after the query has been cancelled, deferring the
     * release of the native buffers back to the direct buffer pool until all
     * tasks for the query are known to be done.
     * 
     * @see BLZG-1658 MemoryManager should know when it has been closed
     * 
     * FIXME We need to have distinct events for the query evaluation life cycle
     * and the query results life cycle. Really, this means that temporary
     * solution sets are scoped to the parent query. This is a matter of the
     * scope of the allocation context for the {@link DirectBufferPoolAllocator}
     * and releasing that scope when the parent query is done (in cancel()).
     * [Also consider scoping the temporary solution sets to a transaction or an
     * HTTP session, e.g., by an integration with the NSS using traditional
     * session concepts.]
     */
    protected void releaseNativeMemoryForQuery() {
        
        assert lock.isHeldByCurrentThread();
        
        // clear reference, returning old value.
        final MemoryManager memoryManager = this.memoryManager.getAndSet(null);

        if (memoryManager != null) {
            
            // release resources.  See BLZG-1658
            memoryManager.close();
            
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
     * 
     * @todo Reconcile {@link #acceptChunk(IChunkMessage)} and
     *       {@link #consumeChunk()}. Why {@link #consumeChunk()} is also used
     *       by the {@link QueryEngine}.
     */
    abstract protected boolean acceptChunk(final IChunkMessage<IBindingSet> msg);

    /**
     * Instruct the {@link IRunningQuery} to consume an {@link IChunkMessage}
     * already on its input queue.
     */
    abstract protected void consumeChunk();
    
    @Override
    final public ICloseableIterator<IBindingSet[]> iterator() {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (queryIterator == null)
            throw new UnsupportedOperationException();

        return queryIterator;

    }

    @Override
    final public void halt(final Void v) {

        lock.lock();

        try {

            // signal normal completion.
            future.halt((Void) v);

            // interrupt anything which is running.
            cancel(true/* mayInterruptIfRunning */);

        } finally {

            lock.unlock();

        }

    }

    @Override
    final public <T extends Throwable> T halt(final T t) {

        if (t == null)
            throw new IllegalArgumentException();

        lock.lock();

        try {

            try {

                // halt the query, return [t].
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
    @Override
    final public boolean cancel(final boolean mayInterruptIfRunning) {
        /*
         * Set if we notice an interrupt during clean up of the query and then
         * propagated to the caller in the finally {} clause.
         */
        boolean interrupted = false;
        lock.lock();
        try {
            // halt the query.
            boolean cancelled = future.cancel(mayInterruptIfRunning);
            if (didQueryTearDown
                    .compareAndSet(false/* expect */, true/* update */)) {
                /*
                 * Do additional cleanup exactly once.
                 */
                if (realSource != null)
                    realSource.release();
                // close() IAsynchronousIterators for accepted messages.
                releaseAcceptedMessages();
                /*
                 * Cancel any running operators for this query on this node.
                 * 
                 * Note: This can interrupt *this* thread. E.g., when SLICE
                 * calls halt().
                 */
                cancelled |= cancelRunningOperators(mayInterruptIfRunning);
                /*
                 * Test and clear the interrupt status.
                 * 
                 * Note: This prevents a thread from interrupting itself during
                 * the query tear down. If we do not do this then the interrupt
                 * tends to get "noticed" by the next lock acquisition, which
                 * happens to be the one where we release the native memory
                 * buffers.
                 * 
                 * TODO It may be possible for interrupts to be thrown inside of
                 * these methods after we have tested and cleared the interrupt
                 * status of the Thread. That would result in a wrapped
                 * exception and the cancelQueryOnPeers() or queryBuffer.close()
                 * might not be processed properly.
                 */
                interrupted |= Thread.interrupted();
                if (controller) {
                    // cancel query on other peers.
                    cancelled |= cancelQueryOnPeers(future.getCause(),
                            runState.getServiceIds());
                }
                if (queryBuffer != null) {
                    /*
                     * Close the query buffer so the iterator draining the query
                     * results will recognize that no new results will become
                     * available. Failure to do this will cause the iterator to
                     * hang waiting for more results.
                     */
                    queryBuffer.close();
                }
                // release native buffers.
                releaseNativeMemoryForQuery();
                // mark done time.
                doneTime.set(System.currentTimeMillis());
                // log summary statistics for the query.
                if (isController())
                    QueryLog.log(this);
//                final String tag = getQuery().getProperty(QueryHints.TAG,
//                        QueryHints.DEFAULT_TAG);
//                final Counters c = tag == null ? null : queryEngine
//                        .getCounters(tag);
                // track #of done queries.
                queryEngine.counters.queryDoneCount.increment();
//                if (c != null)
//                    c.doneCount.increment();
                // track elapsed run time of done queries.
                final long elapsed = getElapsed();
                queryEngine.counters.elapsedMillis.add(elapsed);
//                if (c != null)
//                    c.elapsedMillis.add(elapsed);
                if (future.getCause() != null) {
                    // track #of queries with abnormal termination.
                    queryEngine.counters.queryErrorCount.increment();
//                    if (c != null)
//                        c.errorCount.increment();
                }
                // remove from the collection of running queries.
                queryEngine.halt(this);
            }
            // true iff we cancelled something.
            return cancelled;
        } finally {
            lock.unlock();
            if(interrupted) {
                // Propagate the interrupt.
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Cancel any running operators for this query on this node (internal API).
     * 
     * @return <code>true</code> if any operators were cancelled.
     */
    abstract protected boolean cancelRunningOperators(
            final boolean mayInterruptIfRunning);

    /**
     * Close the {@link IAsynchronousIterator} for any {@link IChunkMessage}s
     * which have been <em>accepted</em> for this queue on this node (internal
     * API).
     * <p>
     * Note: This must be invoked while holding a lock which is exclusive with
     * the lock used to hand off {@link IChunkMessage}s to operator tasks
     * otherwise we could wind up invoking {@link IAsynchronousIterator#close()}
     * from on an {@link IAsynchronousIterator} running in a different thread.
     * That would cause visibility problems in the close() semantics unless the
     * {@link IAsynchronousIterator} is thread-safe for close (e.g., volatile
     * write, synchronized, etc.). The appropriate lock for this is
     * {@link AbstractRunningQuery#lock}. This method is only invoked out of
     * {@link AbstractRunningQuery#cancel(boolean)} which owns that lock.
     */
    abstract protected void releaseAcceptedMessages();

    // {
    // boolean cancelled = false;
    //
    // final Iterator<ConcurrentHashMap<ChunkFutureTask,ChunkFutureTask>> fitr =
    // operatorFutures.values().iterator();
    //
    // while (fitr.hasNext()) {
    //
    // final ConcurrentHashMap<ChunkFutureTask,ChunkFutureTask> set =
    // fitr.next();
    //
    // for(ChunkFutureTask f : set.keySet()) {
    //
    // if (f.cancel(mayInterruptIfRunning))
    // cancelled = true;
    //        
    // }
    //    
    // }
    //
    // return cancelled;
    //
    // }

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
    protected boolean cancelQueryOnPeers(final Throwable cause,
            final Set<UUID/*ServiceId*/> startedOn) {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return false;

    }

    @Override
    final public Void get() throws InterruptedException, ExecutionException {

        return future.get();

    }

    @Override
    final public Void get(final long arg0, final TimeUnit arg1)
            throws InterruptedException, ExecutionException, TimeoutException {

        return future.get(arg0, arg1);

    }

    @Override
    final public boolean isCancelled() {

        return future.isCancelled();

    }

    @Override
    final public boolean isDone() {

        return future.isDone();

    }

    @Override
    final public Throwable getCause() {

        return future.getCause();

    }

    @Override
    final public Throwable getAsThrownCause() {

        return future.getAsThrownCause();

    }

    @Override
    public IBigdataFederation<?> getFederation() {

        return queryEngine.getFederation();

    }

    @Override
    public IIndexManager getLocalIndexManager() {

        return queryEngine.getIndexManager();

    }

    /**
     * Return the #of instances of the operator which are concurrently
     * executing.
     */
    protected long getRunningCount(final int bopId) {

        // Note: lock is NOT required.
        
        return runState.getRunningCount(bopId);
        
    }

    /**
     * Return the #of shards or nodes on which the operator has started
     * evaluation. This is basically a measure of the fan out of the operator
     * across the cluster. For example, it will report the #of shards on which a
     * sharded join has read based on the solutions being mapped across that
     * join. The units are shards if the operator is sharded and nodes if the
     * operator is hash partitioned.
     * 
     * @param bopId
     *            The operator identifier.
     * 
     * @return The #of shards or nodes on which the operator has started.
     */
    protected int getStartedOnCount(final int bopId) {
        
        // Note: lock is NOT required.
        
        return runState.getStartedOnCount(bopId);
        
    }
    
    @Override
    public IMemoryManager getMemoryManager() {
        MemoryManager memoryManager = this.memoryManager.get();
        if (memoryManager == null) {
            lock.lock();
            try {
                memoryManager = this.memoryManager.get();
                if (memoryManager == null) {
                    this.memoryManager.set(memoryManager = newMemoryManager());
                }
            } finally {
                lock.unlock();
            }
        }
        return memoryManager;
    }
    
    private final AtomicReference<MemoryManager> memoryManager = new AtomicReference<MemoryManager>();
    
    /**
     * Allocate a memory manager for the query.
     * 
     * @see QueryHints#ANALYTIC_MAX_MEMORY_PER_QUERY
     * 
     * @see QueryHints#DEFAULT_ANALYTIC_MAX_MEMORY_PER_QUERY
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-42" > Per query
     *      memory limit for analytic query mode. </a>
     */
    private MemoryManager newMemoryManager() {
        
        // The native memory pool that will be used by this query.
        final DirectBufferPool pool = DirectBufferPool.INSTANCE;
        
        // Figure out how much memory may be allocated by this query.
        long maxMemoryBytesPerQuery = QueryHints.DEFAULT_ANALYTIC_MAX_MEMORY_PER_QUERY;
        if (maxMemoryBytesPerQuery < 0) {
            // Ignore illegal values.
            maxMemoryBytesPerQuery = 0L;
        }

        final boolean blocking;
        final int nsectors;
        if (maxMemoryBytesPerQuery == 0) {
            /*
             * Allocation are blocking IFF there is no bound on the memory for
             * the query.
             */
            blocking = true; // block until allocation is satisfied.
            nsectors = Integer.MAX_VALUE; // no limit
        } else {
            /*
             * Allocations do not block if we run out of native memory for this
             * query. Instead a memory allocation exception will be thrown and
             * the query will break.
             * 
             * The #of sectors is computed by dividing through by the size of
             * the backing native ByteBuffers and then rounding up.
             */
            blocking = false; // throw exception if query uses too much RAM.

            // The capacity of the buffers in this pool.
            final int bufferCapacity = pool.getBufferCapacity();

            // Figure out the maximum #of buffers (rounding up).
            nsectors = (int) Math.ceil(maxMemoryBytesPerQuery
                    / (double) bufferCapacity);

        }

        return new MemoryManager(pool, nsectors, blocking, null/* properties */);

    }

    @Override
    final public IQueryAttributes getAttributes() {
        
        return queryAttributes;
        
    }
    
    private final IQueryAttributes queryAttributes = new DefaultQueryAttributes();

    /**
     * Report a snapshot of the known (declared) child {@link IRunningQuery}s
     * for this {@link IRunningQuery} and (recursively) for any children of this
     * {@link IRunningQuery}.
     * 
     * @return An array providing a snapshot of the known child
     *         {@link IRunningQuery}s and never <code>null</code>.
     */
    final public IRunningQuery[] getChildren() {

        synchronized (children) {

            if (children.isEmpty()) {

                // Fast path if no children.
                return EMPTY_ARRAY;

            }

            // Add in all direct child queries.
            final List<IRunningQuery> tmp = new LinkedList<IRunningQuery>(
                    children.values());

            // Note: Do not iterator over [tmp] to avoid concurrent modification.
            for (IRunningQuery c : children.values()) {

                // Recursive for each child.
                tmp.addAll(Arrays.asList(((AbstractRunningQuery) c)
                        .getChildren()));

            }

            // Convert to array.
            return tmp.toArray(new IRunningQuery[tmp.size()]);

        }

    }

    private static final IRunningQuery[] EMPTY_ARRAY = new IRunningQuery[0];

    /**
     * Attach a child query.
     * <p>
     * Queries as submitted do not know about parent/child relationships
     * 
     * @param childQuery
     *            The child query.
     * 
     * @return <code>true</code> if the child query was not already declared.
     */
    final public boolean addChild(final IRunningQuery childQuery) {
    
        synchronized(children) {
        
            final UUID childId = childQuery.getQueryId();
            
            if (children.containsKey(childId)) {

                return false;
                
            }

            if (future.isDone()) { // BLZG-1418
                childQuery.cancel(true/* mayInterruptIfRunning */);
                throw new RuntimeException("Query is done");
            }

            children.put(childId, childQuery);
            
            return true;
            
        }
        
    }

    final private LinkedHashMap<UUID, IRunningQuery> children = new LinkedHashMap<UUID, IRunningQuery>();

    /**
     * Return the textual representation of the {@link RunState} of this query.
     * <p>
     * Note: Exposed for log messages in derived classes since {@link #runState}
     * is private.
     */
    protected String runStateString() {
        lock.lock();
        try {
            return runState.toString();
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getName());
        sb.append("{queryId=" + queryId);
        /*
         * Note: Obtaining the lock here is required to avoid concurrent
         * modification exception in RunState's toString() when there is a
         * concurrent change in the RunState. It also makes the isDone() and
         * isCancelled() reporting atomic.
         */
        lock.lock();
        try {
            sb.append(",elapsed=" + getElapsed());
            sb.append(",deadline=" + runState.getDeadline());
            sb.append(",isDone=" + isDone());
            sb.append(",isCancelled=" + isCancelled());
            sb.append(",runState=" + runState);
        } finally {
            lock.unlock();
        }
        sb.append(",controller=" + controller);
        sb.append(",clientProxy=" + clientProxy);
        sb.append(",query=" + query);
        sb.append("}");
        return sb.toString();
    }

    // abstract protected IChunkHandler getChunkHandler();

	/**
	 * Return <code>true</code> iff the root cause of the {@link Throwable} was
	 * an interrupt. This checks for any of the different kinds of exceptions
	 * which can be thrown when an interrupt is encountered.
	 * 
	 * @param t
	 *            The throwable.
	 * @return <code>true</code> iff the root cause was an interrupt.  
	 * 
	 * TODO This could be optimized by checking once at each level for any of
	 *         the indicated exceptions.
	 */
	static public boolean isRootCauseInterrupt(final Throwable t) {
		if (InnerCause.isInnerCause(t, InterruptedException.class)) {
			return true;
		} else if (InnerCause.isInnerCause(t, ClosedByInterruptException.class)) {
			return true;
		} else if (InnerCause.isInnerCause(t, InterruptedException.class)) {
			return true;
		}
		return false;
	}
	
   @Override
   public void setStaticAnalysisStats(StaticAnalysisStats saStats) {
      this.saStats = saStats;
   }

   @Override
   public StaticAnalysisStats getStaticAnalysisStats() {
      return saStats;
   }


}
