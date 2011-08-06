/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Dec 30, 2010
 */

package com.bigdata.bop.engine;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
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
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryContext;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.QueryEngine.Counters;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.DirectBufferPoolAllocator;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.QueryHints;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.rwstore.sector.MemoryManager;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.util.concurrent.Haltable;
import com.bigdata.util.concurrent.IHaltable;

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
 * @version $Id$
 */
abstract public class AbstractRunningQuery implements IRunningQuery, IQueryContext {

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
     * The query deadline. The value is the system clock time in milliseconds
     * when the query is due and {@link Long#MAX_VALUE} if there is no deadline.
     * In order to have a guarantee of a consistent clock, the deadline is
     * interpreted by the query controller.
     */
    final private AtomicLong deadline = new AtomicLong(Long.MAX_VALUE);

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
    protected final ReentrantLock lock = new ReentrantLock();

    /**
     * The run state of this query and <code>null</code> unless this is the
     * query controller.
     */
    final private RunState runState;

    /**
     * Flag used to prevent retriggering of {@link #lifeCycleTearDownQuery()}.
     */
    private final AtomicBoolean didQueryTearDown = new AtomicBoolean(false);

    /**
     * A collection reporting on whether or not a given operator has been torn
     * down. This collection is used to provide the guarantee that an operator
     * is torn down exactly once, regardless of the #of invocations of the
     * operator or the #of errors which might occur during query processing.
     * 
     * @see PipelineOp#tearDown()
     */
    private final Map<Integer/* bopId */, AtomicBoolean> tornDown = new LinkedHashMap<Integer, AtomicBoolean>();

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

    final public long getDeadline() {

        return deadline.get();
        
    }

    final public long getStartTime() {
        
        return startTime.get();
        
    }

    final public long getDoneTime() {
        
        return doneTime.get();
        
    }

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
    final public IQueryClient getQueryController() {

        return clientProxy;

    }

    /**
     * The unique identifier for this query.
     */
    final public UUID getQueryId() {

        return queryId;

    }

    /**
     * Return the operator tree for this query.
     */
    final public PipelineOp getQuery() {

        return query;

    }

    /**
     * Return <code>true</code> iff this is the query controller.
     */
    final public boolean isController() {

        return controller;

    }

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
        
        return statsMap.get(bopId);
        
    }
    
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

                queryBuffer = new BlockingBufferWithStats<IBindingSet[]>(query,
                        queryStats);

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

        statsMap.put(bopId, bop.newStats(this));

        if (!op.getProperty(BOp.Annotations.CONTROLLER,
                BOp.Annotations.DEFAULT_CONTROLLER)) {
            /*
             * Visit children, but not if this is a CONTROLLER operator since
             * its children belong to a subquery.
             */
            final Iterator<BOp> itr = op.argIterator();

            while(itr.hasNext()) {
            
                final BOp t = itr.next();
            
                // visit children (recursion)
                populateStatsMap(t);
                
            }
            
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

            lifeCycleSetUpQuery();

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
     *            The {@link StartOpMessage}.
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    final protected void startOp(final StartOpMessage msg) {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (msg == null)
            throw new IllegalArgumentException();

        if (!queryId.equals(msg.queryId))
            throw new IllegalArgumentException();

        lock.lock();

        try {

            if(log.isTraceEnabled())
                log.trace(msg.toString());
            
            if (runState.startOp(msg)) {

                /*
                 * Set a flag in this collection so we will know that this
                 * operator needs to be torn down (we do not bother to tear down
                 * operators which have never been setup).
                 */
                tornDown.put(msg.bopId, new AtomicBoolean(false));

                /*
                 * TODO It is a bit dangerous to hold the lock while we do this
                 * but this needs to be executed before any other thread can
                 * start an evaluation task for that operator.
                 */
                lifeCycleSetUpOperator(msg.bopId);
                
            }

        } catch (TimeoutException ex) {

            halt(ex);

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
     *            The {@link HaltOpMessage}
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    /*final*/ protected void haltOp(final HaltOpMessage msg) {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (msg == null)
            throw new IllegalArgumentException();

        if (!queryId.equals(msg.queryId))
            throw new IllegalArgumentException();

        lock.lock();

        try {

            if(log.isTraceEnabled())
                log.trace(msg.toString());

            // update per-operator statistics.
            final BOpStats tmp = statsMap.putIfAbsent(msg.bopId, msg.taskStats);

            // combine stats, but do not combine a stats object with itself.
            if (tmp != null && tmp != msg.taskStats) {
                tmp.add(msg.taskStats);
            }

            if (runState.haltOp(msg)) {

                /*
                 * No more chunks can appear for this operator so invoke its end
                 * of life cycle hook IFF it has not yet been invoked.
                 */

                final AtomicBoolean tornDown = AbstractRunningQuery.this.tornDown
                        .get(msg.bopId);

                if (tornDown.compareAndSet(false/* expect */, true/* update */)) {

                    lifeCycleTearDownOperator(msg.bopId);

                }

                if (runState.isAllDone()) {

                    // Normal termination.
                    halt((Void)null);

                }

            }

        } catch (Throwable t) {

            halt(t);

        } finally {

            lock.unlock();

        }

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

//  /**
//   * Return <code>true</code> iff there is already an instance of the operator
//   * running.
//   * 
//   * @param bopId
//   *            The bopId of the operator.
//   *            
//   * @return True iff there is at least one instance of the operator running
//   *         (globally for this query).
//   */
//  public boolean isOperatorRunning(final int bopId) {
//
//      lock.lock();
//
//      try {
//
//          final AtomicLong nrunning = runState.runningMap.get(bopId);
//
//          if (nrunning == null)
//              return false;
//
//          return nrunning.get() > 0;
//          
//      } finally {
//          
//          lock.unlock();
//          
//      }
//      
//    }

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

        final BOp op = getBOpIndex().get(bopId);

        if (op instanceof PipelineOp) {

            try {

                ((PipelineOp) op).setUp();
                
            } catch (Exception ex) {
                
                throw new RuntimeException(ex);
                
            }

        }

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

        final BOp op = getBOpIndex().get(bopId);

        if (op instanceof PipelineOp) {

            try {
                
                ((PipelineOp) op).tearDown();
                
            } catch (Exception ex) {
                
                throw new RuntimeException(ex);
                
            }
            
        }
        
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

        final Iterator<Map.Entry<Integer/* bopId */, AtomicBoolean/* tornDown */>> itr = tornDown
                .entrySet().iterator();
        
        while(itr.hasNext()) {
            
            final Map.Entry<Integer/* bopId */, AtomicBoolean/* tornDown */> entry = itr
                    .next();
            
            final AtomicBoolean tornDown = entry.getValue();

            if (tornDown.compareAndSet(false/* expect */, true/* update */)) {

                /*
                 * Guaranteed one time tear down for this operator.
                 */
                lifeCycleTearDownOperator(entry.getKey()/* bopId */);

            }
            
        }

        releaseNativeMemoryForQuery();
        
        if (log.isTraceEnabled())
            log.trace("queryId=" + queryId);

    }

    /**
     * Release native memory associated with this operator, if any.
     * <p>
     * Note: Operators are responsible for releasing their child
     * {@link IMemoryManager} context, if any, when they terminate. If they do
     * not release the {@link IMemoryManager} context then it will be released
     * no later than the termination of the query.
     * 
     * @param bopId
     * 
     * @see #releaseNativeMemoryForQuery()
     * 
     *      FIXME Verify operator's Future isDone() before releasing native
     *      memory!!!! We should know this by examining the {@link RunState} for
     *      the operator.
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
     * FIXME We need to have distinct events for the query evaluation life cycle
     * and the query results life cycle. Really, this means that temporary
     * solution sets are scoped to the iterator from which the caller is
     * draining the solutions. This is a matter of the scope of the allocation
     * context for the {@link DirectBufferPoolAllocator} and releasing that
     * scope when the {@link #queryIterator} is closed. [For temporary solution
     * sets we might need the ability to read the solution set more than once
     * and we most definitely need the ability to buffer the solution set on the
     * native heap and scope its life cycle to the parent query or even to a
     * concept such as an http session, e.g., by an integration with the NSS
     * using traditional session concepts.]
     */
    protected void releaseNativeMemoryForQuery() {
        
        assert lock.isHeldByCurrentThread();
        
        // clear reference, returning old value.
        final IMemoryManager memoryManager = this.memoryManager.getAndSet(null);

        if (memoryManager != null) {
            
            // release resources.
            memoryManager.clear();
            
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
    
    final public IAsynchronousIterator<IBindingSet[]> iterator() {

        if (!controller)
            throw new UnsupportedOperationException(ERR_NOT_CONTROLLER);

        if (queryIterator == null)
            throw new UnsupportedOperationException();

        return queryIterator;

    }

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
                if (realSource != null)
                    realSource.release();
                // close() IAsynchronousIterators for accepted messages.
                releaseAcceptedMessages();
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
                final String tag = getQuery().getProperty(QueryHints.TAG,
                        QueryHints.DEFAULT_TAG);
                final Counters c = tag == null ? null : queryEngine
                        .getCounters(tag);
                // track #of done queries.
                queryEngine.counters.doneCount.increment();
                if (c != null)
                    c.doneCount.increment();
                // track elapsed run time of done queries.
                final long elapsed = getElapsed();
                queryEngine.counters.elapsedMillis.add(elapsed);
                if (c != null)
                    c.elapsedMillis.add(elapsed);
                if (future.getCause() != null) {
                    // track #of queries with abnormal termination.
                    queryEngine.counters.errorCount.increment();
                    if (c != null)
                        c.errorCount.increment();
                }
                // remove from the collection of running queries.
                queryEngine.halt(this);
            }
            // true iff we cancelled something.
            return cancelled;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Cancel any running operators for this query on this node (internal API).
     * <p>
     * Note: This will wind up invoking the tear down methods for each operator
     * which was running or which could have been re-triggered.
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

    /**
     * {@inheritDoc}
     * <p>
     * FIXME See PipelineOp.Annotations#MAX_MEMORY.
     * <p>
     * It would be nice to have the concept of a limit on the amount of native
     * memory which an operator may use. However, there is currently no way to
     * specify this for a child allocation context on the memory manager. Also,
     * even if we do this for the root MemoryManager, that leads to a situation
     * in which allocations will deadlock.
     * <p>
     * I think that we really want one of two things. Either we want to control
     * the amount of native memory which will be used before we begin evictions
     * to disk or we want to have the child allocation context simply toss an
     * error if it attempts to use too much native memory.
     * <p>
     * To place a limit on memory before eviction to disk (as opposed to
     * eviction to the memory manager) we need to create a new IRawStore
     * interface which maintains a map from an addr to the appropriate backing
     * persistence store BUT we have to somehow mark the addresses as being on
     * the MemoryManager or on an RWStore backed by DISK. This gets into the
     * question of whether or not there is a bit which is clean and available
     * for this purpose. (Consider that we also want such a bit for the HTree to
     * mark bucket pages versus directory pages in the address).
     * <p>
     * If we throw out an exception from the child memory manager if the memory
     * allocation limit is exceeded then we can bound the memory easily enough,
     * but it could lead to an unpleasant surprise.
     * <p>
     * If we do NOT bound the memory, then this could lead to swapping or a
     * kernel over commit error if the total memory burden of the native process
     * grows too large. It could also eat into the OS memory available to buffer
     * the disk.
     * <p>
     * For the moment I am NOT going to put a bound on the native memory which
     * can be allocated by an operator or a query and rely on the maximum
     * concurrency of the queries to have reasonable bounds on the memory
     * demand, but we should think about our options here.
     * <p>
     * I guess we could try the exception and re-do the operator against disk if
     * we have too.
     * <p>
     * Or if memory demand is high the query controller might throttle the start
     * of new queries, only allowing those which are apparently selective (based
     * on some inspection) to execute until more memory has been release...
     * <p>
     * But also see ChunkedRunningQuery#scheduleNext() which places bounds on
     * how much data can be buffered for an operator before it is evaluated.
     * That is the other way to interpret MAX_MEMORY, as a limit on the buffered
     * IBindingSet[]s which are input to the operator (assuming that they are
     * buffered on the native heap) rather than as a limit to the among of
     * native memory the operator may use while it is running.
     */
    public IMemoryManager getMemoryManager() {
        IMemoryManager memoryManager = this.memoryManager.get();
        if (memoryManager == null) {
            lock.lock();
            try {
                memoryManager = this.memoryManager.get();
                if (memoryManager == null) {
                    this.memoryManager.set(memoryManager = new MemoryManager(
                            DirectBufferPool.INSTANCE));
                }
            } finally {
                lock.unlock();
            }
        }
        return memoryManager;
    }

    private final AtomicReference<IMemoryManager> memoryManager = new AtomicReference<IMemoryManager>();
    
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
            sb.append(",deadline=" + deadline.get());
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

}
