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
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
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
            final IQueryClient clientProxy, final PipelineOp query) {

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

        statsMap.put(bopId, bop.newStats());

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

    		if(log.isInfoEnabled())//FIXME TRACE
    			log.info(msg.toString());
    		
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

    		if(log.isInfoEnabled())//FIXME TRACE
    			log.info(msg.toString());

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

//			if (isDone()) {
//				// The query has already halted.
//				throw new InterruptedException();
//			}
    		
			return runState.isAtOnceReady(bopId);
    		
    	} finally {
    		
    		lock.unlock();
    		
    	}
    	
    }

//	/**
//	 * Return <code>true</code> iff there is already an instance of the operator
//	 * running.
//	 * 
//	 * @param bopId
//	 *            The bopId of the operator.
//	 *            
//	 * @return True iff there is at least one instance of the operator running
//	 *         (globally for this query).
//	 */
//	public boolean isOperatorRunning(final int bopId) {
//
//		lock.lock();
//
//		try {
//
//			final AtomicLong nrunning = runState.runningMap.get(bopId);
//
//			if (nrunning == null)
//				return false;
//
//			return nrunning.get() > 0;
//    		
//    	} finally {
//    		
//    		lock.unlock();
//    		
//    	}
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
		
        if (log.isTraceEnabled())
            log.trace("queryId=" + queryId);

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
     * Cancel any running operators for this query on this node (internal API).
     * <p>
     * Note: This will wind up invoking the tear down methods for each operator
     * which was running or which could have been re-triggered.
     * 
     * @return <code>true</code> if any operators were cancelled.
     */
    abstract protected boolean cancelRunningOperators(
            final boolean mayInterruptIfRunning);

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
