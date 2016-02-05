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
 * Created on Sep 15, 2010
 */

package com.bigdata.bop.engine;

import java.text.DateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.NoBOpIdException;
import com.bigdata.bop.NoSuchBOpException;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.join.PipelineJoinStats;

/**
 * The run state for an {@link IRunningQuery}. This class is thread-safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class RunState {

    static private final Logger log = Logger.getLogger(RunState.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static private boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static private boolean DEBUG = log.isDebugEnabled();
    
    /**
     * True iff the {@link #log} level is TRACE or less.
     */
    final static private boolean TRACE = log.isTraceEnabled();
    
    /**
     * Inner class provides a 2nd logger used for tabular representations.
     */
    static private class TableLog {

        static private final Logger tableLog = Logger.getLogger(TableLog.class);

        final static private boolean INFO = tableLog.isInfoEnabled();

    }

    /**
     * Message if the query has already started evaluation.
     */
    static private final transient String ERR_QUERY_STARTED = "Query already running.";

    /**
     * Message if query evaluation has already halted.
     */
    static private final transient String ERR_QUERY_HALTED = "Query already halted.";

    /**
     * Message if an operator addressed by a {@link IHaltOpMessage} was never started.
     */
    static private final transient String ERR_OP_NOT_STARTED = "Operator never ran.";

    /**
     * Message if an operator addressed by a message has been halted.
     */
    static private final transient String ERR_OP_HALTED = "Operator is not running.";

    /**
     * Message if a query deadline has been exceeded.
     */
    static private final transient String ERR_DEADLINE = "Query deadline is expired.";

    /**
     * The state of the {@link RunState} object. By factoring this into an inner
     * class referenced by a private field we are able to write unit tests which
     * directly manipulate the internal state of the {@link RunState} object
     * while protecting its state from modification.
     */
    static class InnerState {

        /**
         * Constructor.
         */
        InnerState(final BOp query, final UUID queryId, final long deadline,
                final long begin, final Map<Integer, BOp> bopIndex) {

            if (query == null)
                throw new IllegalArgumentException();

            if (queryId == null)
                throw new IllegalArgumentException();

            if (deadline <= 0L)
                throw new IllegalArgumentException();

            if (begin <= 0L)
                throw new IllegalArgumentException();

            if (bopIndex == null)
                throw new IllegalArgumentException();

            this.query = query;

            this.queryId = queryId;

            this.deadline = new AtomicLong(deadline);

            this.bopIndex = bopIndex;

            this.begin = begin;

        }

        /**
         * The query.
         */
        private final BOp query;
        
        /**
         * An index from {@link BOp.Annotations#BOP_ID} to {@link BOp} for the
         * {@link #query}.
         */
        final Map<Integer,BOp> bopIndex;

        /**
         * The query identifier.
         */
        final UUID queryId;

        /**
         * The timestamp when the {@link RunState} was created (milliseconds
         * since the epoch).
         */
        final long begin;
        
        /**
         * The query deadline (milliseconds since the epoch).
         * 
         * @see BOp.Annotations#TIMEOUT
         * @see IRunningQuery#getDeadline()
         */
        final AtomicLong deadline;

        /**
         * Set to <code>true</code> iff the query evaluation has begun.
         * 
         * @see #startQuery(IChunkMessage)
         */
        final AtomicBoolean started = new AtomicBoolean(false);
        
        /**
         * Set to <code>true</code> iff the query evaluation is complete due to
         * normal termination.
         * 
         * @see #haltOp(IHaltOpMessage)
         */
        final AtomicBoolean allDone = new AtomicBoolean(false);
        
        /**
         * The #of run state transitions which have occurred for this query.
         */
        final AtomicLong stepCount = new AtomicLong();

        /**
         * The #of tasks for this query which have started but not yet halted.
         */
        final AtomicLong totalRunningCount = new AtomicLong();

        /**
         * The #of {@link IChunkMessage} for the query which a running task has made
         * available but which have not yet been accepted for processing by another
         * task.
         * <p>
         * Note: {@link ChunkedRunningQuery} drops {@link IChunkMessage}s on the
         * {@link QueryEngine} as soon as they are generated. A
         * {@link IChunkMessage} MAY be taken for evaluation as soon as it is
         * published to the {@link QueryEngine}. This means that the operator task
         * which will consume that {@link IChunkMessage} can begin to execute
         * <em>before</em> {@link ChunkedRunningQuery#haltOp(IHaltOpMessage)} is invoked to
         * indicate the end of the operator task which produced that
         * {@link IChunkMessage}. Due to the potential overlap in these schedules
         * {@link RunState#totalAvailableCount} may be <em>transiently negative</em>
         * . This is the expected behavior.
         */
        final AtomicLong totalAvailableCount = new AtomicLong();

        /**
         * The #of operators which have requested last pass evaluation and whose
         * last pass evaluation phase is not yet complete. This is incremented
         * each time we observe a {@link IStartOpMessage} for an operator not yet
         * found in {@link #lastPassRequested}. When last pass evaluation begins
         * for an operator, its {@link #startedOn} set is copied and set as its
         * {@link #doneOn} set. The entries in the {@link #doneOn} set are
         * removed as we observe {@link IHaltOpMessage}s for that operator during
         * last pass evaluation. The {@link #totalLastPassRemainingCount} is
         * decremented each time we count the {@link #doneOn} set for an
         * operator becomes empty.
         */
        final AtomicLong totalLastPassRemainingCount = new AtomicLong();
        
        /**
         * A map reporting the #of {@link IChunkMessage} available for each operator
         * in the pipeline. The total #of {@link IChunkMessage}s available across
         * all operators in the pipeline is reported by {@link #totalAvailableCount}
         * .
         * <p>
         * The movement of the intermediate binding set chunks forms an acyclic
         * directed graph. This map is used to track the #of chunks available for
         * each {@link BOp} in the pipeline. When a {@link BOp} has no more incoming
         * chunks, we send an asynchronous message to all nodes on which that
         * {@link BOp} had executed informing the {@link QueryEngine} on that node
         * that it should immediately release all resources associated with that
         * {@link BOp}.
         * <p>
         * Note: Since the map contains {@link AtomicLong}s it can not be readily
         * exposed as {@link Map} object. If we were to expose the map, it would
         * have to be via a get(key) style interface. Even an immutable map does
         * not help because of the mutable {@link AtomicLong}s.
         */
        final Map<Integer/* bopId */, AtomicLong/* availableChunkCount */> availableMap = new LinkedHashMap<Integer, AtomicLong>();

        /**
         * A collection reporting on the #of instances of a given {@link BOp} which
         * are concurrently executing. An entry in this map indicates that the
         * operator has been evaluated at least once.
         * <p>
         * Note: Since the map contains {@link AtomicLong}s it can not be readily
         * exposed as {@link Map} object. If we were to expose the map, it would
         * have to be via a get(key) style interface. Even an immutable map does
         * not help because of the mutable {@link AtomicLong}s.
         */
        final Map<Integer/* bopId */, AtomicLong/* runningCount */> runningMap = new LinkedHashMap<Integer, AtomicLong>();

        /**
         * The set of services on which this query has started an operator. This
         * will be just the query controller for purely local evaluation. In
         * scale-out, this will include each service on which the query has been
         * evaluated. This collection is maintained based on the
         * {@link #startQuery(IChunkMessage)} and {@link #startOp(IStartOpMessage)}
         * messages.
         */
        final Set<UUID/* serviceId */> serviceIds = new LinkedHashSet<UUID>();
        
        /**
         * A map associating each operator for which evaluation has begun with the
         * the set of distinct nodes or shards where the operator has been submitted
         * for evaluation. The {@link Set} associated with each operator will be a
         * <code>Set&lt;serviceId&gt;</code> or <code>Set&lt;shardId&gt;</code> (iff
         * sharded evaluation). Whether the set stores serviceIds or shardIds
         * depends on the {@link BOpEvaluationContext} of the operator. (For purely
         * local invocations we will either store the {@link UUID} of the query
         * controller service or the special shardId <code>-1</code>.)
         * <p>
         * If {@link #getOperatorRunState(int, boolean)} reports
         * {@link RunStateEnum#StartLastPass}, then that a copy of that
         * <code>Set&lt;serviceId&gt;</code> or <code>Set&lt;shardId&gt;</code> is
         * put on {@link #doneOn}. The {@link #doneOn} set for each operator is made
         * available to the {@link IRunningQuery} so it can send out the appropriate
         * messages for the last evaluation pass.
         * <p>
         * {@link #doneOn} is used to count off lastPass() invocations for an
         * operator which requires them. The operator is not really and truly done
         * until doneOn.get(bopId) either returns <code>null</code> (the operator
         * did not request lastPass() invocation) or it returns an empty set (the
         * operator requested lastPass() invocation and we have seen the
         * {@link IHaltOpMessage} for each node or shard on which the operator ran).
         * <p>
         * {@link #startQuery(IChunkMessage)} and {@link #startOp(IStartOpMessage)}
         * are responsible for maintaining the {@link #startedOn} state while
         * {@link #_haltOp(int)} is responsible for maintaining the {@link #doneOn}
         * state.
         * <p>
         * {@link #getOperatorRunState(int, boolean)} considers {@link #doneSet} in
         * addition to the other run state metadata to decide whether an upstream
         * operator is still running. {@link RunStateEnum#StartLastPass} is
         * recognized when {@link #doneOn} is <code>null</code> for an operator
         * which can not be otherwise triggered and whose last evaluation pass has
         * been requested. Once the {@link #startedOn} set has been propagated to
         * the {@link #doneOn} set for an operator, it is recognized as being in the
         * {@link RunStateEnum#RunningLastPass} state until its {@link #doneOn} set
         * becomes empty.
         */
        final Map<Integer/* bopId */,Set<?>> startedOn = new LinkedHashMap<Integer,Set<?>>();

        /**
         * A collection for each operator which has executed at least once and for
         * which the optional final evaluation phase was requested. Together with
         * {@link #startedOn}, {@link #doneOn} captures the state change from when
         * an operator can not be triggered by external inputs to when it can no
         * longer be triggered by the final evaluation pass. The criteria for an
         * operator being "done" therefore includes (a) it has run at least once;
         * (b) it can not be retriggered by input from other operators; and (c) its
         * optional last evaluation pass either has been evaluated or will not be
         * evaluated.
         * 
         * @see #startedOn
         */
        @SuppressWarnings("rawtypes")
        final Map<Integer/* bopId */, Set> doneOn = new LinkedHashMap<Integer, Set>();

        /**
         * The set of operators for which a last evaluation pass was requested.
         * 
         * @see #startOp(IStartOpMessage, boolean)
         */
        final Set<Integer/* bopId */> lastPassRequested = new LinkedHashSet<Integer>();

        /**
         * The set of operators for which at-once evaluation is required. The operator
         * is removed from this set as soon as it is started.
         * 
         * @see #startOp(IStartOpMessage, boolean)
         * @see <a href="http://trac.blazegraph.com/ticket/868"> COUNT(DISTINCT) returns no rows rather than ZERO. </a>
         */
        final Set<Integer/* bopId */> atOnceRequired = new LinkedHashSet<Integer>();

        @Override
        public String toString() {

            return toString(new StringBuilder()).toString();
            
        }
        
        StringBuilder toString(final StringBuilder sb) {
            sb.append("{nsteps=" + stepCount);
            sb.append(",allDone=" + allDone);
            sb.append(",totalRunning=" + totalRunningCount);
            sb.append(",totalAvailable=" + totalAvailableCount);
            sb.append(",totalLastPassRemaining=" + totalLastPassRemainingCount);
            sb.append(",services=" + serviceIds);
            sb.append(",startedOn=" + startedOn);
            sb.append(",doneOn=" + doneOn);
            sb.append(",running=" + runningMap);
            sb.append(",available=" + availableMap);
            sb.append(",lastPassRequested=" + lastPassRequested);
            sb.append(",atOnceRequired=" + atOnceRequired);
            sb.append("}");
            return sb;
        }

    }

    // Note: This MUST be private to protect the internal run state.
    final private InnerState innerState; 
    
    /**
     * Return the query identifier specified to the constructor.
     */
    final public UUID getQueryId() {
        return innerState.queryId;
    }

    /**
     * Return the deadline specified to the constructor.
     */
    final public long getDeadline() {
        return innerState.deadline.get();
    }

    /**
     * Set the deadline for the query.
     * 
     * @param deadline
     *            The deadline.
     * 
     * @throws IllegalArgumentException
     *             if the deadline is non-positive.
     * @throws IllegalStateException
     *             if the deadline has already been set.
     * @throws QueryTimeoutException
     *             if the deadline is already expired.
     */
    final public void setDeadline(final long deadline) throws QueryTimeoutException {

        if (deadline <= 0)
            throw new IllegalArgumentException();

        // set the deadline.
        if (!innerState.deadline.compareAndSet(Long.MAX_VALUE/* expect */,
                deadline/* update */)) {

            // the deadline is already set.
            throw new IllegalStateException();

        }

        if (deadline < System.currentTimeMillis()) {

            // deadline has already expired.
            throw new QueryTimeoutException();

        }

    }
    
    /**
     * Return <code>true</code> if evaluation of the query has been initiated
     * using {@link #startQuery(IChunkMessage)}.
     */
    final public boolean isStarted() {
        return innerState.started.get();
    }

    /**
     * Return <code>true</code> if the query is known to be completed based on
     * the {@link #haltOp(IHaltOpMessage)}.
     */
    final public boolean isAllDone() {
        return innerState.allDone.get();
    }

    /**
     * The #of run state transitions which have occurred for this query.
     */
    final public long getStepCount() {
        return innerState.stepCount.get();
    }

    /**
     * The #of tasks for this query which have started but not yet halted.
     */
    final public long getTotalRunningCount() {
        
        return innerState.totalRunningCount.get();
        
    }

    /**
     * The #of {@link IChunkMessage} for the query which a running task has made
     * available but which have not yet been accepted for processing by another
     * task.
     */
    final public long getTotalAvailableCount() {
        
        return innerState.totalAvailableCount.get();
        
    }

    /**
     * The number of operators which have requested last pass evaluation and
     * whose last pass evaluation phase is not yet complete.
     */
    final public long getTotalLastPassRemainingCount() {

        return innerState.totalLastPassRemainingCount.get();
        
    }

    /**
     * Return an unmodifiable set containing the service UUID of each service on
     * which the query has been started.
     */
    final Set<UUID/*serviceId*/> getServiceIds() {

        return Collections.unmodifiableSet(innerState.serviceIds);

    }

    /**
     * Return the #of instances of this operator which are concurrently
     * executing.
     * 
     * @param bopId
     *            The operator.
     * 
     * @return The #of instances of the operator which are currently executing.
     */
    final long getRunningCount(final int bopId) {
        
        final AtomicLong runningCount = innerState.runningMap.get(bopId);
        
        if(runningCount == null)
            return 0L;
        
        return runningCount.get();
        
    }
    
    /**
     * Return the #of shards or nodes on which the operator has been started.
     * This is a measure of how much the operator has "fanned out" across the
     * shards or the cluster. The units will be shards if the operator is
     * sharded and nodes if the operator is hash partitioned.
     * 
     * @param bopId
     *            The operator.
     * 
     * @return The #of shards or nodes on which the operator has been started.
     */
    final int getStartedOnCount(final int bopId) {
        
        final Set<?> startedOn = innerState.startedOn.get(bopId);
        
        if(startedOn == null)
            return 0;
        
        return startedOn.size();
        
    }

    /**
     * Return the set of either shardIds or the set of serviceIds on which the
     * operator had been started and has not yet received its last pass
     * invocation. The query engine is responsible for generating the last pass
     * messages for the operator on each of these shards or services.
     * 
     * @param bopId
     *            The operator.
     * 
     * @return The set (either of shardIds or serviceIds, depending on the
     *         {@link BOpEvaluationContext} of the operator) -or-
     *         <code>null</code> if no operator having that bopId has entered
     *         into its last pass evaluation stage.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    final Set/*shardOrServiceId*/ getDoneOn(final int bopId) {

        final Set set = innerState.doneOn.get(bopId);
        
        if(set == null)
            return null;
        
        return Collections.unmodifiableSet(set);
        
    }

    /**
     * Return the set of bopIds for operators which have requested last pass
     * evaluation.
     */
    final Set<Integer/*bopId*/> getLastPassRequested() {

        return Collections.unmodifiableSet(innerState.lastPassRequested);
        
    }
    
	/**
	 * Return the set of bopIds for operators that must be evaluated once and
	 * only once.
	 */
    final Set<Integer/*bopId*/> getAtOnceRequired() {

        return Collections.unmodifiableSet(innerState.atOnceRequired);
        
    }
    
    /**
     * Constructor.
     * 
     * @param query
     *            The {@link IRunningQuery}.
     */
    public RunState(final IRunningQuery query) {

        /*
         * Note: The deadline is Long.MAX_VALUE until set. query.getDeadline()
         * delegates back to RunState so we can not use it to setup our own
         * state.
         */

        this(new InnerState(query.getQuery(), query.getQueryId(),
                Long.MAX_VALUE/* query.getDeadline() */,
                System.currentTimeMillis(), query.getBOpIndex()));

    }

    /**
     * Constructor used by unit tests.
     */
    // Note: package private to expose to the unit tests.
    RunState(final InnerState innerState) {
        
        if (innerState == null)
            throw new IllegalArgumentException();
        
        this.innerState = innerState;
        
    }

    /**
     * Update the {@link RunState} to indicate that the query evaluation will
     * begin with one initial {@link IChunkMessage} available for consumption.
     * 
     * @param msg
     *            The message.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalStateException
     *             if the query is already running.
     * @throws IllegalStateException
     *             if the query is already done.
     * @throws QueryTimeoutException
     *             if the deadline for the query has passed.
     */
    synchronized
    public void startQuery(final IChunkMessage<?> msg) throws QueryTimeoutException {

        if (msg == null)
            throw new IllegalArgumentException();

        if (innerState.allDone.get())
            throw new IllegalStateException(ERR_QUERY_HALTED);

        checkDeadline();

        if (!innerState.started.compareAndSet(false/* expect */, true/* update */))
            throw new IllegalStateException(ERR_QUERY_STARTED);

        innerState.stepCount.incrementAndGet();

        messagesProduced(msg.getBOpId(), 1/* nmessages */);

        /*
         * Any operator requesting last pass evaluation on the controller needs
         * to be added to the last pass evaluation or at-once evaluation set now. This provides the
         * guarantee that such operators will be run at least once (assuming
         * that the query terminates normally).
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/377#comment:12
         * @see http://trac.blazegraph.com/ticket/868
         */
        final UUID controllerId = msg.getQueryControllerId();
        {
            final Iterator<BOp> itr = BOpUtility.preOrderIterator(innerState.query);
            while(itr.hasNext()) {
                final BOp op = itr.next();
                if (op.getEvaluationContext() != BOpEvaluationContext.CONTROLLER)
                    continue;
                final boolean lastPassRequested = ((PipelineOp) op).isLastPassRequested();
                final boolean atOnceRequired = ((PipelineOp) op).isAtOnceEvaluation();
                if(atOnceRequired || lastPassRequested) {
	                final Integer id = (Integer) op.getProperty(BOp.Annotations.BOP_ID);
	                if (id == null)
	                    throw new NoBOpIdException(op.toString());
	                boolean didAdd = false;
					if (atOnceRequired)
						didAdd = innerState.atOnceRequired.add(id);
					if (lastPassRequested)
						if(didAdd = innerState.lastPassRequested.add(id))
							innerState.totalLastPassRemainingCount.incrementAndGet();
	                if (didAdd) {
	                    /*
	                     * Mock an evaluation pass for this operator. It will look
	                     * as if the operator has been evaluated (various
	                     * collections have entries for that bopId) but that it was
	                     * evaluated ZERO (0) times (various counters are zero).
	                     */
	                    innerState.runningMap.put(id.intValue(), new AtomicLong());
	                    @SuppressWarnings("rawtypes")
	                    final Set set = new LinkedHashSet();
	                    set.add(controllerId);
	                    innerState.startedOn.put(id, set);
	                    messagesConsumed(id.intValue(), 0/*nmessages*/);
	                }
                }
            }
        }
        
//        /*
//         * Note: RunState is only used by the query controller so this will not
//         * do an RMI and the RemoteException will not be thrown.
//         */
//        final UUID serviceId;
//        try {
//            serviceId = msg.getQueryController().getServiceUUID();
//        } catch (RemoteException ex) {
//            throw new AssertionError(ex);
//        }
        innerState.serviceIds.add(controllerId);
        
        if (TableLog.INFO) {
//            TableLog.tableLog.info("\n\nqueryId=" + queryId + "\n");
            
            TableLog.tableLog.info(getTableHeader());
            TableLog.tableLog.info(getTableRow("startQ", controllerId, msg
                    .getBOpId(), -1/* shardId */, 1/* fanIn */,
                    null/* cause */, null/* stats */));
        }

        if(INFO)
            log.info("startQ : " + toString());

        if (TRACE)
            log.trace(msg.toString());

    }

    /**
     * Update the {@link RunState} to indicate that the operator identified in
     * the {@link IStartOpMessage} will execute and will consume the one or more
     * {@link IChunkMessage}s.
     * 
     * @return <code>true</code> if this is the first time we will evaluate the
     *         op.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws QueryTimeoutException
     *             if the deadline for the query has passed.
     */
    synchronized
    public boolean startOp(final IStartOpMessage msg) throws QueryTimeoutException {

        if (msg == null)
            throw new IllegalArgumentException();

        if (innerState.allDone.get())
            throw new IllegalStateException(ERR_QUERY_HALTED);

        checkDeadline();
        
        innerState.stepCount.incrementAndGet();

        final boolean firstTime = _startOp(msg);

        messagesConsumed(msg.getBOpId(), msg.getChunkMessageCount());

        final Integer bopId = msg.getBOpId();
        
        final PipelineOp bop = (PipelineOp) innerState.bopIndex.get(bopId);

        // Note: This is an effective NOP since we init the last pass data structures in startQuery().
        if (bop.isLastPassRequested())
            if (innerState.lastPassRequested.add(bopId))
                innerState.totalLastPassRemainingCount.incrementAndGet();
        
        innerState.serviceIds.add(msg.getServiceId());
        
        if (TableLog.INFO) {
            TableLog.tableLog.info(getTableRow("startOp", msg.getServiceId(),
                    msg.getBOpId(), msg.getPartitionId(), msg.getChunkMessageCount(),
                    null/* cause */, null/* stats */));
        }

        if (INFO)
            log.info("startOp: " + toString() + " : bop=" + msg.getBOpId());

        if (TRACE)
            log.trace(msg.toString());

        return firstTime;

    }

    /**
     * Type safe enumeration of the summary run state for an operator as
     * reported in response to a {@link IHaltOpMessage}.  This information
     * is maintained by the query controller in the {@link RunState} for
     * the {@link IRunningQuery}.
     */
    static enum RunStateEnum {

        /**
         * The operator can be re-triggered either by upstream operators in the
         * pipeline which are not yet {@link #AllDone} or by
         * buffered {@link IChunkMessage}(s).
         */
        Running,
        /**
         * No instance of the operator is running and the operator can no longer
         * be re-triggered by upstream operators or buffered
         * {@link IChunkMessage}(s). The optional last evaluation pass for the
         * operator should be executed.
         */
        StartLastPass,
        /**
         * The operator awaiting last pass evaluation messages on each node or
         * shard where it was started.
         */
        RunningLastPass,
        /**
         * No instance of the operator is running and optional last evaluation
         * pass either has been executed or will not run. Any resources
         * associated with that operator should be released.
         */
        AllDone;

    } // RunStateEnum

    /**
     * Check the query to see whether its deadline has expired.
     * 
     * @throws QueryTimeoutException
     *             if the query deadline has expired.
     */
    protected void checkDeadline() throws QueryTimeoutException {

        if (innerState.deadline.get() < System.currentTimeMillis())
            throw new QueryTimeoutException(ERR_DEADLINE);

    }
    
    /**
     * Update the {@link RunState} to reflect the post-condition of the
     * evaluation of an operator against one or more {@link IChunkMessage}s,
     * adjusting the #of messages available for consumption by the operator
     * accordingly.
     * <p>
     * Note: If the query terminated normally then {@link #allDone} is set to
     * <code>true</code> as a side effect.
     * 
     * @return <code>true</code> if the operator life cycle is over.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalStateException
     *             if the query is not running.
     * @throws IllegalStateException
     *             if the operator addressed by the message is not running.
     * @throws QueryTimeoutException
     *             if the deadline has expired.
     * @throws ExecutionException
     *             if the {@link IHaltOpMessage#cause} was non-<code>null</code>
     *             , if which case it wraps {@link IHaltOpMessage#cause}.
     */
    synchronized
    public RunStateEnum haltOp(final IHaltOpMessage msg) throws QueryTimeoutException,
            ExecutionException {

        if (msg == null)
            throw new IllegalArgumentException();

        if (innerState.allDone.get())
            throw new IllegalStateException(ERR_QUERY_HALTED);

        checkDeadline();

        innerState.stepCount.incrementAndGet();

        final PipelineOp bop = (PipelineOp) innerState.bopIndex.get(msg
                .getBOpId());

        if (bop == null)
            throw new IllegalArgumentException();
        
        if (msg.getSinkMessagesOut() != 0) {
            /*
             * Note: The sinkId is not a required PipelineOp annotation. It
             * defaults to the parent of the operator, which has the semantics
             * of being the operator which is "downstream" from this operator.
             * It might be worthwhile to pre-compile this information into the
             * query so we can avoid the search for the parent. Alternatively,
             * the search for the parent could be moved into
             * getEffectiveDefaultSink such that it was only performed when the
             * SINK_REF was not explicitly given as an annotation.
             */
            // parent (null if this is the root of the operator tree).
            final BOp p = BOpUtility.getParent(innerState.query, bop);
            final Integer sinkId = BOpUtility.getEffectiveDefaultSink(bop, p);
//            final Integer sinkId = msg.getSinkId();
            if (sinkId != null) {
                /*
                 * Note: The sink will be null for the last operator in the
                 * query plan. That operator is understood to target the buffer
                 * from which the solutions are drained on the query controller.
                 */
                messagesProduced(sinkId, msg.getSinkMessagesOut());
            }
        }

        if (msg.getAltSinkMessagesOut() != 0) {
            final Integer altSinkId = (Integer) bop
                    .getRequiredProperty(PipelineOp.Annotations.ALT_SINK_REF);
            messagesProduced(altSinkId, msg.getAltSinkMessagesOut());
        }

        _haltOp(msg);

        /*
         * Figure out the current RunStateEnum for this operator.
         */
        final RunStateEnum state = getOperatorRunState(msg.getBOpId());

//        if (RunStateEnum.StartLastPass == state) {
//            // should not be triggered unless requested.
//            assert innerState.lastPassRequested.contains(msg.bopId);
//            if (innerState.doneOn.get(msg.bopId) == null) {
//                /*
//                 * When we convert into the LastPass phase for this operator we
//                 * copy the [startedOn] set for the operator into the [doneOn]
//                 * set and then increment [totalLastPassRemainingCount] by the
//                 * size of the copied set. That sets up the criteria that we
//                 * need to count off the last pass evaluations of the operator,
//                 * which are essentially self-triggered.
//                 * 
//                 * Note: Both availableCount and the availableMap counter for
//                 * this bopId have to be bumped by the #of expected messages,
//                 * otherwise those messages will not be "expected" when they
//                 * arrive.
//                 */
//                final Set<?> set = innerState.startedOn.get(msg.bopId);
//                if (set != null) {
//                    final int nexpected = set.size();
//                    innerState.doneOn.put(msg.bopId, new LinkedHashSet(set)); // Copy!
//                    innerState.totalAvailableCount.addAndGet(nexpected);
//                    innerState.availableMap.get(msg.bopId).addAndGet(nexpected);
//                }
//            }
//        }

        // true iff the entire query is done.
        final boolean isAllDone = innerState.totalRunningCount.get() == 0
                && innerState.totalAvailableCount.get() == 0
                && innerState.totalLastPassRemainingCount.get() == 0
                && innerState.atOnceRequired.isEmpty()
                ;

        if (isAllDone)
            innerState.allDone.set(true);

        /*
         * Note: It is possible for the per-operator availableMap counters to
         * have not been updated yet but [isAllDone] to be true. Since the query
         * is defacto done when [isAllDone] is satisfied, this tests for that
         * condition first and then for isOperatorDone().
         */
//        final boolean isOpDone = isAllDone || state == RunStateEnum.AllDone;

//        if (isAllDone && !isOpDone)
//            throw new RuntimeException("Whoops!: "+this);
        
        if (TableLog.INFO) {
            final int fanOut = msg.getSinkMessagesOut() + msg.getAltSinkMessagesOut();
            TableLog.tableLog.info(getTableRow("haltOp", msg.getServiceId(),
                    msg.getBOpId(), msg.getPartitionId(), fanOut, msg.getCause(),
                    msg.getStats()));
        }

        if (INFO)
            log.info("haltOp : " + toString() + " : bop=" + msg.getBOpId()
                    + ",opRunState=" + state + ",queryAllDone=" + isAllDone);

        if (TRACE)
            log.trace(msg.toString());

        if (msg.getCause() != null) {

            /*
             * Note: just wrap and throw rather than logging since this class
             * does not have enough insight into non-error exceptions while
             * Haltable does.
             */
            throw new ExecutionException(msg.getCause());

        }

        return state;

    }

    /**
     * Return the {@link RunStateEnum} for the operator given the
     * {@link RunState} of the {@link IRunningQuery}.
     * <p>
     * The movement of the intermediate binding set chunks during query
     * processing forms an acyclic directed graph. We can decide whether or not
     * a {@link BOp} in the query plan can be triggered by the current activity
     * pattern by inspecting {@link RunState} for the {@link BOp} and its
     * operands recursively. For the purposes of this method, only
     * {@link BOp#args() operands} are considered as descendants (this is the
     * query pipeline).
     * <p>
     * Operators which specify the {@link PipelineOp.Annotations#LAST_PASS}
     * annotation will self-trigger once per service or shard on which they were
     * evaluated. This self-triggering phase provides the last pass evaluation
     * semantics.
     * <p>
     * Note: The caller MUST hold a lock across this operation in order for it
     * to be atomic with respect to the concurrent evaluation of other operators
     * for the same query.
     * <p>
     * Note: While could also report whether or not the operator has ever
     * started here, this method is only invoked when processing a
     * {@link IHaltOpMessage} and we already know in that context (and can
     * readily verify) that the operator had started evaluation.
     * 
     * @param bopId
     *            The identifier for an operator which appears in the query
     *            plan.
     * 
     * @return The current {@link RunStateEnum} for the operator.
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws NoSuchBOpException
     *             if <i>bopId</i> is not found in the query index.
     */
    RunStateEnum getOperatorRunState(final int bopId) {

        final BOp op = innerState.bopIndex.get(bopId);

        if (op == null)
            throw new NoSuchBOpException(bopId);

        // look at the operators that could feed chunks into this operator.
        final Iterator<BOp> itr = BOpUtility.preOrderIterator(op);

        while (itr.hasNext()) {

            final BOp t = itr.next();

            final Integer id = (Integer) t.getProperty(BOp.Annotations.BOP_ID);

            if (id == null)
                throw new NoBOpIdException(t.toString());

            {

                /*
                 * If the operator is running then it is, defacto, "not done."
                 * 
                 * If any descendants of the operator are running, then they
                 * could cause the operator to be re-triggered and it is "not
                 * done."
                 */

                final AtomicLong runningCount = innerState.runningMap.get(id);

                if (runningCount != null && runningCount.get() != 0) {

                    if (DEBUG)
                        log.debug("Operator can be triggered: op=" + op
                                + ", possible trigger=" + t + " is running.");

                    return RunStateEnum.Running;

                }

            }

            {

                /*
                 * Any chunks available for the operator in question or any of
                 * its descendants could cause that operator to be triggered.
                 */

                final AtomicLong availableChunkCount = innerState.availableMap
                        .get(id);

                if (availableChunkCount != null
                        && availableChunkCount.get() != 0) {

                    if (DEBUG)
                        log.debug("Operator can be triggered: op=" + op
                                + ", possible trigger=" + t + " has "
                                + availableChunkCount + " chunks available.");

                    return RunStateEnum.Running;

                }

            }

            if (bopId != id.intValue()) {
            	
            	if (innerState.atOnceRequired.contains(id)) {
                
					/*
					 * Any predecessor of the operator (but not the operator
					 * itself) requires at-once evaluation and has not yet run.
					 */

            		if (DEBUG)
                        log.debug("Operator can be triggered: op=" + op
                                + ", possible trigger=" + t
                                + " awaiting at-once evaluation of predecessor.");

                    return RunStateEnum.Running;
                    
            	}

            	if (innerState.lastPassRequested.contains(id)) {

	                /*
	                 * Any predecessor of the operator (but not the operator itself)
	                 * requested a last pass evaluation phase and that last pass
	                 * evaluation phase is still in progress (as recognized by a
	                 * non-null entry in [doneOn] for that operator).
	                 */
	
	                // Examine the doneSet for this operator.
	                final Set doneSet = innerState.doneOn.get(id);
	
	                // If null, then has not started lastPass evaluation.
	                if (doneSet == null) {
	
	                    if (DEBUG)
	                        log.debug("Operator can be triggered: op=" + op
	                                + ", possible trigger=" + t
	                                + " has not started last pass evaluation.");
	
	                    return RunStateEnum.Running;
	
	                }
	
	                // If non-empty, then has not finished lastPass evaluation.
	                if (!doneSet.isEmpty()) {
	
	                    if (DEBUG)
	                        log.debug("Operator can be triggered: op=" + op
	                                + ", possible trigger=" + t
	                                + " awaiting last pass evaluation for doneSet="
	                                + doneSet);
	
	                    return RunStateEnum.Running;
	
	                }

            	}
            	
            }

        }

        /*
         * The operator can not be triggered by upstream operators or messages
         * already generated by upstream operators.
         */
        
        // handle at-once operator.
        final boolean atOnceRequired = innerState.atOnceRequired.contains(bopId); 
        
        // handle pipeline operator with last-pass evaluation requested.
        final boolean lastPassRequest = innerState.lastPassRequested.contains(bopId);
        
		if ((atOnceRequired || lastPassRequest)
				&& innerState.runningMap.containsKey(bopId)) {

            /*
			 * The operator has requested either (last pass evaluation -or-
			 * at-once evaluation) AND (either has in fact been evaluated (the
			 * running count map has an entry for this operator) -OR- is an
			 * operator which runs on the query controller).
			 * 
			 * Now we need to determine whether or not the operator should start
			 * its last pass evaluation phase or if it is waiting for the last
			 * pass evaluation phase to terminate.
			 */
            
            // Examine the doneSet for this operator.
            final Set doneSet = innerState.doneOn.get(bopId);

            // If null, then has not started lastPass eval.
            if (doneSet == null) {

                if (DEBUG)
                    log.debug("Operator will self-trigger last pass evaluation: op="
                            + op);

                /*
                 * When we convert into the LastPass phase for this operator we
                 * copy the [startedOn] set for the operator into the [doneOn]
                 * set and then increment [totalLastPassRemainingCount] by the
                 * size of the copied set. That sets up the criteria that we
                 * need to count off the last pass evaluations of the operator,
                 * which are essentially self-triggered.
                 * 
                 * Note: Both availableCount and the availableMap counter for
                 * this bopId have to be bumped by the #of expected messages,
                 * otherwise those messages will not be "expected" when they
                 * arrive.
                 */
                final Set<?> set = innerState.startedOn.get(bopId);
                if (set != null) {
                    final int nexpected = set.size();
                    innerState.doneOn.put(bopId, new LinkedHashSet(set)); // Copy!
                    innerState.totalAvailableCount.addAndGet(nexpected);
                    innerState.availableMap.get(bopId).addAndGet(nexpected);
                }

                return RunStateEnum.StartLastPass;

            }

            // If non-empty, then has not finished lastPass eval.
            if (!doneSet.isEmpty()) {

                if (DEBUG)
                    log.debug("Operator will self-trigger last pass evaluation: op="
                            + op
                            + ", awaiting last pass evaluation for doneSet="
                            + doneSet);

                return RunStateEnum.RunningLastPass;

            }
            
        }
        
        if (INFO)
            log.info("Operator can not be triggered: op=" + op);

        return RunStateEnum.AllDone;

    }

    /**
     * Return <code>true</code> iff the running query state is such that the
     * "at-once" evaluation of the specified operator may proceed (no
     * predecessors are running or could be triggered and the operator has not
     * been evaluated).
     * <p>
     * Note: The specific requirements are: (a) the operator is not running and
     * has not been started; (b) no predecessor in the pipeline is running; and
     * (c) no predecessor in the pipeline can be triggered.
     * 
     * @param bopId
     *            The identifier for an operator which appears in the query
     *            plan.
     * 
     * @return <code>true</code> iff the "at-once" evaluation of the operator
     *         may proceed.
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws NoSuchBOpException
     *             if <i>bopId</i> is not found in the query index.
     */
    boolean isAtOnceReady(final int bopId) {

        final BOp op = innerState.bopIndex.get(bopId);

        if (op == null)
            throw new NoSuchBOpException(bopId);

		final AtomicLong counter = innerState.runningMap.get(bopId);
		final boolean didStart = counter != null && counter.get() != 0L;
        
		if (didStart) {
            
            // Evaluation has already run (or begun) for this operator.
            if (INFO)
                log.info("Already ran/running: " + bopId);
            
            return false;
            
        }

        final Iterator<BOp> itr = BOpUtility.preOrderIterator(op);

        while (itr.hasNext()) {

            final BOp t = itr.next();

            final Integer id = (Integer) t.getProperty(BOp.Annotations.BOP_ID);

            if (id == null)
                throw new NoBOpIdException(t.toString());

			if (bopId == id.intValue()) {

                // Ignore self.
                continue;
                
            }
            
            {

                /*
                 * If any descendants (aka predecessors) of the operator are
                 * running, then they could cause produce additional solutions
                 * so the operator is not ready for "at-once" evaluation.
                 */

                final AtomicLong runningCount = innerState.runningMap.get(id);

                if (runningCount != null && runningCount.get() != 0) {

                    if (DEBUG)
                        log.debug("Predecessor running: predecessorId=" + id
                                + ", predecessorRunningCount=" + runningCount);

                    return false;

                }

            }

            {

                /*
                 * Any chunks available for a descendant (aka predecessor) of
                 * the operator could produce additional solutions as inputs to
                 * the operator so it is not ready for "at-once" evaluation.
                 */

                final AtomicLong availableChunkCount = innerState.availableMap
                        .get(id);

                if (availableChunkCount != null
                        && availableChunkCount.get() != 0) {
                    /*
                     * We are looking at some other predecessor of the specified
                     * operator.
                     */
                    if (DEBUG)
                        log.debug("Predecessor can be triggered: predecessorId="
                                + id + " has " + availableChunkCount
                                + " chunks available.");

                    return false;
                }

            }

        }

        // Success.
        if (INFO)
            log.info("Ready for 'at-once' evaluation: " + bopId);

        return true;

    }

    /**
     * Update the {@link RunState} to reflect that fact that a new evaluation
     * phase has begun for an operator.
     * 
     * @param bopId
     *            The operator identifier.
     * 
     * @return <code>true</code> iff this is the first time that operator is
     *         being evaluated within the context of this query {@link RunState}
     *         .
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private boolean _startOp(final IStartOpMessage msg) {

        final long running = innerState.totalRunningCount.incrementAndGet();

        assert running >= 1 : "running=" + running + " :: runState=" + this;

        final boolean firstTime;
        {

            final Integer bopId = msg.getBOpId();
            
            AtomicLong n = innerState.runningMap.get(bopId);

            if (n == null)
                innerState.runningMap.put(bopId, n = new AtomicLong());

            final long tmp = n.incrementAndGet();

            assert tmp >= 0 : "runningCount=" + tmp + " for bopId=" + bopId
                    + " :: runState=" + this;

            Set set = innerState.startedOn.get(bopId);
            if (set == null) {
                set = new LinkedHashSet();
                innerState.startedOn.put(bopId, set);
                firstTime = true;
            } else {
                firstTime = false;
            }
            final BOp bop = innerState.bopIndex.get(bopId);
            switch (bop.getEvaluationContext()) {
            case ANY:
            case CONTROLLER:
            case HASHED:
                set.add(msg.getServiceId());
                break;
            case SHARDED:
                set.add(Integer.valueOf(msg.getPartitionId()));
                break;
            }

        }

        return firstTime;

    }

    /**
     * Update the {@link RunState} to reflect the fact that an operator
     * execution phase is finished.
     * 
     * @param msg
     */
    private void _haltOp(final IHaltOpMessage msg) {

        final Integer bopId = msg.getBOpId();
        
        // one less task is running.
        final long running = innerState.totalRunningCount.decrementAndGet();

        assert running >= 0 : "running=" + running + " :: runState=" + this;

        {

            final AtomicLong n = innerState.runningMap.get(bopId);

            if (n == null) {
            	/*
				 * I have observed this exception once. It may have been caused
				 * by the interrupt of an RMI during the handling of a startOp
				 * message on a cluster node.
				 */
                throw new IllegalArgumentException(ERR_OP_NOT_STARTED);
            }

            if (n.get() <= 0)
                throw new IllegalArgumentException(ERR_OP_HALTED);

            n.decrementAndGet();

        }

        //if it is halt for at-once operator than we need to remove it from
        //the set of at-once operators that we still need to run
        if(innerState.atOnceRequired.contains(bopId)) {
            innerState.atOnceRequired.remove(bopId);
        }

        /*
         * If we are processing our doneSet, then handle that first so the
         * changes will be reflected when we decide the new run state for the
         * operator.
         */
        {

            final Set<?> set = innerState.doneOn.get(bopId);

            if (set != null) {

                /*
                 * Remove the shardId or serviceId from our doneSet.
                 */

                if (!set.remove(msg.getPartitionId()) && !set.remove(msg.getServiceId())) {

                    throw new RuntimeException("Not in doneSet: msg=" + msg
                            + ", doneSet=" + set);

                }

                if (set.isEmpty()) {
                    if(innerState.lastPassRequested.contains(bopId)) {
                        // End of last pass evaluation for this operator.
                        innerState.totalLastPassRemainingCount.decrementAndGet();
                    }
                }
                
            }

        }
        
    }

    /**
     * Update the {@link RunState} to reflect that the operator has consumed
     * some number of {@link IChunkMessage}s.
     * 
     * @param bopId
     *            The operator identifier.
     * @param nmessages
     *            The #of messages which were consumed by the operator.
     */
    private void messagesConsumed(final int bopId, final int nmessages) {

        innerState.totalAvailableCount.addAndGet(-nmessages);

        AtomicLong n = innerState.availableMap.get(bopId);

        if (n == null)
            innerState.availableMap.put(bopId, n = new AtomicLong());

        n.addAndGet(-nmessages);

    }

    /**
     * Update the {@link RunState} to reflect that some operator has generated
     * some number of {@link IChunkMessage}s which are available to be consumed
     * by the specified target operator.
     * 
     * @param targetId
     *            The target operator.
     * @param nmessages
     *            The #of of messages which were made available to that
     *            operator.
     */
    private void messagesProduced(final Integer targetId, final int nmessages) {

        if(targetId == null)
            throw new IllegalArgumentException();
        
        if (nmessages < 0)
            throw new IllegalArgumentException();

        if (nmessages == 0) {
            // NOP
            return;
        }

        if (!innerState.bopIndex.containsKey(targetId))
            throw new IllegalArgumentException();

        innerState.totalAvailableCount.addAndGet(nmessages);

        AtomicLong n = innerState.availableMap.get(targetId);

        if (n == null)
            innerState.availableMap.put(targetId, n = new AtomicLong());

        n.addAndGet(nmessages);

    }

    /*
     * Human readable representations of the query run state.
     */

    /**
     * Human readable summary of the current {@link RunState}.
     *<p>
     * Note: You must holding the lock guarding the {@link RunState} to
     * guarantee that will return a consistent representation.
     */
    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getName());
        
        innerState.toString(sb);
        
        return sb.toString();

    }

    private String getTableHeader() {

        final StringBuilder sb = new StringBuilder();

        final Integer[] bopIds = innerState.bopIndex.keySet().toArray(new Integer[0]);

        Arrays.sort(bopIds);

        sb.append("queryId");
        sb.append("\tbegin");
        sb.append("\telapsed");
        sb.append("\tstep");
        sb.append("\tlabel");
        sb.append("\tbopId");
        sb.append("\tserviceId");
        sb.append("\tevalContext");
        sb.append("\tcontroller");
        sb.append("\tcause");
        sb.append("\tbop");
        sb.append("\tshardId");
        sb.append("\tfanIO");
        sb.append("\tnservices");
        sb.append("\tnavail(query)");
        sb.append("\tnrun(query)");
        sb.append("\tnlastPassRemaining");
        sb.append("\tallDone");

        for (int i = 0; i < bopIds.length; i++) {

            final Integer id = bopIds[i];

        	final BOp bop = innerState.bopIndex.get(id);
        	
        	if(!(bop instanceof PipelineOp)) 
    			continue; // skip non-pipeline operators.

        	sb.append("\tnavail(id=" + id + ")");

            sb.append("\tnrun(id=" + id + ")");

            sb.append("\tnstartedOn(id=" + id + ")");

            sb.append("\tndoneOn(id=" + id + ")");

        }

        sb.append("\telapsed");
        sb.append("\tchunksIn");
        sb.append("\tunitsIn");
        sb.append("\tchunksOut");
        sb.append("\tunitsOut");
        sb.append("\taccessPathDups");
        sb.append("\taccessPathCount");
        sb.append("\taccessPathRangeCount");
        sb.append("\taccessPathChunksIn");
        sb.append("\taccessPathUnitsIn");
        //{chunksIn=1,unitsIn=100,chunksOut=4,unitsOut=313,accessPathDups=0,accessPathCount=100,chunkCount=100,elementCount=313}

        sb.append('\n');

        return sb.toString();

    }

    /**
     * Return a tabular representation of the query {@link RunState}.
     * <p>
     * Note: You must holding the lock guarding the {@link RunState} to
     * guarantee this method will return a consistent representation.
     * 
     * @param label
     *            The state change level (startQ, startOp, haltOp).
     * @param serviceId
     *            The node on which the operator will be / was executed.
     * @param bopId
     *            The identifier for the bop which will be / was executed.
     * @param shardId
     *            The index partition against which the operator was running and
     *            <code>-1</code> if the operator was not evaluated against a
     *            specific index partition.
     * @param fanIO
     *            This is the #of {@link IChunkMessage}s accepted by the
     *            operator ("fanIn" for startQ,startOp) or output by the
     *            operator ("fanOut" for haltOp). (Termination is decided by
     *            counting {@link IChunkMessage}s rather than by counting
     *            solutions or chunks of solutions.)
     * @param cause
     *            The {@link Throwable} in a {@link IHaltOpMessage} and
     *            <code>null</code> for other messages or if the
     *            {@link Throwable} was null.
     * @param stats
     *            The statistics from the operator evaluation and
     *            <code>null</code> unless {@link #haltOp(IHaltOpMessage)} is
     *            the invoker.
     */
    private String getTableRow(final String label, final UUID serviceId,
            final int bopId, final int shardId, final int fanIO,
            final Throwable cause, final BOpStats stats) {

        final StringBuilder sb = new StringBuilder();

        final DateFormat dateFormat = DateFormat.getDateTimeInstance(
                DateFormat.FULL, DateFormat.FULL);
        
        final long elapsed = System.currentTimeMillis() - innerState.begin;
        
        sb.append(innerState.queryId);
        sb.append('\t');
        sb.append(dateFormat.format(new Date(innerState.begin)));
        sb.append('\t');
        sb.append(elapsed);
        sb.append('\t');
        sb.append(Long.toString(innerState.stepCount.get()));
        sb.append('\t');
        sb.append(label);

        sb.append('\t');
        sb.append(Integer.toString(bopId));

        // the serviceId : will be null unless scale-out.
        sb.append('\t');
        sb.append(serviceId == null ? "N/A" : serviceId.toString());

		{
			final BOp bop = innerState.bopIndex.get(bopId);
			sb.append('\t');
			sb.append(bop.getEvaluationContext());
			sb.append('\t');
			sb.append(bop.getProperty(BOp.Annotations.CONTROLLER,
					BOp.Annotations.DEFAULT_CONTROLLER));
		}

        // the thrown cause.
        sb.append('\t');
        if (cause != null)
            sb.append(cause.getLocalizedMessage());

		// the operator.
		sb.append('\t');
		if (innerState.stepCount.get() == 1) {
			/*
			 * For the startQ row @ nsteps==1, show the entire query. This is
			 * the only way people will be able to see the detailed annotations
			 * on predicates used in joins. New line characters are translated
			 * out to keep things in the table format.
			 */
			sb.append(BOpUtility.toString(innerState.query).replace('\n', ' '));
        } else {
        	// Otherwise how just this bop.
            sb.append(innerState.bopIndex.get(bopId).toString());
        }

        sb.append('\t');
        sb.append(Integer.toString(shardId));
        sb.append('\t');
        sb.append(Integer.toString(fanIO));
        sb.append('\t');
        sb.append(innerState.serviceIds.size());
        sb.append('\t');
        sb.append(Long.toString(innerState.totalAvailableCount.get()));
        sb.append('\t');
        sb.append(Long.toString(innerState.totalRunningCount.get()));
        sb.append('\t');
        sb.append(Long.toString(innerState.totalLastPassRemainingCount.get()));
        sb.append('\t');
        sb.append(innerState.allDone.get());

        final Integer[] bopIds = innerState.bopIndex.keySet().toArray(new Integer[0]);

        Arrays.sort(bopIds);

        for (int i = 0; i < bopIds.length; i++) {

            final Integer id = bopIds[i];

        	final BOp bop = innerState.bopIndex.get(id);
        	
        	if(!(bop instanceof PipelineOp)) 
    			continue; // skip non-pipeline operators.
        	
            final AtomicLong nrunning = innerState.runningMap.get(id);

            final AtomicLong navailable = innerState.availableMap.get(id);

            final Set<?> startedSet = innerState.startedOn.get(id);

            final Set<?> doneSet = innerState.doneOn.get(id);
            
            sb.append("\t" + (navailable == null ? "N/A" : navailable.get()));

            sb.append("\t" + (nrunning == null ? "N/A" : nrunning.get()));

            sb.append("\t" + (startedSet == null ? "N/A" : startedSet.size()));

            sb.append("\t" + (doneSet == null ? "N/A" : doneSet.size()));

        }

		/*
		 * The statistics. This is at the end to keep the table pretty.
		 * Different kinds of operators may have additional statistics. They
		 * have to be explicitly handled here to format them into a table.
		 */
        if (stats != null) {
            sb.append('\t');
        	sb.append(stats.elapsed.get());
            sb.append('\t');
        	sb.append(stats.chunksIn.get());
            sb.append('\t');
        	sb.append(stats.unitsIn.get());
            sb.append('\t');
        	sb.append(stats.chunksOut.get());
			sb.append('\t');
			sb.append(stats.unitsOut.get());
			if (stats instanceof PipelineJoinStats) {
				final PipelineJoinStats t = (PipelineJoinStats) stats;
				sb.append('\t');
				sb.append(t.accessPathDups.get());
				sb.append('\t');
				sb.append(t.accessPathCount.get());
				sb.append('\t');
				sb.append(t.accessPathRangeCount.get());
				sb.append('\t');
				sb.append(t.accessPathChunksIn.get());
				sb.append('\t');
				sb.append(t.accessPathUnitsIn.get());
        	}
        }

        sb.append('\n');

        return sb.toString();

    }

} // class RunState
