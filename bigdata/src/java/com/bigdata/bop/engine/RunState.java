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
 * Created on Sep 15, 2010
 */

package com.bigdata.bop.engine;

import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * The run state for a {@link RunningQuery}. This class is thread-safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class RunState {

    static private final Logger log = Logger.getLogger(RunState.class);

    /**
     * Inner class provides a 2nd logger used for tabular representations.
     */
    static private class TableLog {

        static private final Logger tableLog = Logger.getLogger(TableLog.class);

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
     * Message if an operator addressed by a {@link HaltOpMessage} was never started.
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
     * {@link RunningQuery#handleOutputChunk(BOp, int, IBlockingBuffer)} drops
     * {@link IChunkMessage}s onto {@link RunningQuery#chunksIn} and drops the
     * {@link RunningQuery} on {@link QueryEngine#runningQueries} as soon as
     * output {@link IChunkMessage}s are generated. A {@link IChunkMessage} MAY
     * be taken for evaluation as soon as it is published. This means that the
     * operator which will consume that {@link IChunkMessage} can begin to
     * execute <em>before</em> {@link RunningQuery#haltOp(HaltOpMessage)} is
     * invoked to indicate the end of the operator which produced that
     * {@link IChunkMessage}.
     * <p>
     * This is all fine. However, due to the potential overlap in these
     * schedules {@link RunState#totalAvailableCount} can become transiently
     * negative. This flag disables asserts which would otherwise fail on legal
     * transient negatives.
     */
    static private final boolean availableMessageCountMayBeNegative = true;
    
    /**
     * Flag may be used to turn on stderr output.
     */
    static private final boolean debug = true;
    
    /**
     * The query.
     */
    private final BOp query;
    
    /**
     * An index from {@link BOp.Annotations#BOP_ID} to {@link BOp} for the
     * {@link #query}.
     */
    private final Map<Integer,BOp> bopIndex;

    /**
     * The query identifier.
     */
    private final UUID queryId;

    /**
     * The query deadline.
     * 
     * @see BOp.Annotations#TIMEOUT
     * @see RunningQuery#getDeadline()
     */
    private final long deadline;

    /**
     * Set to <code>true</code> iff the query evaluation has begun.
     * 
     * @see #startQuery(IChunkMessage)
     */
    private final AtomicBoolean started = new AtomicBoolean(false);
    
    /**
     * Set to <code>true</code> iff the query evaluation is complete due to
     * normal termination.
     * 
     * @see #haltOp(HaltOpMessage)
     */
    private final AtomicBoolean allDone = new AtomicBoolean(false);
    
    /**
     * The #of run state transitions which have occurred for this query.
     */
    private final AtomicLong nsteps = new AtomicLong();

    /**
     * The #of tasks for this query which have started but not yet halted.
     */
    private final AtomicLong totalRunningCount = new AtomicLong();

    /**
     * The #of {@link IChunkMessage} for the query which a running task has made
     * available but which have not yet been accepted for processing by another
     * task.
     */
    private final AtomicLong totalAvailableCount = new AtomicLong();

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
     * Note: This collection is package private in order to expose its state to
     * the unit tests. Since the map contains {@link AtomicLong}s it can not be
     * readily exposed as {@link Map} object. If we were to expose the map, it
     * would have to be via a get(key) style interface.
     */
    /* private */final Map<Integer/* bopId */, AtomicLong/* availableChunkCount */> availableMap = new ConcurrentHashMap<Integer, AtomicLong>();

    /**
     * A collection reporting on the #of instances of a given {@link BOp} which
     * are concurrently executing.
     * <p>
     * Note: This collection is package private in order to expose its state to
     * the unit tests. Since the map contains {@link AtomicLong}s it can not be
     * readily exposed as {@link Map} object. If we were to expose the map, it
     * would have to be via a get(key) style interface.
     */
    /* private */final Map<Integer/* bopId */, AtomicLong/* runningCount */> runningMap = new ConcurrentHashMap<Integer, AtomicLong>();

    /**
     * A collection of the operators which have executed at least once.
     */
    private final Set<Integer/* bopId */> startedSet = new LinkedHashSet<Integer>();

    /**
     * Return the query identifier specified to the constructor.
     */
    final public UUID getQueryId() {
        return queryId;
    }

    /**
     * Return the deadline specified to the constructor.
     */
    final public long getDeadline() {
        return deadline;
    }

    /**
     * Return <code>true</code> if evaluation of the query has been initiated
     * using {@link #startQuery(IChunkMessage)}.
     */
    final public boolean isStarted() {
        return started.get();
    }

    /**
     * Return <code>true</code> if the query is known to be completed based on
     * the {@link #haltOp(HaltOpMessage)}.
     */
    final public boolean isAllDone() {
        return allDone.get();
    }

    /**
     * The #of run state transitions which have occurred for this query.
     */
    final public long getStepCount() {
        return nsteps.get();
    }

    /**
     * The #of tasks for this query which have started but not yet halted.
     */
    final public long getTotalRunningCount() {
        return totalRunningCount.get();
    }

    /**
     * The #of {@link IChunkMessage} for the query which a running task has made
     * available but which have not yet been accepted for processing by another
     * task.
     */
    final public long getTotalAvailableCount() {
        return totalAvailableCount.get();
    }

    /**
     * Return an unmodifiable set containing the {@link BOp.Annotations#BOP_ID}
     * for each operator which has been started at least once as indicated by a
     * {@link #startQuery(IChunkMessage)} message or a
     * {@link #startOp(StartOpMessage)} message.
     */
    final public Set<Integer> getStartedSet() {
        return Collections.unmodifiableSet(startedSet);
    }

    public RunState(final RunningQuery query) {

        this(query.getQuery(), query.getQueryId(), query.getDeadline(),
                query.bopIndex);

    }

    /**
     * Constructor used by unit tests.
     */
    RunState(final BOp query, final UUID queryId, final long deadline,
            final Map<Integer, BOp> bopIndex) {

        if (query == null)
            throw new IllegalArgumentException();

        if (queryId == null)
            throw new IllegalArgumentException();

        if (deadline <= 0L)
            throw new IllegalArgumentException();

        if (bopIndex == null)
            throw new IllegalArgumentException();

        this.query = query;

        this.queryId = queryId;

        this.deadline = deadline;

        this.bopIndex = bopIndex;

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
     * @throws TimeoutException
     *             if the deadline for the query has passed.
     */
    synchronized
    public void startQuery(final IChunkMessage<?> msg) throws TimeoutException {

        if (msg == null)
            throw new IllegalArgumentException();

        if (allDone.get())
            throw new IllegalStateException(ERR_QUERY_HALTED);

        if (!started.compareAndSet(false/* expect */, true/* update */))
            throw new IllegalStateException(ERR_QUERY_STARTED);

        if (deadline < System.currentTimeMillis())
            throw new TimeoutException(ERR_DEADLINE);

        nsteps.incrementAndGet();

        messagesProduced(msg.getBOpId(), 1/* nmessages */);

        if (log.isInfoEnabled())
            log.info(msg.toString());

        if (TableLog.tableLog.isInfoEnabled()) {
            /*
             * Note: RunState is only used by the query controller so this will
             * not do an RMI and the RemoteException will not be thrown.
             */
            final UUID serviceId;
            try {
                serviceId = msg.getQueryController().getServiceUUID();
            } catch (RemoteException ex) {
                throw new AssertionError(ex);
            }
            TableLog.tableLog.info("\n\nqueryId=" + queryId + "\n");
            TableLog.tableLog.info(getTableHeader());
            TableLog.tableLog.info(getTableRow("startQ", serviceId, msg
                    .getBOpId(), -1/* shardId */, 1/* fanIn */,
                    null/* cause */, null/* stats */));
        }

        if (debug)
            System.err.println("startQ : " + toString());

    }

    /**
     * Update the {@link RunState} to indicate that the operator identified in
     * the {@link StartOpMessage} will execute and will consume the one or more
     * {@link IChunkMessage}s. Both the total #of available messages and the #of
     * messages available for that operator are incremented by
     * {@link StartOpMessage#nmessages}.
     * 
     * @return <code>true</code> if this is the first time we will evaluate the
     *         op.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws TimeoutException
     *             if the deadline for the query has passed.
     */
    synchronized
    public boolean startOp(final StartOpMessage msg) throws TimeoutException {

        if (msg == null)
            throw new IllegalArgumentException();

        if (allDone.get())
            throw new IllegalStateException(ERR_QUERY_HALTED);

        if (deadline < System.currentTimeMillis())
            throw new TimeoutException(ERR_DEADLINE);

        nsteps.incrementAndGet();

        final boolean firstTime = startOp(msg.bopId);

        messagesConsumed(msg.bopId, msg.nmessages);

        if (log.isTraceEnabled())
            log.trace(msg.toString());

        if (TableLog.tableLog.isInfoEnabled()) {
            TableLog.tableLog.info(getTableRow("startOp", msg.serviceId,
                    msg.bopId, msg.partitionId, msg.nmessages/* fanIn */,
                    null/* cause */, null/* stats */));
        }

        if (debug)
            System.err
                    .println("startOp: " + toString() + " : bop=" + msg.bopId);

        return firstTime;

    }

    /**
     * Update the {@link RunState} to reflect the post-condition of the
     * evaluation of an operator against one or more {@link IChunkMessage},
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
     * @throws TimeoutException
     *             if the deadline has expired.
     * @throws ExecutionException
     *             if the {@link HaltOpMessage#cause} was non-<code>null</code>,
     *             if which case it wraps {@link HaltOpMessage#cause}.
     */
    synchronized
    public boolean haltOp(final HaltOpMessage msg) throws TimeoutException,
            ExecutionException {

        if (msg == null)
            throw new IllegalArgumentException();

        if (allDone.get())
            throw new IllegalStateException(ERR_QUERY_HALTED);

        if (deadline < System.currentTimeMillis())
            throw new TimeoutException(ERR_DEADLINE);

        nsteps.incrementAndGet();

        if (msg.sinkId != null)
            messagesProduced(msg.sinkId.intValue(), msg.sinkMessagesOut);

        if (msg.altSinkId != null)
            messagesProduced(msg.altSinkId.intValue(), msg.altSinkMessagesOut);

        haltOp(msg.bopId);

        // true if this operator is done.
        final boolean isOpDone = isOperatorDone(msg.bopId);

        // true if the entire query is done.
        final boolean isAllDone = getTotalRunningCount() == 0
                && getTotalAvailableCount() == 0;

        if (isAllDone)
            this.allDone.set(true);

        if (log.isTraceEnabled())
            log.trace(msg.toString());

        if (TableLog.tableLog.isInfoEnabled()) {
            final int fanOut = msg.sinkMessagesOut + msg.altSinkMessagesOut;
            TableLog.tableLog.info(getTableRow("haltOp", msg.serviceId,
                    msg.bopId, msg.partitionId, fanOut, msg.cause,
                    msg.taskStats));
        }

        if (debug)
            System.err.println("haltOp : " + toString() + " : bop=" + msg.bopId
                    + ",isOpDone=" + isOpDone);

        if (msg.cause != null) {

            /*
             * Note: just wrap and throw rather than logging since this class
             * does not have enough insight into non-error exceptions while
             * Haltable does.
             */
            throw new ExecutionException(msg.cause);

        }

        return isOpDone;

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
    private boolean isOperatorDone(final int bopId) {

        return PipelineUtility.isDone(bopId, query, bopIndex, runningMap,
                availableMap);

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
    private boolean startOp(final int bopId) {

        final long running = totalRunningCount.incrementAndGet();

        assert running >= 1 : "running=" + running + " :: runState=" + this;

        final boolean firstTime;
        {

            AtomicLong n = runningMap.get(bopId);

            if (n == null)
                runningMap.put(bopId, n = new AtomicLong());

            final long tmp = n.incrementAndGet();

            assert tmp >= 0 : "runningCount=" + tmp + " for bopId=" + bopId
                    + " :: runState=" + this;

            firstTime = startedSet.add(bopId);

        }

        return firstTime;

    }

    /**
     * Update the {@link RunState} to reflect the fact that an operator
     * execution phase is finished.
     * 
     * @param bopId
     *            The operator identifier.
     */
    private void haltOp(final int bopId) {

        // one less task is running.
        final long running = totalRunningCount.decrementAndGet();

        assert running >= 0 : "running=" + running + " :: runState=" + this;

        {

            final AtomicLong n = runningMap.get(bopId);

            if (n == null)
                throw new IllegalArgumentException(ERR_OP_NOT_STARTED);

            if (n.get() <= 0)
                throw new IllegalArgumentException(ERR_OP_HALTED);

            n.decrementAndGet();

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

        final long available = totalAvailableCount.addAndGet(-nmessages);

        assert availableMessageCountMayBeNegative || available >= 0 : "available="
                + available
                + " :: runState="
                + this
                + ", nmessages="
                + nmessages;

        {

            AtomicLong n = availableMap.get(bopId);

            if (n == null)
                availableMap.put(bopId, n = new AtomicLong());

            final long tmp = n.addAndGet(-nmessages);

            assert availableMessageCountMayBeNegative || tmp >= 0 : "available="
                    + tmp
                    + " for bopId="
                    + bopId
                    + " :: runState="
                    + this
                    + ", nmessages=" + nmessages;

        }

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
    private void messagesProduced(final int targetId, final int nmessages) {

        final long available = totalAvailableCount.addAndGet(nmessages);

        assert availableMessageCountMayBeNegative || available >= 0 : "available="
                + available
                + " :: runState="
                + this
                + ", targetId="
                + targetId
                + ", nmessages=" + nmessages;

        AtomicLong n = availableMap.get(targetId);

        if (n == null)
            availableMap.put(targetId, n = new AtomicLong());

        final long tmp = n.addAndGet(nmessages);

        assert availableMessageCountMayBeNegative || tmp >= 0 : "available="
                + tmp + " for bopId=" + targetId + " :: runState=" + this
                + ", targetId=" + targetId + ", nmessages=" + nmessages;

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
    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getName());
        sb.append("{nsteps=" + nsteps);
        sb.append(",allDone=" + allDone);
        sb.append(",totalRunning=" + totalRunningCount);
        sb.append(",totalAvailable=" + totalAvailableCount);
        sb.append(",started=" + startedSet);
        sb.append(",running=" + runningMap);
        sb.append(",available=" + availableMap);
        sb.append("}");

        return sb.toString();

    }

    private String getTableHeader() {

        final StringBuilder sb = new StringBuilder();

        final Integer[] bopIds = bopIndex.keySet().toArray(new Integer[0]);

        Arrays.sort(bopIds);

        sb.append("step");
        sb.append("\tlabel");
        sb.append("\tbopId");
        sb.append("\tserviceId");
        sb.append("\tcause");
        sb.append("\tbop");
        sb.append("\tshardId");
        sb.append("\tfanIO");
        sb.append("\tnavail(query)");
        sb.append("\tnrun(query)");
        sb.append("\tallDone");

        for (int i = 0; i < bopIds.length; i++) {

            final Integer id = bopIds[i];

            sb.append("\tnavail(id=" + id + ")");

            sb.append("\tnrun(id=" + id + ")");

        }

        sb.append("\tstats");

        sb.append('\n');

        return sb.toString();

    }

    /**
     * Return a tabular representation of the query {@link RunState}.
     *<p>
     * Note: You must holding the lock guarding the {@link RunState} to
     * guarantee that will return a consistent representation.
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
     *            The fanIn (startQ,startOp) or fanOut (haltOp).
     * @param cause
     *            The {@link Throwable} in a {@link HaltOpMessage} and
     *            <code>null</code> for other messages or if the
     *            {@link Throwable} was null.
     * @param stats
     *            The statistics from the operator evaluation and
     *            <code>null</code> unless {@link #haltOp(HaltOpMessage)} is the
     *            invoker.
     */
    private String getTableRow(final String label, final UUID serviceId,
            final int bopId, final int shardId, final int fanIO,
            final Throwable cause, final BOpStats stats) {

        final StringBuilder sb = new StringBuilder();

        sb.append(Long.toString(nsteps.get()));
        sb.append('\t');
        sb.append(label);

        sb.append('\t');
        sb.append(Integer.toString(bopId));

        // the serviceId : will be null unless scale-out.
        sb.append('\t');
        sb.append(serviceId == null ? "N/A" : serviceId.toString());

        // the thrown cause.
        sb.append('\t');
        if (cause != null)
            sb.append(cause.getLocalizedMessage());

        // the operator.
        sb.append('\t');
        sb.append(bopIndex.get(bopId));

        sb.append('\t');
        sb.append(Integer.toString(shardId));
        sb.append('\t');
        sb.append(Integer.toString(fanIO));
        sb.append('\t');
        sb.append(Long.toString(totalAvailableCount.get()));
        sb.append('\t');
        sb.append(Long.toString(totalRunningCount.get()));
        sb.append('\t');
        sb.append(allDone.get());

        final Integer[] bopIds = bopIndex.keySet().toArray(new Integer[0]);

        Arrays.sort(bopIds);

        for (int i = 0; i < bopIds.length; i++) {

            final Integer id = bopIds[i];

            final AtomicLong nrunning = runningMap.get(id);

            final AtomicLong navailable = availableMap.get(id);

            sb.append("\t" + (navailable == null ? "N/A" : navailable.get()));

            sb.append("\t" + (nrunning == null ? "N/A" : nrunning.get()));

        }

        // the statistics : this is at the end to keep the table pretty.
        sb.append('\t');
        if (stats != null) {
            // @todo use a multi-column version of stats.
            sb.append(stats.toString());
        }

        sb.append('\n');

        return sb.toString();

    }

} // class RunState
