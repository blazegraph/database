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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.util.InnerCause;

/**
 * The run state for a {@link RunningQuery}. This class is NOT thread-safe.
 * {@link RunningQuery} uses an internal lock to serialize requests against the
 * public methods of this class.
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
     * Note: Due to concurrency, it is possible for an {@link IChunkMessage} to
     * be accepted and the corresponding chunk task started, before a
     * {@link RunState#startOp(StartOpMessage)} transition has been fully
     * processed. This means that the {@link RunState#totalAvailableChunkCount}
     * can become transiently negative. This flag disables asserts which would
     * otherwise fail on legal transient negatives.
     */
    static private boolean availableChunkCountMayBeNegative = true;
    
    /**
     * The query.
     */
    private final RunningQuery query;

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
     * Set to <code>true</code> iff the query evaluation is complete due to
     * normal termination.
     * <p>
     * Note: This is package private to expose it to {@link RunningQuery}.
     * 
     * @see #haltOp(HaltOpMessage)
     */
    /*private*/ final AtomicBoolean allDone = new AtomicBoolean(false);
    
    /**
     * The #of run state transitions which have occurred for this query.
     */
    private long nsteps = 0;

    /**
     * The #of tasks for this query which have started but not yet halted.
     */
    private long totalRunningTaskCount = 0;

    /**
     * The #of chunks for this query of which a running task has made available
     * but which have not yet been accepted for processing by another task.
     */
    private long totalAvailableChunkCount = 0;

    /**
     * A map reporting the #of chunks available for each operator in the
     * pipeline (we only report chunks for pipeline operators). The total #of
     * chunks available across all operators in the pipeline is reported by
     * {@link #totalAvailableChunkCount}.
     * <p>
     * The movement of the intermediate binding set chunks forms an acyclic
     * directed graph. This map is used to track the #of chunks available for
     * each {@link BOp} in the pipeline. When a {@link BOp} has no more incoming
     * chunks, we send an asynchronous message to all nodes on which that
     * {@link BOp} had executed informing the {@link QueryEngine} on that node
     * that it should immediately release all resources associated with that
     * {@link BOp}.
     */
    private final Map<Integer/* bopId */, AtomicLong/* availableChunkCount */> availableChunkCountMap = new LinkedHashMap<Integer, AtomicLong>();

    /**
     * A collection reporting on the #of instances of a given {@link BOp} which
     * are concurrently executing.
     */
    private final Map<Integer/* bopId */, AtomicLong/* runningCount */> runningTaskCountMap = new LinkedHashMap<Integer, AtomicLong>();

    /**
     * A collection of the operators which have executed at least once.
     */
    private final Set<Integer/* bopId */> startedSet = new LinkedHashSet<Integer>();

    public RunState(final RunningQuery query) {

        this.query = query;

        this.queryId = query.getQueryId();

        this.deadline = query.getDeadline();
        
        // this.nops = query.bopIndex.size();

    }

    public void startQuery(final IChunkMessage<?> msg) {

        nsteps++;

        // query.lifeCycleSetUpQuery();

        if (log.isInfoEnabled())
            log.info(msg.toString());

        final Integer bopId = Integer.valueOf(msg.getBOpId());

        totalAvailableChunkCount++;

        assert totalAvailableChunkCount == 1 : "totalAvailableChunkCount="
                + totalAvailableChunkCount + " :: msg=" + msg;

        {

            AtomicLong n = availableChunkCountMap.get(bopId);

            if (n == null)
                availableChunkCountMap.put(bopId, n = new AtomicLong());

            final long tmp = n.incrementAndGet();

            assert tmp == 1 : "availableChunkCount=" + tmp + " for bopId="
                    + msg.getBOpId() + " :: msg=" + msg;

        }

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
            // TableLog.tableLog.info(query.getQuery().toString()+"\n");
            TableLog.tableLog.info(getTableHeader());
            TableLog.tableLog.info(getTableRow("startQ", serviceId, msg
                    .getBOpId(), -1/* shardId */, 1/* fanIn */,
                    null/* cause */, null/* stats */));
        }

//        System.err.println("startQ : nstep="+nsteps+", bopId=" + bopId
//                + ",totalRunningTaskCount=" + totalRunningTaskCount
//                + ",totalAvailableTaskCount=" + totalAvailableChunkCount);

    }

    /**
     * @return <code>true</code> if this is the first time we will evaluate the
     *         op.
     *         
     * @throws TimeoutException 
     *             if the deadline for the query has passed.
     */
    public boolean startOp(final StartOpMessage msg) throws TimeoutException {

        nsteps++;

        if (log.isTraceEnabled())
            log.trace(msg.toString());

        final Integer bopId = Integer.valueOf(msg.bopId);

        totalRunningTaskCount++;

        assert totalRunningTaskCount >= 1 : "runningTaskCount="
                + totalRunningTaskCount + " :: runState=" + this + ", msg="
                + msg;
        
        final boolean firstTime;
        {

            AtomicLong n = runningTaskCountMap.get(bopId);

            if (n == null)
                runningTaskCountMap.put(bopId, n = new AtomicLong());

            final long tmp = n.incrementAndGet();

            assert tmp >= 0 : "runningTaskCount=" + tmp + " for bopId="
                    + msg.bopId + " :: runState=" + this + ", msg=" + msg;

            firstTime = startedSet.add(bopId);
            //
            // // first evaluation pass for this operator.
            // query.lifeCycleSetUpOperator(bopId);
            //
            // }

        }

        totalAvailableChunkCount -= msg.nchunks;

        assert availableChunkCountMayBeNegative || totalAvailableChunkCount >= 0 : "totalAvailableChunkCount="
                + totalAvailableChunkCount + " :: runState=" + this + ", msg="
                + msg;

        {

            AtomicLong n = availableChunkCountMap.get(bopId);

            if (n == null)
                availableChunkCountMap.put(bopId, n = new AtomicLong());

            final long tmp = n.addAndGet(-msg.nchunks);

            assert availableChunkCountMayBeNegative || tmp >= 0 : "availableChunkCount=" + tmp + " for bopId="
                    + msg.bopId + " :: runState=" + this + ", msg=" + msg;

        }

//        System.err.println("startOp: nstep=" + nsteps + ", bopId=" + bopId
//                + ",totalRunningTaskCount=" + totalRunningTaskCount
//                + ",totalAvailableChunkCount=" + totalAvailableChunkCount
//                + ",fanIn=" + msg.nchunks);

        if (TableLog.tableLog.isInfoEnabled()) {
            TableLog.tableLog.info(getTableRow("startOp", msg.serviceId,
                    msg.bopId, msg.partitionId, msg.nchunks/* fanIn */,
                    null/* cause */, null/* stats */));
        }

        // check deadline.
        if (deadline < System.currentTimeMillis()) {

            if (log.isTraceEnabled())
                log.trace("expired: queryId=" + queryId + ", deadline="
                        + deadline);

            throw new TimeoutException();

        }
        
        return firstTime;
    }

    /**
     * Update termination criteria counters. If the query evaluation is over due
     * to normal termination then {@link #allDone} is set to <code>true</code>
     * as a side effect.
     * 
     * @return <code>true</code> if the operator life cycle is over.
     * 
     * @throws TimeoutException
     *             if the deadline has expired.
     * @throws ExecutionException
     *             if the {@link HaltOpMessage#cause} was non-<code>null</code>,
     *             if which case it wraps {@link HaltOpMessage#cause}.
     */
    public boolean haltOp(final HaltOpMessage msg) throws TimeoutException,
            ExecutionException {

        nsteps++;

        if (log.isTraceEnabled())
            log.trace(msg.toString());

        // chunks generated by this task.
        final int fanOut = msg.sinkChunksOut + msg.altSinkChunksOut;
        {

            totalAvailableChunkCount += fanOut;

            assert availableChunkCountMayBeNegative || totalAvailableChunkCount >= 0 : "totalAvailableChunkCount="
                    + totalAvailableChunkCount + " :: runState=" + this
                    + ", msg=" + msg;

            if (msg.sinkId != null) {
                AtomicLong n = availableChunkCountMap.get(msg.sinkId);
                if (n == null)
                    availableChunkCountMap
                            .put(msg.sinkId, n = new AtomicLong());

                final long tmp = n.addAndGet(msg.sinkChunksOut);

                assert availableChunkCountMayBeNegative || tmp >= 0 : "availableChunkCount=" + tmp + " for bopId="
                        + msg.sinkId + " :: runState=" + this + ", msg=" + msg;

            }

            if (msg.altSinkId != null) {

                AtomicLong n = availableChunkCountMap.get(msg.altSinkId);

                if (n == null)
                    availableChunkCountMap.put(msg.altSinkId,
                            n = new AtomicLong());

                final long tmp = n.addAndGet(msg.altSinkChunksOut);

                assert availableChunkCountMayBeNegative || tmp >= 0 : "availableChunkCount=" + tmp + " for bopId="
                        + msg.altSinkId + " :: runState=" + this + ", msg="
                        + msg;

            }

        }

        // one less task is running.
        totalRunningTaskCount--;

        assert totalRunningTaskCount >= 0 : "runningTaskCount="
                + totalRunningTaskCount + " :: runState=" + this + ", msg="
                + msg;

        {

            final AtomicLong n = runningTaskCountMap.get(msg.bopId);

            if (n == null)
                throw new AssertionError();

            final long tmp = n.decrementAndGet();

            assert tmp >= 0 : "runningTaskCount=" + tmp + " for bopId="
                    + msg.bopId + " :: runState=" + this + ", msg=" + msg;

        }

//        System.err.println("haltOp : nstep=" + nsteps + ", bopId=" + msg.bopId
//                + ",totalRunningTaskCount=" + totalRunningTaskCount
//                + ",totalAvailableTaskCount=" + totalAvailableChunkCount
//                + ",fanOut=" + fanOut);

        if (TableLog.tableLog.isInfoEnabled()) {
            TableLog.tableLog.info(getTableRow("haltOp", msg.serviceId,
                    msg.bopId, msg.partitionId, fanOut, msg.cause,
                    msg.taskStats));
        }

//        if (log.isTraceEnabled())
//            log.trace("bopId=" + msg.bopId + ",partitionId=" + msg.partitionId
//                    + ",serviceId=" + query.getQueryEngine().getServiceUUID()
//                    + ", nchunks=" + fanOut + " : totalRunningTaskCount="
//                    + totalRunningTaskCount + ", totalAvailableChunkCount="
//                    + totalAvailableChunkCount);

        /*
         * Test termination criteria
         */

        // true if this operator is done.
        final boolean isOpDone = isOperatorDone(msg.bopId);
        
        // true if the entire query is done.
        final boolean isAllDone = totalRunningTaskCount == 0
                && totalAvailableChunkCount == 0;

        if (msg.cause != null) {

//            /*
//             * @todo probably just wrap and throw rather than logging since this
//             * class does not have enough insight into non-error exceptions
//             * while Haltable does.
//             */
//            if (!InnerCause.isInnerCause(msg.cause, InterruptedException.class)
//                    && !InnerCause.isInnerCause(msg.cause,
//                            TimeoutException.class)) {
//
//                // operator failed on this chunk.
//                log.error("Error: Canceling query: queryId=" + queryId
//                        + ",bopId=" + msg.bopId + ",partitionId="
//                        + msg.partitionId, msg.cause);
//            }

            throw new ExecutionException(msg.cause);

        } else if (isAllDone) {

            // success (all done).
            if (log.isTraceEnabled())
                log.trace("success: queryId=" + queryId);

            this.allDone.set(true);
            
        } else if (deadline < System.currentTimeMillis()) {

            if (log.isTraceEnabled())
                log.trace("expired: queryId=" + queryId + ", deadline="
                        + deadline);

            throw new TimeoutException();

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
    protected boolean isOperatorDone(final int bopId) {

        return PipelineUtility.isDone(bopId, query.getQuery(), query.bopIndex,
                runningTaskCountMap, availableChunkCountMap);

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
        sb.append(",totalRunningTaskCount=" + totalRunningTaskCount);
        sb.append(",totalAvailableTaskCount=" + totalAvailableChunkCount);
        sb.append("}");

        return sb.toString();

    }

    private String getTableHeader() {

        final StringBuilder sb = new StringBuilder();

        final Integer[] bopIds = query.bopIndex.keySet()
                .toArray(new Integer[0]);

        Arrays.sort(bopIds);

        // header 2.
        sb.append("step\tlabel\tbopId\tshardId\tfanIO\tavail\trun");

        for (int i = 0; i < bopIds.length; i++) {

            final Integer id = bopIds[i];

            sb.append("\trun#" + id + "\tavail#" + id);

        }

        sb.append("\tserviceId");

        sb.append("\tbop");

        sb.append("\tcause");

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
            final Throwable cause,
            final BOpStats stats) {

        final StringBuilder sb = new StringBuilder();

        sb.append(Long.toString(nsteps));
        sb.append('\t');
        sb.append(label);
        sb.append('\t');
        sb.append(Integer.toString(bopId));
        sb.append('\t');
        sb.append(Integer.toString(shardId));
        sb.append('\t');
        sb.append(Integer.toString(fanIO));
        sb.append('\t');
        sb.append(Long.toString(totalAvailableChunkCount));
        sb.append('\t');
        sb.append(Long.toString(totalRunningTaskCount));

        final Integer[] bopIds = query.bopIndex.keySet()
                .toArray(new Integer[0]);

        Arrays.sort(bopIds);

        for (int i = 0; i < bopIds.length; i++) {

            final Integer id = bopIds[i];

            final AtomicLong nrunning = runningTaskCountMap.get(id);

            final AtomicLong navailable = availableChunkCountMap.get(id);

            sb.append("\t" + (navailable == null ? "N/A" : navailable.get()));

            sb.append("\t" + (nrunning == null ? "N/A" : nrunning.get()));

        }

        // Note: At the end to keep the table pretty. Will be null unless s/o.
        sb.append('\t');
        sb.append(serviceId == null ? "N/A" : serviceId.toString());

        // the operator.
        sb.append('\t');
        sb.append(query.bopIndex.get(bopId));

        // the thrown cause.
        sb.append('\t');
        if (cause != null)
            sb.append(cause.getLocalizedMessage());
        
        // the statistics.
        sb.append('\t');
        if (stats != null) {
            // @todo use a multi-column version of stats.
            sb.append(stats.toString());
        }            
        
        sb.append('\n');

        return sb.toString();

    }

} // class RunState
