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
 * Created on Sep 21, 2010
 */

package com.bigdata.bop.engine;

import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Test suite for {@link RunState}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Unit tests of a query with multiple operators where the of
 *       {@link IChunkMessage}s generated from a non-terminal operator is zero.
 *       The query should terminate even though there are additional operators
 *       since there are no message available to trigger those operators.
 * 
 * @todo Unit tests where some messages target the alternative sink.
 * 
 * @todo normal termination tests.
 * 
 * @todo Write query cancelled, query deadline expired, and operator error
 *       tests, including verifying that the {@link RunState} correctly reflects
 *       that nothing is left running.
 * 
 * @todo concurrent stress tests.
 * 
 * @todo test subquery evaluation, including things like cancelling a query
 *       terminates the subquery (slice in the subquery).
 */
public class TestRunState extends TestCase2 {

    /**
     * 
     */
    public TestRunState() {
    }

    /**
     * @param name
     */
    public TestRunState(String name) {
        super(name);
    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single,
     * empty {@link IBindingSet}.
     * 
     * @param bindingSet
     *            the binding set.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet bindingSet) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { bindingSet } });

    }

    /**
     * Turn an array into a {@link Set}. 
     * 
     * @param a
     *            The array.
     *            
     * @return The {@link Set}.
     */
    protected Set<Integer> newSet(int[] a) {
        
        final Set<Integer> s = new LinkedHashSet<Integer>();
        
        for(int x : a) {
            
            s.add(x);
            
        }
        
        return s;
        
    }

    /**
     * Turn two correlated arrays into a {@link Map}.
     * 
     * @param ids
     *            The keys.
     * @param vals
     *            The values.
     *            
     * @return The {@link Map}.
     */
    protected Map<Integer,AtomicLong> newMap(final int[] ids,final long[] vals) {
        
        assertEquals(ids.length, vals.length);
        
        final Map<Integer, AtomicLong> m = new LinkedHashMap<Integer, AtomicLong>();
        
        for(int i=0; i<ids.length; i++) {
            
            m.put(ids[i],new AtomicLong(vals[i]));
            
        }

        return m;
        
    }

    /**
     * Unit test for the constructor (correct acceptance).
     */
    public void test_ctor() {
       
        final int startId = 1;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query = startOp;
        
        final UUID queryId = UUID.randomUUID();

        final long deadline = System.currentTimeMillis() + 12;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        final RunState runState = new RunState(query, queryId, deadline,
                bopIndex);

        // verify visible constructor arguments.
        assertEquals("queryId", queryId, runState.getQueryId());
        assertEquals("deadline", deadline, runState.getDeadline());
        
        // verify the initial conditions.
        assertFalse("started", runState.isStarted());
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 0L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] {}, new long[] {}),
                runState.availableMap);
        assertEquals("running", newMap(new int[] {}, new long[] {}),
                runState.runningMap);
        assertEquals("started", newSet(new int[] {}), runState.getStartedSet());
        
    }
    
    /**
     * Unit test for the constructor (correct rejection).
     */
    public void test_ctor_correctRejection() {
       
        final int startId = 1;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query = startOp;

        final UUID queryId = UUID.randomUUID();

        final long deadline = System.currentTimeMillis() + 12;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        try {
            new RunState(null/* query */, queryId, deadline, bopIndex);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            new RunState(query, null/* queryId */, deadline, bopIndex);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            new RunState(query, queryId, 0L/* deadline */, bopIndex);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            new RunState(query, queryId, -1L/* deadline */, bopIndex);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            new RunState(query, queryId, deadline, null/* bopIndex */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

    /**
     * Very simple unit test for the {@link RunState} API. A query with a single
     * {@link StartOp} operator is created and the {@link RunState} for that
     * query is directly manipulated in accordance with a simple evaluation
     * schedule for the query (startQuery, startOp, haltOp, allDone). The
     * post-condition of the {@link RunState} object is verified after each
     * action.
     * 
     * @throws TimeoutException
     * @throws ExecutionException
     */
    public void test_runSingleOperatorQuery() throws TimeoutException,
            ExecutionException {

        final IQueryClient queryController = new MockQueryController();

        final UUID serviceId = UUID.randomUUID();

        final int startId = 1;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query = startOp;
        
        final UUID queryId = UUID.randomUUID();

        final long deadline = Long.MAX_VALUE;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        // step0
        final RunState runState = new RunState(query, queryId, deadline,
                bopIndex);

//        System.err.println(runState.toString());

        assertFalse("started", runState.isStarted());
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 0L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] {}, new long[] {}),
                runState.availableMap);
        assertEquals("running", newMap(new int[] {}, new long[] {}),
                runState.runningMap);
        assertEquals("started", newSet(new int[] {}), runState.getStartedSet());

        // step1 : startQuery, notice that a chunk is available.
        runState.startQuery(new LocalChunkMessage<IBindingSet>(queryController,
                queryId, startId, -1/* partitionId */,
                newBindingSetIterator(new HashBindingSet())));

//        System.err.println(runState.toString());
        
        assertTrue("started", runState.isStarted());
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 1L, runState.getStepCount());
        assertEquals("availableCount", 1L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] { startId },
                new long[] { 1L }), runState.availableMap);
        assertEquals("running", newMap(new int[] {}, new long[] {}),
                runState.runningMap);
        assertEquals("started", newSet(new int[] {}), runState.getStartedSet());

        // step2 : start operator.
        runState.startOp(new StartOpMessage(queryId, startId,
                -1/* partitionId */, serviceId, 1/* nmessages */));

//        System.err.println(runState.toString());

        assertTrue("started", runState.isStarted());
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 2L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 1L, runState.getTotalRunningCount());
        // the message was "consumed" by the start event.
        assertEquals("available", newMap(new int[] { startId },
                new long[] { 0L }), runState.availableMap);
        // the operator is now running.
        assertEquals("running",
                newMap(new int[] { startId }, new long[] { 1L }),
                runState.runningMap);
        assertEquals("started", newSet(new int[] { startId }), runState
                .getStartedSet());

        /*
         * step3 : halt operator. in this case no chunk messages are produced
         * and the query should halt.
         */
        {

            final BOpStats stats = new BOpStats();

            runState.haltOp(new HaltOpMessage(queryId, startId,
                    -1/* partitionId */, serviceId, null/* cause */,
                    null/* sinkId */, 0/* sinkMessagesOut */,
                    null/* altSinkId */, 0/* altSinkMessagesOut */, stats));

        }

//        System.err.println(runState.toString());

        assertTrue("started", runState.isStarted());
        assertTrue("allDone", runState.isAllDone());
        assertEquals("stepCount", 3L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] { startId },
                new long[] { 0L }), runState.availableMap);
        assertEquals("running",
                newMap(new int[] { startId }, new long[] { 0L }),
                runState.runningMap);
        assertEquals("started", newSet(new int[] { startId }), runState
                .getStartedSet());

    }

    /**
     * Run a two operator query where the first operator produces a single
     * output message and the second operator consumes that message.
     * 
     * @throws TimeoutException
     * @throws ExecutionException
     */
    public void test_runTwoOperatorQuery() throws TimeoutException,
            ExecutionException {

        final IQueryClient queryController = new MockQueryController();

        final UUID serviceId = UUID.randomUUID();

        final int startId = 1;
        final int otherId = 2;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp otherOp = new StartOp(new BOp[] { startOp },
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, otherId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        }));

        final PipelineOp query = otherOp;

        final UUID queryId = UUID.randomUUID();

        final long deadline = Long.MAX_VALUE;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        // step0
        final RunState runState = new RunState(query, queryId, deadline,
                bopIndex);

        // System.err.println(runState.toString());

        assertFalse("started", runState.isStarted());
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 0L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] {}, new long[] {}),
                runState.availableMap);
        assertEquals("running", newMap(new int[] {}, new long[] {}),
                runState.runningMap);
        assertEquals("started", newSet(new int[] {}), runState.getStartedSet());

        // step1 : startQuery, notice that a chunk is available.
        runState.startQuery(new LocalChunkMessage<IBindingSet>(queryController,
                queryId, startId, -1/* partitionId */,
                newBindingSetIterator(new HashBindingSet())));

        // System.err.println(runState.toString());

        assertTrue("started", runState.isStarted());
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 1L, runState.getStepCount());
        assertEquals("availableCount", 1L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] { startId },
                new long[] { 1L }), runState.availableMap);
        assertEquals("running", newMap(new int[] {}, new long[] {}),
                runState.runningMap);
        assertEquals("started", newSet(new int[] {}), runState.getStartedSet());

        // step2 : start operator.
        runState.startOp(new StartOpMessage(queryId, startId,
                -1/* partitionId */, serviceId, 1/* nmessages */));

        // System.err.println(runState.toString());

        assertTrue("started", runState.isStarted());
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 2L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 1L, runState.getTotalRunningCount());
        // the message was "consumed" by the start event.
        assertEquals("available", newMap(new int[] { startId },
                new long[] { 0L }), runState.availableMap);
        // the operator is now running.
        assertEquals("running",
                newMap(new int[] { startId }, new long[] { 1L }),
                runState.runningMap);
        assertEquals("started", newSet(new int[] { startId }), runState
                .getStartedSet());

        // step3 : halt operator : the operator produced one chunk.
        {

            final BOpStats stats = new BOpStats();

            runState.haltOp(new HaltOpMessage(queryId, startId,
                    -1/* partitionId */, serviceId, null/* cause */,
                    otherId/* sinkId */, 1/* sinkMessagesOut */,
                    null/* altSinkId */, 0/* altSinkMessagesOut */, stats));

        }

        // System.err.println(runState.toString());

        assertTrue("started", runState.isStarted());
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 3L, runState.getStepCount());
        assertEquals("availableCount", 1L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] { startId, otherId },
                new long[] { 0L, 1L }), runState.availableMap);
        assertEquals("running",
                newMap(new int[] { startId }, new long[] { 0L }),
                runState.runningMap);
        assertEquals("started", newSet(new int[] { startId }), runState
                .getStartedSet());

        // start the 2nd operator.
        runState.startOp(new StartOpMessage(queryId, otherId,
                -1/* partitionId */, serviceId, 1/* nmessages */));

        assertTrue("started", runState.isStarted());
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 4L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 1L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] { startId, otherId },
                new long[] { 0L, 0L }), runState.availableMap);
        assertEquals("running", newMap(new int[] { startId, otherId },
                new long[] { 0L, 1L }), runState.runningMap);
        assertEquals("started", newSet(new int[] { startId, otherId }),
                runState.getStartedSet());

        // step3 : halt operator : the operator produced no chunk messages.
        {

            final BOpStats stats = new BOpStats();

            runState.haltOp(new HaltOpMessage(queryId, otherId,
                    -1/* partitionId */, serviceId, null/* cause */,
                    null/* sinkId */, 0/* sinkMessagesOut */,
                    null/* altSinkId */, 0/* altSinkMessagesOut */, stats));

        }

        assertTrue("started", runState.isStarted());
        assertTrue("allDone", runState.isAllDone());
        assertEquals("stepCount", 5L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] { startId, otherId },
                new long[] { 0L, 0L }), runState.availableMap);
        assertEquals("running", newMap(new int[] { startId, otherId },
                new long[] { 0L, 0L }), runState.runningMap);
        assertEquals("started", newSet(new int[] { startId, otherId }),
                runState.getStartedSet());
        
    }

    /**
     * Unit test verifies that attempting to start a query twice is an error.
     * 
     * @throws TimeoutException
     * @throws ExecutionException
     * 
     * @todo Write a unit test verifies that attempting to start an operator
     *       after the query is done is an error.
     */
    public void test_startQueryTwice() throws TimeoutException,
            ExecutionException {

        final IQueryClient queryController = new MockQueryController();

//        final UUID serviceId = UUID.randomUUID();

        final int startId = 1;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query = startOp;

        final UUID queryId = UUID.randomUUID();

        final long deadline = Long.MAX_VALUE;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        // step0
        final RunState runState = new RunState(query, queryId, deadline,
                bopIndex);

        // System.err.println(runState.toString());

        assertFalse("started", runState.isStarted());
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 0L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] {}, new long[] {}),
                runState.availableMap);
        assertEquals("running", newMap(new int[] {}, new long[] {}),
                runState.runningMap);
        assertEquals("started", newSet(new int[] {}), runState.getStartedSet());

        // step1 : startQuery, notice that a chunk is available.
        runState.startQuery(new LocalChunkMessage<IBindingSet>(queryController,
                queryId, startId, -1/* partitionId */,
                newBindingSetIterator(new HashBindingSet())));

        // System.err.println(runState.toString());

        assertTrue("started", runState.isStarted());
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 1L, runState.getStepCount());
        assertEquals("availableCount", 1L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] { startId },
                new long[] { 1L }), runState.availableMap);
        assertEquals("running", newMap(new int[] {}, new long[] {}),
                runState.runningMap);
        assertEquals("started", newSet(new int[] {}), runState.getStartedSet());

        try {
            runState.startQuery(new LocalChunkMessage<IBindingSet>(
                    queryController, queryId, startId, -1/* partitionId */,
                    newBindingSetIterator(new HashBindingSet())));
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

    /**
     * Unit test for correct interpretation of a deadline. There are a series of
     * deadline tests which verify that the deadline is detected by the various
     * action methods (startQuery, startOp, haltOp). For this test, the deadline
     * should be recognized by {@link RunState#startQuery(IChunkMessage)}.
     * 
     * @throws TimeoutException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_deadline_startQ() throws TimeoutException,
            ExecutionException, InterruptedException {

        final IQueryClient queryController = new MockQueryController();

        // final UUID serviceId = UUID.randomUUID();

        final int startId = 1;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query = startOp;
        
        final UUID queryId = UUID.randomUUID();

        final long delay = 100; // ms.
        
        final long deadline = System.currentTimeMillis() + delay;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        // step0
        final RunState runState = new RunState(query, queryId, deadline,
                bopIndex);
        
        assertEquals("deadline", deadline, runState.getDeadline());
        
        // initial conditions.
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 0L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());

        // wait for the deadline to pass.
        Thread.sleep(delay + delay / 2);
        
        // verify that the deadline has passed.
        assertTrue(System.currentTimeMillis() > deadline);

        // no state change.
        assertFalse("allDone", runState.isAllDone());
        assertEquals("deadline", deadline, runState.getDeadline());
        assertEquals("stepCount", 0L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());

        // start the query.
        try {
            runState.startQuery(new LocalChunkMessage<IBindingSet>(
                    queryController, queryId, startId, -1/* partitionId */,
                    newBindingSetIterator(new HashBindingSet())));
            fail("Expected: " + TimeoutException.class);
        } catch (TimeoutException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // verify post-conditions
        assertFalse("allDone", runState.isAllDone());
        assertEquals("deadline", deadline, runState.getDeadline());
        assertEquals("stepCount", 0L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());

    }
    
    /**
     * Unit test for correct interpretation of a deadline. There are a series of
     * deadline tests which verify that the deadline is detected by the various
     * action methods (startQuery, startOp, haltOp). For this test, the deadline
     * should be recognized by {@link RunState#startOp(StartOpMessage)}.
     * 
     * @throws TimeoutException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_deadline_startOp() throws TimeoutException,
            ExecutionException, InterruptedException {

        final IQueryClient queryController = new MockQueryController();
        
        final UUID serviceId = UUID.randomUUID();

        final int startId = 1;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query = startOp;
        
        final UUID queryId = UUID.randomUUID();

        final long delay = 100; // ms.
        
        final long deadline = System.currentTimeMillis() + delay;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        // step0
        final RunState runState = new RunState(query, queryId, deadline,
                bopIndex);
        
        assertEquals("deadline", deadline, runState.getDeadline());
        
        // initial conditions.
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 0L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());

        // start the query.
        runState.startQuery(new LocalChunkMessage<IBindingSet>(queryController,
                queryId, startId, -1/* partitionId */,
                newBindingSetIterator(new HashBindingSet())));
        
        // verify post-conditions.
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 1L, runState.getStepCount());
        assertEquals("availableCount", 1L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());
        assertEquals(newMap(new int[] { startId }, new long[] { 1L }),
                runState.availableMap);
        assertEquals(newMap(new int[] {}, new long[] {}),
                runState.runningMap);
        assertEquals(newSet(new int[] {}), runState.getStartedSet());

        // wait for the deadline to pass.
        Thread.sleep(delay + delay / 2);

        // verify that the deadline has passed.
        assertTrue(System.currentTimeMillis() > deadline);

        try {
            // step2 : start operator.
            runState.startOp(new StartOpMessage(queryId, startId,
                    -1/* partitionId */, serviceId, 1/* nmessages */));
            fail("Expected: " + TimeoutException.class);
        } catch (TimeoutException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // verify post-conditions
        assertFalse("allDone", runState.isAllDone());
        assertEquals("deadline", deadline, runState.getDeadline());
        assertEquals("stepCount", 1L, runState.getStepCount());
        assertEquals("availableCount", 1L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());

    }

    /**
     * Unit test for correct interpretation of a deadline. There are a series of
     * deadline tests which verify that the deadline is detected by the various
     * action methods (startQuery, startOp, haltOp). For this test, the deadline
     * should recognized by {@link RunState#haltOp(HaltOpMessage)}.
     * 
     * @throws TimeoutException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_deadline_haltOp() throws TimeoutException,
            ExecutionException, InterruptedException {

        final IQueryClient queryController = new MockQueryController();
        
        final UUID serviceId = UUID.randomUUID();

        final int startId = 1;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query = startOp;
        
        final UUID queryId = UUID.randomUUID();

        final long delay = 100; // ms.
        
        final long deadline = System.currentTimeMillis() + delay;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        // step0
        final RunState runState = new RunState(query, queryId, deadline,
                bopIndex);

        assertEquals("deadline", deadline, runState.getDeadline());

        // initial conditions.
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 0L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());

        // start the query.
        runState.startQuery(new LocalChunkMessage<IBindingSet>(queryController,
                queryId, startId, -1/* partitionId */,
                newBindingSetIterator(new HashBindingSet())));
        
        // verify post-conditions.
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 1L, runState.getStepCount());
        assertEquals("availableCount", 1L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 0L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] { startId },
                new long[] { 1L }), runState.availableMap);
        assertEquals("running", newMap(new int[] {}, new long[] {}),
                runState.runningMap);
        assertEquals("started", newSet(new int[] {}), runState.getStartedSet());

        // step2 : start operator.
        runState.startOp(new StartOpMessage(queryId, startId,
                -1/* partitionId */, serviceId, 1/* nmessages */));

        // verify post-conditions.
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 2L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 1L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] { startId },
                new long[] { 0L }), runState.availableMap);
        assertEquals("running",
                newMap(new int[] { startId }, new long[] { 1L }),
                runState.runningMap);
        assertEquals("started", newSet(new int[] { startId }), runState
                .getStartedSet());

        // wait for the deadline to pass.
        Thread.sleep(delay + delay / 2);

        // verify that the deadline has passed.
        assertTrue(System.currentTimeMillis() > deadline);

        try {

            final BOpStats stats = new BOpStats();

            runState.haltOp(new HaltOpMessage(queryId, startId,
                    -1/* partitionId */, serviceId, null/* cause */,
                    null/* sinkId */, 0/* sinkMessagesOut */,
                    null/* altSinkId */, 0/* altSinkMessagesOut */, stats));

            fail("Expected: " + TimeoutException.class);

        } catch (TimeoutException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // verify post-conditions
        assertFalse("allDone", runState.isAllDone());
        assertEquals("stepCount", 2L, runState.getStepCount());
        assertEquals("availableCount", 0L, runState.getTotalAvailableCount());
        assertEquals("runningCount", 1L, runState.getTotalRunningCount());
        assertEquals("available", newMap(new int[] { startId },
                new long[] { 0L }), runState.availableMap);
        assertEquals("running",
                newMap(new int[] { startId }, new long[] { 1L }),
                runState.runningMap);
        assertEquals("started", newSet(new int[] { startId }), runState
                .getStartedSet());

    }

    /*
     * Test helpers
     */
    
    /**
     * Mock object.
     */
    private static class MockQueryController implements IQueryClient {

        public void haltOp(HaltOpMessage msg) throws RemoteException {
        }

        public void startOp(StartOpMessage msg) throws RemoteException {
        }

        public void bufferReady(IChunkMessage<IBindingSet> msg)
                throws RemoteException {
        }

        public void declareQuery(IQueryDecl queryDecl) {
        }

        public UUID getServiceUUID() throws RemoteException {
            return null;
        }

        public PipelineOp getQuery(UUID queryId)
                throws RemoteException {
            return null;
        }

        public void cancelQuery(UUID queryId, Throwable cause)
                throws RemoteException {
        }

		public UUID[] getRunningQueries() {
			return null;
		}

    }

    /**
     * Compare two maps whose keys are {@link Integer}s and whose values are
     * {@link AtomicLong}s.
     * 
     * @param expected
     * @param actual
     */
    private void assertEquals(Map<Integer, AtomicLong> expected,
            Map<Integer, AtomicLong> actual) {
        assertEquals("", expected, actual);
    }

    /**
     * Compare two maps whose keys are {@link Integer}s and whose values are
     * {@link AtomicLong}s.
     * 
     * @param expected
     * @param actual
     */
    private void assertEquals(String msg,
            final Map<Integer, AtomicLong> expected,
            final Map<Integer, AtomicLong> actual) {

        if (msg == null) {
            msg = "";
        } else if (msg.length() > 0) {
            msg = msg + " : ";
        }
        
        assertEquals(expected.size(), actual.size());

        final Iterator<Map.Entry<Integer, AtomicLong>> eitr = expected.entrySet().iterator();

        while (eitr.hasNext()) {

            final Map.Entry<Integer, AtomicLong> entry = eitr.next();

            final Integer k = entry.getKey();

            final AtomicLong e = expected.get(k);

            final AtomicLong a = actual.get(k);

            if (e == a) {
                // Same reference, including when e is null.
                continue;
            }

            if (a == null)
                fail(msg + "Not expecting null: key=" + k);

            /*
             * Note: Must get() on AtomicLong before comparing their values
             * (equals does not work!)
             */
            if (e.get() != a.get()) {

                fail(msg + "Wrong value: key=" + k + ", expected=" + e
                        + ", actual=" + a);

            }

        }

    }

}
