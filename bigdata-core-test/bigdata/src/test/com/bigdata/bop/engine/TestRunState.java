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
 * Created on Sep 21, 2010
 */

package com.bigdata.bop.engine;

import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.engine.RunState.InnerState;
import com.bigdata.bop.engine.RunState.RunStateEnum;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Test suite for {@link RunState}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Tests for {@link RunState#isAtOnceReady(int)}
 *  
 * @todo Unit tests of a query with multiple operators where the of
 *       {@link IChunkMessage}s generated from a non-terminal operator is zero.
 *       The query should terminate even though there are additional operators
 *       since there are no message available to trigger those operators.
 * 
 * @todo Unit tests where some messages target the alternative sink.
 * 
 * @todo Write query cancelled and operator error tests, including verifying
 *       that the {@link RunState} correctly reflects that nothing is left
 *       running.
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
     * Turn an array into a {@link Set}. 
     * 
     * @param a
     *            The array.
     *            
     * @return The {@link Set}.
     */
    protected <T> Set<T> newSet(T[] a) {
        
        final Set<T> s = new LinkedHashSet<T>();
        
        for(T x : a) {
            
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
    protected Map<Integer, AtomicLong> newMap(final int[] ids, final long[] vals) {

        assertEquals(ids.length, vals.length);

        final Map<Integer, AtomicLong> m = new LinkedHashMap<Integer, AtomicLong>();

        for (int i = 0; i < ids.length; i++) {

            m.put(ids[i], new AtomicLong(vals[i]));

        }

        return m;

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
    protected <T> Map<Integer, T> newMap(final int[] ids, final T[] vals) {

        assertEquals(ids.length, vals.length);

        final Map<Integer, T> m = new LinkedHashMap<Integer, T>();

        for (int i = 0; i < ids.length; i++) {

            m.put(ids[i], vals[i]);

        }

        return m;

    }
    
    /**
     * Turn two correlated arrays into a {@link Map} associating {@link Integer}
     * keys with {@link Set}s. The 2nd array is two dimension and specifies the
     * set of values for each entry in the {@link Map}.
     * 
     * @param ids
     *            The keys.
     * @param vals
     *            The values.
     * 
     * @return The {@link Map}.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected Map<Integer,Set> newMap(final int[] ids,final Object[][] vals) {
        
        assertEquals(ids.length, vals.length);
        
        final Map<Integer, Set> m = new LinkedHashMap<Integer, Set>();
        
        for (int i = 0; i < ids.length; i++) {

            final LinkedHashSet set = new LinkedHashSet();
            
            final Object[] b = vals[i];

            for (Object o : b)
                set.add(o);

            m.put(ids[i], set);

        }

        return m;
        
    }

    private void assertSameState(final InnerState expected,
            final InnerState actual) {

        if (log.isInfoEnabled())
            log.info("actual=" + actual);

        assertEquals("queryId", expected.queryId, actual.queryId);

        assertEquals("deadline", expected.deadline.get(), actual.deadline.get());

        assertEquals("started", expected.started.get(), actual.started.get());

        assertEquals("allDone", expected.allDone.get(), actual.allDone.get());

        assertEquals("stepCount", expected.stepCount.get(),
                actual.stepCount.get());

        assertEquals("totalAvailableCount", expected.totalAvailableCount.get(),
                actual.totalAvailableCount.get());

        assertEquals("totalRunningCount", expected.totalRunningCount.get(),
                actual.totalRunningCount.get());

        assertEquals("totalLastPassRemainingCount",
                expected.totalLastPassRemainingCount.get(),
                actual.totalLastPassRemainingCount.get());

        assertEquals("availableMap", expected.availableMap,
                actual.availableMap);
        
        assertEquals("runningMap", expected.runningMap,
                actual.runningMap);
        
        assertEquals("serviceIds", expected.serviceIds,
                actual.serviceIds);
        
        assertEquals("startedOn", expected.startedOn,
                actual.startedOn);
        
        assertEquals("doneOn", expected.doneOn,
                actual.doneOn);
        
        assertEquals("lastPassRequested", expected.lastPassRequested,
                actual.lastPassRequested);

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

        final long begin = System.currentTimeMillis();
        
        final long deadline = begin + 12;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        final InnerState expected = new InnerState(query, queryId, deadline,
                begin, bopIndex);

        final InnerState actual = new InnerState(query, queryId, deadline,
                begin, bopIndex);

        final RunState runState = new RunState(actual);

        assertSameState(expected, actual);

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
        
        final long begin = System.currentTimeMillis();

        final long deadline = begin + 12;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        try {
            new RunState(new InnerState(null/* query */, queryId, deadline,
                    begin, bopIndex));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            new RunState(new InnerState(query, null/* queryId */, deadline,
                    begin, bopIndex));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            new RunState(new InnerState(query, queryId, 0L/* deadline */,
                    begin, bopIndex));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            new RunState(new InnerState(query, queryId, -1L/* deadline */,
                    begin, bopIndex));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            new RunState(
                    new InnerState(query, queryId, deadline, begin, null/* bopIndex */));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

    /**
     * Unit test for {@link RunState#getOperatorRunState(int)}
     */
    public void test_getOperatorRunState() {

        final int startId = 1;
        final int joinId1 = 2;
        final int joinId2 = 4;
        
        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(PipelineOp.Annotations.BOP_ID, startId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        }));
                
        final PipelineOp join1Op = new MockPipelineOp(new BOp[] { startOp },
                new NV(PipelineOp.Annotations.BOP_ID, joinId1)//
        );

        final PipelineOp join2Op = new MockPipelineOp(new BOp[] { join1Op }, //
                new NV(PipelineOp.Annotations.BOP_ID, joinId2)//
        );

        final PipelineOp query = join2Op;

        final UUID queryId = UUID.randomUUID();

        final long begin = System.currentTimeMillis();

        final long deadline = Long.MAX_VALUE;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        final InnerState actual = new InnerState(query, queryId, deadline,
                begin, bopIndex);

        final RunState runState = new RunState(actual);

        /*
         * The initial run state of the query is inactive (nothing running, no
         * chunks available).
         * 
         * If the query is inactive (nothing running, no chunks available) then
         * it is trivially true for any operator in the query plan that it can
         * not be triggered and will not be executed.
         */
        {

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId2));

        }

        /*
         * Modify the activity state such that one chunk is available for the
         * start operator and verify that the start operator and both join
         * operators can be triggered.
         */
        {
         
            actual.availableMap.put(startId, new AtomicLong(1L));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

        }

        /*
         * Modify the activity state such that one chunk is available for join1
         * and verify that the start operator is done but that both joins can be
         * triggered.
         */
        {

            assertNotNull(actual.availableMap.remove(startId));

            actual.availableMap.put(joinId1, new AtomicLong(1L));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

        }

        /*
         * Modify the activity state such that one chunk is available for join2
         * and verify that the start operator and first join are done but that
         * the 2nd join can be triggered.
         */
        {

            assertNotNull(actual.availableMap.remove(joinId1));

            actual.availableMap.put(joinId2, new AtomicLong(1L));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

        }

        /*
         * Modify the activity state such no chunks are available but the start
         * operator is running and verify that the join operators both can be
         * triggered.
         */
        {

            assertNotNull(actual.availableMap.remove(joinId2));

            actual.runningMap.put(startId, new AtomicLong(1L));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

        }

        /*
         * Modify the activity state such no chunks are available but the 1st
         * join operator is running and verify that the 2nd join operators can
         * be triggered.
         */
        {

            assertNotNull(actual.runningMap.remove(startId));

            actual.runningMap.put(joinId1, new AtomicLong(1L));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

        }

        /*
         * Modify the activity state such no chunks are available but the 2nd
         * join operator is running and verify that the 2nd join operator can be
         * triggered.
         */
        {

            assertNotNull(actual.runningMap.remove(joinId1));
            actual.runningMap.put(joinId2, new AtomicLong(1L));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

        }
        
    }

    /**
     * Unit test for {@link RunState#getOperatorRunState(int)} when some
     * operators specify the {@link PipelineOp.Annotations#LAST_PASS}
     * annotation.
     * 
     * TODO Do another variant where there are two such operators. For this
     * variant, we want to make sure that the last pass evaluation for each
     * operator is executed properly.
     */
    public void test_getOperatorRunState_lastPassRequested() {
    
        final int startId = 1;
        final int joinId1 = 2;
        final int joinId2 = 4;
        final int orderId = 6;
        
        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(PipelineOp.Annotations.BOP_ID, startId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        }));
        
        final PipelineOp join1Op = new MockPipelineOp(new BOp[] { startOp },
                new NV(PipelineOp.Annotations.BOP_ID, joinId1)//
        );

        final PipelineOp join2Op = new MockPipelineOp(new BOp[] { join1Op }, //
                new NV(PipelineOp.Annotations.BOP_ID, joinId2)//
        );

        final PipelineOp orderOp = new MockPipelineOp(new BOp[] { join2Op }, //
                new NV(PipelineOp.Annotations.BOP_ID, orderId),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1) //
        );

        final PipelineOp query = orderOp;

        final UUID queryId = UUID.randomUUID();

        final long begin = System.currentTimeMillis();

        final long deadline = Long.MAX_VALUE;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        final InnerState actual = new InnerState(query, queryId, deadline,
                begin, bopIndex);

        final RunState runState = new RunState(actual);      
        
        actual.lastPassRequested.add(orderId);
        
        final UUID serviceId = UUID.randomUUID();
        
        /*
         * If the query is inactive (nothing running, no chunks available) then
         * it is trivially true for any operator in the query plan that it can
         * not be triggered and will not be executed.
         */
        {

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(orderId));

        }

        /*
         * Modify the activity state such that one chunk is available for the
         * start operator and verify that all downstream operators can be
         * triggered.
         */
        {

            actual.availableMap.put(startId, new AtomicLong(1L));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(orderId));

        }

        /*
         * Modify the activity state such that one chunk is available for join1
         * and verify that the start operator is done but that join1 and all
         * downstream operators (join2, orderBy) can be triggered.
         */
        {

            assertNotNull(actual.availableMap.remove(startId));

            actual.availableMap.put(joinId1, new AtomicLong(1L));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(orderId));

        }

        /*
         * Modify the activity state such that one chunk is available for join2
         * and verify that the start operator and first join are done but that
         * the 2nd join and orderBy can be triggered.
         */
        {

            assertNotNull(actual.availableMap.remove(joinId1));

            actual.availableMap.put(joinId2, new AtomicLong(1L));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(orderId));

        }

        /*
         * Modify the activity state such no chunks are available but the start
         * operator is running and verify that all downstream operators can be
         * triggered.
         */
        {

            assertNotNull(actual.availableMap.remove(joinId2));

            actual.runningMap.put(startId, new AtomicLong(1L));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(orderId));

        }

        /*
         * Modify the activity state such no chunks are available but the 1st
         * join operator is running and verify that the 2nd join operator and
         * the orderBy operator can be triggered.
         */
        {

            assertNotNull(actual.runningMap.remove(startId));

            actual.runningMap.put(joinId1, new AtomicLong(1L));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(orderId));

        }

        /*
         * Modify the activity state such no chunks are available but the 2nd
         * join operator is running and verify that the 2nd join operator and
         * the orderBy operator can be triggered.
         */
        {

            assertNotNull(actual.runningMap.remove(joinId1));
            actual.runningMap.put(joinId2, new AtomicLong(1L));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(orderId));

        }

        /*
         * Modify the activity state such that no chunks are available no
         * operators are running and verify that all operators report 'AllDone'.
         */
        {

            assertNotNull(actual.runningMap.remove(joinId2));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(orderId));

        }

        /*
         * Modify the activity state such that the order by operator has been
         * evaluated (runningCountMap is non-empty) but is not currently
         * running. Verify that the order by operator will now start its last
         * evaluation pass phase.
         */
        {
            
            assertNull(actual.runningMap.put(orderId, new AtomicLong(0)));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.StartLastPass,
                    runState.getOperatorRunState(orderId));

        }

        /*
         * Modify the activity state such that the doneSet for the orderBy is
         * non-empty and verify that the orderBy operator now reports that it is
         * in its last evaluation phase.
         */
        {

            assertEquals(null, actual.doneOn.get(orderId));
            actual.doneOn.put(orderId, Collections.singleton(serviceId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.RunningLastPass,
                    runState.getOperatorRunState(orderId));

        }

        /*
         * Modify the run state such that the doneSet for the orderBy operator
         * is an empty set. This is the signal that the operator has finished
         * its last pass evaluation.
         */
        {

            assertNotNull(actual.doneOn.remove(orderId));
            actual.doneOn.put(orderId, Collections.emptySet());

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(orderId));

        }

    }

    /**
     * Unit test for {@link RunState#getOperatorRunState(int)} when some
     * operators require at-once evaluation.
     *
     * @see <a href="http://trac.blazegraph.com/ticket/868"> COUNT(DISTINCT) returns no rows rather than ZERO. </a>
     */
    public void test_getOperatorRunState_atOnceRequested() {

        final int startId = 1;
        final int joinId1 = 2;
        final int joinId2 = 4;
        final int countId = 6;

        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(PipelineOp.Annotations.BOP_ID, startId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp join1Op = new MockPipelineOp(new BOp[] { startOp },
                new NV(PipelineOp.Annotations.BOP_ID, joinId1)//
        );

        final PipelineOp join2Op = new MockPipelineOp(new BOp[] { join1Op }, //
                new NV(PipelineOp.Annotations.BOP_ID, joinId2)//
        );

        final PipelineOp countOp = new MockPipelineOp(new BOp[] { join2Op }, //
                new NV(PipelineOp.Annotations.BOP_ID, countId),//
                new NV(PipelineOp.Annotations.PIPELINED, false),//
                new NV(PipelineOp.Annotations.MAX_MEMORY, 0)
        );

        final PipelineOp query = countOp;

        final UUID queryId = UUID.randomUUID();

        final long begin = System.currentTimeMillis();

        final long deadline = Long.MAX_VALUE;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        final InnerState actual = new InnerState(query, queryId, deadline,
                begin, bopIndex);

        final RunState runState = new RunState(actual);

        actual.atOnceRequired.add(countId);

        final UUID serviceId = UUID.randomUUID();

        /*
         * If the query is inactive (nothing running, no chunks available) then
         * it is trivially true for any operator in the query plan that it can
         * not be triggered and will not be executed.
         */
        {

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(countId));

        }

        /*
         * Modify the activity state such that one chunk is available for the
         * start operator and verify that all downstream operators can be
         * triggered.
         */
        {

            actual.availableMap.put(startId, new AtomicLong(1L));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(countId));

        }

        /*
         * Modify the activity state such that one chunk is available for join1
         * and verify that the start operator is done but that join1 and all
         * downstream operators (join2, count) can be triggered.
         */
        {

            assertNotNull(actual.availableMap.remove(startId));

            actual.availableMap.put(joinId1, new AtomicLong(1L));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(countId));

        }

        /*
         * Modify the activity state such that one chunk is available for join2
         * and verify that the start operator and first join are done but that
         * the 2nd join and count can be triggered.
         */
        {

            assertNotNull(actual.availableMap.remove(joinId1));

            actual.availableMap.put(joinId2, new AtomicLong(1L));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(countId));

        }

        /*
         * Modify the activity state such no chunks are available but the start
         * operator is running and verify that all downstream operators can be
         * triggered.
         */
        {

            assertNotNull(actual.availableMap.remove(joinId2));

            actual.runningMap.put(startId, new AtomicLong(1L));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(countId));

        }

        /*
         * Modify the activity state such no chunks are available but the 1st
         * join operator is running and verify that the 2nd join operator and
         * the count operator can be triggered.
         */
        {

            assertNotNull(actual.runningMap.remove(startId));

            actual.runningMap.put(joinId1, new AtomicLong(1L));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(countId));

        }

        /*
         * Modify the activity state such no chunks are available but the 2nd
         * join operator is running and verify that the 2nd join operator and
         * the count operator can be triggered.
         */
        {

            assertNotNull(actual.runningMap.remove(joinId1));
            actual.runningMap.put(joinId2, new AtomicLong(1L));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.Running,
                    runState.getOperatorRunState(countId));

        }

        /*
         * Modify the activity state such that no chunks are available no
         * operators are running and verify that all operators report 'AllDone'.
         */
        {

            assertNotNull(actual.runningMap.remove(joinId2));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(countId));

        }

        /*
         * Modify the activity state such that the count operator has been
         * evaluated (runningCountMap is non-empty) but is not currently
         * running. Verify that the count operator will now start its last
         * evaluation pass phase.
         */
        {

            assertNull(actual.runningMap.put(countId, new AtomicLong(0)));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.StartLastPass,
                    runState.getOperatorRunState(countId));

        }

        /*
         * Modify the activity state such that the doneSet for the count is
         * non-empty and verify that the count operator now reports that it is
         * in its last evaluation phase.
         */
        {

            assertEquals(null, actual.doneOn.get(countId));
            actual.doneOn.put(countId, Collections.singleton(serviceId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.RunningLastPass,
                    runState.getOperatorRunState(countId));

        }

        /*
         * Modify the run state such that the doneSet for the count operator
         * is an empty set. This is the signal that the operator has finished
         * its last pass evaluation.
         */
        {

            assertNotNull(actual.doneOn.remove(countId));
            actual.doneOn.put(countId, Collections.emptySet());

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(startId));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId1));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(joinId2));

            assertEquals(RunStateEnum.AllDone,
                    runState.getOperatorRunState(countId));
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
     * @throws RemoteException 
     */
    public void test_runSingleOperatorQuery() throws TimeoutException,
            ExecutionException {

        final UUID serviceId = UUID.randomUUID();

        final IQueryClient queryController = new MockQueryController(serviceId);

        final int startId = 1;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query = startOp;
        
        final UUID queryId = UUID.randomUUID();

        final long begin = System.currentTimeMillis();
        
        final long deadline = Long.MAX_VALUE;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        // step0
        final InnerState expected = new InnerState(query, queryId,
                deadline, begin, bopIndex);
        
        final InnerState actual = new InnerState(query, queryId,
                deadline, begin, bopIndex);

        final RunState runState = new RunState(actual);

        assertSameState(expected, actual);
        
        // step1 : startQuery, notice that a chunk is available.
        runState.startQuery(new LocalChunkMessage(queryController,
                queryId, startId, -1/* partitionId */,
                new HashBindingSet()));
   
        expected.started.set(true);
        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.incrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(1L));
        expected.serviceIds.add(serviceId);
        
        assertSameState(expected, actual);

        // step2 : start operator.
        runState.startOp(new StartOpMessage(queryId, startId,
                -1/* partitionId */, serviceId, 1/* nmessages */
//                startOp.getEvaluationContext(),
//                startOp.isLastPassRequested()
                ));

        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.decrementAndGet();
        expected.totalRunningCount.incrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(0L));
        expected.runningMap.put(startId, new AtomicLong(1L));
        expected.startedOn.put(startId, newSet(new UUID[] { serviceId }));
        
        assertSameState(expected, actual);

        /*
         * step3 : halt operator. in this case no chunk messages are produced
         * and the query should halt.
         */
        {

            final BOpStats stats = new BOpStats();

            runState.haltOp(new HaltOpMessage(queryId, startId,
                    -1/* partitionId */, serviceId, null/* cause */,
                    //null/* sinkId */,
                    0,// sinkMessagesOut
//                    null/* altSinkId */,
                    0, // altSinkMessagesOut
                    stats)
                    );

        }

        expected.allDone.set(true);
        expected.stepCount.incrementAndGet();
        expected.totalRunningCount.decrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(0L));
        expected.runningMap.put(startId, new AtomicLong(0L));
        
        assertSameState(expected, actual);

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

        final UUID serviceId = UUID.randomUUID();

        final IQueryClient queryController = new MockQueryController(serviceId);

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

        final long begin = System.currentTimeMillis();
        
        final long deadline = Long.MAX_VALUE;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        final InnerState expected = new InnerState(query, queryId,
                deadline, begin, bopIndex);

        final InnerState actual = new InnerState(query, queryId, deadline,
                begin, bopIndex);
        
        final RunState runState = new RunState(actual);

        // step0
        assertSameState(expected, actual);
        
        // step1 : startQuery, notice that a chunk is available.
        runState.startQuery(new LocalChunkMessage(queryController, queryId,
                startId, -1/* partitionId */, new HashBindingSet()));

        expected.started.set(true);
        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.incrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(1L));
        expected.serviceIds.add(serviceId);
        
        assertSameState(expected, actual);

        // step2 : start operator.
        runState.startOp(new StartOpMessage(queryId, startId,
                -1/* partitionId */, serviceId, 1/* nmessages */
//                startOp.getEvaluationContext(),
//                startOp.isLastPassRequested()
                ));

        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.decrementAndGet();
        expected.totalRunningCount.incrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(0L));
        expected.runningMap.put(startId, new AtomicLong(1L));
        expected.startedOn.put(startId, newSet(new UUID[] { serviceId }));
        
        assertSameState(expected, actual);

        // step3 : halt operator : the operator produced one chunk.
        {

            final BOpStats stats = new BOpStats();

            runState.haltOp(new HaltOpMessage(queryId, startId,
                    -1/* partitionId */, serviceId, null/* cause */,
//                    otherId/* sinkId */, 
                    1/* sinkMessagesOut */,
//                    null/* altSinkId */, 
                    0/* altSinkMessagesOut */, stats)
                    );

        }

        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.incrementAndGet();
        expected.totalRunningCount.decrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(0L));
        expected.availableMap.put(otherId, new AtomicLong(1L));
        expected.runningMap.put(startId, new AtomicLong(0L));
        
        assertSameState(expected, actual);

        // start the 2nd operator.
        runState.startOp(new StartOpMessage(queryId, otherId,
                -1/* partitionId */, serviceId, 1/* nmessages */
//                otherOp.getEvaluationContext(),
//                otherOp.isLastPassRequested()
                ));

        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.decrementAndGet();
        expected.totalRunningCount.incrementAndGet();
        expected.availableMap.put(otherId, new AtomicLong(0L));
        expected.runningMap.put(otherId, new AtomicLong(1L));
        expected.startedOn.put(otherId, newSet(new UUID[] { serviceId }));

        assertSameState(expected, actual);
        
        // step3 : halt operator : the operator produced no chunk messages.
        {

            final BOpStats stats = new BOpStats();

            runState.haltOp(new HaltOpMessage(queryId, otherId,
                    -1/* partitionId */, serviceId, null/* cause */,
                    //null/* sinkId */, 
                    0/* sinkMessagesOut */,
//                    null/* altSinkId */, 
                    0/* altSinkMessagesOut */, stats)
                    );

        }

        expected.allDone.set(true);
        expected.stepCount.incrementAndGet();
        expected.totalRunningCount.decrementAndGet();
        expected.availableMap.put(otherId, new AtomicLong(0L));
        expected.runningMap.put(otherId, new AtomicLong(0L));
        
        assertSameState(expected, actual);

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

        final UUID serviceId = UUID.randomUUID();

        final IQueryClient queryController = new MockQueryController(serviceId);

        final int startId = 1;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query = startOp;

        final UUID queryId = UUID.randomUUID();

        final long begin = System.currentTimeMillis();
        
        final long deadline = Long.MAX_VALUE;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        final InnerState expected = new InnerState(query, queryId,
                deadline, begin, bopIndex);

        final InnerState actual = new InnerState(query, queryId, deadline,
                begin, bopIndex);
        
        final RunState runState = new RunState(actual);

        // step0
        assertSameState(expected, actual);

        // step1 : startQuery, notice that a chunk is available.
        runState.startQuery(new LocalChunkMessage(queryController, queryId,
                startId, -1/* partitionId */, new HashBindingSet()));

        expected.started.set(true);
        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.incrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(1L));
        expected.serviceIds.add(serviceId);
        
        assertSameState(expected, actual);

        try {
            runState.startQuery(new LocalChunkMessage(queryController, queryId,
                    startId, -1/* partitionId */, new HashBindingSet()));
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // state is unchanged.
        assertSameState(expected, actual);

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

        final UUID serviceId = UUID.randomUUID();

        final IQueryClient queryController = new MockQueryController(serviceId);

        final int startId = 1;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query = startOp;
        
        final UUID queryId = UUID.randomUUID();

        final long begin = System.currentTimeMillis();
        
        final long delay = 100; // ms.
        
        final long deadline = begin + delay;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        final InnerState expected = new InnerState(query, queryId,
                deadline, begin, bopIndex);

        final InnerState actual = new InnerState(query, queryId, deadline,
                begin, bopIndex);
        
        final RunState runState = new RunState(actual);

        // step0
        assertSameState(expected, actual);

        // wait for the deadline to pass.
        Thread.sleep(delay + delay / 2);
        
        // verify that the deadline has passed.
        assertTrue(System.currentTimeMillis() > deadline);

        // no state change.
        assertSameState(expected, actual);

        // start the query.
        try {
            runState.startQuery(new LocalChunkMessage(queryController, queryId,
                    startId, -1/* partitionId */, new HashBindingSet()));
            fail("Expected: " + TimeoutException.class);
        } catch (TimeoutException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        /*
         * Note: RunState does not track the error. That is tracked by the
         * Future (Haltable).
         */
        // verify post-conditions
//        expected.started.set(true);
//      expected.allDone.set(true);
        assertSameState(expected, actual);

    }
    
    /**
     * Unit test for correct interpretation of a deadline. There are a series of
     * deadline tests which verify that the deadline is detected by the various
     * action methods (startQuery, startOp, haltOp). For this test, the deadline
     * should be recognized by {@link RunState#startOp(IStartOpMessage)}.
     * 
     * @throws TimeoutException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_deadline_startOp() throws TimeoutException,
            ExecutionException, InterruptedException {
        
        final UUID serviceId = UUID.randomUUID();

        final IQueryClient queryController = new MockQueryController(serviceId);

        final int startId = 1;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query = startOp;
        
        final UUID queryId = UUID.randomUUID();

        final long begin = System.currentTimeMillis();
        
        final long delay = 100; // ms.
        
        final long deadline = begin + delay;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        final InnerState expected = new InnerState(query, queryId,
                deadline, begin, bopIndex);

        final InnerState actual = new InnerState(query, queryId, deadline,
                begin, bopIndex);
        
        final RunState runState = new RunState(actual);

        // step0
        assertSameState(expected, actual);

        // start the query.
        runState.startQuery(new LocalChunkMessage(queryController, queryId,
                startId, -1/* partitionId */, new HashBindingSet()));
        
        // verify post-conditions.
        expected.started.set(true);
        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.incrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(1L));
        expected.serviceIds.add(serviceId);
        
        assertSameState(expected, actual);

        // wait for the deadline to pass.
        Thread.sleep(delay + delay / 2);

        // verify that the deadline has passed.
        assertTrue(System.currentTimeMillis() > deadline);

        try {
            // step2 : start operator.
            runState.startOp(new StartOpMessage(queryId, startId,
                    -1/* partitionId */, serviceId, 1/* nmessages */
//                    startOp.getEvaluationContext(),
//                    startOp.isLastPassRequested()
                    ));
            fail("Expected: " + TimeoutException.class);
        } catch (TimeoutException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // verify post-conditions
        assertSameState(expected, actual);

    }

    /**
     * Unit test for correct interpretation of a deadline. There are a series of
     * deadline tests which verify that the deadline is detected by the various
     * action methods (startQuery, startOp, haltOp). For this test, the deadline
     * should recognized by {@link RunState#haltOp(IHaltOpMessage)}.
     * 
     * @throws TimeoutException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_deadline_haltOp() throws TimeoutException,
            ExecutionException, InterruptedException {

        final UUID serviceId = UUID.randomUUID();

        final IQueryClient queryController = new MockQueryController(serviceId);
        
        final int startId = 1;

        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query = startOp;
        
        final UUID queryId = UUID.randomUUID();

        final long begin = System.currentTimeMillis();
        
        final long delay = 100; // ms.
        
        final long deadline = begin + delay;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);


        final InnerState expected = new InnerState(query, queryId,
                deadline, begin, bopIndex);

        final InnerState actual = new InnerState(query, queryId, deadline,
                begin, bopIndex);
        
        final RunState runState = new RunState(actual);

        // step0
        assertSameState(expected, actual);

        // start the query.
        runState.startQuery(new LocalChunkMessage(queryController, queryId,
                startId, -1/* partitionId */, new HashBindingSet()));
        
        // verify post-conditions.
        expected.started.set(true);
        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.incrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(1L));
        expected.serviceIds.add(serviceId);
        
        assertSameState(expected, actual);
        
        // step2 : start operator.
        runState.startOp(new StartOpMessage(queryId, startId,
                -1/* partitionId */, serviceId, 1/* nmessages */
                //,startOp.getEvaluationContext(),//
                //,startOp.isLastPassRequested()
                ));

        // verify post-conditions.
        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.decrementAndGet();
        expected.totalRunningCount.incrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(0L));
        expected.runningMap.put(startId, new AtomicLong(1L));
        expected.startedOn.put(startId, newSet(new UUID[]{serviceId}));
        
        assertSameState(expected, actual);

        // wait for the deadline to pass.
        Thread.sleep(delay + delay / 2);

        // verify that the deadline has passed.
        assertTrue(System.currentTimeMillis() > deadline);

        try {

            final BOpStats stats = new BOpStats();

            runState.haltOp(new HaltOpMessage(queryId, startId,
                    -1/* partitionId */, serviceId, null/* cause */,
//                    null/* sinkId */, 
                    0/* sinkMessagesOut */,
//                    null/* altSinkId */, 
                    0/* altSinkMessagesOut */, stats));

            fail("Expected: " + TimeoutException.class);

        } catch (TimeoutException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // verify post-conditions
        assertSameState(expected, actual);

    }

    /**
     * Unit tests for an operator which requests a final evaluation pass.
     * 
     * @see PipelineOp.Annotations#LAST_PASS
     */
    public void test_lastPassRequested() throws TimeoutException,
            ExecutionException, InterruptedException {

        final UUID serviceId = UUID.randomUUID();

        final IQueryClient queryController = new MockQueryController(serviceId);

        final int startId = 1;
        final int otherId = 2;
        final int orderId = 3;

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

        final PipelineOp orderOp = new StartOp(new BOp[] { otherOp },
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, orderId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.LAST_PASS,true),//
                        new NV(PipelineOp.Annotations.MAX_PARALLEL,1),//
                        }));

        final PipelineOp query = orderOp;

        final UUID queryId = UUID.randomUUID();

        final long begin = System.currentTimeMillis();
        
        final long deadline = Long.MAX_VALUE;

        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);

        final InnerState expected = new InnerState(query, queryId,
                deadline, begin, bopIndex);

        final InnerState actual = new InnerState(query, queryId, deadline,
                begin, bopIndex);
        
        final RunState runState = new RunState(actual);

        // step0
        assertSameState(expected, actual);
        
        // step1 : startQuery, notice that a chunk is available.
        runState.startQuery(new LocalChunkMessage(queryController, queryId,
                startId, -1/* partitionId */, new HashBindingSet()));

        expected.started.set(true);
        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.incrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(1L));
        expected.serviceIds.add(serviceId);
        // mock "zero" evaluations of operator requesting last pass semantics.
        expected.totalLastPassRemainingCount.incrementAndGet();
        expected.startedOn.put(orderId, newSet(new UUID[] { serviceId }));
        expected.runningMap.put(orderId, new AtomicLong(0L));
        expected.availableMap.put(orderId, new AtomicLong(0L));
        expected.lastPassRequested.add(orderId);

        assertSameState(expected, actual);

        // step2 : start operator.
        runState.startOp(new StartOpMessage(queryId, startId,
                -1/* partitionId */, serviceId, 1/* nmessages */
//                startOp.getEvaluationContext(),
//                startOp.isLastPassRequested()
                ));

        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.decrementAndGet();
        expected.totalRunningCount.incrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(0L));
        expected.runningMap.put(startId, new AtomicLong(1L));
        expected.startedOn.put(startId, newSet(new UUID[] { serviceId }));
        
        assertSameState(expected, actual);

        // step3 : halt operator : the operator produced one chunk.
        {

            final BOpStats stats = new BOpStats();

            final IHaltOpMessage msg = new HaltOpMessage(queryId, startId,
                    -1/* partitionId */, serviceId, null/* cause */,
//                    otherId/* sinkId */, 
                    1/* sinkMessagesOut */,
//                    null/* altSinkId */, 
                    0/* altSinkMessagesOut */, stats);
            
            assertEquals(RunStateEnum.AllDone, runState.haltOp(msg));

        }

        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.incrementAndGet();
        expected.totalRunningCount.decrementAndGet();
        expected.availableMap.put(startId, new AtomicLong(0L));
        expected.availableMap.put(otherId, new AtomicLong(1L));
        expected.runningMap.put(startId, new AtomicLong(0L));
        
        assertSameState(expected, actual);

        // start the 2nd operator.
        runState.startOp(new StartOpMessage(queryId, otherId,
                -1/* partitionId */, serviceId, 1/* nmessages */
//                otherOp.getEvaluationContext(),
//                otherOp.isLastPassRequested()
                ));

        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.decrementAndGet();
        expected.totalRunningCount.incrementAndGet();
        expected.availableMap.put(otherId, new AtomicLong(0L));
        expected.runningMap.put(otherId, new AtomicLong(1L));
        expected.startedOn.put(otherId, newSet(new UUID[] { serviceId }));

        assertSameState(expected, actual);
        
        // step3 : halt operator : the operator produced one chunk message.
        {

            final BOpStats stats = new BOpStats();

            final IHaltOpMessage msg = new HaltOpMessage(queryId, otherId,
                    -1/* partitionId */, serviceId, null/* cause */,
                    //orderOp.getId()/* sinkId */, 
                    1/* sinkMessagesOut */,
//                    null/* altSinkId */, 
                    0/* altSinkMessagesOut */, stats);

            assertEquals(RunStateEnum.AllDone, runState.haltOp(msg));

        }
        
        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.incrementAndGet();
        expected.totalRunningCount.decrementAndGet();
        expected.availableMap.put(orderId, new AtomicLong(1L));
        expected.runningMap.put(otherId, new AtomicLong(0L));

        assertSameState(expected, actual);

        // step4: start the 3rd operator.
        runState.startOp(new StartOpMessage(queryId, orderId,
                -1/* partitionId */, serviceId, 1/* nmessages */
//                orderOp.getEvaluationContext(),
//                orderOp.isLastPassRequested()
                ));

        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.decrementAndGet();
        expected.totalRunningCount.incrementAndGet();
//        expected.totalLastPassRemainingCount.incrementAndGet(); // already accounted for.
        expected.availableMap.put(orderId, new AtomicLong(0L));
        expected.runningMap.put(orderId, new AtomicLong(1L));
//        expected.startedOn.put(orderId, newSet(new UUID[] { serviceId })); // already accounted for.
//        expected.lastPassRequested.add(orderId); // already accounted for.

        assertSameState(expected, actual);

        /*
         * step5 : halt operator : the operator produced one chunk message. the
         * operator requests last pass evaluation so the startedSet should have
         * been copied to the doneSet. evaluation should not terminate until we
         * have observed a HaltOpMessage for each entry in the doneSet.
         */
        {

            final BOpStats stats = new BOpStats();

            final IHaltOpMessage msg = new HaltOpMessage(queryId, orderId,
                    -1/* partitionId */, serviceId, null/* cause */,
//                    null/* sinkId (queryBuffer)*/, 
                    1/* sinkMessagesOut */,
//                    null/* altSinkId */, 
                    0/* altSinkMessagesOut */, stats);
            
            assertEquals(RunStateEnum.StartLastPass, runState.haltOp(msg));

        }
        
        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.incrementAndGet();
        expected.totalRunningCount.decrementAndGet();
        expected.availableMap.put(orderId, new AtomicLong(1L));
        expected.runningMap.put(orderId, new AtomicLong(0L));
        expected.doneOn.put(orderId, newSet(new UUID[]{serviceId}));

        assertSameState(expected, actual);

        // step4: start the last pass evaluation for the 3nd operator.
        assertFalse(runState
                .startOp(new StartOpMessage(queryId, orderId,
                        -1/* partitionId */, serviceId, 1/* nmessages */
//                        orderOp.getEvaluationContext(), orderOp
//                                .isLastPassRequested()
                )));

        expected.stepCount.incrementAndGet();
        expected.totalAvailableCount.decrementAndGet();
        expected.totalRunningCount.incrementAndGet();
        expected.availableMap.put(orderId, new AtomicLong(0L));
        expected.runningMap.put(orderId, new AtomicLong(1L));
        
        assertSameState(expected, actual);

        /*
         * step6 : halt operator : the operator produced one chunk message
         * (which will go to the query buffer).
         */
        {

            final BOpStats stats = new BOpStats();

            final IHaltOpMessage msg = new HaltOpMessage(queryId, orderId,
                    -1/* partitionId */, serviceId, null/* cause */,
                    //null/* sinkId (queryBuffer)*/
                    1/* sinkMessagesOut */,
                    //null/* altSinkId */
                    0/* altSinkMessagesOut */, stats);

            assertEquals(RunStateEnum.AllDone, runState.haltOp(msg));

        }
        
        expected.allDone.set(true);
        expected.stepCount.incrementAndGet();
        expected.totalRunningCount.decrementAndGet();
        expected.totalLastPassRemainingCount.decrementAndGet();
        expected.runningMap.put(orderId, new AtomicLong(0L));
        expected.doneOn.put(orderId, Collections.emptySet());

        assertSameState(expected, actual);

    }
    
//    /**
//     * Unit tests for an operator which requests a final evaluation pass but
//     * which was never triggered during normal evaluation. The final evaluation
//     * pass is still triggered for such cases.
//     * 
//     * @see PipelineOp.Annotations#LAST_PASS
//     */
//    public void test_lastPassRequested_neverTriggered() throws TimeoutException,
//            ExecutionException, InterruptedException {
//
//        final UUID serviceId = UUID.randomUUID();
//
//        final IQueryClient queryController = new MockQueryController(serviceId);
//
//        final int startId = 1;
//        final int otherId = 2;
//        final int orderId = 3;
//
//        final PipelineOp startOp = new StartOp(new BOp[] {}, NV
//                .asMap(new NV[] {//
//                new NV(Predicate.Annotations.BOP_ID, startId),//
//                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
//                        BOpEvaluationContext.CONTROLLER),//
//                }));
//
//        // Note: For this test, we will simulate dropping all solutions which 
//        // are passed to [otherOp].
//        final PipelineOp otherOp = new StartOp(new BOp[] { startOp },
//                NV.asMap(new NV[] {//
//                        new NV(Predicate.Annotations.BOP_ID, otherId),//
//                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
//                                BOpEvaluationContext.CONTROLLER),//
//                        }));
//
//        final PipelineOp orderOp = new StartOp(new BOp[] { otherOp },
//                NV.asMap(new NV[] {//
//                        new NV(Predicate.Annotations.BOP_ID, orderId),//
//                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
//                                BOpEvaluationContext.CONTROLLER),//
//                        new NV(PipelineOp.Annotations.LAST_PASS,true),//
//                        new NV(PipelineOp.Annotations.MAX_PARALLEL,1),//
//                        }));
//
//        final PipelineOp query = orderOp;
//
//        final UUID queryId = UUID.randomUUID();
//
//        final long begin = System.currentTimeMillis();
//        
//        final long deadline = Long.MAX_VALUE;
//
//        final Map<Integer, BOp> bopIndex = BOpUtility.getIndex(query);
//
//        final InnerState expected = new InnerState(query, queryId,
//                deadline, begin, bopIndex);
//
//        final InnerState actual = new InnerState(query, queryId, deadline,
//                begin, bopIndex);
//        
//        final RunState runState = new RunState(actual);
//
//        // step0
//        assertSameState(expected, actual);
//        
//        /*
//         * step1 : startQuery, notice that a chunk is available.
//         * 
//         * Note: The operator is immediately registered for last pass evaluation
//         * since it will run on the query controller.
//         */
//        runState.startQuery(new LocalChunkMessage<IBindingSet>(queryController,
//                queryId, startId, -1/* partitionId */,
//                newBindingSetIterator(new HashBindingSet())));
//
//        expected.started.set(true);
//        expected.stepCount.incrementAndGet();
//        expected.totalAvailableCount.incrementAndGet();
//        expected.availableMap.put(startId, new AtomicLong(1L));
//        expected.serviceIds.add(serviceId);
//        expected.totalLastPassRemainingCount.incrementAndGet();
//        expected.startedOn.put(orderId, newSet(new UUID[] { serviceId }));
//        expected.runningMap.put(orderId, new AtomicLong(0L));
//        expected.availableMap.put(orderId, new AtomicLong(0L));
//        expected.lastPassRequested.add(orderId);
//
//        assertSameState(expected, actual);
//
//        // step2 : start operator.
//        runState.startOp(new StartOpMessage(queryId, startId,
//                -1/* partitionId */, serviceId, 1/* nmessages */,
//                startOp.getEvaluationContext(),
//                startOp.isLastPassRequested()));
//
//        expected.stepCount.incrementAndGet();
//        expected.totalAvailableCount.decrementAndGet();
//        expected.totalRunningCount.incrementAndGet();
//        expected.availableMap.put(startId, new AtomicLong(0L));
//        expected.runningMap.put(startId, new AtomicLong(1L));
//        expected.startedOn.put(startId, newSet(new UUID[] { serviceId }));
//        
//        assertSameState(expected, actual);
//
//        // step3 : halt operator : the operator produced one chunk.
//        {
//
//            final BOpStats stats = new BOpStats();
//
//            final HaltOpMessage msg = new HaltOpMessage(queryId, startId,
//                    -1/* partitionId */, serviceId, null/* cause */,
//                    otherId/* sinkId */, 1/* sinkMessagesOut */,
//                    null/* altSinkId */, 0/* altSinkMessagesOut */, stats);
//            
//            assertEquals(RunStateEnum.AllDone, runState.haltOp(msg));
//
//        }
//
//        expected.stepCount.incrementAndGet();
//        expected.totalAvailableCount.incrementAndGet();
//        expected.totalRunningCount.decrementAndGet();
//        expected.availableMap.put(startId, new AtomicLong(0L));
//        expected.availableMap.put(otherId, new AtomicLong(1L));
//        expected.runningMap.put(startId, new AtomicLong(0L));
//        
//        assertSameState(expected, actual);
//
//        // start the 2nd operator: Note: This operator will drop its solutions.
//        runState.startOp(new StartOpMessage(queryId, otherId,
//                -1/* partitionId */, serviceId, 1/* nmessages */,
//                otherOp.getEvaluationContext(),
//                otherOp.isLastPassRequested()));
//
//        expected.stepCount.incrementAndGet();
//        expected.totalAvailableCount.decrementAndGet();
//        expected.totalRunningCount.incrementAndGet();
//        expected.availableMap.put(otherId, new AtomicLong(0L));
//        expected.runningMap.put(otherId, new AtomicLong(1L));
//        expected.startedOn.put(otherId, newSet(new UUID[] { serviceId }));
//
//        assertSameState(expected, actual);
//        
//        /*
//         * step3 : halt operator : the operator produced NO chunk message.
//         * 
//         * Note: even though nothing was output, the availableMap is updated to
//         * now contain an entry for the target operator (orderId). It will show
//         * that there are ZERO (0) chunks available for that operator.
//         */
//        {
//
//            final BOpStats stats = new BOpStats();
//
//            final HaltOpMessage msg = new HaltOpMessage(queryId, otherId,
//                    -1/* partitionId */, serviceId, null/* cause */,
//                    orderOp.getId()/* sinkId */, 0/* sinkMessagesOut */,
//                    null/* altSinkId */, 0/* altSinkMessagesOut */, stats);
//
//            assertEquals(RunStateEnum.AllDone, runState.haltOp(msg));
//
//        }
//        
//        expected.stepCount.incrementAndGet();
////        expected.totalAvailableCount.incrementAndGet();
//        expected.totalRunningCount.decrementAndGet();
//        expected.availableMap.put(orderId, new AtomicLong(0L));
//        expected.runningMap.put(otherId, new AtomicLong(0L));
//
//        assertSameState(expected, actual);
//
//        /*
//         * At this point normal evaluation (based on the flow of chunk messages)
//         * is complete since no chunk messages were output from otherId (the 2nd
//         * operator). However, we still need to run a final evaluation pass for
//         * the 3rd operator since it declares the LAST_PASS annotation (and is
//         * being run on the query controller, so we will trigger it even though
//         * it never received a chunk message).
//         */
//
//        // step4: start the last pass evaluation for the 3rd operator.
//        assertFalse(runState
//                .startOp(new StartOpMessage(queryId, orderId,
//                        -1/* partitionId */, serviceId, 1/* nmessages */,
//                        orderOp.getEvaluationContext(), orderOp
//                                .isLastPassRequested())));
//
//        expected.stepCount.incrementAndGet();
//        expected.totalAvailableCount.decrementAndGet();
//        expected.totalRunningCount.incrementAndGet();
//        expected.availableMap.put(orderId, new AtomicLong(-1L));
//        expected.runningMap.put(orderId, new AtomicLong(1L));
//        
//        assertSameState(expected, actual);
//
//        /*
//         * step6 : halt operator : the operator produced one chunk message
//         * (which will go to the query buffer).
//         */
//        {
//
//            final BOpStats stats = new BOpStats();
//
//            final HaltOpMessage msg = new HaltOpMessage(queryId, orderId,
//                    -1/* partitionId */, serviceId, null/* cause */,
//                    null/* sinkId (queryBuffer)*/, 1/* sinkMessagesOut */,
//                    null/* altSinkId */, 0/* altSinkMessagesOut */, stats);
//
//            assertEquals(RunStateEnum.AllDone, runState.haltOp(msg));
//
//        }
//        
//        expected.allDone.set(true);
//        expected.stepCount.incrementAndGet();
//        expected.totalRunningCount.decrementAndGet();
//        expected.totalLastPassRemainingCount.decrementAndGet();
//        expected.runningMap.put(orderId, new AtomicLong(0L));
//        expected.doneOn.put(orderId, Collections.emptySet());
//
//        assertSameState(expected, actual);
//
//    }
//    
//    /**
//     * FIXME Write unit tests for the last pass invocation on a cluster for
//     * sharded or hash partitioned operators. These tests need to verify that
//     * the {@link RunState} expects the correct number of last pass invocations
//     * (on for each shard or service on which the query was started).
//     */
//    public void test_lastPassRequested_cluster_byServiceId() {
//        
//        fail("write tests");
//        
//    }
    
    /*
     * Test helpers
     */
    
    /**
     * Mock {@link PipelineOp}.
     */
    private static class MockPipelineOp extends PipelineOp {

        private static final long serialVersionUID = 1L;

        public MockPipelineOp(BOp[] args, Map<String, Object> annotations) {
            super(args, annotations);
        }

        public MockPipelineOp(MockPipelineOp op) {
            super(op);
        }

        public MockPipelineOp(final BOp[] args, NV... annotations) {
            this(args, NV.asMap(annotations));
        }

        @Override
        public FutureTask<Void> eval(BOpContext<IBindingSet> context) {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * Mock object.
     */
    private static class MockQueryController implements IQueryClient {

        private final UUID serviceId;
        
        MockQueryController(final UUID serviceId) {
            this.serviceId = serviceId;
        }
        
        public void haltOp(IHaltOpMessage msg) throws RemoteException {
        }

        public void startOp(IStartOpMessage msg) throws RemoteException {
        }

        public void bufferReady(IChunkMessage<IBindingSet> msg)
                throws RemoteException {
        }

        public void declareQuery(IQueryDecl queryDecl) {
        }

        public UUID getServiceUUID() throws RemoteException {
            return serviceId;
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
    private void assertEquals(//
            final Map<Integer, AtomicLong> expected,//
            final Map<Integer, AtomicLong> actual//
            ) {
        
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
        
        assertEquals(msg, expected.size(), actual.size());

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
        
    /**
     * Compare two maps whose keys are {@link Integer}s.
     * 
     * @param expected
     * @param actual
     */
    private <T> void assertSameMap(//
            final Map<Integer, T> expected,//
            final Map<Integer, T> actual//
            ) {
        
        assertEquals("", expected, actual);
        
    }

    /**
     * Compare two maps whose keys are {@link Integer}s.
     * 
     * @param expected
     * @param actual
     */
    private <T> void assertSameMap(String msg,
            final Map<Integer, T> expected,
            final Map<Integer, T> actual) {

        if (msg == null) {
            msg = "";
        } else if (msg.length() > 0) {
            msg = msg + " : ";
        }
        
        assertEquals(expected.size(), actual.size());

        final Iterator<Map.Entry<Integer, T>> eitr = expected.entrySet().iterator();

        while (eitr.hasNext()) {

            final Map.Entry<Integer, T> entry = eitr.next();

            final Integer k = entry.getKey();

            final T e = expected.get(k);

            final T a = actual.get(k);

            if (e == a) {
                // Same reference, including when e is null.
                continue;
            }

            if (a == null)
                fail(msg + "Not expecting null: key=" + k);

            if (!e.equals(a)) {

                fail(msg + "Wrong value: key=" + k + ", expected=" + e
                        + ", actual=" + a);

            }

        }

    }
        
}
