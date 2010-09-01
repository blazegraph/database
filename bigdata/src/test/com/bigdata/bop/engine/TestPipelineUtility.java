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
 * Created on Sep 1, 2010
 */

package com.bigdata.bop.engine;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineStartOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.join.PipelineJoin;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestPipelineUtility extends TestCase2 {

    /**
     * 
     */
    public TestPipelineUtility() {
    }

    /**
     * @param name
     */
    public TestPipelineUtility(String name) {
        super(name);
    }

    /**
     * Unit test for
     * {@link PipelineUtility#isDone(int, com.bigdata.bop.BOp, java.util.Map, java.util.Map)}
     * .
     */
    public void test_isDone() {

        final int startId = 1;
        final int joinId1 = 2;
        final int predId1 = 3;
        final int joinId2 = 4;
        final int predId2 = 5;
        
        final String namespace = "ns";
        
        final BindingSetPipelineOp startOp = new PipelineStartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        }));
        
        final Predicate<?> pred1Op = new Predicate<E>(new IVariableOrConstant[] {
                Var.var("x"), Var.var("y") }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.PARTITION_ID,
                                Integer.valueOf(-1)),//
                        new NV(Predicate.Annotations.OPTIONAL,
                                Boolean.FALSE),//
                        new NV(Predicate.Annotations.CONSTRAINT, null),//
                        new NV(Predicate.Annotations.EXPANDER, null),//
                        new NV(Predicate.Annotations.BOP_ID, predId1),//
                }));
        
        final Predicate<?> pred2Op = new Predicate<E>(new IVariableOrConstant[] {
                Var.var("y"), Var.var("z") }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.PARTITION_ID,
                                Integer.valueOf(-1)),//
                        new NV(Predicate.Annotations.OPTIONAL,
                                Boolean.FALSE),//
                        new NV(Predicate.Annotations.CONSTRAINT, null),//
                        new NV(Predicate.Annotations.EXPANDER, null),//
                        new NV(Predicate.Annotations.BOP_ID, predId2),//
                }));
        
        final BindingSetPipelineOp join1Op = new PipelineJoin(startOp, pred1Op,
                NV.asMap(new NV[] { new NV(Predicate.Annotations.BOP_ID,
                        joinId1),//
                        }));

        final BindingSetPipelineOp join2Op = new PipelineJoin(join1Op, pred2Op,
                NV.asMap(new NV[] { new NV(Predicate.Annotations.BOP_ID,
                        joinId2),//
                        }));
        
        final BindingSetPipelineOp queryPlan = join2Op;

        final Map<Integer,BOp> queryIndex = BOpUtility.getIndex(queryPlan);
        final Map<Integer,AtomicLong> runningCountMap = new LinkedHashMap<Integer, AtomicLong>();
        final Map<Integer,AtomicLong> availableChunkCountMap = new LinkedHashMap<Integer, AtomicLong>();

        /*
         * If the query is inactive (nothing running, no chunks available) then
         * it is trivially true for any operator in the query plan that it can
         * not be triggered and will not be executed.
         */
        {
            
            assertTrue(PipelineUtility.isDone(startId, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertTrue(PipelineUtility.isDone(joinId1, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertTrue(PipelineUtility.isDone(joinId2, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));

        }

        /*
         * Modify the activity state such that one chunk is available for the
         * start operator and verify that the start operator and both join
         * operators can be triggered.
         */
        {
         
            availableChunkCountMap.put(startId, new AtomicLong(1L));
            
            assertFalse(PipelineUtility.isDone(startId, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertFalse(PipelineUtility.isDone(joinId1, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertFalse(PipelineUtility.isDone(joinId2, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));

        }

        /*
         * Modify the activity state such that one chunk is available for join1
         * and verify that the start operator is done but that both joins can be
         * triggered.
         */
        {
            
            assertNotNull(availableChunkCountMap.remove(startId));
            availableChunkCountMap.put(joinId1, new AtomicLong(1L));

            assertTrue(PipelineUtility.isDone(startId, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertFalse(PipelineUtility.isDone(joinId1, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertFalse(PipelineUtility.isDone(joinId2, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
        }

        /*
         * Modify the activity state such that one chunk is available for join2
         * and verify that the start operator and first join are done but that
         * the 2nd join can be triggered.
         */
        {
            
            assertNotNull(availableChunkCountMap.remove(joinId1));
            availableChunkCountMap.put(joinId2, new AtomicLong(1L));

            assertTrue(PipelineUtility.isDone(startId, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertTrue(PipelineUtility.isDone(joinId1, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertFalse(PipelineUtility.isDone(joinId2, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
        }

        /*
         * Modify the activity state such no chunks are available but the start
         * operator is running and verify that the join operators both can be
         * triggered.
         * 
         * @todo we might report isDone:=true for the probe operator in this
         * case since it can not be retriggered even though it is currently
         * running.
         */
        {
            
            assertNotNull(availableChunkCountMap.remove(joinId2));
            runningCountMap.put(startId, new AtomicLong(1L));

            assertFalse(PipelineUtility.isDone(startId, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertFalse(PipelineUtility.isDone(joinId1, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertFalse(PipelineUtility.isDone(joinId2, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
        }

        /*
         * Modify the activity state such no chunks are available but the 1st
         * join operator is running and verify that the 2nd join operators can
         * be triggered.
         */
        {
            
            assertNotNull(runningCountMap.remove(startId));
            runningCountMap.put(joinId1, new AtomicLong(1L));

            assertTrue(PipelineUtility.isDone(startId, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertFalse(PipelineUtility.isDone(joinId1, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertFalse(PipelineUtility.isDone(joinId2, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
        }

        /*
         * Modify the activity state such no chunks are available but the 2nd
         * join operator is running and verify that the 2nd join operator can be
         * triggered.
         */
        {
            
            assertNotNull(runningCountMap.remove(joinId1));
            runningCountMap.put(joinId2, new AtomicLong(1L));

            assertTrue(PipelineUtility.isDone(startId, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertTrue(PipelineUtility.isDone(joinId1, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
            assertFalse(PipelineUtility.isDone(joinId2, queryPlan, queryIndex,
                    runningCountMap, availableChunkCountMap));
            
        }
        
    }

}
