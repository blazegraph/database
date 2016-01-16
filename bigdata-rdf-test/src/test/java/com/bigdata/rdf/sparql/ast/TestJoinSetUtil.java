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
 * Created on Oct 21, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Collections;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.sparql.ast.JoinSetUtil.VertexJoinSet;

/**
 * Test suite for {@link JoinSetUtil}.
 * 
 * TODO Expand the test suite. This will be easier to do if we refactor the
 * state of the {@link JoinSetUtil} into an interface so we can have a mock
 * object with the expected state.
 * 
 * TODO Test when knownBound is not empty/null. (Note that there is an inherent
 * recursion since we need to figure out the join order within the parent so we
 * can figure out what is known bound on entry to a child.)
 * 
 * TODO Test with filters which can not run until after the optionals.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestJoinSetUtil extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestJoinSetUtil() {
    }

    /**
     * @param name
     */
    public TestJoinSetUtil(String name) {
        super(name);
    }

    public void test_joinSetUtil_BSBMQ5() {
        
        final BSBMQ5Setup s = new BSBMQ5Setup(store);

        final StaticAnalysis sa = new StaticAnalysis(s.queryRoot);
        
        final JoinSetUtil fixture = new JoinSetUtil(sa, null/* knownBound */,
                s.queryRoot.getWhereClause());

        // The group.
        assertEquals("group", s.queryRoot.getWhereClause(), fixture.group);

        // The known bound variables on entry.
        assertEquals("knownBound", Collections.emptySet(), fixture.knownBound);
        
        // Verify it reports the required joins in the found order.
        assertEquals(
                new IJoinNode[] { s.p0, s.p1, s.p2, s.p3, s.p4, s.p5, s.p6 },
                fixture.requiredJoins);

        assertEquals("requiredJoinCount", 7, fixture.requiredJoinCount);

        // Verify the partitioning on the filters by when they will run.
        {

            assertEquals("preFilters", asSet(new FilterNode[] {}),
                    fixture.preFilters);

            assertEquals("joinFilters", asSet(new FilterNode[] { s.c0, s.c1,
                    s.c2 }), fixture.joinFilters);

            assertEquals("postFilters", asSet(new FilterNode[] {}),
                    fixture.postFilters);
            
        }
        
        // Verify the variables for each vertex.
        {
            assertEquals(asSet(new IVariable[] { Var.var("product"),
                    Var.var("productLabel") }), fixture.vars[0]);

            assertEquals(asSet(new IVariable[] { Var.var("prodFeature") }),
                    fixture.vars[1]);

            assertEquals(asSet(new IVariable[] { Var.var("product"),
                    Var.var("prodFeature") }), fixture.vars[2]);

            assertEquals(asSet(new IVariable[] { Var.var("origProperty1") }),
                    fixture.vars[3]);

            assertEquals(asSet(new IVariable[] { Var.var("product"),
                    Var.var("simProperty1") }), fixture.vars[4]);

            assertEquals(asSet(new IVariable[] { Var.var("origProperty2") }),
                    fixture.vars[5]);

            assertEquals(asSet(new IVariable[] { Var.var("product"),
                    Var.var("simProperty2") }), fixture.vars[6]);
        }
        
        // Verify the canJoin matrix (upper diagonal).
        final int canJoin[][] = new int[][] {
                new int[]{0,0,1,0,1,0,1},//
                new int[]{0,0,1,0,0,0,0},//
                new int[]{0,0,0,0,1,0,1},//
                new int[]{0,0,0,0,0,0,0},//
                new int[]{0,0,0,0,0,0,1},//
                new int[]{0,0,0,0,0,0,0},//
                new int[]{0,0,0,0,0,0,0},//
        };
        for (int i = 0; i < fixture.requiredJoinCount; i++) {
            assertEquals("row=" + i, canJoin[i], fixture.canJoin[i]);
        }

        /*
         * Verify the sum of that matrix (when reflected). This tells us how
         * many direct one-step joins there are for each vertex.
         */
        assertEquals(new int[] { 3, 1, 4, 0, 3, 0, 3 }, fixture.directJoinCount);

        /*
         * Verify the disjoint join sets (sets of vertices which are
         * transitively connected by joins on a directly shared variable).
         */
        {

            assertTrue(fixture.directJoinSets.contains(new VertexJoinSet(//
                    asSet(new Integer[] { 0, 2, 1, 4, 6 }),//
                    asSet(new IVariable[] {//
                            Var.var("product"),//
                            Var.var("productLabel"),//
                            Var.var("prodFeature"),//
                            Var.var("simProperty1"),//
                            Var.var("simProperty2")//
                    }))));

            assertTrue(fixture.directJoinSets.contains(new VertexJoinSet(//
                    asSet(new Integer[] { 3 }),//
                    asSet(new IVariable[] {//
                    Var.var("origProperty1") //
                    }))));

            assertTrue(fixture.directJoinSets.contains(new VertexJoinSet(//
                    asSet(new Integer[] { 5 }),//
                    asSet(new IVariable[] {//
                    Var.var("origProperty2") //
                    }))));

            assertEquals("#of join sets", 3, fixture.directJoinSets.size());

        }

    }
    
}
