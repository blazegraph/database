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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.join.AbstractHashJoinUtilityTestCase;
import com.bigdata.bop.join.AbstractMergeJoin;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase.TestHelper;

/**
 * Data driven test suite.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMergeJoin extends AbstractDataDrivenSPARQLTestCase {

    /**
     *
     */
    public TestMergeJoin() {
    }

    /**
     * @param name
     */
    public TestMergeJoin(String name) {
        super(name);
    }

    /**
     * Test of a required merge-join against a query which has the right "shape"
     * to be recognized as a merge join without a dependency on structural
     * rewrites of the query.
     * <p>
     * Note: This test is against the same data that we use to test the low
     * level merge join code. If there is a problem with the merge join
     * operator, then debug it against its test suite rather than this test.
     * <p>
     * Note: You can verify that the test has the correct expected behavior by
     * running the same test with the merge join optimization explicitly
     * disabled.
     * <p>
     * The original query is:
     * 
     * <pre>
     * PREFIX : <http://www.bigdata.com/>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT ?a ?x ?y
     * WITH {
     *   SELECT ?a ?x {?a :x ?x}
     * } as %set1
     * WITH {
     *   SELECT ?a ?y {?a :x ?y}
     * } as %set2
     * WHERE {
     *    INCLUDE %set1
     *    INCLUDE %set2
     * }
     * </pre>
     * 
     * <pre>
     * (?a,     ?x)   (?a,     ?y)
     * (john, mary)   (john, brad)   // many-to-many join
     * (john, leon)   (john, fred) 
     *                (john, leon) 
     * (mary, john)   (mary, brad)   // 1-1 join.
     * (fred, brad)                  // does not join (would join if OPTIONAL).   
     *                (brad, fred)   // does not join.
     * (leon, john)   (leon, brad)   // many-1 join.
     * (leon, mary)                  // 
     * (paul, leon)   (paul, leon)   // 1-many join.
     *                (paul, brad)
     * </pre>
     * 
     * @see AbstractHashJoinUtilityTestCase#test_mergeJoin01()
     */
    public void test_merge_join_01() throws Exception {

        final ASTContainer astContainer = new TestHelper("merge-join-01")
                .runTest();

        // Verify that a MERGE JOIN operator was used.
        assertTrue(
                "No merge join?",
                BOpUtility.visitAll(astContainer.getQueryPlan(),
                        AbstractMergeJoin.class).hasNext());

    }

    /**
     * Variant of the test above in which we disable the merge join with a query
     * hint and verify that the merge join operator was NOT used in the query
     * plan.
     */
    public void test_merge_join_01a() throws Exception {

        final ASTContainer astContainer = new TestHelper(//
                "merge-join-01a", // testURI,
                "merge-join-01a.rq",// queryFileURL
                "merge-join-01.trig",// dataFileURL
                "merge-join-01.srx"// resultFileURL
        ).runTest();

        // Verify that a MERGE JOIN operator was used.
        assertFalse(
                "Merge join should have been suppressed",
                BOpUtility.visitAll(astContainer.getQueryPlan(),
                        AbstractMergeJoin.class).hasNext());

    }

    /**
     * Test of an optional merge-join.
     * <p>
     * Note: This test is against the same data that we use to test the low
     * level merge join code.
     * 
     * <pre>
     * prefix : <http://www.bigdata.com/>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT ?a ?x ?y
     * WITH {
     *   SELECT ?a ?x {?a :x ?x}
     * } as %set1
     * WITH {
     *   SELECT ?a ?y {?a :y ?y}
     * } as %set2
     * WHERE {
     *    INCLUDE %set1.
     *    OPTIONAL {INCLUDE %set2}.
     * }
     * </pre>
     * 
     * @see AbstractHashJoinUtilityTestCase#test_mergeJoin01()
     */
    public void test_merge_join_02() throws Exception {

        final ASTContainer astContainer = new TestHelper("merge-join-02")
                .runTest();

        // Verify that a MERGE JOIN operator was used.
        assertTrue(
                "No merge join?",
                BOpUtility.visitAll(astContainer.getQueryPlan(),
                        AbstractMergeJoin.class).hasNext());

    }

}
