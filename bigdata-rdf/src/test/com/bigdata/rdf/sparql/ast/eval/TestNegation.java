/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Nov 22, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.optimizers.ASTBottomUpOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTExistsOptimizer;

/**
 * Test suite for SPARQL negation (EXISTS and MINUS).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNegation extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestNegation() {
    }

    /**
     * @param name
     */
    public TestNegation(String name) {
        super(name);
    }

    /**
     * Unit test for an query with an EXISTS filter. The EXISTS filter is
     * modeled as an ASK sub-query which projects an anonymous variable and a
     * simple test of the truth state of that anonymous variable.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT DISTINCT ?x
     * WHERE {
     *   ?x ?p ?o .
     *   FILTER ( EXISTS {?x rdf:type foaf:Person} ) 
     * }
     * </pre>
     */
    public void test_exists_1() throws Exception {

        new TestHelper(
                "exists-1", // testURI,
                "exists-1.rq",// queryFileURL
                "exists-1.trig",// dataFileURL
                "exists-1.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * Sesame Unit <code>sparql11-exists-05</code>.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * WHERE {
     *     ?a :p ?n
     *     FILTER NOT EXISTS {
     *         ?a :q ?n .
     *     }
     * }
     * </pre>
     */
    public void test_sparql11_exists_05() throws Exception {

        new TestHelper(
                "sparql11-exists-05", // testURI,
                "sparql11-exists-05.rq",// queryFileURL
                "sparql11-exists-05.ttl",// dataFileURL
                "sparql11-exists-05.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }
    
    /**
     * Sesame Unit <code>sparql11-exists-06</code>, which appears to be the same
     * as an example in the LCWD.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * WHERE {
     *     ?a :p ?n
     *     FILTER NOT EXISTS {
     *         ?a :q ?m .
     *         FILTER(?n = ?m)
     *     }
     * }
     * </pre>
     * 
     * <pre>
     * @prefix : <http://example/> .
     * 
     * :a :p 1 ; :q 1, 2 .
     * :b :p 3.0 ; :q 4.0, 5.0 .
     * </pre>
     * 
     * <pre>
     * { a = b, n = 3 }
     * </pre>
     * 
     * Note: There are several problems which are related to this test failure
     * <p>
     * First, the {@link ASTBottomUpOptimizer} is incorrectly deciding that
     * <code>?n</code> is not in scope within the inner FILTER. This causes the
     * variable to be renamed in that context in order to model bottom up
     * evaluation semantics which is why the inner FILTER always fails.
     * <p>
     * Second, the ASK subquery is not projecting in all variables which are in
     * scope. This is simply how that subquery was put together by the
     * {@link ASTExistsOptimizer}.
     * <p>
     * Finally, neither EXISTS not NOT EXISTS may cause any new bindings to be
     * made. Therefore, we need to filter out all bindings (including on the
     * anonymous variable) which are made in order to help us answer a (NOT)
     * EXISTS FILTER.
     * <p>
     * This issue was resolved by a change to {@link ASTBottomUpOptimizer} to
     * test when a visited {@link JoinGroupNode} was actually a graph pattern
     * inside of a {@link FilterNode}. In this case, the group is simply skipped
     * over as it will be properly handled when we visit the
     * {@link JoinGroupNode} to which the {@link FilterNode} is attached.
     */
    public void test_sparql11_exists_06() throws Exception {

        new TestHelper(
                "sparql11-exists-06", // testURI,
                "sparql11-exists-06.rq",// queryFileURL
                "sparql11-exists-06.ttl",// dataFileURL
                "sparql11-exists-06.srx" // resultFileURL,
                ).runTest();

    }

    /**
     * A Sesame test.
     */
    public void test_sparql11_minus_01() throws Exception {

        new TestHelper(
                "sparql11-minus-01", // testURI,
                "sparql11-minus-01.rq",// queryFileURL
                "sparql11-minus-01.ttl",// dataFileURL
                "sparql11-minus-01.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test based on the SPARQL 1.1 LCWD.
     * 
     * <pre>
     * SELECT *
     * WHERE { ?s ?p ?o 
     *         MINUS { ?x ?y ?z } 
     * }
     * </pre>
     * 
     * There is only one solution to the first statement pattern. Since the
     * MINUS group binds different variables, no solutions are removed and the
     * sole solution to the <code>?s ?p ?o</code> statement pattern should be
     * reported.
     * <p>
     * Note: Since there are no shared variables, we have to lift out the MINUS
     * group into a named subquery in order to have bottom up evaluation
     * semantics.
     */
    public void test_sparql11_minus_02() throws Exception {

        new TestHelper(
                "sparql11-minus-02", // testURI,
                "sparql11-minus-02.rq",// queryFileURL
                "sparql11-minus-02.ttl",// dataFileURL
                "sparql11-minus-02.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * 
     * WHERE {
     *     ?a :p ?n
     *     MINUS {
     *         ?a :q ?n .
     *     }
     * }     *
     * </pre>
     */
    public void test_sparql11_minus_05() throws Exception {

        new TestHelper(
                "sparql11-minus-05", // testURI,
                "sparql11-minus-05.rq",// queryFileURL
                "sparql11-minus-05.ttl",// dataFileURL
                "sparql11-minus-05.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * 
     * WHERE {
     *     ?a :p ?n
     *     MINUS {
     *         ?a :q ?m .
     *         FILTER(?n = ?m)
     *     }
     * }
     * </pre>
     * 
     * The variable <code>?n</code> is not bound inside of the FILTER (unless it
     * is an exogenous variable) because the right hand side of MINUS does not
     * have visibility into the variables on the left hand side of MINUS.
     */
    public void test_sparql11_minus_06() throws Exception {

        new TestHelper(
                "sparql11-minus-06", // testURI,
                "sparql11-minus-06.rq",// queryFileURL
                "sparql11-minus-06.ttl",// dataFileURL
                "sparql11-minus-06.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * 
     * WHERE {
     *     ?a :p ?n
     *     MINUS {
     *         ?a :q ?m .
     *         OPTIONAL {?a :r ?n} 
     *         FILTER(?n = ?m)
     *    } 
     * }
     * </pre>
     * 
     * <pre>
     * @prefix : <http://example/> .
     * 
     * :a :p 1 ; :q 1, 2 .
     * :b :p 3.0 ; :q 4.0, 5.0 .
     * </pre>
     * 
     * <pre>
     * {?a=:a, ?n=1}
     * {?a=:b, ?n=3.0}
     * </pre>
     * 
     * In this case the FILTER in the MINUS group COULD have a binding for
     * <code>?n</code> from the OPTIONAL group.
     * 
     * <p>
     * 
     * Note: This is actually a badly formed left-join pattern. The MINUS group
     * members get lifted into a named subquery which is then INCLUDEd into the
     * MINUS group.
     */
    public void test_sparql11_minus_07() throws Exception {

        new TestHelper(
                "sparql11-minus-07", // testURI,
                "sparql11-minus-07.rq",// queryFileURL
                "sparql11-minus-07.ttl",// dataFileURL
                "sparql11-minus-07.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

}
