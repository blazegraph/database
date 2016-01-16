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

/**
 * Data driven test suite for complex queries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestComplexQuery extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestComplexQuery() {
    }

    /**
     * @param name
     */
    public TestComplexQuery(String name) {
        super(name);
    }

    /**
     * A complex query including a named subquery, a subselect, and aggregation.
     */
    public void test_aggregate_divide() throws Exception {

        new TestHelper("aggregate-divide").runTest();
    
    }

//    /**
//     * TODO In order to verify the behavior of GROUP_CONCAT, the test harness
//     * needs to verify that the actual solutions are some permutation of the
//     * possible solutions. The <code>?phrase</code> binding can be computed from
//     * the solutions in any order since there is no ORDER BY clause in the
//     * query, and even an ORDER BY will only order the groups but might not
//     * order the GROUP_CONCAT operation (unless we group first and then
//     * aggregate).
//     * 
//     * <pre>
//     * PREFIX : <http://example.org/>
//     * SELECT ?s (GROUP_CONCAT(?o ; separator=";") AS ?phrase) 
//     * {
//     *   ?s :p ?o .
//     * }
//     * GROUP BY ?s
//     * </pre>
//     * 
//     * The test (and the test files) should probably be removed.
//     */
//    public void test_group_concat_default() throws Exception {
//
//        new TestHelper("group-concat-default").runTest();
//
//    }
    
}
