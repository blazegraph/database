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
package com.bigdata.rdf.sparql.ast.eval;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.eval.reif.TestReificationDoneRightEval;

/**
 * Aggregates test suites into increasing dependency order.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("AST Evaluation");

        /*
         * Data driven tests.
         */
        
        // Basic query.
        suite.addTestSuite(TestBasicQuery.class);

        // Test suite for CONSTRUCT queries.
        suite.addTestSuite(TestConstruct.class);

        // Test suite for DESCRIBE and the DESCRIBE cache.
        suite.addTestSuite(TestDescribe.class);

        // Port of unit tests original written at the AST layer.
        suite.addTestSuite(TestAST.class);

        suite.addTestSuite(TestUnsigned.class);
        
        /*
         * Test suite for named and default graph access patterns, including
         * those ported from TestNamedGraphs in the sail package.
         */
        suite.addTestSuite(TestNamedGraphs.class);

        /*
         * Test suite for default graph access patterns ported from
         * TestDefaultGraphAccessPatterns.
         */
        suite.addTestSuite(TestDefaultGraphs.class);

        /*
         * Test suite for virtual graphs support.
         */
        suite.addTestSuite(TestVirtualGraphs.class);

        // Test suite for OPTIONAL groups.
        suite.addTestSuite(TestOptionals.class);

        // Test suite for UNIONs.
        suite.addTestSuite(TestUnions.class);

        // Test suite for different combinations of joins.
        suite.addTestSuite(TestComboJoins.class);

        // Test suite for SPARQL subqueries.
        suite.addTestSuite(TestSubQuery.class);
        
        // Test suite for NAMED SUBQUERIES
        suite.addTestSuite(TestNamedSubQuery.class);
        
		// Test suite for INCLUDE of pre-existing named solution sets.
		suite.addTestSuite(TestInclude.class);

        // Test suite for negation (EXISTS, NOT EXISTS, MINUS).
        suite.addTestSuite(TestNegation.class);

        // Test suite for a merge join pattern
        suite.addTestSuite(TestMergeJoin.class);
        
        // Test suite for explain hint annotationss
        suite.addTestSuite(TestExplainHints.class);

        // Test suite for aggregation queries.
        suite.addTestSuite(TestAggregationQuery.class);

        // Test suite for FILTER evaluation.
        suite.addTestSuite(TestFilters.class);

        // Test suite for SPARQL 1.1 BINDINGS clause
        suite.addTestSuite(TestBindings.class);
        suite.addTestSuite(TestBindHeisenbug708.class);
        suite.addTestSuite(TestTicket887.class);

        // Test suite for SPARQL 1.1 BINDINGS clause
        suite.addTestSuite(TestJoinOrder.class);

        // Complex queries.
        suite.addTestSuite(TestComplexQuery.class);
        
        /*
         * Some persnickety DAWK test cases, mainly things dealing with bottom
         * up evaluation semantics.
         */
        suite.addTestSuite(TestTCK.class);

        /* test suite for complex BIND operations creating values that are
         * reused in other parts of the query, targeted at covering problems
         * with dictionary resolving these constructed values correctly (in
         * order to resolve mocked IDs)
         */
        suite.addTestSuite(TestTicket1007.class);

        // additional bottom-up evaluation tests.
        suite.addTestSuite(TestTicket1087.class);

        // test static analysis for quads constructs in triples mode, raising
        // an early exception when accessing named graphs in triples mode
        suite.addTest(TestTicket1105.suite());

		if (QueryHints.DEFAULT_REIFICATION_DONE_RIGHT) {

			/*
			 * Test suite for the SPARQL extension for "reification done right".
			 */
			suite.addTestSuite(TestReificationDoneRightEval.class);

        }
        
        // Test suite for embedded bigdata query hints.
        suite.addTestSuite(TestQueryHints.class);

        // Test suite with explicitly enabled hash joins.
        suite.addTestSuite(TestHashJoin.class);

        // Test suite for pipelined hash join
        suite.addTestSuite(TestPipelinedHashJoin.class);
        
        
        /*
         * Tests corresponding to various trouble tickets.
         */
        suite.addTestSuite(TestTickets.class);
        
        suite.addTestSuite(TestUnionMinus.class);
        
        suite.addTestSuite(TestSubSelectFilterExist725.class);
        suite.addTestSuite(TestTwoPropertyPaths734.class);
        
        // test suite for inline constraints: GT, LT, GTE, LTE
        suite.addTestSuite(TestInlineConstraints.class);

        // test suite for custom functions.
        suite.addTestSuite(TestCustomFunction.class);

        // test suite for a sub-select with an empty PROJECTION.
        suite.addTestSuite(TestTicket946.class);
        
        suite.addTestSuite(TestCompressedTimestampExtensionSPARQL.class);

        // SELECT COUNT(...) (DISTINCT|REDUCED) {single-triple-pattern}
        // @see #1037 (fast-range-count optimizer)
        suite.addTest(TestFastRangeCountOptimizer.suite());

        // SELECT (DISTINCT|REDUCED) ?property WHERE { ?x ?property ?y . }
        // @see #1035 (distinct-term-scan optimizer)
        suite.addTest(TestDistinctTermScanOptimizer.suite());

        // SELECT (COUNT(*) as ?count) ?z WHERE {  ?x rdf:type ?z  } GROUP BY ?z
        // @see #1059 (combination of fast-range-count and distinct-term-scan)
        suite.addTest(TestSimpleGroupByAndCountOptimizer.suite());
        
        /*
         * Runtime Query Optimizer (RTO).
         */
        suite.addTest(com.bigdata.rdf.sparql.ast.eval.rto.TestAll.suite());

        /*
         * SPARQL 1.1 UPDATE
         */
        suite.addTest(com.bigdata.rdf.sparql.ast.eval.update.TestAll.suite());
        
        /*
         * SPARQL 1.1 Federated Query.
         */
        suite.addTest(com.bigdata.rdf.sparql.ast.eval.service.TestAll.suite());

        return suite;
        
    }
    
}
