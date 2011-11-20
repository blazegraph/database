/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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

        // Port of unit tests original written at the AST layer.
        suite.addTestSuite(TestAST.class);

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

        // Test suite for OPTIONAL groups.
        suite.addTestSuite(TestOptionals.class);

        // Test suite for UNIONs.
        suite.addTestSuite(TestUnions.class);

        // Test suite for different combinations of joins.
        suite.addTestSuite(TestComboJoins.class);

        // Different kinds of subqueries.
        suite.addTestSuite(TestSubQuery.class);

        // Test suite for a merge join pattern
        suite.addTestSuite(TestMergeJoin.class);

        // Test suite for aggregation queries.
        suite.addTestSuite(TestAggregationQuery.class);

        // Test suite for FILTER evaluation.
        suite.addTestSuite(TestFilters.class);
        
        // Full text search
        suite.addTestSuite(TestSearch.class);

        // Complex queries.
        suite.addTestSuite(TestComplexQuery.class);
        
        /*
         * Some persnickety DAWK test cases, mainly things dealing with bottom
         * up evaluation semantics.
         */
        suite.addTestSuite(TestTCK.class);

        // Test suite for embedded bigdata query hints.
        suite.addTestSuite(TestQueryHints.class);

        // Test suite with explicitly enabled hash joins.
        suite.addTestSuite(TestHashJoin.class);

        /*
         * Tests corresponding to various trouble tickets.
         */
        suite.addTestSuite(TestTickets.class);
        
        return suite;
        
    }
    
}
