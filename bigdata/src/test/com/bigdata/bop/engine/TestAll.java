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
package com.bigdata.bop.engine;

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
	 * 
	 * Most of the aggregation operators either can not "run" or can not emit
	 * their outputs until all of the solutions have been materialized. For
	 * example:
	 * <ul>
	 * <li>ORDER BY : This operator must buffer the solutions until there no
	 * more operators can feed it solutions, at which point it must apply the
	 * sort order to all of the buffered solutions.</li>
	 * <li>GROUP BY : When targeting a hash table, GROUP BY can pipeline
	 * aggregations, but it must do so into a hash table instance which is
	 * shared by each GROUP BY invocation for that query. <br/>
	 * GROUP BY can also run on the result of an ORDER BY, in which case it is
	 * again pipelined, but it can not run until the ORDER BY begins to emit its
	 * solutions.</li>
	 * <li>DISTINCT : When implemented using a hash table, this operator can be
	 * pipelined, but all invocations of the operator must share the same hash
	 * table state.<br/>
	 * When implemented using an ORDER BY, this operator will run once and must
	 * wait for the ORDER BY to begin emitting its solutions before it can
	 * execute.</li>
	 * </ul>
	 * 
	 * @todo It should be possible to test the ability of the operators to
	 *       accept multiple chunks of solutions before applying their semantics
	 *       given an appropriate MockRunningQuery, but we can't really test for
	 *       correct management of concurrency without a live integration with
	 *       the QueryEngine.
	 * 
	 * @todo tests need to be developed of the distributed evaluation of these
	 *       operators.
	 */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("query engine");

        // test suite for the bop statistics class.
        suite.addTestSuite(TestBOpStats.class);
        
        // test suite for some pipeline evaluation utility methods.
        suite.addTestSuite(TestPipelineUtility.class);

        // test suite for the RunState class, which decides query termination.
        suite.addTestSuite(TestRunState.class);
        
        // test suite for query evaluation (basic JOINs).
        suite.addTestSuite(TestQueryEngine.class);

		/*
		 * The following integration tests examine the behavior of various
		 * operators which must either buffer the solutions or otherwise use
		 * shared state during their evaluation. The correctness of those
		 * operator implementations can not be judged without presenting
		 * multiple chunks of solutions.
		 */
        
        // stress test for SLICE
        suite.addTestSuite(TestQueryEngine_Slice.class);

        // stress test for ORDER_BY
        suite.addTestSuite(TestQueryEngine_SortOp.class);

        // stress test for DISTINCT.
        suite.addTestSuite(TestQueryEngine_DistinctOp.class);

        // stress test for GROUP_BY.
    	suite.addTestSuite(TestQueryEngine_GroupByOp.class);

        return suite;
        
    }
    
}
