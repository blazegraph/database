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
    public TestAll(final String arg0) {
     
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
	 */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("query engine");

        // test suite for the bop statistics class.
        suite.addTestSuite(TestBOpStats.class);

        // test suite for startOp messages.
        suite.addTestSuite(TestStartOpMessage.class);
        
        // test suite for haltOp messages.
        suite.addTestSuite(TestHaltOpMessage.class);
        
        // test suite for local (same JVM) chunk messages.
        suite.addTestSuite(TestLocalChunkMessage.class);
        
        // test suite for local (same JVM) chunk messages stored on the native heap.
        suite.addTestSuite(TestLocalNativeChunkMessage.class);
        
        // test suite for the RunState class.
        suite.addTestSuite(TestRunState.class);

        // test suite for query deadline ordering semantics.
        suite.addTestSuite(TestQueryDeadlineOrder.class);

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
