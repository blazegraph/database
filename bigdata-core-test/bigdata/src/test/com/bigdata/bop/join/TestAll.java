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
package com.bigdata.bop.join;

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

        final TestSuite suite = new TestSuite("join operators");

        // Test suite for pipeline join.
        suite.addTestSuite(TestPipelineJoin.class);

        // Test suite for the guts of the JVM hash join logic.
        suite.addTestSuite(TestJVMHashJoinUtility.class);

        // Test suite for the guts of the HTree hash join logic.
        suite.addTestSuite(TestHTreeHashJoinUtility.class);
        
        // Test suite for a hash join with an access path.
        suite.addTestSuite(TestJVMHashJoinOp.class); // JVM
        suite.addTestSuite(TestHTreeHashJoinOp.class); // HTree
        
        // Test suite for building a hash index from solutions and joining that
        // hash index back into the pipeline.
        suite.addTestSuite(TestJVMHashIndexOp.class);
        suite.addTestSuite(TestHTreeHashIndexOp.class);
        suite.addTestSuite(TestHTreeSolutionSetHashJoin.class);

        /*
         * Test suite for a nested loop join using an index scan for each source
         * solution read from the pipeline.
         */
		suite.addTestSuite(TestNestedLoopJoinOp.class);

		suite.addTestSuite(TestFastRangeCountOp.class);
		
		/*
		 * TODO These tests must be specific to the IV layer. They can not be
		 * written for a relation whose elements are (String,String) tuples.
		 * However, we now have test coverage for this at the AST / SPARQL QUERY
		 * execution layer.
		 */
//		suite.addTestSuite(TestDistinctTermScanOp.class);

        return suite;
        
    }
    
}
