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

        // stress test for SliceOp.
        suite.addTestSuite(TestQueryEngine_Slice.class);

        // test suite for optional join groups.
		suite.addTestSuite(TestQueryEngineOptionalJoins.class);
		
        // @todo test suite for query evaluation (DISTINCT, ORDER BY, GROUP BY).
//        suite.addTestSuite(TestQueryEngine2.class);

        return suite;
        
    }
    
}
