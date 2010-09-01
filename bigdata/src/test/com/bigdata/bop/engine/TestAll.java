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

        // test suite for a non-Callable/Runnable Future.
        suite.addTestSuite(TestHaltable.class);

        // test suite for some pipeline evaluation utility methods.
        suite.addTestSuite(TestPipelineUtility.class);

        /*
         * test suites for receiving buffers and files from a remote service in
         * support of distributed query evaluation.
         * 
         * @todo The local copy of BufferService and its test suites needs to be
         * reconciled back into the trunk and also into the HA branch, from
         * which this version was derived.
         */
        suite.addTestSuite(TestReceiveBuffer.class);
        suite.addTestSuite(TestReceiveFile.class);
        
        // test suite for query evaluation (basic JOINs).
        suite.addTestSuite(TestQueryEngine.class);

        // test suite for query evaluation (DISTINCT, ORDER BY, GROUP BY).
        suite.addTestSuite(TestQueryEngine2.class);

        return suite;
        
    }
    
}
