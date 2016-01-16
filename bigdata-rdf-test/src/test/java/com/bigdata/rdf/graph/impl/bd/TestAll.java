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
package com.bigdata.rdf.graph.impl.bd;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestAll.java 6116 2012-03-13 20:39:17Z thompsonbry $
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

        final TestSuite suite = new TestSuite("Bigdata Graph Engine");
        
        /*
         * Some basic correctness tests against the bigdata backend.
         * 
         * FIXME TEST: Extend/override the analytic test suite to also work for
         * the bigdata backend. This is a bit tricky because the bigdata backend
         * opererates in IV and ISPO objects that are not automatically resolved
         * to Value and Statement objects. Maybe we can add a resolution pattern
         * to the test suites (wrapping the GAS outputs) and get better reuse
         * out of them?
         */
        suite.addTestSuite(TestGather.class);
        suite.addTestSuite(TestBFS.class);
        suite.addTestSuite(TestSSSP.class);
        
        return suite;
        
    }
    
}
