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
package com.bigdata.bop.fed;


import com.bigdata.service.EmbeddedFederation;
import com.bigdata.service.IBigdataFederation;

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
     * @todo I would like to be able to run these tests by instantiating map and
     *       receive operators in a single JVM without any additional context.
     *       However, there is a strong dependency right now on the
     *       {@link IBigdataFederation}. While this means that we have to write
     *       the operator level unit tests to the {@link EmbeddedFederation}, we
     *       can write additional test suites which deal solely with the NIO
     *       communications used by the map/receive operators.
     */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("scale-out operator evaluation");

        /*
         * Test the NIO operations used to send/receive the binding sets, etc.
         * 
         * @todo This NIO test suite could be run much earlier in the total test
         * suite as it has not dependency on the operators.
         */
        suite.addTestSuite(TestSendReceiveBuffers.class);
        
        // unit tests for mapping binding sets over shards. 
        suite.addTestSuite(TestMapBindingSetsOverShards.class);

        // unit tests for mapping binding sets over nodes. 
        suite.addTestSuite(TestMapBindingSetsOverShards.class);

        return suite;
        
    }
    
}
