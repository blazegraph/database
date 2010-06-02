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
/*
 * Created on Oct 14, 2006
 */

package com.bigdata.quorum;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites in increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME Test with HA Journal integration (once this test suite has
 *          been built out).
 * 
 *          FIXME Test with Zookeeper integration (in the bigdata-jini module).
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

        final TestSuite suite = new TestSuite("quorum");

        // Simple unit tests of a singleton quorum.
        suite.addTestSuite(TestMockQuorum.class);

        /*
         * Test the fixture used to test the quorums (the fixture builds on the
         * same base class).
         */
        suite.addTestSuite(TestMockQuorumFixture.class);

        /*
         * Test the quorum semantics for a singleton quorum. This unit test
         * allows us to verify that each quorum state change is translated into
         * the appropriate methods against the public API of the quorum client
         * or quorum member.
         */
        suite.addTestSuite(TestSingletonQuorumSemantics.class);

        /*
         * Test the quorum semantics for a highly available quorum of 3
         * nodes. The main points to test here are the particulars of events not
         * observable with a singleton quorum, including a service join which
         * does not trigger a quorum meet, a service leave which does not
         * trigger a quorum break, a leader leave, etc.
         */
        suite.addTestSuite(TestHA3QuorumSemantics.class);
        
        return suite;
        
    }
    
}
