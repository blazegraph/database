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

package com.bigdata.journal.jini.ha;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.journal.Journal;

/**
 * Test suite for highly available configurations of the standalone
 * {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestAll_LBS extends TestCase {

    /**
     * 
     */
    public TestAll_LBS() {
    }

    /**
     * @param arg0
     */
    public TestAll_LBS(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("HALoadBalancer");

        // proxy to leader for updates. local forward for reads.
        suite.addTestSuite(TestHA3LoadBalancer_NOP.class);
        
        // round-robin.
        suite.addTestSuite(TestHA3LoadBalancer_RoundRobin.class);

        // ganglia.
        suite.addTestSuite(TestHA3LoadBalancer_GangliaLBS.class);

        // HTTPD based reporting of platform OS performance metrics.
        suite.addTestSuite(TestHA3LoadBalancer_CountersLBS.class);

        return suite;

    }

}
