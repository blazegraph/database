/*

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
 * Created on Nov 29, 2007
 */

package com.bigdata.service;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Test suite for embedded services.
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
     * FIXME refactor service tests from the jini module use here against
     * embedded services (w/o jini). Ideally the jini module could then add its
     * own unit tests to verify the basic jini integration and then re-run the
     * same test suite to verify the integratation at the bigdata services level -
     * the main point to test there is the (de-)serialization of objects being
     * passed by RMI to and from the services.
     * 
     * @todo write tests for {@link RangeQueryIterator}
     * @todo write tests for {@link PartitionedRangeQueryIterator}
     * 
     * @todo write tests for parallelized operations.
     */
    public static Test suite()
    {

        TestSuite suite = new TestSuite("embedded services");

        /*
         * Stress test of concurrent clients writing on a single data service.
         */
        suite.addTestSuite( StressTestConcurrent.class );

        return suite;
        
    }
    
}
