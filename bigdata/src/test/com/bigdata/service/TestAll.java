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
 * Created on Jun 26, 2006
 */
package com.bigdata.service;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates tests in dependency order - see {@link AbstractServerTestCase} for
 * <strong>required</strong> system properties in order to run this test suite.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestAll extends TestCase {

    public TestAll() {}
    
    public TestAll(String name) {super(name);}
    
    public static Test suite()
    {

        TestSuite suite = new TestSuite("services");

        //        suite.addTestSuite( TestServer.class ); // Does not implement TestCase.

        /*
         * Test of a single client talking to a single data service instance
         * without the use of the metadata service or a transaction manager.
         */
        suite.addTestSuite( TestDataServer0.class );

        /*
         * Test of a single client talking to a single metadata service
         * instance.
         */
        suite.addTestSuite( TestMetadataServer0.class );

        /*
         * Test of a single client talking to a bigdata federation.
         */
        suite.addTestSuite( TestBigdataClient.class );

        /*
         * Stress test of concurrent clients writing on a single data service.
         */
        suite.addTestSuite( StressTestConcurrent.class );
        
        /*
         * @todo test correctness when services fail at various points in
         * distributed operations, e.g., during the FSA for registering a
         * scale-out index the data service onto which the first index partition
         * will be mapped may fail during registration - or may overflow during
         * registration, all of which lead to interesting places that need to
         * be handled correctly.
         */
        
        /*
         * @todo test service failover and restart-safety.
         */
        
        /*
         * @todo test telemetry (define a telemetry service that will collect
         * data from other services on their operation load and on server load
         * that will be used to inform load-balancing decisions, including which
         * data service to map a new partition onto and when to have a partition
         * moved to a new data service - either choosing a secondary service to
         * become primary and thereby changing hosts (and either making the
         * primary into a secondary or choosing a new secondary to maintain
         * replication) or by selecting a new host and bringing the state onto
         * that host).
         */
        
        return suite;
        
    }
    
}
