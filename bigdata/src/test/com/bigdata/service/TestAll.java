/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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

        TestSuite suite = new TestSuite(TestAll.class.getName());

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
