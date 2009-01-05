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
package com.bigdata.service.jini;

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

        // Optional boolean property may be used to force skip of these tests.
        final boolean forceSkip;
        {
            String val = System
                    .getProperty("maven.test.services.skip", "false");

            if (val != null) {
                
                forceSkip = Boolean.parseBoolean(val);
                
            } else {
                
                forceSkip = false;
                
            }
            
        }
        
        final boolean willRun = !forceSkip && JiniServicesHelper.isJiniRunning();

        final TestSuite suite = new TestSuite("jini-based services");

        if(willRun) {

            /*
             * Note: The service tests require that Jini is running, that you
             * have specified a suitable security policy, and that the codebase
             * parameter is set correctly. See the test suites for more detail
             * on how to setup to run these tests.
             */

            //        suite.addTestSuite( TestServer.class ); // Does not implement TestCase.

            //@todo test delete of zookeeper node when service is destroyed (but not if shutdown?)
//            /*
//             * Basic integration tests for zookeeper.
//             */
//            suite.addTestSuite( TestZookeeperServerEntry.class );
//            suite.addTestSuite( TestZookeeper.class );
            
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
             * @todo test data replication at the service level.
             * 
             * work through the setup of the test cases. each of the remote stores
             * needs to be a service, most likely an extension of the data service
             * where read committed operations are allowed but writes are refused
             * except via the data transfers used to support replication. the
             * "local" store is, of course, just another data service.
             * 
             * work through startup when the remote store services need to be
             * discovered, failover when a remote service dies, and restart. on
             * restart, should we wait until the historical replicas can be
             * re-discovered or deemed "lost"? it may not matter since we can
             * re-synchronize a "lost" replica once it comes back online simply be
             * transferring the missing delta from the tail of the local store.
             * 
             * @todo test service failover and restart-safety.
             */
            
            /*
             * The map/reduce test suite.
             */
            suite.addTest(com.bigdata.service.mapReduce.TestAll.suite());

        }
        
        return suite;
        
    }
    
}
