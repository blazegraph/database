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
 * Created on Sep 26, 2007
 */

package com.bigdata.service.mapReduce;

import com.bigdata.service.AbstractServerTestCase;
import com.bigdata.service.BigdataClient;
import com.bigdata.service.DataServer;
import com.bigdata.service.MetadataServer;
import com.bigdata.service.mapReduce.Master.MapReduceServiceDiscoveryManager;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMaster extends AbstractServerTestCase {

    /**
     * 
     */
    public TestMaster() {
        super();
    }

    /**
     * @param arg0
     */
    public TestMaster(String arg0) {
        super(arg0);
    }

    /**
     * Starts in {@link #setUp()}.
     */
    MapServer mapServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    ReduceServer reduceServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    MetadataServer metadataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    BigdataClient client;
    /**
     * Starts in {@link #setUp()}.
     */
    MapReduceServiceDiscoveryManager serviceDiscoveryManager;
    
    /**
     * Starts up a metadata service, a data service, a map service and a reduce
     * service.
     */
    public void setUp() throws Exception {

        super.setUp();
        
        /*
         * Start the metadata server.
         */
        metadataServer0 = new MetadataServer(
                new String[] { "src/resources/config/standalone/MetadataServer0.config"
                        });
        
        new Thread() {

            public void run() {
                
                metadataServer0.run();
                
            }
            
        }.start();

        dataServer0 = new DataServer(
                new String[] { "src/resources/config/standalone/DataServer0.config"
                        });

        new Thread() {

            public void run() {
                
                dataServer0.run();
                
            }
            
        }.start();

        client = new BigdataClient(
                new String[] { "src/resources/config/standalone/Client.config"
                        });

        mapServer0 = new MapServer(
                new String[] { "src/resources/config/standalone/MapServer0.config"
                      });
        
        reduceServer0 = new ReduceServer(
                new String[] { "src/resources/config/standalone/ReduceServer0.config"
                      });
        
        // Wait until all the services are up.
        getServiceID(metadataServer0);
        getServiceID(dataServer0);
        getServiceID(mapServer0);
        getServiceID(reduceServer0);
        
        // verify that the client has/can get the metadata service.
        assertNotNull("metadataService", client.getMetadataService());

        serviceDiscoveryManager = new MapReduceServiceDiscoveryManager(client);
        
    }
    
    /**
     * Destroy the test services.
     */
    public void tearDown() throws Exception {

        serviceDiscoveryManager.terminate();

        if (mapServer0 != null) {
            
            mapServer0.destroy();

            mapServer0 = null;
            
        }
        
        if (reduceServer0 != null) {
            
            reduceServer0.destroy();

            reduceServer0 = null;
            
        }
        
        if(metadataServer0!=null) {

            metadataServer0.destroy();
        
            metadataServer0 = null;

        }

        if(dataServer0!=null) {

            dataServer0.destroy();
        
            dataServer0 = null;

        }

        if(client!=null) {

            client.terminate();

            client = null;
            
        }
        
        super.tearDown();
        
    }

    public void testCountKeywords() {
        
        MapReduceJob job = new CountKeywordJob(100/* m */, 2/* n */);
//      MapReduceJob job = new CountKeywordJob(1/* m */, 1/* n */);

        // non-zero to submit no more than this many map inputs.
        job.setMaxMapTasks( 10 );

        // the timeout for a map task.
//        job.setMapTaskTimeout(2*1000/*millis*/);

        // the timeout for a reduce task.
//        job.setReduceTaskTimeout(2*1000/*millis*/);

        // the maximum #of times a map task will be retried (zero disables retry).
//        job.setMaxMapTaskRetry(3);

        // the maximum #of times a reduce task will be retried (zero disables retry).
//        job.setMaxReduceTaskRetry(3);

        /*
         * Run the map/reduce operation.
         */

        AbstractMaster master = new Master(job, client, serviceDiscoveryManager);
        
        master.run(.9d, .9d);
        
        System.out.println(master.status());
        
        assertTrue("map problem?",master.getMapPercentSuccess()>0.9);

        assertTrue("reduce problem?", master.getReducePercentSuccess()>0.9);

    }
    
}
