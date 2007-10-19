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
 * Created on Jul 25, 2007
 */

package com.bigdata.rdf.scaleout;

import junit.framework.TestCase;

import com.bigdata.btree.BTree;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.service.AbstractServerTestCase;
import com.bigdata.service.BigdataClient;
import com.bigdata.service.DataServer;
import com.bigdata.service.MetadataServer;

/**
 * An abstract test harness that sets up (and tears down) the metadata and data
 * services required for a bigdata federation using JINI to handle service
 * discovery.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractDistributedBigdataFederationTestCase extends TestCase {

    public AbstractDistributedBigdataFederationTestCase() {
        super();
    }
    
    public AbstractDistributedBigdataFederationTestCase(String name) {
        super(name);
    }
    
    /**
     * Starts in {@link #setUp()}.
     */
    MetadataServer metadataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer1;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    BigdataClient client;
    
    /**
     * FIXME Work the setup, teardown, and APIs until I can use either an
     * embedded database or a client-service divide with equal ease -- and note
     * that there is also a distinction at this time between an intrinsically
     * local rdf database and one that is network capable but running locally
     * (e.g., whether or not it was written using the {@link BigdataClient}
     * class or direct use of a {@link BTree}).
     * <p>
     * This will be especially important for the {@link TempTripleStore}, which
     * normally uses only local resources. The best way is to defined an
     * interface IBigdataClient and then provide both embedded and data-server
     * implementations.
     */
    public void setUp() throws Exception {

        // @todo verify that this belongs here vs in a main(String[]).
        System.setSecurityManager(new SecurityManager());

//      final String groups = ".groups = new String[]{\"" + getName() + "\"}";
      
      /*
       * Start up a data server before the metadata server so that we can make
       * sure that it is detected by the metadata server once it starts up.
       */
      dataServer1 = new DataServer(new String[] {
              "src/resources/config/standalone/DataServer1.config"
//              , AbstractServer.ADVERT_LABEL+groups 
              });

      new Thread() {

          public void run() {
              
              dataServer1.run();
              
          }
          
      }.start();

      /*
       * Start the metadata server.
       */
      metadataServer0 = new MetadataServer(
              new String[] { "src/resources/config/standalone/MetadataServer0.config"
//                      , AbstractServer.ADVERT_LABEL+groups
                      });
      
      new Thread() {

          public void run() {
              
              metadataServer0.run();
              
          }
          
      }.start();

      /*
       * Start up a data server after the metadata server so that we can make
       * sure that it is detected by the metadata server once it starts up.
       */
      dataServer0 = new DataServer(
              new String[] { "src/resources/config/standalone/DataServer0.config"
//                      , AbstractServer.ADVERT_LABEL+groups
                      });

      new Thread() {

          public void run() {
              
              dataServer0.run();
              
          }
          
      }.start();

      client = new BigdataClient(
              new String[] { "src/resources/config/standalone/Client.config"
//                      , BigdataClient.CLIENT_LABEL+groups
                      });

      // Wait until all the services are up.
      AbstractServerTestCase.getServiceID(metadataServer0);
      AbstractServerTestCase.getServiceID(dataServer0);
      AbstractServerTestCase.getServiceID(dataServer1);
      
      // verify that the client has/can get the metadata service.
      assertNotNull("metadataService", client.getMetadataService());

    }

    public void tearDown() throws Exception {

        /*
         * @todo consider fed.destroy().
         */
        
        if(client!=null) {

            client.terminate();

            client = null;
            
        }
        
        if(metadataServer0!=null) {

            metadataServer0.destroy();
        
            metadataServer0 = null;

        }

        if(dataServer0!=null) {

            dataServer0.destroy();
        
            dataServer0 = null;

        }
        
        if (dataServer1 != null) {
            
            dataServer1.destroy();

            dataServer1 = null;
            
        }

    }
    
}
