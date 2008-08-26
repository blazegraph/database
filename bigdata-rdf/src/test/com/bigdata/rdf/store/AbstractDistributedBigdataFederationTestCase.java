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
 * Created on Jul 25, 2007
 */

package com.bigdata.rdf.store;

import junit.framework.TestCase2;

import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.DataServer;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.LoadBalancerServer;
import com.bigdata.service.jini.MetadataServer;
import com.bigdata.service.jini.ResourceLockServer;
import com.bigdata.service.jini.TimestampServer;

/**
 * An abstract test harness that sets up (and tears down) the metadata and data
 * services required for a bigdata federation using JINI to handle service
 * discovery.
 * <p>
 * Note: The configuration options for the (meta)data services are set in their
 * respective <code>properties</code> files NOT by the System properties!
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractDistributedBigdataFederationTestCase extends TestCase2 {

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
    protected LoadBalancerServer loadBalancerServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    protected TimestampServer timestampServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    protected ResourceLockServer resourceLockServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    JiniClient client;
    
    public void setUp() throws Exception {

        // @todo verify that this belongs here vs in a main(String[]).
        System.setSecurityManager(new SecurityManager());

//      final String groups = ".groups = new String[]{\"" + getName() + "\"}";
      
        /*
         * Start up a resource lock server.
         */
        resourceLockServer0 = new ResourceLockServer(new String[] {
                "src/resources/config/standalone/ResourceLockServer0.config"
//                , AbstractServer.ADVERT_LABEL+groups 
                });

        new Thread() {

            public void run() {
                
                resourceLockServer0.run();
                
            }
            
        }.start();

        /*
         * Start up a timestamp server.
         */
        timestampServer0 = new TimestampServer(new String[] {
                "src/resources/config/standalone/TimestampServer0.config"
//                , AbstractServer.ADVERT_LABEL+groups 
                });

        new Thread() {

            public void run() {
                
                timestampServer0.run();
                
            }
            
        }.start();

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
       * Start up a load balancer server.
       */
      loadBalancerServer0 = new LoadBalancerServer(new String[] {
              "src/resources/config/standalone/LoadBalancerServer0.config"
//              , AbstractServer.ADVERT_LABEL+groups 
              });

      new Thread() {

          public void run() {
              
              loadBalancerServer0.run();
              
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

      client = JiniClient.newInstance(
              new String[] { "src/resources/config/standalone/Client.config"
//                      , BigdataClient.CLIENT_LABEL+groups
                      });

      // Wait until all the services are up.
      AbstractServerTestCase.getServiceID(resourceLockServer0);
      AbstractServerTestCase.getServiceID(timestampServer0);
      AbstractServerTestCase.getServiceID(metadataServer0);
      AbstractServerTestCase.getServiceID(dataServer0);
      AbstractServerTestCase.getServiceID(dataServer1);
      AbstractServerTestCase.getServiceID(loadBalancerServer0);

      IBigdataFederation fed = client.connect();
      
      // wait/verify that the client has/can get the various services.
        for (int i = 0; i < 3; i++) {

            int nwaiting = 0;

            if (fed.getResourceLockService() == null) {
                log.warn("No resource lock service yet...");
                nwaiting++;
            }

            if (fed.getMetadataService() == null) {
                log.warn("No metadata service yet...");
                nwaiting++;
            }

            if (fed.getTimestampService() == null) {
                log.warn("No timestamp service yet...");
                nwaiting++;
            }

            if (fed.getLoadBalancerService() == null) {
                log.warn("No load balancer service yet...");
                nwaiting++;
            }

            if (nwaiting > 0) {

                log.warn("Waiting for " + nwaiting + " services");

                Thread.sleep(1000/* ms */);
                
            }

      }

      assertNotNull("No lock service?", fed.getResourceLockService());

      assertNotNull("No timestamp service?", fed.getTimestampService());

      assertNotNull("No metadata service?", fed.getMetadataService());

      assertNotNull("No load balancer service?", fed.getLoadBalancerService());

    }

    public void tearDown() throws Exception {

        if (client != null && client.isConnected()) {

            client.disconnect(true/* immediateShutdown */);

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

        if (loadBalancerServer0 != null) {
            
            loadBalancerServer0.destroy();

            loadBalancerServer0 = null;
            
        }

        if (timestampServer0 != null) {
            
            timestampServer0.destroy();

            timestampServer0 = null;
            
        }

    }
    
}
