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
 * Created on Oct 18, 2007
 */

package com.bigdata.rdf.store;

import java.io.File;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.journal.ITx;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.DataServer;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.LoadBalancerServer;
import com.bigdata.service.jini.MetadataServer;
import com.bigdata.service.jini.TimestampServer;

/**
 * Proxy test suite for {@link ScaleOutTripleStore} running against an
 * distributed federation using Jini services.
 * <p>
 * Note: The tests in this suite setup a bigdata federation for each test. The
 * various data services for the federation all run in the same process, but the
 * data services register themselves and are discovered using jini and
 * communications with those data services use remote procedure calls (at least
 * logically since some JVMs may optimize away the RPC to a local endpoint).
 * <p>
 * Jini MUST be running.
 * <p>
 * You MUST specify at least the following properties to the JVM and have access
 * to the resources in <code>src/resources/config</code>.
 * 
 * <pre>
 *         -Djava.security.policy=policy.all
 * </pre>
 * 
 * Note: You will sometimes see a connection refused exception thrown while
 * trying to register the scale-out indices. This happens when some data
 * services were abruptly terminated and those services have not yet been
 * de-registered from jini. A little wait or a restart of Jini fixes this
 * problem.
 * <p>
 * Note: All configuration of the services and the client are performed using
 * the files in <code>src/resources/config/standalone</code>. The
 * <code>.config</code> files have the jini aspect of the client and services
 * configurations. The <code>.property</code> files have the bigdata aspect of
 * the client and services configurations.
 * <p>
 * Note: Normally the services are destroyed after each test, which has the
 * effect of removing all files create by each service. Therefore tests normally
 * start from a clean slate. If you have test setup problems, such as "metadata
 * index exists", first verify that the <code>standalone</code> directory in
 * which the federation is created has been properly cleaned. If files have been
 * left over due to an improper cleanup by a prior test run then you can see
 * such startup problems.
 * 
 * @todo consider reusing a single federation for all unit tests in order to
 *       reduce the setup time for the tests. this would have the consequence
 *       that we are not guarenteed a clean slate when we connect to the
 *       federation. we would need to use dropIndex.
 * 
 * @todo the jini service browser log contains exceptions - presumably because
 *       we have not setup a codebase, e.g., using an embedded class server,
 *       such that it is unable to resolve the various bigdata classes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestScaleOutTripleStoreWithJiniFederation extends AbstractTestCase {

    public TestScaleOutTripleStoreWithJiniFederation() {

    }

    public TestScaleOutTripleStoreWithJiniFederation(String name) {

        super(name);
        
    }
    
    public static Test suite() {

        final TestScaleOutTripleStoreWithJiniFederation delegate = new TestScaleOutTripleStoreWithJiniFederation(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Scale-Out Triple Store Test Suite (jini federation)");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
//        // writes on the term:id and id:term indices.
//        suite.addTestSuite(TestTermAndIdsIndex.class);
//
//        // writes on the statement indices.
//        suite.addTestSuite(TestStatementIndex.class);
               
        /*
         * Proxied test suite for use only with the LocalTripleStore.
         * 
         * @todo test unisolated operation semantics.
         */

//        suite.addTestSuite(TestFullTextIndex.class);

//        suite.addTestSuite(TestLocalTripleStoreTransactionSemantics.class);

        /*
         * Pickup the basic triple store test suite. This is a proxied test
         * suite, so all the tests will run with the configuration specified in
         * this test class and its optional .properties file.
         */
        
        suite.addTest(TestTripleStoreBasics.suite());

        return suite;

    }

    /**
     * Starts in {@link #setUpFederation()}.
     */
    protected MetadataServer metadataServer0;
    /**
     * Starts in {@link #setUpFederation()}.
     */
    protected DataServer dataServer1;
    /**
     * Starts in {@link #setUpFederation()}.
     */
    protected DataServer dataServer0;
    /**
     * Starts in {@link #setUpFederation()}.
     */
    protected TimestampServer timestampServer0;
    /**
     * Starts in {@link #setUpFederation()}.
     */
    protected LoadBalancerServer loadBalancerServer0;
    /**
     * Starts in {@link #setUpFederation()}.
     */
    protected JiniClient client;
    
    public void setUp() throws Exception {
        
        super.setUp();

        /*
         * Note: This is the data directory configured in the various .config
         * and .properties files in src/resources/config/standlone.
         */
        File dataDir = new File("standalone");
        
        if(dataDir.exists() && dataDir.isDirectory()) {

            recursiveDelete( dataDir );
            
        }

        System.setSecurityManager(new SecurityManager());
        
    }

    public void tearDown() throws Exception {
        
        super.tearDown();
        
    }

    /**
     * This starts each of the services in the federartion, all of which will
     * run in the local process of this JVM. Since jini is used for service
     * discovery, all service requests will in fact use RMI.
     * 
     * @throws Exception
     */
    public void setUpFederation() throws Exception {

//      final String groups = ".groups = new String[]{\"" + getName() + "\"}";

        log.warn("Starting data services.");
        
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
//                , AbstractServer.ADVERT_LABEL+groups 
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

      log.warn("Starting client.");

      client = JiniClient.newInstance(
              new String[] { "src/resources/config/standalone/Client.config"
//                      , BigdataClient.CLIENT_LABEL+groups
                      });

      // Wait until all the services are up.
      AbstractServerTestCase.getServiceID(timestampServer0);
      AbstractServerTestCase.getServiceID(metadataServer0);
      AbstractServerTestCase.getServiceID(dataServer0);
      AbstractServerTestCase.getServiceID(dataServer1);
      AbstractServerTestCase.getServiceID(loadBalancerServer0);
      
      IBigdataFederation fed = client.connect();
      
      // verify that the client has/can get the metadata service.
      assertNotNull("metadataService", fed.getMetadataService());

      log.warn("Federation is running.");
      
    }

    /**
     * This terminates client processing and destroys each of the services
     * (their persistent state is also destroyed).
     * 
     * @throws Exception
     */
    public void tearDownFederation() throws Exception {

        log.warn("Destroying federation.");

        if (client != null && client.isConnected()) {

            client.disconnect(true/*immediateShutdown*/);

            client = null;

        }

        if (metadataServer0 != null) {

            metadataServer0.destroy();

            metadataServer0 = null;

        }

        if (dataServer0 != null) {

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
    
    /**
     * Data files are placed into a directory named by the test. If the
     * directory exists, then it is removed before the federation is set up.
     */
    public void setUp(ProxyTestCase testCase) throws Exception {

        setUpFederation();
        
        super.setUp(testCase);
        
    }
    
    public void tearDown(ProxyTestCase testCase) throws Exception {

        super.tearDown();
        
        tearDownFederation();
        
    }
    
    protected AbstractTripleStore getStore() {
        
        // Connect to the triple store.
        return new ScaleOutTripleStore(client, "test", ITx.UNISOLATED);
        
    }
 
    /**
     * Re-open the same backing store.
     * <p>
     * This basically disconnects the client
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is closed, or if the store can not
     *                be re-opened, e.g., from failure to obtain a file lock,
     *                etc.
     */
    protected AbstractTripleStore reopenStore(AbstractTripleStore store) {

        store.close();
        
        // close the client connection to the federation.
        client.disconnect(true/*immediateShutdown*/);
        
        // re-connect to the federation.
        client.connect();
        
        // obtain view of the triple store.
        return getStore();
        
    }

}
