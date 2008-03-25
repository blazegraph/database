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

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.service.jini.BigdataClient;
import com.bigdata.service.jini.DataServer;
import com.bigdata.service.jini.MetadataServer;

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
 * -Djava.security.policy=policy.all
 * </pre>
 * 
 * Note: You will sometimes see a connection refused exception thrown while
 * trying to register the scale-out indices. This happens when some data
 * services were abruptly terminated and those services have not yet been
 * de-registered from jini. A little wait or a restart of Jini fixes this
 * problem.
 * <p>
 * Note: You may see a "metadata index exists" exception. This occurs when the
 * store files backing the data services were not removed before the unit test.
 * This is fixed by deleting the store files and re-running.
 * 
 * @todo consider reusing a single federation for all unit tests in order to
 *       reduce the setup time for the tests. this would have the consequence
 *       that we are not guarenteed a clean slate when we connect to the
 *       federation and we would definately have to implement dropIndex on the
 *       metadata service for this to work.
 * 
 * @todo the java service browser log contains exceptions - presumably because
 *       we have not setup a codebase, e.g., using an embedded class server,
 *       such that it is unable to resolve the various bigdata classes.
 * 
 * @todo the following tests are failing:
 *       <p>
 *       {@link TestRestartSafe#test_restartSafe()} - IllegalStateException
 *       during re-open of the store.
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

//        suite.addTestSuite(TestLocalTripleStoreTransactionSemantics.class);

        /*
         * Pickup the basic triple store test suite. This is a proxied test
         * suite, so all the tests will run with the configuration specified in
         * this test class and its optional .properties file.
         */
        
        suite.addTest(TestTripleStoreBasics.suite());

        return suite;

    }

//    /**
//     * Properties used by tests in the file and in this proxy suite.
//     */
//    public Properties getProperties() {
//
//        Properties properties = new Properties( super.getProperties() );
//
////         Note: this reduces the disk usage at the expense of memory usage.
////        properties.setProperty(EmbeddedBigdataFederation.Options.BUFFER_MODE,
////                BufferMode.Transient.toString());
//
////        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());
//
////        properties.setProperty(Options.CREATE_TEMP_FILE,"true");
//
////        properties.setProperty(Options.DELETE_ON_EXIT,"true");
//
//        /*
//         * Note: there are also properties to control the #of data services
//         * created in the embedded federation.
//         */
//        
//        return properties;
//
//    }

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
    protected BigdataClient client;
    
    public void setUp() throws Exception {
        
        super.setUp();

        System.setSecurityManager(new SecurityManager());
        
    }

    public void tearDown() throws Exception {
        
        super.tearDown();
        
    }
    
    public void setUpFederation() throws Exception {

//      final String groups = ".groups = new String[]{\"" + getName() + "\"}";

        log.warn("Starting data services.");
        
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

      log.warn("Starting client.");

      client = BigdataClient.newInstance(
              new String[] { "src/resources/config/standalone/Client.config"
//                      , BigdataClient.CLIENT_LABEL+groups
                      });

      // Wait until all the services are up.
      AbstractServerTestCase.getServiceID(metadataServer0);
      AbstractServerTestCase.getServiceID(dataServer0);
      AbstractServerTestCase.getServiceID(dataServer1);
      
      // verify that the client has/can get the metadata service.
      assertNotNull("metadataService", client.getMetadataService());

      log.warn("Federation is running.");
      
    }

    public void tearDownFederation() throws Exception {

        log.warn("Destroying federation.");
        
        /*
         * @todo consider fed.destroy().
         */
        
        if(client!=null) {

            client.shutdownNow();

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
    
    /**
     * Data files are placed into a directory named by the test. If the
     * directory exists, then it is removed before the federation is set up.
     */
    public void setUp(ProxyTestCase testCase) throws Exception {

        setUpFederation();
        
        super.setUp(testCase);
        
//        File dataDir = new File( testCase.getName() );
//        
//        if(dataDir.exists() && dataDir.isDirectory()) {
//
//            recursiveDelete( dataDir );
//            
//        }

    }
    
    public void tearDown(ProxyTestCase testCase) throws Exception {

//        // Note: close() is disconnecting from the embedded federation.
////        client.terminate();
//
//        // delete on disk federation (if any).
//        recursiveDelete(new File(testCase.getName()));
        
        super.tearDown();
        
        tearDownFederation();
        
    }
    
//    /**
//     * Recursively removes any files and subdirectories and then removes the
//     * file (or directory) itself.
//     * 
//     * @param f A file or directory.
//     */
//    private void recursiveDelete(File f) {
//        
//        if(f.isDirectory()) {
//            
//            File[] children = f.listFiles();
//            
//            for(int i=0; i<children.length; i++) {
//                
//                recursiveDelete( children[i] );
//                
//            }
//            
//        }
//        
//        if (f.exists()) {
//
//            log.warn("Removing: " + f);
//
//            if (!f.delete()) {
//
//                throw new RuntimeException("Could not remove: " + f);
//
//            }
//
//        }
//
//    }
    
    protected AbstractTripleStore getStore() {
        
        // Connect to the federation.
        ScaleOutTripleStore store = new ScaleOutTripleStore(client.connect(),
                getProperties());

        // register indices.
        store.registerIndices();
        
        return store;
        
    }
 
    /**
     * Re-open the same backing store.
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

        // close the client connection to the federation.
        store.close();
        
        // re-connect to the federation.
        return getStore();
        
    }

}
