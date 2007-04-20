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
 * Created on Apr 20, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;


/**
 * Test of client-server communications. The test starts a {@link DataServer}
 * and then verifies that basic operations can be carried out against that
 * server. The server is stopped when the test is torn down.
 * <p>
 * Note: This test uses the <code>DataServer0.config</code> file from the
 * src/resources/config/standalone package.
 * <p>
 * Note: The <code>bigdata</code> JAR must be current in order for the client
 * and the service to agree on interface definitions, etc. You can use
 * <code>build.xml</code> in the root of this module to update that JAR.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo write another test class that accesses more than one
 *       {@link DataService} instance, e.g., by placing a different index on
 *       each {@link DataService}.
 */
public class TestDataServer0 extends TestCase2 {

    /**
     * 
     */
    public TestDataServer0() {
    }

    /**
     * @param arg0
     */
    public TestDataServer0(String arg0) {
        super(arg0);
    }
    
    String[] args = new String[]{
            "src/resources/config/standalone/DataServer0.config"
    };
    
    DataServer dataServer0;

    // start server in its own thread.
    public void setUp() throws Exception {
        
        dataServer0 = new DataServer(args);
        
        new Thread() {

            public void run() {
                
                dataServer0.run();
                
            }
            
        }.start();
        
    }
    
    /**
     * destroy the test service.
     */
    public void tearDown() throws Exception {
        
        dataServer0.destroy();
        
    }
   
    /**
     * Return the {@link ServiceID} of the specific service that we launched in
     * #setUp().
     * 
     * @exception AssertionFailedError
     *                If the {@link ServiceID} can not be found after a timeout.
     * 
     * @exception InterruptedException
     *                if the thread is interrupted while it is waiting to retry.
     */
    protected ServiceID getServiceID() throws AssertionFailedError, InterruptedException {

        ServiceID serviceID = null;

        for(int i=0; i<10 && serviceID == null; i++) {

            /*
             * Note: This can be null since the serviceID is not assigned
             * synchonously by the registrar.
             */

            serviceID = dataServer0.getServiceID();
            
            if(serviceID == null) {
                
                /*
                 * We wait a bit and retry until we have it or timeout.
                 */
                
                Thread.sleep(200);
                
            }
            
        }
        
        assertNotNull("serviceID",serviceID);

        return serviceID;
        
    }
    
    /**
     * Exercises the basic features of the {@link IDataService} interface using
     * unisolated operations, including creating a B+Tree, insert, contains,
     * lookup, and remove operations on key-value pairs in that B+Tree, and
     * dropping the B+Tree.
     * 
     * @throws Exception
     * 
     * @todo test {@link IDataService#submit(long, IProcedure)}.
     */
    public void test_serverRunning() throws Exception {

        ServiceID serviceID = getServiceID();
        
        IDataService proxy = lookupDataService(serviceID); 
        
        assertNotNull("service not discovered",proxy);
        
        final String name = "testIndex";
        
        // add an index.
        proxy.registerIndex(name, UUID.randomUUID());
        
        // batch insert into that index.
        proxy.batchInsert(IDataService.UNISOLATED, name, 1,
                new byte[][] { new byte[] { 1 } },
                new byte[][] { new byte[] { 1 } });

        // verify keys that exist/do not exist.
        boolean contains[] = proxy.batchContains(IDataService.UNISOLATED, name, 2,
                new byte[][] { new byte[] { 1 }, new byte[] { 2 } });

        assertNotNull(contains);
        assertEquals(2,contains.length);
        assertTrue(contains[0]);
        assertFalse(contains[1]);

        // lookup keys that do and do not exist. 
        byte[][] values = proxy.batchLookup(IDataService.UNISOLATED, name, 2,
                new byte[][] { new byte[] { 1 }, new byte[] { 2 } });

        assertNotNull(values);
        assertEquals(2,values.length);
        assertEquals(new byte[]{1},values[0]);
        assertEquals(null,values[1]);
        
        // rangeCount the all partitions of the index on the data service.
        assertEquals(1, proxy.rangeCount(IDataService.UNISOLATED, name, null,
                null));
        
        /*
         * visit all keys and values for the partitions of the index on the data
         * service.
         */
        
        final int flags = IDataService.KEYS | IDataService.VALS;
        
        ResultSet rset = proxy.rangeQuery(IDataService.UNISOLATED, name, null,
                null, 100, flags );
        
        assertEquals("rangeCount",1,rset.getRangeCount());
        assertEquals("ntuples",1,rset.getNumTuples());
        assertTrue("exhausted",rset.isExhausted());
        assertEquals("lastKey",new byte[]{1},rset.getLastKey());
        assertNotNull("keys",rset.getKeys());
        assertEquals("keys.length",1,rset.getKeys().length);
        assertEquals(new byte[]{1},rset.getKeys()[0]);
        assertNotNull("vals",rset.getValues());
        assertEquals("vals.length",1,rset.getValues().length);
        assertEquals(new byte[]{1},rset.getValues()[0]);
        
        // remove key that exists, verifying the returned values.
        values = proxy.batchRemove(IDataService.UNISOLATED, name, 2,
                new byte[][] { new byte[] { 1 }, new byte[] { 2 } }, true);

        assertNotNull(values);
        assertEquals(2,values.length);
        assertEquals(new byte[]{1},values[0]);
        assertEquals(null,values[1]);

        // remove again, verifying that the returned values are now null.
        values = proxy.batchRemove(IDataService.UNISOLATED, name, 2,
                new byte[][] { new byte[] { 1 }, new byte[] { 2 } }, true);

        assertNotNull(values);
        assertEquals(2,values.length);
        assertEquals(null,values[0]);
        assertEquals(null,values[1]);

        proxy.dropIndex(name);
        
    }
    
    /**
     * Lookup a {@link DataService} by its {@link ServiceID}.
     * 
     * @param serviceID
     *            The {@link ServiceID}.
     */
    public IDataService lookupDataService(ServiceID serviceID)
            throws IOException, ClassNotFoundException, InterruptedException {

        /* 
         * Lookup the discover service (unicast on localhost).
         */

        // get the hostname.
        InetAddress addr = InetAddress.getLocalHost();
        String hostname = addr.getHostName();

        // Find the service registrar (unicast protocol).
        final int timeout = 4*1000; // seconds.
        System.err.println("hostname: "+hostname);
        LookupLocator lookupLocator = new LookupLocator("jini://"+hostname);
        ServiceRegistrar serviceRegistrar = lookupLocator.getRegistrar( timeout );

        /*
         * Prepare a template for lookup search.
         * 
         * Note: The client needs a local copy of the interface in order to be
         * able to invoke methods on the service without using reflection. The
         * implementation class will be downloaded from the codebase identified
         * by the server.
         */
        ServiceTemplate template = new ServiceTemplate(//
                /*
                 * use this to request the service by its serviceID.
                 */
                serviceID,
                /*
                 * Use this to filter services by an interface that they expose.
                 */
//                new Class[] { IDataService.class },
                null,
                /*
                 * use this to filter for services by Entry attributes.
                 */
                null);

        /*
         * Lookup a service. This can fail if the service registrar has not
         * finished processing the service registration. If it does, you can
         * generally just retry the test and it will succeed. However this
         * points out that the client may need to wait and retry a few times if
         * you are starting everthing up at once (or just register for
         * notification events for the service if it is not found and enter a
         * wait state).
         */
        
        IDataService service = null;
        
        for (int i = 0; i < 10 && service == null; i++) {
        
            service = (IDataService) serviceRegistrar
                    .lookup(template /* , maxMatches */);
            
            if (service == null) {
            
                System.err.println("Service not found: sleeping...");
                
                Thread.sleep(200);
                
            }
            
        }

        if(service!=null) {

            System.err.println("Service found.");
            
        }
        
        return service;
        
    }
    
}
