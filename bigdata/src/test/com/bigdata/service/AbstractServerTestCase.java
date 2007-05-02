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
 * Created on Apr 22, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.net.InetAddress;

import com.bigdata.scaleup.IPartitionMetadata;
import com.bigdata.scaleup.IResourceMetadata;
import com.bigdata.service.BigdataClient.PartitionMetadataWithSeparatorKeys;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractServerTestCase extends TestCase2 {

    /**
     * Equal to {@link IDataService#UNISOLATED}.
     */
    protected final long UNISOLATED = IDataService.UNISOLATED; 
    
    /**
     * 
     */
    public AbstractServerTestCase() {
    }

    /**
     * @param arg0
     */
    public AbstractServerTestCase(String arg0) {
        super(arg0);
    }
    
    /**
     * Return the {@link ServiceID} of a server that we started ourselves. The
     * method waits until the {@link ServiceID} becomes available on
     * {@link AbstractServer#getServiceID()}.
     * 
     * @exception AssertionFailedError
     *                If the {@link ServiceID} can not be found after a timeout.
     * 
     * @exception InterruptedException
     *                if the thread is interrupted while it is waiting to retry.
     */
    protected ServiceID getServiceID(AbstractServer server) throws AssertionFailedError, InterruptedException {

        ServiceID serviceID = null;

        for(int i=0; i<10 && serviceID == null; i++) {

            /*
             * Note: This can be null since the serviceID is not assigned
             * synchonously by the registrar.
             */

            serviceID = server.getServiceID();
            
            if(serviceID == null) {
                
                /*
                 * We wait a bit and retry until we have it or timeout.
                 */
                
                Thread.sleep(200);
                
            }
            
        }
        
        assertNotNull("serviceID",serviceID);
        
        /*
         * Verify that we have discovered the _correct_ service. This is a
         * potential problem when starting a stopping services for the test
         * suite.
         */
        assertEquals("serviceID", server.getServiceID(), serviceID);

        return serviceID;
        
    }
    
    /**
     * Lookup a {@link DataService} by its {@link ServiceID} using unicast
     * discovery on localhost.
     * 
     * @param serviceID
     *            The {@link ServiceID}.
     * 
     * @return The service.
     * 
     * @todo Modify to return the service item?
     * 
     * @todo Modify to not be specific to {@link DataService} vs
     *       {@link MetadataService} (we need a common base interface for both
     *       that carries most of the functionality but allows us to make
     *       distinctions easily during discovery).
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

    protected void assertEquals(IPartitionMetadata expected, IPartitionMetadata actual) {
        
        assertEquals("partitionId",expected.getPartitionId(), actual.getPartitionId());
        
        assertEquals("dataServices",expected.getDataServices(),actual.getDataServices());

        final IResourceMetadata[] expectedResources = expected.getResources();

        final IResourceMetadata[] actualResources = actual.getResources();
        
        assertEquals("#resources",expectedResources.length,actualResources.length);

        for(int i=0;i<expected.getResources().length; i++) {
            
            // verify by components so that it is obvious what is wrong.
            
            assertEquals("filename[" + i + "]", expectedResources[i].getFile(),
                    actualResources[i].getFile());

            assertEquals("size[" + i + "]", expectedResources[i].size(),
                    actualResources[i].size());

            assertEquals("UUID[" + i + "]", expectedResources[i].getUUID(),
                    actualResources[i].getUUID());

            assertEquals("state[" + i + "]", expectedResources[i].state(),
                    actualResources[i].state());
            
            // verify by equals.
            assertTrue("resourceMetadata",expectedResources[i].equals(actualResources[i]));
            
        }
        
    }
    
    protected void assertEquals(PartitionMetadataWithSeparatorKeys expected,
            IPartitionMetadata actual) {

        assertEquals((IPartitionMetadata) expected, actual);

        assertTrue("Class",
                actual instanceof PartitionMetadataWithSeparatorKeys);

        assertEquals("leftSeparatorKey", expected.getLeftSeparatorKey(),
                ((PartitionMetadataWithSeparatorKeys) actual)
                        .getLeftSeparatorKey());

        assertEquals("rightSeparatorKey", expected.getRightSeparatorKey(),
                ((PartitionMetadataWithSeparatorKeys) actual)
                        .getRightSeparatorKey());

    }
    
}
