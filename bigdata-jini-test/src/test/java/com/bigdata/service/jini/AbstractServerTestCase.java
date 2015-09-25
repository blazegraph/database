/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Apr 22, 2007
 */

package com.bigdata.service.jini;

import java.io.IOException;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;

import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.MetadataService;
import com.bigdata.util.config.NicUtil;

/**
 * Abstract base class for tests of remote services.
 * <p>
 * Note: jini and zookeeper MUST be running. You MUST specify a sufficiently lax
 * security policy.
 * 
 * <pre>
 * -Djava.security.policy=policy.all
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractServerTestCase extends TestCase2 {
    
    public AbstractServerTestCase() {
    }

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
    static public ServiceID getServiceID(final AbstractServer server)
            throws AssertionFailedError, InterruptedException {

        ServiceID serviceID = null;

        for (int i = 0; i < 10 && serviceID == null; i++) {

            /*
             * Note: This can be null since the serviceID is not assigned
             * synchronously by the registrar.
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
    public IDataService lookupDataService(final ServiceID serviceID)
            throws IOException, ClassNotFoundException, InterruptedException {

        /* 
         * Lookup the discover service (unicast on localhost).
         */

        // get the hostname.
        final String hostname = NicUtil.getIpAddress("default.nic", "default",
                true);

        final int timeout = 4*1000; // seconds.
        
        if (log.isInfoEnabled())
            log.info("hostname: " + hostname);
        
        final LookupLocator lookupLocator = new LookupLocator("jini://"
                + hostname);

        // Find the service registrar (unicast protocol).
        final ServiceRegistrar serviceRegistrar = lookupLocator
                .getRegistrar(timeout);

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
         * you are starting everything up at once (or just register for
         * notification events for the service if it is not found and enter a
         * wait state).
         */
        
        IDataService service = null;
        
        for (int i = 0; i < 10 && service == null; i++) {
        
            service = (IDataService) serviceRegistrar
                    .lookup(template /* , maxMatches */);
            
            if (service == null) {
            
                log.warn("Service not found: sleeping...");
                
                Thread.sleep(200);
                
            }
            
        }

        if (service != null) {

            if (log.isInfoEnabled())
                log.info("Service found.");

        }

        return service;
        
    }

    /**
     * Compares two representations of the {@link PartitionLocator}
     * without the left- and right-separator keys that bound the index
     * partition.
     * 
     * @param expected
     * @param actual
     */
    protected void assertEquals(final PartitionLocator expected,
            final PartitionLocator actual) {
        
        assertEquals("partitionId", expected.getPartitionId(), actual
                .getPartitionId());

        assertEquals("dataServiceUUID", expected.getDataServiceUUID(), actual
                .getDataServiceUUID());

    }
    
    /**
     * Compares two representations of the {@link LocalPartitionMetadata} for an
     * index partition including the optional resource descriptions.
     * 
     * @param expected
     * @param actual
     */
    protected void assertEquals(final LocalPartitionMetadata expected,
            final LocalPartitionMetadata actual) {

        assertEquals("partitionId",expected.getPartitionId(), actual.getPartitionId());

        assertEquals("leftSeparatorKey", expected.getLeftSeparatorKey(),
                ((LocalPartitionMetadata) actual)
                        .getLeftSeparatorKey());

        assertEquals("rightSeparatorKey", expected.getRightSeparatorKey(),
                ((LocalPartitionMetadata) actual)
                        .getRightSeparatorKey());

        final IResourceMetadata[] expectedResources = expected.getResources();

        final IResourceMetadata[] actualResources = actual.getResources();
        
        assertEquals("#resources",expectedResources.length,actualResources.length);

        for (int i = 0; i < expected.getResources().length; i++) {
            
            // verify by components so that it is obvious what is wrong.
            
            assertEquals("filename[" + i + "]", expectedResources[i].getFile(),
                    actualResources[i].getFile());

//            assertEquals("size[" + i + "]", expectedResources[i].size(),
//                    actualResources[i].size());

            assertEquals("UUID[" + i + "]", expectedResources[i].getUUID(),
                    actualResources[i].getUUID());

            // verify by equals.
            assertTrue("resourceMetadata",expectedResources[i].equals(actualResources[i]));
            
        }
        
    }

}
