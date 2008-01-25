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
 * Created on Apr 22, 2007
 */

package com.bigdata.service;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;

import com.bigdata.journal.ITx;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.PartitionMetadataWithSeparatorKeys;
import com.sun.jini.tool.ClassServer;

/**
 * Abstract base class for tests of remote services.
 * <p>
 * Note: jini MUST be running. You can get the jini starter kit and install it
 * to get jini running.
 * </p>
 * <p>
 * Note: You MUST specify a security policy that is sufficiently lax.
 * </p>
 * <p>
 * Note: You MUST specify the codebase for downloadable code.
 * </p>
 * <p>
 * Note: The <code>bigdata</code> JAR must be current in order for the client
 * and the service to agree on interface definitions, etc. You can use
 * <code>build.xml</code> in the root of this module to update that JAR.
 * </p>
 * <p>
 * Note: A {@link ClassServer} will be started on port 8081 by default. If that
 * port is in use then you MUST specify another port.
 * </p>
 * 
 * The following system properties will do the trick unless you have something
 * running on port 8081.
 * 
 * <pre>
 * -Djava.security.policy=policy.all -Djava.rmi.server.codebase=http://localhost:8081
 * </pre>
 * 
 * To use another port, try:
 * 
 * <pre>
 * -Djava.security.policy=policy.all -Dbigdata.test.port=8082 -Djava.rmi.server.codebase=http://localhost:8082
 * </pre>
 * 
 * You can enable NIO using:
 * <pre>
 * -Dcom.sun.jini.jeri.tcp.useNIO=true
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractServerTestCase extends TestCase2 {

    /**
     * Equal to {@link ITx#UNISOLATED}.
     */
    protected final long UNISOLATED = ITx.UNISOLATED; 
    
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

//    /**
//     * Return an open port on current machine. Try the suggested port first. If
//     * suggestedPort is zero, just select a random port
//     */
//    private static int getPort(int suggestedPort) throws IOException {
//        ServerSocket openSocket;
//        try {
//            openSocket = new ServerSocket(suggestedPort);
//        } catch (BindException ex) {
//            // the port is busy, so look for a random open port
//            openSocket = new ServerSocket(0);
//        }
//
//        int port = openSocket.getLocalPort();
//        openSocket.close();
//
//        return port;
//    }

    /**
     * This may be used to verify that a specific port is available. The method
     * will return iff the port is available at the time that this method was
     * called. The method will retry a few times since sometimes it takes a bit
     * for a socket to get released and we are reusing the same socket for the
     * {@link ClassServer} for each test.
     * 
     * @param port
     *            The port to try.
     * 
     * @exception AssertionFailedError
     *                if the port is not available.
     */
    protected static void assertOpenPort(int port) throws IOException {
        
        ServerSocket openSocket;

        int i = 0;
        
        final int maxTries = 3;
        
        while (i < maxTries) {
        
            try {

                // try to open a server socket on that port.
                openSocket = new ServerSocket(port);

                // close the socket - it is available for the moment.
                openSocket.close();

                return;

            } catch (BindException ex) {

                if(i++<maxTries) {

                    log.warn("Port "+port+" is busy - retrying: " + ex);
                    
                    try {
                        Thread.sleep(100/*ms*/);
                    } catch(InterruptedException t) {
                        /* ignore */
                    }
                    
                } else {
                    
                    fail("Port is busy: "+ex+" - use "+PORT_OPTION+" to specify another port?");

                }

            }

        }
        
    }

    private ClassServer classServer;
    
    /**
     * The name of the System property that may be used to change the port on which
     * the {@link ClassServer} will be started.
     */
    public static final String PORT_OPTION = "bigdata.test.port";
    
    /**
     * The default port on which the {@link ClassServer} will be started.
     */
    public static final String DEFAULT_PORT = "8081";
    
    /**
     * Starts a {@link ClassServer} that supports downloadable code for the unit
     * test. The {@link ClassServer} will start on the port named by the System
     * property {@link #PORT_OPTION} and on port {@link #DEFAULT_PORT} if that
     * system property is not set.
     * 
     * @throws IOException 
     */
    protected void startClassServer() throws IOException {

        // Note: See below.
//        if(true) return;
        
        Logger.getLogger("com.sun.jini.tool.ClassServer").setLevel(Level.ALL);
        
        /*
         * Obtain port from System.getProperties() so that other ports may be
         * used.
         */
        final int port = Integer.parseInt(System.getProperty(PORT_OPTION,DEFAULT_PORT));
        
        /*
         * The directories containing the JARs and the compiled classes for the
         * bigdata project.
         */
        String dirlist = 
            "lib"+File.pathSeparatorChar+
            "lib"+File.separatorChar+"icu"+File.pathSeparatorChar+
            "lib"+File.separatorChar+"jini"+File.pathSeparatorChar
            /*
             * FIXME This does not seem to be resolving the bigdata classes
             * necessitating that we list that jar explictly below (and that it
             * be up to date). The problem can be seen in the Jini Service
             * Browser and the console for the Service Browser.  In fact, the
             * test suite executes just fine if you do NOT use the ClassServer!
             * 
             * I can only get this working right now by placing bigdata.jar into
             * the lib directory (or some other directory below the current
             * working directory, but not ant-build since that gives the ant
             * script fits).
             * 
             * I still see a ClassNotFound problem in the Jini console complaining
             * that it can not find IDataService, but only when I select the 
             * registrar on which the services are running!
             */
//            +
//            "bin"
            //+File.pathSeparatorChar+
//            "ant-build"
            ;
        
        assertOpenPort(port);
        
        classServer = new ClassServer(
                port,
                dirlist,
                true, // trees - serve up files inside of JARs,
                true // verbose
                );
        
        classServer.start();

    }
    
    public void setUp() throws Exception {

        log.info(getName());
        
        startClassServer();
        
    }

    /**
     * Stops the {@link ClassServer}.
     */
    public void tearDown() throws Exception {
        
        if(classServer!=null) {

            classServer.terminate();
            
        }
        
        super.tearDown();

        log.info(getName());
        
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
    static public ServiceID getServiceID(AbstractServer server) throws AssertionFailedError, InterruptedException {

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

    /**
     * Compares two representations of the metadata for an index partition
     * without the left- and right-separator keys that bound the index
     * partition.
     * 
     * @param expected
     * @param actual
     */
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

    /**
     * Compares two representations of the metadata for an index partition
     * including the left- and right-separator keys that bound the index
     * partition.
     * 
     * @param expected
     * @param actual
     */
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
