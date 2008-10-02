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
 * Created on Jun 19, 2006
 */
package com.bigdata.service.jini;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ExportException;
import java.util.Properties;

import net.jini.admin.Administrable;
import net.jini.admin.JoinAdmin;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.LookupDiscovery;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.export.Exporter;
import net.jini.lease.LeaseListener;
import net.jini.lease.LeaseRenewalEvent;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.DiscoveryAdmin;
import net.jini.lookup.JoinManager;
import net.jini.lookup.ServiceIDListener;

import org.apache.log4j.Logger;

import com.bigdata.journal.Journal;
import com.sun.jini.admin.DestroyAdmin;
import com.sun.jini.admin.StorageLocationAdmin;
import com.sun.jini.start.ServiceStarter;

/**
 * Launches a server used by the test. The server is launched in a separate
 * thread that will die after a timeout, taking the server with it. The server
 * exposes some methods for testing, notably a method to test remote method
 * invocation and one to shutdown the server.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 * 
 * @todo work through use the {@link ServiceStarter} (in start.jar). I am having
 *       trouble getting past some classpath errors using
 * 
 * <pre>
 *         java -Djava.security.policy=policy.all -classpath ant-deploy\reggie.jar;ant-deploy\jini-core.jar;ant-deploy\jini-ext.jar;ant-deploy\sun-util.jar;ant-deploy\bigdata.jar -jar ant-deploy\start.jar src\test\org\CognitiveWeb\bigdata\jini\TestServer.config
 * </pre>
 * 
 * @todo support NIO protocol for data intensive APIs (data service, file
 *       transfer). Research how heavy mashalling is and what options exist to
 *       make it faster and lighter.
 * 
 * @todo Support JASS.
 * 
 * @todo {@link JoinAdmin} is not actually implemented (it is declared as a work
 *       around for {@link DestroyAdmin} which otherwise crashes the Jini
 *       ServiceBrowser with a NPE when you try to destroy the service).
 *       Consider using Wonderly's <code>startNow</code> PersistentJiniService
 *       as a base rather than rolling our own. That might be more robust -- see
 *       {@link TestPersistentJiniService}.
 * 
 * @todo consider {@link StorageLocationAdmin}
 * 
 * @todo consider {@link DiscoveryAdmin}
 */
public class TestServer implements LeaseListener, ServiceIDListener
{
    
    public static final transient Logger log = Logger
            .getLogger(TestServer.class);
    
    private ServiceID serviceID;
    private DiscoveryManagement discoveryManager;
    private JoinManager joinManager;
    private Configuration config;
    private TestServiceImpl impl;
    private Exporter exporter;
    private ITestService proxy;
    protected File serviceIdFile = null;

    /**
     * Server startup performs asynchronous multicast lookup discovery. The
     * {@link #discovered(DiscoveryEvent)} method is invoked asynchronously to
     * register a proxy for a {@link TestServiceImpl} instance. The protocol for
     * remote communications between the proxy and the {@link TestServiceImpl}
     * is specified by a {@link Configuration}.
     */
    public TestServer(String[] args) {

        final String SERVICE_LABEL = "ServiceDescription";

        final String ADVERT_LABEL = "AdvertDescription";
        
        Entry[] entries = null;
        LookupLocator[] unicastLocators = null;
        String[] groups = null;

        try {
            
            config = ConfigurationProvider.getInstance(args); 

            /*
             * Extract how the service will perform service discovery.
             */

            groups = (String[]) config.getEntry(ADVERT_LABEL, "groups",
                    String[].class, LookupDiscovery.ALL_GROUPS/* default */);

            unicastLocators = (LookupLocator[]) config.getEntry(
                    ADVERT_LABEL, "unicastLocators",
                    LookupLocator[].class, null/* default */);

            /*
             * Extract how the service will advertise itself from the
             * Configuration.
             */

            entries = (Entry[]) config.getEntry(ADVERT_LABEL, "entries",
                    Entry[].class, null/* default */);

            /*
             * Extract how the service will provision itself from the
             * Configuration.
             */

            // use the configuration to construct an exporter
            exporter = (Exporter) config.getEntry(//
                    SERVICE_LABEL, // component
                    "exporter", // name
                    Exporter.class // type (of the return object)
                    );

            serviceIdFile = (File) config.getEntry(SERVICE_LABEL,
                    "serviceIdFile", File.class); // default

            if(serviceIdFile.exists()) {

                try {

                    serviceID = readServiceId(serviceIdFile);
                    
                } catch(IOException ex) {
                    
                    log.fatal("Could not read serviceID from existing file: "
                            + serviceIdFile);

                    System.exit(1);
                    
                }
                
            } else {
                
                log.info("New service instance - ServiceID will be assigned");
                
            }
            
            /*
             * Access the properties file used to configure the service.
             */

            File propertyFile = (File) config.getEntry(SERVICE_LABEL,
                    "propertyFile", File.class);

            Properties properties = new Properties();
            
            try {
            
                InputStream is = new BufferedInputStream(new FileInputStream(
                        propertyFile));
                
                properties.load(is);
                
                is.close();
                
            } catch (IOException ex) {

                log.fatal("Configuration error: "+ex, ex);
                
                System.exit(1);
                
            }

            // create the service object.
            impl = new TestServiceImpl(this,properties);

            // export a proxy object for this service instance.
            proxy = (ITestService) exporter.export(impl);

            log.info("Proxy is " + proxy + "(" + proxy.getClass() + ")");

        } catch(ConfigurationException ex) {
            
            log.fatal("Configuration error: "+ex, ex);
            
            System.exit(1);
            
        } catch (ExportException ex) {
            
            log.fatal("Export error: "+ex, ex);
            
            System.exit(1);
            
        }
        
        try {

            /*
             * Note: This class will perform multicast discovery if ALL_GROUPS
             * is specified and otherwise requires you to specify one or more
             * unicast locators (URIs of hosts running discovery services). As
             * an alternative, you can use LookupDiscovery, which always does
             * multicast discovery.
             */
            discoveryManager = new LookupDiscoveryManager(
                    groups, unicastLocators, null // DiscoveryListener
            );

//            DiscoveryManagement discoveryManager = new LookupDiscovery(
//                    groups);
            
            if (serviceID != null) {
                /*
                 * We read the serviceID from local storage.
                 */
                joinManager = new JoinManager(proxy, // service proxy
                        entries, // attr sets
                        serviceID, // ServiceIDListener
                        discoveryManager, // DiscoveryManager
                        new LeaseRenewalManager());
            } else {
                /*
                 * We are requesting a serviceID from the registrar.
                 */
                joinManager = new JoinManager(proxy, // service proxy
                        entries, // attr sets
                        this, // ServiceIDListener
                        discoveryManager, // DiscoveryManager
                        new LeaseRenewalManager());
            }
            
        } catch (IOException ex) {
            
            log.fatal("Lookup service discovery error: "+ex, ex);

            try {
                /* unexport the proxy */
                unexport(true);
                joinManager.terminate();
                discoveryManager.terminate();
            } catch (Throwable t) {
                /* ignore */
            }
            
            System.exit(1);
            
        }

    }

//    /*
//     * @todo look into this as an alternative means to shutdown a service.
//     */
//    void shutdown() {    
//        try {
//            Object admin = ((Administrable) proxy).getAdmin();
//            DestroyAdmin destroyAdmin = (DestroyAdmin) admin;
//            destroyAdmin.destroy();
//        } catch (RemoteException e) { // handle
//            // exception
//            //
//        }
//    }


    /**
     * Unexports the proxy.
     * 
     * @param force
     *            When true, the object is unexported even if there are pending
     *            or in progress service requests.
     * 
     * @return true iff the object is (or was) unexported.
     * 
     * @see Exporter#unexport(boolean)
     */
    public boolean unexport(boolean force) {

        if(exporter.unexport(true)) {
        
            proxy = null;
        
            return true;
            
        }
        
        return false;

    }

    /**
     * Read and return the {@link ServiceID} from an existing local file.
     * 
     * @param file
     *            The file whose contents are the serialized {@link ServiceID}.
     * 
     * @return The {@link ServiceID} read from that file.
     * 
     * @exception IOException
     *                if the {@link ServiceID} could not be read from the file.
     */
    public ServiceID readServiceId(File file) throws IOException {

        FileInputStream is = new FileInputStream(file);
        
        ServiceID serviceID = new ServiceID(new DataInputStream(is));
        
        is.close();

        log.info("Read ServiceID=" + serviceID+" from "+file);

        return serviceID;
        
    }

    /**
     * This method is responsible for saving the {@link ServiceID} on stable
     * storage when it is invoked. It will be invoked iff the {@link ServiceID}
     * was not defined and one was therefore assigned.
     * 
     * @param serviceID
     *            The assigned {@link ServiceID}.
     */
    public void serviceIDNotify(ServiceID serviceID) {

        log.info("serviceID=" + serviceID);

        if (serviceIdFile != null) {
            
            try {
            
                DataOutputStream dout = new DataOutputStream(
                        new FileOutputStream(serviceIdFile));
            
                serviceID.writeBytes(dout);
                
                dout.flush();
                
                dout.close();
                
                log.info("ServiceID saved: " + serviceIdFile);

            } catch (Exception ex) {

                log.error("Could not save ServiceID", ex);
                
            }
            
        }

    }

    /**
     * Note: This is only invoked if the automatic lease renewal by the lease
     * manager is denied by the service registrar.
     * 
     * @todo how should we handle being denied a lease? Wait a bit and try
     *       re-registration? There can be multiple discovery services and this
     *       is only one lease rejection, so perhaps the service is still under
     *       lease on another discovery service?
     */
    public void notify(LeaseRenewalEvent event) {

        log.error("Lease could not be renewed: " + event);
        
    }
    
    /**
     * Launch the server in a separate thread.
     * <p>
     * Note: The location of the test service configuration is hardwired to a
     * test resource.
     */
    public static void launchServer() {
        new Thread("launchServer") {
            public void run() {
                TestServer
                        .main(new String[] { "src/test/com/bigdata/service/TestServer.config" });
            }
        }.start();
        log.info("Starting service.");
    }

    /**
     * Run the server. It will die after a timeout.
     * 
     * @param args
     *            Ignored.
     */
    public static void main(String[] args) {
        final long lifespan = 5 * 1000; // life span in seconds.
        log.info("Will start test server.");
        TestServer testServer = new TestServer(args);
        log.info("Started test server.");
        try {
            Thread.sleep(lifespan);
        }
        catch( InterruptedException ex ) {
            log.warn(ex);
        }
        testServer.shutdownNow();
//        /*
//         * Note: The reference to the service instance here forces a hard
//         * reference to remain for the test server. If you comment out this log
//         * statement, then you need to do something else to hold onto the hard
//         * reference.
//         */
//        log.info("Server will die: "+testServer);
    }
    
    void shutdownNow() {
        
        /*
         * Terminate manager threads.
         */
        try {
            log.info("Terminating manager threads.");
            joinManager.terminate();
            discoveryManager.terminate();
        } catch (Exception ex) {
            log.error("Could not terminate: "+ex, ex);
        }
        /*
         * Unexport the proxy, making the service no longer available. If you do
         * not do this then the client can still make requests even after you
         * have terminated the join manager and the service is no longer visible
         * in the service browser.
         */
        log.info("Unexporting the service proxy.");
        unexport(true);
        
    }
    
//    /**
//     * {@link Status} is abstract so a service needs to provide their own
//     * concrete implementation.
//     * 
//     * @version $Id$
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @download
//     */
//    public static class MyStatus extends Status {
//
//        /**
//         * 
//         */
//        private static final long serialVersionUID = 3431522046169284463L;
//        
//        /**
//         * Deserialization constructor (required).
//         */
//        public MyStatus(){}
//        
//        public MyStatus(StatusType statusType) {
//
//            /*
//             * Note: This just sets the read/write public [severity] field on
//             * the super class.
//             */
//            super(statusType);
//            
//        }
//        
//    }
//    
//    /**
//     * {@link ServiceType} is abstract so a service basically needs to provide
//     * their own concrete implementation. This class does not support icons
//     * (always returns null for {@link ServiceType#getIcon(int)}. See
//     * {@link java.beans.BeanInfo} for how to interpret and support the
//     * getIcon() method.
//     * 
//     * @version $Id$
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
//     *         </a>
//     * @download
//     */
//    public static class MyServiceType extends ServiceType
//    {
//
//        /**
//         * 
//         */
//        private static final long serialVersionUID = -2088608425852657477L;
//        
//        public String displayName;
//        public String shortDescription;
//        
//        /**
//         * Deserialization constructor (required).
//         */
//        public MyServiceType() {}
//
//        public MyServiceType(String displayName, String shortDescription) {
//            this.displayName = displayName;
//            this.shortDescription = shortDescription;
//        }
//        
//        public String getDisplayName() {
//            return displayName;
//        }
//        
//        public String getShortDescription() {
//            return shortDescription;
//        }
//        
//    }

    /**
     * The remote service implementation object. This implements the
     * {@link Remote} interface and uses JERI to create a proxy for the remote
     * object and configure and manage the protocol for communications between
     * the client (service proxy) and the remote object (the service
     * implementation).
     * <p>
     * Note: You have to implement {@link JoinAdmin} in order to show up as an
     * administerable service (blue folder) in the jini Service Browser.
     * 
     * @version $Id$
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
     *         </a>
     */
    public static class TestServiceImpl implements ITestService, RemoteAdministrable, RemoteDestroyAdmin, RemoteJoinAdmin
    {
     
        private TestServer server;
        private Journal journal;
        
        public Journal getJournal() {
            
            return journal;
            
        }

        /**
         * Service constructor.
         * 
         * @param properties
         */
        public TestServiceImpl(TestServer server,Properties properties) {

            log.info("Created: " + this );

            journal = new Journal(properties);
            
            this.server = server;
            
        }

        public void invoke() {

            log.info("invoked: "+this);
            
        }

        public Object getAdmin() throws RemoteException {

            log.info("");

            return server.proxy;
            
        }

        /*
         * DestroyAdmin
         */

        /**
         * Destroy the service (including its persistent state).
         * 
         * @throws RemoteException
         */
        public void destroy() throws RemoteException {

            log.info("");

            boolean destroyEnabled = true;

            if (destroyEnabled) {
            
                new Thread() {
                    
                    public void run() {

                        server.shutdownNow();

                        try {

                            getJournal().destroy();

                            log.info("Journal deleted.");
                            
                        } catch (Throwable t) {

                            log.warn("Could not delete persistent state: " + t,
                                    t);

                        }

                        if (!server.serviceIdFile.delete()) {

                            log.warn("Could not delete file: "
                                    + server.serviceIdFile);

                        }

                        try {
                            Thread.sleep(3);
                        } catch (InterruptedException ex) {
                        }

                        log.info("Service Stop requested");

                        System.exit(1);

                    }

                }.start();
            } else {
                throw new RemoteException(
                        "Service Destroy Not Enabled, Operation Ignored");
            }

        }

        /*
         * JoinAdmin
         */
        
        public void addLookupAttributes(Entry[] arg0) throws RemoteException {
            
            log.info("");

            // TODO Auto-generated method stub
            
        }

        public void addLookupGroups(String[] arg0) throws RemoteException {

            log.info("");

            // TODO Auto-generated method stub

        }

        public void addLookupLocators(LookupLocator[] arg0) throws RemoteException {

            log.info("");

            // TODO Auto-generated method stub
            
        }

        public Entry[] getLookupAttributes() throws RemoteException {

            log.info("");

            // TODO Auto-generated method stub
            return null;
        }

        public String[] getLookupGroups() throws RemoteException {
         
            log.info("");

            // TODO Auto-generated method stub
            return null;
        }

        public LookupLocator[] getLookupLocators() throws RemoteException {
         
            log.info("");

            // TODO Auto-generated method stub
            return null;
        }

        public void modifyLookupAttributes(Entry[] arg0, Entry[] arg1) throws RemoteException {
         
            log.info("");

            // TODO Auto-generated method stub
            
        }

        public void removeLookupGroups(String[] arg0) throws RemoteException {
            log.info("");

            // TODO Auto-generated method stub
            
        }

        public void removeLookupLocators(LookupLocator[] arg0) throws RemoteException {
            log.info("");

            // TODO Auto-generated method stub
            
        }

        public void setLookupGroups(String[] arg0) throws RemoteException {
            log.info("");

            // TODO Auto-generated method stub
            
        }

        public void setLookupLocators(LookupLocator[] arg0) throws RemoteException {
            log.info("");

            // TODO Auto-generated method stub
            
        }

    }

    /*
     * Note: You need to extend Remote in order for these APIs to be exported!
     */
    
    public static interface RemoteAdministrable extends Remote, Administrable {
        
    }
    
    public static interface RemoteDestroyAdmin extends Remote, DestroyAdmin {

    }

    public static interface RemoteJoinAdmin extends Remote, JoinAdmin {

    }

    public static interface RemoteDiscoveryAdmin extends Remote, DiscoveryAdmin {

    }

    public static interface RemoteStorageLocationAdmin extends Remote, StorageLocationAdmin {

    }

}
