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
 * Created on Mar 18, 2007
 */

package com.bigdata.service;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.Remote;
import java.rmi.server.ExportException;
import java.util.Properties;

import net.jini.admin.JoinAdmin;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.LookupDiscovery;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.export.Exporter;
import net.jini.lease.LeaseListener;
import net.jini.lease.LeaseRenewalEvent;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.JoinManager;
import net.jini.lookup.ServiceIDListener;

import org.apache.log4j.Logger;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Abstract base class for configurable services discoverable using JINI.
 * Services are started using a <code>main</code> routine:
 * <pre>
    public static void main(String[] args) {

        new MyServer(args).run();
                
    }
 *  </pre>
 *
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractServer implements LeaseListener, ServiceIDListener
{
    
    public static final transient Logger log = Logger
            .getLogger(AbstractServer.class);
    
    private ServiceID serviceID;
    private DiscoveryManagement discoveryManager;
    private JoinManager joinManager;
    private Configuration config;
    /**
     * The file where the {@link ServiceID} will be written/read. 
     */
    private File serviceIdFile;
    /**
     * Responsible for exporting a proxy for the service.
     */
    private Exporter exporter;
    /**
     * The service implementation object.
     */
    private Remote impl;
    /**
     * The exported proxy for the service implementation object.
     */
    private Remote proxy;

    /**
     * Server startup reads {@link Configuration} data from the file(s) named by
     * <i>args</i>, starts the service, and advertises the service for
     * discovery. Aside from the server class to start, the behavior is more or
     * less entirely parameterized by the {@link Configuration}.
     * 
     * @param args
     *            The command line arguments.
     */
    protected AbstractServer(String[] args) {

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

            serviceIdFile = (File) config.getEntry(ADVERT_LABEL,
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
             * Extract how the service will provision itself from the
             * Configuration.
             */

            // use the configuration to construct an exporter
            exporter = (Exporter) config.getEntry(//
                    SERVICE_LABEL, // component
                    "exporter", // name
                    Exporter.class // type (of the return object)
                    );

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
            impl = newService(properties);

            // export a proxy object for this service instance.
            proxy = exporter.export(impl);

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
     * Run the server (this should be invoked from <code>main</code>.
     * 
     * FIXME work through the install a signal handler that will shutdown the
     * service politely when it is invoked.  Do we need -Xrs on the command
     * line for this to work?  Which signals should be trapped?  Does this
     * vary by OS?
     * 
     * SIGINT Interactive attention (CTRL-C). JVM will exit normally. Yes <br>
     * SIGTERM Termination request. JVM will exit normally. Yes <br>
     * SIGHUP Hang up. JVM will exit normally. Yes
     * 
     * @see http://www-128.ibm.com/developerworks/java/library/i-signalhandling/
     */
    protected void run() {

        log.info("Started server.");

        /*
         * Install signal handlers.
         */
        
        ServerShutdownSignalHandler.install("SIGINT",this);
        
//        ServerShutdownSignalHandler.install("SIGTERM",this);
        
        /*
         * Wait until the server is terminated.
         */
        
        Object keepAlive = new Object();
        
        synchronized (keepAlive) {
            
            try {
                
                keepAlive.wait();
                
            } catch (InterruptedException ex) {
                
                log.info(""+ex);
                
            }
            
        }
        
    }

    /**
     * Shutdown the server taking time only to unregister it from jini.
     * 
     * @todo make this extensible? provide for normal shutdown vs this? support
     *       the jini Admin interface.
     */
    private void shutdownNow() {

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
    
    /**
     * Signal handler shuts down the server politely.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ServerShutdownSignalHandler implements SignalHandler {

        private final AbstractServer server;
        
        private SignalHandler oldHandler;

        protected ServerShutdownSignalHandler(AbstractServer server) {

            if(server == null) throw new IllegalArgumentException();
            
            this.server = server;
            
        }

        /**
         * Install the signal handler.
         */
        public static SignalHandler install(String signalName,
                AbstractServer server) {

            Signal signal = new Signal(signalName);

            ServerShutdownSignalHandler newHandler = new ServerShutdownSignalHandler(
                    server);

            newHandler.oldHandler = Signal.handle(signal, newHandler);

            log.info("Installed handler: " + signal + ", oldHandler="
                    + newHandler.oldHandler);

            return newHandler;
            
        }

        public void handle(Signal sig) {

            log.warn("Signal: "+sig);
            
            /*
             * Handle signal.
             */
            server.shutdownNow();
            
            try {
                
                // Chain back to previous handler, if one exists
                if ( oldHandler != SIG_DFL && oldHandler != SIG_IGN ) {

                    oldHandler.handle(sig);
                    
                }
                
            } catch (Exception ex) {
                
                log.fatal("Signal handler failed, reason "+ex);
        
                System.exit(1);
                
            }
            
        }
        
    }

    /**
     * This method is responsible for creating the remote service implementation
     * object. This object MUST declare one or more interfaces that extent the
     * {@link Remote} interface. The server will use JERI to create a proxy for
     * the remote object and configure and manage the protocol for
     * communications between the client (service proxy) and the remote object
     * (the service implementation).
     * <p>
     * Note: You have to implement {@link JoinAdmin} in order to show up as an
     * administerable service (blue folder) in the jini Service Browser.
     * 
     * @param properties
     *            The contents of the {@link Properties} file whose name was
     *            given by the <code>propertyFile</code> value in the
     *            {@link Configuration} identified to <code>main</code> by its
     *            command line arguments.
     */
    abstract protected Remote newService(Properties properties);

//    /**
//     * The remote service implementation object. This implements the
//     * {@link Remote} interface and uses JERI to create a proxy for the remote
//     * object and configure and manage the protocol for communications between
//     * the client (service proxy) and the remote object (the service
//     * implementation).
//     * <p>
//     * Note: You have to implement {@link JoinAdmin} in order to show up as an
//     * administerable service (blue folder) in the jini Service Browser.
//     * 
//     * @version $Id$
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
//     *         </a>
//     */
//    public static class TestServiceImpl implements ITestService
//    {
//
//        /**
//         * Service constructor.
//         * 
//         * @param properties
//         */
//        public TestServiceImpl(Properties properties) {
//
//            log.info("Created: " + this );
//
//            new Journal(properties);
//            
//        }
//
//        public void invoke() {
//
//            log.info("invoked: "+this);
//            
//        }
//
//    }

}
