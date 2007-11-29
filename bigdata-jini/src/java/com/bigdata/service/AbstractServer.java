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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Remote;
import java.rmi.server.ExportException;
import java.util.Arrays;
import java.util.Properties;

import net.jini.admin.Administrable;
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
import net.jini.lookup.DiscoveryAdmin;
import net.jini.lookup.JoinManager;
import net.jini.lookup.ServiceIDListener;

import org.apache.log4j.Logger;

import com.sun.jini.admin.DestroyAdmin;
import com.sun.jini.admin.StorageLocationAdmin;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.start.NonActivatableServiceDescriptor;
import com.sun.jini.start.ServiceDescriptor;
import com.sun.jini.start.ServiceStarter;

/**
 * <p>
 * Abstract base class for configurable services discoverable using JINI.
 * </p>
 * The recommended way to start a server is using the {@link ServiceStarter}.
 * 
 * <pre>
 *         java -Djava.security.policy=policy.all -cp lib\jini-ext.jar;lib\start.jar com.sun.jini.start.ServiceStarter src/test/com/bigdata/service/TestServerStarter.config
 * </pre>
 * 
 * Other command line options MAY be recommended depending on the server that
 * you are starting, e.g., <code>-server -XX:MaxDirectMemorySize=256M </code>.
 * <p>
 * The server MAY be started using a <code>main</code> routine:
 * </p>
 * 
 * <pre>
 * public static void main(String[] args) {
 * 
 *     new MyServer(args).run();
 * 
 * }
 * </pre>
 * 
 * <p>
 * The service may be <em>terminated</em> by terminating the server process.
 * Termination implies that the server stops execution but that it MAY be
 * restarted. A {@link Runtime#addShutdownHook(Thread)} is installed by the
 * server so that you can also stop the server using ^C (Windows) and possibly
 * <code>kill</code> <i>pid</i> (Un*x). You can record the PID of the process
 * running the server when you start it under Un*x using a shell script. Note
 * that if you are starting multiple services at once with the
 * {@link ServiceStarter} then these methods (^C or kill <i>pid</i>) will take
 * down all servers running in the same VM.
 * </p>
 * <p>
 * Services are <em>destroyed</em> using {@link DestroyAdmin}, e.g., through
 * the Jini service browser. Note that this tends to imply that all persistent
 * data associated with that service is also destroyed!
 * </p>
 * 
 * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6380355, which
 *      describes a bug in the service browser that will display a
 *      "NullPointerException" dialog box if you destroy a service which
 *      implements {@link DestroyAdmin} but not {@link JoinAdmin}.
 * 
 * @see http://java.sun.com/products/jini/2.0/doc/api/com/sun/jini/start/ServiceStarter.html
 *      for documentation on how to use the ServiceStarter.
 * 
 * @todo put a lock on the serviceIdFile while the server is running.
 * 
 * @todo the {@link DestroyAdmin} implementation on the {@link DataServer} is
 *       not working correctly. Untangle the various ways in which things can be
 *       stopped vs destroyed.
 * 
 * @todo document exit status codes and unify their use in this and derived
 *       classes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractServer implements LeaseListener, ServiceIDListener
{
    
    public static final transient Logger log = Logger
            .getLogger(AbstractServer.class);

    /**
     * The label in the {@link Configuration} file for the service
     * description.
     */
    protected final static transient String SERVICE_LABEL = "ServiceDescription";

    /**
     * The label in the {@link Configuration} file for the service advertisment
     * data.
     */
    protected final static transient String ADVERT_LABEL = "AdvertDescription";

    private ServiceID serviceID;
    private DiscoveryManagement discoveryManager;
    private JoinManager joinManager;
    private Configuration config;
    /**
     * The file where the {@link ServiceID} will be written/read. 
     */
    protected File serviceIdFile;
    /**
     * Responsible for exporting a proxy for the service.
     */
    private Exporter exporter;
    /**
     * The service implementation object.
     */
    protected Remote impl;
    /**
     * The exported proxy for the service implementation object.
     */
    protected Remote proxy;

    /**
     * The name of the host on which the server is running.
     */
    private final String hostname;
    
    /**
     * The name of the host on which the server is running (best effort during
     * startup and unchanging thereafter).
     */
    protected String getHostName() {
        
        return hostname;
        
    }
    
    private boolean open = false;

    /**
     * The object used to inform the hosting environment that the server is
     * unregistering (terminating). A fake object is used when the server is run
     * from the command line, otherwise the object is supplied by the
     * {@link NonActivatableServiceDescriptor}.
     */
    private LifeCycle lifeCycle;

    /**
     * The exported proxy for the service implementation object.
     */
    public Remote getProxy() {
        
        return proxy;
        
    }
    
    /**
     * Return the assigned {@link ServiceID}. If this is a new service, then
     * the {@link ServiceID} will be <code>null</code> until it has been
     * assigned by a service registrar.
     */
    public ServiceID getServiceID() {
        
        return serviceID;
        
    }

    protected DiscoveryManagement getDiscoveryManagement() {
        
        return discoveryManager;
        
    }
    
    protected JoinManager getJoinManager() {
        
        return joinManager;
        
    }

    /**
     * Conditionally install a suitable security manager if there is none in
     * place. This is required before the server can download code. The code
     * will be downloaded from the HTTP server identified by the
     * <code>java.rmi.server.codebase</code> property specified for the VM
     * running the service.
     */
    protected void setSecurityManager() {

        SecurityManager sm = System.getSecurityManager();
        
        if (sm == null) {

            System.setSecurityManager(new SecurityManager());
         
            log.info("Set security manager");

        } else {
            
            log.info("Security manager already in place: "+sm.getClass());
            
        }

    }
    
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

        this( args, new FakeLifeCycle() );
        
    }

    /**
     * Server startup invoked by the ServerStarter
     * 
     * @param args
     *            Arguments from the {@link ServiceDescriptor}.
     * @param lifeCycle
     *            The life cycle object.
     * 
     * @see NonActivatableServiceDescriptor
     */
    private AbstractServer(String[] args, LifeCycle lifeCycle ) {
        
        if (lifeCycle == null)
            throw new IllegalArgumentException();
        
        this.lifeCycle = lifeCycle;

        setSecurityManager();

        /*
         * resolve the host name (for informational purposes).
         */
        {
            String hostname;
            try {
                // DNS lookup
                hostname = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException ex) {
                hostname = "<unknown>";
            }
            this.hostname = hostname;
        }
        
        Entry[] entries = null;
        LookupLocator[] unicastLocators = null;
        String[] groups = null;

        try {
            
            config = ConfigurationProvider.getInstance(args); 

            /*
             * Extract how the service will advertise itself from the
             * Configuration.
             */

            entries = (Entry[]) config.getEntry(ADVERT_LABEL, "entries",
                    Entry[].class, null/* default */);

            groups = (String[]) config.getEntry(ADVERT_LABEL, "groups",
                    String[].class, LookupDiscovery.ALL_GROUPS/* default */);

            log.info("groups="+Arrays.toString(groups));
            
            unicastLocators = (LookupLocator[]) config.getEntry(
                    ADVERT_LABEL, "unicastLocators",
                    LookupLocator[].class, null/* default */);

            /*
             * Extract how the service will provision itself from the
             * Configuration.
             */

            // The exporter used to expose the service proxy.
            exporter = (Exporter) config.getEntry(//
                    SERVICE_LABEL, // component
                    "exporter", // name
                    Exporter.class // type (of the return object)
                    );

            // The file on which the ServiceID will be written. 
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

            // The properties file used to configure the service.

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

            open = true;
            
            log.info("Impl is "+impl);
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
                    groups, unicastLocators, null /*DiscoveryListener*/
            );

//            DiscoveryManagement discoveryManager = new LookupDiscovery(
//                    groups);
            
            if (serviceID != null) {
                /*
                 * We read the serviceID from local storage.
                 */
                joinManager = new JoinManager(proxy, // service proxy
                        entries, // attr sets
                        serviceID, // ServiceID
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

        /*
         * The runtime shutdown hook appears to be a robust way to handle ^C by
         * providing a clean service termination.
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));

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

        this.serviceID = serviceID;
        
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

        log.warn("Lease could not be renewed: " + event);
        
    }

    /**
     * This is run from within the {@link ShutdownThread} in response to a
     * request to destroy the service. This method shutdowns the server by
     * unregistering it from jini. If the service implements
     * {@link IServiceShutdown} then its {@link IServiceShutdown#shutdownNow()}
     * method will be invoked.
     */
    synchronized public void shutdownNow() {

        if(!open) return;

        open = false;
        
        /*
         * Terminate manager threads.
         */
        
        try {

            terminateServiceManagementThreads();
        
            /*
             * Hand-shaking with the NonActivableServiceDescriptor.
             */
            lifeCycle.unregister(this);
            
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

        if(impl instanceof IServiceShutdown) {
            
            /*
             * Invoke the services own logic to shutdown its processing.
             */

            ((IServiceShutdown)impl).shutdownNow();
            
        }
        
    }

    protected void terminateServiceManagementThreads() {
        
        log.info("Terminating service management threads.");

        joinManager.terminate();
        
        discoveryManager.terminate();

    }
    
    /**
     * Run the server (this should be invoked from <code>main</code>.
     */
    public void run() {

        log.info("Started server.");

        /*
         * Note: I have found the Runtime shutdown hook to be much more robust
         * than attempting to install a signal handler.  It is installed by
         * the server constructor rather than here so that it will be used 
         * when the server is run by the ServiceStarter as well as from main().
         */
        
//        /*
//         * Install signal handlers.
//        * SIGINT Interactive attention (CTRL-C). JVM will exit normally. <br>
//        * SIGTERM Termination request. JVM will exit normally. <br>
//        * SIGHUP Hang up. JVM will exit normally.<br>
//        * 
//        * @see http://www-128.ibm.com/developerworks/java/library/i-signalhandling/
//        * 
//        * @see http://forum.java.sun.com/thread.jspa?threadID=514860&messageID=2451429
//        *      for the use of {@link Runtime#addShutdownHook(Thread)}.
//        * 
//         */
//        
//        try {
//            ServerShutdownSignalHandler.install("SIGINT",this);
//        } catch(IllegalArgumentException ex) {
//            log.info("Signal handled not installed: "+ex);
//        }
//        
//        try {
//            ServerShutdownSignalHandler.install("SIGTERM",this);
//        } catch(IllegalArgumentException ex) {
//            log.info("Signal handled not installed: "+ex);
//        }
//
//        try {
//            ServerShutdownSignalHandler.install("SIGHUP",this);
//        } catch(IllegalArgumentException ex) {
//            log.info("Signal handled not installed: "+ex);
//        }

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
     * Runs {@link AbstractServer#shutdownNow()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ShutdownThread extends Thread {
        
        final AbstractServer server;
        
        public ShutdownThread(AbstractServer server) {
            
            if (server == null)
                throw new IllegalArgumentException();
            
            this.server = server;
            
        }
        
        public void run() {

            log.info("Running shutdown.");

            server.shutdownNow();
            
        }
        
    }

    /**
     * Contract is to shutdown the services and <em>destroy</em> its
     * persistent state. This implementation calls {@link #shutdownNow()} and
     * then deletes the {@link #serviceIdFile}. {@link #shutdownNow()} will
     * invoke {@link IServiceShutdown} if the service implements that interface.
     * <p>
     * Concrete subclasses MUST extend this method to destroy their persistent
     * state.
     */
    public void destroy() {

        shutdownNow();
        
        log.info("Deleting: "+serviceIdFile);

        if (!serviceIdFile.delete()) {

            log.warn("Could not delete file: "
                    + serviceIdFile);

        }
        
    }
    
//    /**
//     * Signal handler shuts down the server politely.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    static class ServerShutdownSignalHandler implements SignalHandler {
//
//        private final AbstractServer server;
//        
//        private SignalHandler oldHandler;
//
//        protected ServerShutdownSignalHandler(AbstractServer server) {
//
//            if(server == null) throw new IllegalArgumentException();
//            
//            this.server = server;
//            
//        }
//
//        /**
//         * Install the signal handler.
//         */
//        public static SignalHandler install(String signalName,
//                AbstractServer server) {
//
//            Signal signal = new Signal(signalName);
//
//            ServerShutdownSignalHandler newHandler = new ServerShutdownSignalHandler(
//                    server);
//
//            newHandler.oldHandler = Signal.handle(signal, newHandler);
//
//            log.info("Installed handler: " + signal + ", oldHandler="
//                    + newHandler.oldHandler);
//
//            return newHandler;
//            
//        }
//
//        public void handle(Signal sig) {
//
//            log.warn("Processing signal: "+sig);
//            
//            /*
//             * Handle signal.
//             */
//            server.shutdownNow();
//            
//            try {
//                
//                // Chain back to previous handler, if one exists
//                if ( oldHandler != SIG_DFL && oldHandler != SIG_IGN ) {
//
//                    oldHandler.handle(sig);
//                    
//                }
//                
//            } catch (Exception ex) {
//                
//                log.fatal("Signal handler failed, reason "+ex);
//        
//                System.exit(1);
//                
//            }
//            
//        }
//        
//    }

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

    private static class FakeLifeCycle implements LifeCycle {

        public boolean unregister(Object arg0) {
            
            log.info("");
            
            return true;
            
        }
        
    }
    
}
