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
import java.rmi.server.ExportException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import net.jini.admin.JoinAdmin;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.export.Exporter;
import net.jini.lease.LeaseListener;
import net.jini.lease.LeaseRenewalEvent;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.JoinManager;
import net.jini.lookup.ServiceIDListener;
import net.jini.lookup.entry.Name;
import net.jini.lookup.entry.ServiceType;
import net.jini.lookup.entry.StatusType;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.ACL;

import com.bigdata.Banner;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.jini.start.BigdataZooDefs;
import com.bigdata.service.AbstractService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.zookeeper.ZLock;
import com.bigdata.zookeeper.ZNodeLockWatcher;
import com.sun.jini.admin.DestroyAdmin;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.start.NonActivatableServiceDescriptor;
import com.sun.jini.start.ServiceDescriptor;
import com.sun.jini.start.ServiceStarter;

/**
 * <p>
 * Abstract base class for configurable services discoverable using JINI. The
 * recommended way to start a server is using the {@link ServiceStarter}.
 * However, they may also be started from the command line using main(String[]).
 * You must specify a policy file granting sufficient permissions for the server
 * to start.
 * 
 * <pre>
 * java -Djava.security.policy=policy.all ....
 * </pre>
 * 
 * Other command line options MAY be recommended depending on the JVM and the
 * service that you are starting, e.g., <code>-server</code>.
 * <p>
 * The service may be <em>terminated</em> by terminating the server process.
 * Termination implies that the server stops execution but that it MAY be
 * restarted. A {@link Runtime#addShutdownHook(Thread) shutdown hook} is
 * installed by the server so that you can also stop the server using ^C
 * (Windows) and <code>kill</code> <i>pid</i> (Un*x). You can record the PID
 * of the process running the server when you start it under Un*x using a shell
 * script. Note that if you are starting multiple services at once with the
 * {@link ServiceStarter} then these methods (^C or kill <i>pid</i>) will take
 * down all servers running in the same VM.
 * <p>
 * Services may be <em>destroyed</em> using {@link DestroyAdmin}, e.g.,
 * through the Jini service browser. Note that all persistent data associated
 * with that service is also destroyed!
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
 * @todo make the serviceId ASCII hex digits (that is not the jini standard
 *       practice)?
 * 
 * @todo document exit status codes and unify their use in this and derived
 *       classes.
 * 
 * @todo add {@link StatusType} and link its values to the running state of the
 *       service.
 * 
 * @todo add {@link ServiceType} with suitable pretty icons.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractServer implements Runnable, LeaseListener,
        ServiceIDListener {
    
    final static protected Logger log = Logger.getLogger(AbstractServer.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.isDebugEnabled();

    /**
     * The {@link ServiceID} for this server is either read from a local file or
     * assigned by the registrar (iff this is a new service instance). When
     * assigned, it is assigned the asynchronously.
     */
    private ServiceID serviceID;

    /**
     * The file where the {@link ServiceID} will be written/read.
     */
    protected File serviceIdFile;

    /**
     * The zpath (zookeeper path) to the znode for the logical service of which
     * this service is an instance. This is read from the {@link Configuration}.
     */
    protected String logicalServiceZPath;

    /**
     * The path to the ephemeral znode (zookeeper node) for this service
     * instance. This path is assigned when the service creates a
     * {@link CreateMode#EPHEMERAL_SEQUENTIAL} child of
     * {@link #logicalServiceZPath}.
     */
    protected String physicalServiceZPath;
    
    /**
     * The {@link JiniClient} is used to locate the other services in the
     * {@link IBigdataFederation}.
     */
    private JiniClient client;

    /**
     * Used to manage the join/leave of the service hosted by this server with
     * Jini service registrar(s).
     */
    private JoinManager joinManager;

    /**
     * The {@link Configuration} read based on the args[] provided when the
     * server is started.
     */
    protected Configuration config;

    /**
     * A configured name for the service -or- <code>null</code> if no
     * {@link Name} was found in the {@link Configuration}.
     * 
     * @todo javadoc and reconcile with behavior of {@link AbstractService},
     *       which assigns a different default service name.
     */
    private String serviceName;

    /**
     * A configured name for the service -or- <code>null</code> if no
     * {@link Name} was found in the {@link Configuration}.
     */
    public String getServiceName() {
        
        return serviceName;
        
    }

    /**
     * Responsible for exporting a proxy for the service. Note that the
     * {@link Exporter} is paired to a single service instance. It CAN NOT be
     * used to export more than one object at a time! Therefore the
     * {@link Configuration} entry for the <code>exporter</code> only effects
     * how <em>this</em> server exports its service.
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
     * The name of the host on which the server is running (best effort during
     * startup and unchanging thereafter).
     */
    protected String getHostName() {
        
        return AbstractStatisticsCollector.fullyQualifiedHostName;
        
    }
    
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
    
    protected JoinManager getJoinManager() {
        
        return joinManager;
        
    }

    /**
     * The object used to connect to and access the other services in the
     * {@link IBigdataFederation}.
     */
    protected JiniClient getClient() {
        
        return client;
        
    }
    
    /**
     * Conditionally install a suitable security manager if there is none in
     * place. This is required before the server can download code. The code
     * will be downloaded from the HTTP server identified by the
     * <code>java.rmi.server.codebase</code> property specified for the VM
     * running the service.
     */
    final static public void setSecurityManager() {

        final SecurityManager sm = System.getSecurityManager();
        
        if (sm == null) {

            System.setSecurityManager(new SecurityManager());
         
            if (INFO)
                log.info("Set security manager");

        } else {

            if (INFO)
                log.info("Security manager already in place: " + sm.getClass());
            
        }

    }

    /**
     * This method handles fatal exceptions for the server.
     * <p>
     * The default implementation logs the throwable, invokes
     * {@link #shutdownNow()} to terminate any processing and release all
     * resources, wraps the throwable as a runtime exception and rethrows the
     * wrapped exception.
     * <p>
     * This implementation MAY be overriden to invoke {@link System#exit(int)}
     * IFF it is known that the server is being invoked from a command line
     * context. However in no case should execution be allowed to return to the
     * caller.
     */
    protected void fatal(String msg, Throwable t) {
       
        log.fatal(msg, t);
        
        try {

            shutdownNow();
            
        } catch (Throwable t2) {
            
            log.error(t2.getMessage(), t2);
            
        }
        
        throw new RuntimeException( msg, t );
        
    }

    /**
     * Note: AbstractServer(String[]) is private to ensure that the ctor
     * hierarchy always passes down the variant which accepts the {@link LifeCycle}
     * as well.  This simplies additional initialization in subclasses. 
     */
    @SuppressWarnings("unused")
    private AbstractServer(String[] args) {
        
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Server startup reads {@link Configuration} data from the file or URL
     * named by <i>args</i> and applies any optional overrides, starts the
     * service, and advertises the service for discovery. Aside from the server
     * class to start, the behavior is more or less entirely parameterized by
     * the {@link Configuration}.
     * 
     * @param args
     *            Either the command line arguments or the arguments from the
     *            {@link ServiceDescriptor}. Either way they identify the jini
     *            {@link Configuration} (you may specify either a file or URL)
     *            and optional overrides for that {@link Configuration}.
     * @param lifeCycle
     *            The life cycle object. This is used if the server is started
     *            by the jini {@link ServiceStarter}. Otherwise specify a
     *            {@link FakeLifeCycle}.
     * 
     * @see NonActivatableServiceDescriptor
     */
    protected AbstractServer(final String[] args, final LifeCycle lifeCycle) {
        
        // Show the copyright banner during statup.
        Banner.banner();

        if (lifeCycle == null)
            throw new IllegalArgumentException();
        
        this.lifeCycle = lifeCycle;

        setSecurityManager();

        /*
         * Read jini configuration & service properties 
         */

        Entry[] entries = null;
        boolean readServiceIDFromFile = false;
        
        final JiniClientConfig jiniClientConfig;
        try {

            config = ConfigurationProvider.getInstance(args);

            jiniClientConfig = new JiniClientConfig(getClass().getName(), config);

            entries = jiniClientConfig.entries;
            
            /*
             * Extract a name associated with the service.
             */
            {
                
                for (Entry e : entries) {

                    if (e instanceof Name) {

                        // found a name.
                        serviceName = ((Name) e).name;

                        break;

                    }

                }

                if (serviceName == null) {

                    // assign a default service name.
                    
                    final String defaultName = getClass().getName()
                            + "@"
                            + AbstractStatisticsCollector.fullyQualifiedHostName
                            + "#" + hashCode();

                    serviceName = defaultName;

                }
            
            }
            
            /*
             * Extract how the service will provision itself from the
             * Configuration.
             */

            // The exporter used to expose the service proxy.
            exporter = (Exporter) config.getEntry(//
                    getClass().getName(), // component
                    "exporter", // name
                    Exporter.class // type (of the return object)
                    );

            /*
             * The zpath of the logical service.
             * 
             * @todo null allowed if zookeeper not in use. make required if
             * zookeeper is a required integration, but then you will really
             * need to use the ServicesManager to start any service since the
             * zookeeper configuration needs to be established as well.
             */
            logicalServiceZPath = (String) config.getEntry(getClass().getName(),
                    "logicalServiceZPath", String.class, null/* default */);
            
            // The file on which the ServiceID will be written. 
            serviceIdFile = (File) config.getEntry(getClass().getName(),
                    "serviceIdFile", File.class); // default

            if(serviceIdFile.exists()) {

                try {

                    serviceID = readServiceId(serviceIdFile);
                    
                    readServiceIDFromFile = true;
                    
                } catch(IOException ex) {

                    fatal( "Could not read serviceID from existing file: "
                            + serviceIdFile, ex );
                    
                }
                
            } else {
                
                if (INFO)
                    log.info("New service instance - ServiceID will be assigned");
                
                /*
                 * Make sure that the parent directory exists.
                 * 
                 * Note: the parentDir will be null if the serviceIdFile is in
                 * the root directory or if it is specified as a filename
                 * without any parents in the path expression. Note that the
                 * file names a file in the current working directory in the
                 * latter case and the root always exists in the former - and in
                 * both of those cases we do not have to create the parent
                 * directory.
                 */
                final File parentDir = serviceIdFile.getAbsoluteFile()
                        .getParentFile();

                if (parentDir != null && !parentDir.exists()) {

                    log.warn("Creating: " + parentDir);

                    parentDir.mkdirs();
                    
                }
                
            }

        } catch(ConfigurationException ex) {
            
            fatal("Configuration error: "+ex, ex);
            
        }
        
        /*
         * The runtime shutdown hook appears to be a robust way to handle ^C by
         * providing a clean service termination.
         * 
         * Note: This is setup before we start any async threads, including
         * service discovery.
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));
        
        /*
         * Start the client - this provides a means to connect to the other
         * services running in the federation (it runs a DiscoveryManager to do
         * that).
         */
        try {

            // new client - defer connection to fed.
            client = new JiniClient(getClass(), config);

//            // connect to the federation (starts service discovery for the client).
//            client.connect();
            
        } catch(Throwable t) {
            
            fatal("Could not create JiniClient: " + t, t);
            
        }
        
        /*
         * Create the service object.
         */
        JiniFederation fed = null;
        try {
            
            /*
             * Note: By creating the service object here rather than outside of
             * the constructor we potentially create problems for subclasses of
             * AbstractServer since their own constructor will not have been
             * executed yet.
             * 
             * Some of those problems are worked around using a JiniClient to
             * handle all aspects of service discovery (how this service locates
             * the other services in the federation).
             * 
             * Note: If you explicitly assign values to those clients when the
             * fields are declared, e.g., [timestampServiceClient=null] then the
             * ctor will overwrite the values set by [newService] since it is
             * running before those initializations are performed. This is
             * really crufty, may be JVM dependent, and needs to be refactored
             * to avoid this subclass ctor init problem.
             */

            if (INFO)
                log.info("Creating service impl...");

            // init w/ client's properties.
            impl = newService(client.getProperties());
            
            if (INFO)
                log.info("Service impl is " + impl);

            // Connect to the federation (starts service discovery for client).
            final JiniFederation f = fed = client.connect();
            
            /*
             * Add a watcher that will create the ephemeral znode for the
             * federation on zookeeper (re-)connect.
             * 
             * Note: We don't have to do this once we have been connected, but
             * if the service starts without a zookeeper connection and then
             * later connects then it will otherwise fail to create its znode.
             */
            fed.addWatcher(new Watcher() {

                public void process(WatchedEvent event) {

                    switch (event.getState()) {
                    case Disconnected:
                        if (masterElectionFuture != null) {
                            masterElectionFuture
                                    .cancel(true/* mayInterruptIfRunning */);
                            masterElectionFuture = null;
                            log.warn("Lost zookeeper connection: cancelled master election task.");
                        }
                        break;
                    case NoSyncConnected:
                    case SyncConnected:
                        if (serviceID != null) {

                            try {
                                notifyZookeeper(f, JiniUtil
                                        .serviceID2UUID(serviceID));
                            } catch (Throwable t) {
                                log.error(t);
                            }

                        }
                    }
                    
                }
            });
            
            // start the service.
            if(impl instanceof AbstractService) {

                ((AbstractService)impl).start();
                
            }
            
        } catch(Exception ex) {
        
            fatal("Could not start service: "+ex, ex);
            
        }

        /*
         * Export a proxy object for this service instance.
         * 
         * Note: This must be done before we start the join manager since the
         * join manager will register the proxy.
         */
        try {

            proxy = exporter.export(impl);
            
            if (INFO)
                log.info("Proxy is " + proxy + "(" + proxy.getClass() + ")");

        } catch (ExportException ex) {

            fatal("Export error: "+ex, ex);
            
        }
        
        /*
         * Start the join manager. 
         */
        try {

            assert proxy != null;
            
            if (serviceID != null) {

                /*
                 * We read the serviceID from local storage.
                 */
                
                joinManager = new JoinManager(proxy, // service proxy
                        entries, // attr sets
                        serviceID, // ServiceID
                        fed.discoveryManager, // DiscoveryManager
                        new LeaseRenewalManager());
                
            } else {
                
                /*
                 * We are requesting a serviceID from the registrar.
                 */
                
                joinManager = new JoinManager(proxy, // service proxy
                        entries, // attr sets
                        this, // ServiceIDListener
                        fed.discoveryManager, // DiscoveryManager
                        new LeaseRenewalManager());
            
            }
            
        } catch (IOException ex) {
            
            fatal("Lookup service discovery error: "+ex, ex);
            
        }
        
        if (readServiceIDFromFile) {
            
            /*
             * Notify the service that it's service UUID has been set.
             */
            notifyServiceUUID(serviceID);

        }
        
    }

    /**
     * Read and return the content of the properties file.
     * 
     * @param propertyFile
     *            The properties file.
     * 
     * @throws IOException
     */
    protected static Properties getProperties(File propertyFile)
            throws IOException {

        final Properties properties = new Properties();

        InputStream is = null;

        try {

            is = new BufferedInputStream(new FileInputStream(propertyFile));

            properties.load(is);

            return properties;

        } finally {

            if (is != null)
                is.close();

        }

    }
    
    /**
     * Unexports the {@link #proxy} - this is a NOP if the proxy is
     * <code>null</code>.
     * 
     * @param force
     *            When true, the object is unexported even if there are pending
     *            or in progress service requests.
     * 
     * @return true iff the object is (or was) unexported.
     * 
     * @see Exporter#unexport(boolean)
     */
    synchronized protected boolean unexport(boolean force) {

        if (INFO)
            log.info("force=" + force + ", proxy=" + proxy);

        try {
            
            if (proxy != null) {

                if (exporter.unexport(force)) {

                    return true;

                } else {

                    log.warn("Proxy was not unexported?");

                }

            }

            return false;

        } finally {

            proxy = null;

        }

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
    public ServiceID readServiceId(final File file) throws IOException {

        final FileInputStream is = new FileInputStream(file);

        try {

            final ServiceID serviceID = new ServiceID(new DataInputStream(is));

            if (INFO)
                log.info("Read ServiceID=" + serviceID + " from " + file);

            return serviceID;

        } finally {

            is.close();

        }
        
    }

    /**
     * This method is responsible for saving the {@link ServiceID} on stable
     * storage when it is invoked. It will be invoked iff the {@link ServiceID}
     * was not defined and one was therefore assigned.
     * 
     * @param serviceID
     *            The assigned {@link ServiceID}.
     */
    synchronized public void serviceIDNotify(final ServiceID serviceID) {

        if (serviceID == null)
            throw new IllegalArgumentException();
        
        if (INFO)
            log.info("serviceID=" + serviceID);

        if (this.serviceID != null && !this.serviceID.equals(serviceID)) {

            throw new IllegalStateException(
                    "ServiceID may not be changed: ServiceID=" + this.serviceID
                            + ", proposed=" + serviceID);
            
        }
        
        this.serviceID = serviceID;
        
        if (serviceIdFile != null) {
            
            try {
            
                final DataOutputStream dout = new DataOutputStream(
                        new FileOutputStream(serviceIdFile));
            
                try {
                
                    serviceID.writeBytes(dout);

                    dout.flush();
                    
                    if (INFO)
                        log.info("ServiceID saved: file=" + serviceIdFile
                                + ", serviceID=" + serviceID);

                } finally {
                    
                    dout.close();
                    
                }
                
            } catch (Exception ex) {

                log.error("Could not save ServiceID", ex);
                
            }
            
        }

        notifyServiceUUID(serviceID);
        
    }
    
    /**
     * Notify the {@link AbstractService} that it's service UUID has been set.
     */
    protected void notifyServiceUUID(final ServiceID serviceId) {

        if (serviceId == null)
            throw new IllegalArgumentException();
        
        if(impl != null && impl instanceof AbstractService) {

            final UUID serviceUUID = JiniUtil.serviceID2UUID(serviceID);

            final AbstractService service = ((AbstractService) impl);
            
            service.setServiceUUID(serviceUUID);
            
            try {

                final JiniFederation fed = (JiniFederation) service
                        .getFederation();

                notifyZookeeper(fed, serviceUUID);

            } catch (Throwable t) {

                log.error("Could not register service with zookeeper: " + t, t);

            }

        }

    }
    
    /**
     * Create a {@link CreateMode#EPHEMERAL} node in zookeeper that is a child
     * of the {@link #logicalServiceZPath}. The znode name is generated by
     * appending the assigned <i>serviceUUID</i>. That name is <strong>stable</strong>.
     * If the service is shutdown and then restarted it will re-create the SAME
     * znode.
     * <p>
     * Note: we need to monitor (Watcher) the zookeeper connection state. If the
     * client is not connected when this method is invoked when we need to
     * create the ephemeral znode for the physical service when the client
     * becomes connected to zookeeper. This is done as part of ctor.
     * 
     * @param zookeeper
     * @param serviceUUID
     * 
     * @throws KeeperException
     * @throws InterruptedException
     * 
     * @todo Since the order of the physical service znodes for a given logical
     *       service is essentially random, a separate election must be
     *       maintained for the logical service in order to choose the failover
     *       chain (which service is the primary, the secondary, etc.)
     * 
     * @todo Any failover protocol in which the service can restart MUST provide
     *       for re-synchronization of the service when it restarts with the
     *       current primary / active ensemble.
     * 
     * @todo test failover w/ death and restart of both individual zookeeper
     *       instances and of the zookeeper ensemble. unless there are failover
     *       zookeeper instances it is going to lose track of our services (they
     *       are represented by ephemeral znodes). On reconnect to zookeeper,
     *       the service should should verify that its ephemeral node is present
     *       and create it if it is not present and re-negotiate the service
     *       failover chain.
     * 
     * @todo The logical UUID is for compatibility with the bigdata APIs, which
     *       expect to refer to a service by a UUID. Lookup by UUID against jini
     *       will require hashing the physical services by their logical service
     *       UUID in the client, which is not hard.
     */
    private void notifyZookeeper(final JiniFederation fed,
            final UUID serviceUUID) throws KeeperException,
            InterruptedException {

        if (fed == null)
            throw new IllegalArgumentException();

        if (serviceUUID == null)
            throw new IllegalArgumentException();
        
//        if (logicalServiceZPath == null) {
//
//            throw new IllegalStateException(
//                    "Logical service zpath not assigned.");
//
//        }
        
        final ZooKeeper zookeeper = fed.getZookeeper();
        
        if (zookeeper == null) {
            
            /*
             * @todo This is checked in case zookeeper is not integrated. If
             * we decide that zookeeper is a manditory component then change
             * this to throw an exception instead (or perhaps
             * fed.getZookeeper() will throw that exception).
             */

            log.warn("No zookeeper: will not create service znode.");

            return;
            
        }

        if (logicalServiceZPath == null) {

            /*
             * @todo This is checked so that you can use a standalone
             * configuration file without having an assigned logical service
             * zpath that exists in zookeeper.
             * 
             * If we take out this test then all configuration files would have
             * to specify the logical service path, which might be an end state
             * for the zookeeper integration.
             */

            log
                    .warn("No logicalServiceZPath in config file: will not create service znode: cls="
                            + getClass().getName());

            return;
            
        }

        // Note: makes if(physicalServiceZPath!=null) atomic.
        synchronized (logicalServiceZPath) {

            if (physicalServiceZPath != null) {

                throw new IllegalStateException(
                        "Physical service zpath already assigned.");

            }

            final List<ACL> acl = fed.getZooConfig().acl;
            
            /*
             * Note: The znode is created using the assigned service UUID. This
             * means that the total zpath for the physical service is stable and
             * can be re-created on restart of the service.
             */
            physicalServiceZPath = logicalServiceZPath + "/"
                    + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER + "/" + serviceUUID;

            try {
            
                // make sure the parent node exists.
                zookeeper.create(logicalServiceZPath + "/"
                        + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER, new byte[0], acl,
                        CreateMode.PERSISTENT);

            } catch (NodeExistsException ex) {
                // ignore.
            }

            try {

                // create the ephemeral znode for this service.
                zookeeper.create(physicalServiceZPath, new byte[0], acl,
                        CreateMode.EPHEMERAL);
                
            } catch (NodeExistsException ex) {
                
                /*
                 * ignore.
                 * 
                 * Note: We ignore this because this code gets executed on each
                 * zookeeper (re-)connect.
                 */
                
            }

            /*
             * Enter into the master / failover competition for the logical
             * service.
             */
            masterElectionFuture = fed.submitMonitoredTask(new MasterElectionTask());

            if (INFO)
                log.info("registered with zookeeper: zpath="
                        + physicalServiceZPath);
            
        }

    }

    private Future masterElectionFuture = null;
    
    /**
     * Task runs forever competing to become the master.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class MasterElectionTask implements Callable {
        
        public MasterElectionTask() {
            
        }
        
        /**
         * calls {@link #runOnce()} until the service is shutdown, logging any
         * errors.
         */
        public Object call() throws Exception {

            while (!shuttingDown && impl != null) {

                try {

                    runOnce();

                } catch (Throwable t) {

                    log.error(this, t);

                }
                
            }
            
            return null;

        }

        /**
         * Competes for the {@link BigdataZooDefs#MASTER_ELECTION}
         * {@link ZLock}. If it gains the lock, then invoked
         * {@link #runAsMaster()}.
         * 
         * @throws Exception
         */
        protected void runOnce() throws Exception {

            final AbstractService service = ((AbstractService) impl);
            
            final JiniFederation fed = (JiniFederation) service.getFederation();

            final ZooKeeper zookeeper = fed.getZookeeper();
            
            final List<ACL> acl = fed.getZooConfig().acl;

            // zlock object for the master election.
            ZLock zlock = ZNodeLockWatcher.getLock(zookeeper,
                    logicalServiceZPath + "/"
                            + BigdataZooDefs.MASTER_ELECTION, acl);

            // block until we acquire that lock.
            zlock.lock();
            
            try {
                
                runAsMaster(service, zlock);
                
            } finally {

                zlock.unlock();
                
            }

        }

        /**
         * Invoked when this service becomes the master. If there is only one
         * physical service instance running for a given logical service, then
         * it should gain the {@link ZLock} and run as the master. If there is
         * more than one physical service instance for a given logical service,
         * then they will all compete for the same {@link ZLock}. That
         * competition will place them into an order. The order of the services
         * for the lock node is their failover order. The master is always the
         * first service in the queue. If the master dies, then the next
         * surviving service in queue will gain the lock.
         * 
         * @param service
         * @param zlock
         * @throws InterruptedException
         * 
         * @todo If someone deletes the master's lock then the master will
         *       notice (if it checks the zlock) and see that it is no longer
         *       the master.
         *       <p>
         *       In order to prevent two services from running as "masters" at
         *       the same time it is important that the master notice that it
         *       has lost the lock BEFORE zookeeper clears its ephemeral znode.
         *       The clue is the disconnect event from the zookeeper client.
         *       That can set a volatile flag that is used to disable the
         *       master. A disabled master should immediately cease responding,
         *       terminating all outstanding requests.
         * 
         * @todo the behavior needs to be delegated to the service. there is no
         *       API for that right now. all of this is just stubbed out for the
         *       moment.
         */
        protected void runAsMaster(AbstractService service, ZLock zlock)
                throws InterruptedException {

            log.warn("Service is now the master.");

            Thread.sleep(Long.MAX_VALUE);
            
        }
        
    }
    
    /**
     * Logs a message. If the service is no longer registered with any
     * {@link ServiceRegistrar}s then logs an error message.
     * <p>
     * Note: a service that is no longer registered with any
     * {@link ServiceRegistrar}s is no longer discoverable but it remains
     * accessible to clients which already have its proxy. If a new
     * {@link ServiceRegistrar} accepts registration by the service then it will
     * become discoverable again as well.
     * <p>
     * Note: This is only invoked if the automatic lease renewal by the lease
     * manager is denied by the service registrar.
     */
    public void notify(LeaseRenewalEvent event) {
        
        log.warn("Lease could not be renewed: " + event);

        /*
         * Note: Written defensively in case this.joinManager is asynchronously
         * cleared or terminated.
         */
        try {
            
            final JoinManager joinManager = this.joinManager;

            if (joinManager != null) {

                final ServiceRegistrar[] a = joinManager.getJoinSet();

                if (a.length == 0) {

                    log
                            .error("Service not registered with any service registrars");

                } else {

                    if (INFO)
                        log.info("Service remains registered with " + a.length
                                + " service registrars");
                    
                }

            }

        } catch (Exception ex) {

            log.error("Problem obtaining joinSet? : " + ex, ex);

        }

    }

    /**
     * Shutdown the server, including the service and any jini processing. It
     * SHOULD always be safe to invoke this method. The implementation SHOULD be
     * synchronized and SHOULD conditionally handle each class of asynchronous
     * processing or resource, terminating or releasing it iff it has not
     * already been terminated or released.
     * <p>
     * This implementation:
     * <ul>
     * <li>unregisters the proxy, making the service unavailable for future
     * requests and terminating any existing requests</li>
     * <li>{@link IServiceShutdown#shutdownNow()} is invoke if the service
     * implements {@link IServiceShutdown}</li>
     * <li>terminates any asynchronous jini processing on behalf of the server,
     * including service and join management</li>
     * <li>Handles handshaking with the {@link NonActivatableServiceDescriptor}</li>
     * </ul>
     * <p>
     * Note: All errors are trapped, logged, and ignored.
     * <p>
     * Note: Normally, extended shutdown behavior is handled by the service
     * implementation, not the server. However, subclasses MAY extend this
     * method to terminate any additional processing and release any additional
     * resources, taking care to (a) declare the method as <strong>synchronized</strong>,
     * conditionally halt any asynchonrous processing not already halted,
     * conditionally release any resources not already released, and trap, log,
     * and ignored all errors.
     * <p>
     * Note: This is run from within the {@link ShutdownThread} in response to a
     * request to destroy the service.
     */
    synchronized public void shutdownNow() {

        if (shuttingDown) {
            
            // break recursion.
            return;
            
        }
        
        shuttingDown = true;
        
        /*
         * Unexport the proxy, making the service no longer available.
         * 
         * Note: If you do not do this then the client can still make requests
         * even after you have terminated the join manager and the service is no
         * longer visible in the service browser.
         */
        try {
        
            if (INFO)
                log.info("Unexporting the service proxy.");

            unexport(true/* force */);

        } catch (Throwable ex) {

            log.error("Problem unexporting service: " + ex, ex);

            /* Ignore */

        }

        /*
         * Note: We don't have to do this explicitly. The node will go away as
         * soon as we close the Zookeeper client.
         */
//        /*
//         * Unregister the service from zookeeper (delete its ephemeral node).
//         */
//        if (impl != null && impl instanceof AbstractService) {
//
//            try {
//                
//                final JiniFederation fed = (JiniFederation) ((AbstractService) impl)
//                        .getFederation();
//
//                final ZooKeeper zookeeper = fed.getZookeeper();
//
//                if (zookeeper != null) {
//
//                    if (INFO)
//                        log.info("Deleting service znode: "
//                                + physicalServiceZPath);
//
//                    zookeeper.delete(physicalServiceZPath, -1/* version */);
//
//                }
//                
//            } catch (Throwable t) {
//
//                log.error("Problem deleting service znode: " + t, t);
//
//                /* Ignore */
//
//            }
//
//        }

        /*
         * Invoke the service's own logic to shutdown its processing.
         */
        if (impl != null && impl instanceof IServiceShutdown) {
            
            try {
                
                final IServiceShutdown tmp = (IServiceShutdown) impl;

                if (tmp != null && tmp.isOpen()) {

                    /*
                     * Note: The test on isOpen() for the service is deliberate.
                     * The service implementations invoke server.shutdownNow()
                     * from their shutdown() and shutdownNow() methods in order
                     * to terminate the jini facets of the service. Therefore we
                     * test in service.isOpen() here in order to avoid a
                     * recursive invocation of service.shutdownNow().
                     */

                    tmp.shutdownNow();
                    
                }
                
            } catch(Throwable ex) {
                
                log.error("Problem with service shutdown: "+ex, ex);
                
                // ignore.
                
            } finally {
                
                impl = null;
                
            }
            
        }
        
        /*
         * Terminate manager threads.
         */
        
        try {

            terminate();
        
        } catch (Throwable ex) {
            
            log.error("Could not terminate async threads (jini, zookeeper): "
                    + ex, ex);
            
            // ignore.

        }

        /*
         * Hand-shaking with the NonActivableServiceDescriptor.
         */
        if (lifeCycle != null) {
            
            try {

                lifeCycle.unregister(this);

            } catch (Throwable ex) {

                log.error("Could not unregister lifeCycle: " + ex, ex);

                // ignore.

            } finally {

                lifeCycle = null;

            }

        }
        
        // wake up so that run() will exit. 
        synchronized(keepAlive) {
            
            keepAlive.notify();
            
        }
        
    }
    private boolean shuttingDown = false;

    /**
     * Terminates service management threads.
     * <p>
     * Subclasses which start additional service managment threads SHOULD extend
     * this method to terminate those threads. The implementation should be
     * <strong>synchronized</strong>, should conditionally terminate each
     * thread, and should trap, log, and ignore all errors.
     */
    synchronized protected void terminate() {

        if (INFO)
            log.info("Terminating service management threads.");

        if (joinManager != null) {
            
            try {

                joinManager.terminate();

            } catch (Throwable ex) {

                log.error("Could not terminate the join manager: " + ex, ex);

            } finally {
                
                joinManager = null;

            }

        }
        
        if (client != null) {

            if(client.isConnected()) {

                /*
                 * Note: This will close the zookeeper client and that will
                 * cause the ephemeral znode for the service to be removed.
                 */

//                if (INFO)
//                    log.info("Disconnecting from federation");
                
                client.disconnect(true/* immediateShutdown */);
                
            }

            client = null;
            
        }
        
    }
    
    /**
     * Run the server (this should be invoked from <code>main</code>.
     */
    public void run() {

        if (INFO)
            log.info("Started server.");

        /*
         * Name the thread for the class of server that it is running.
         * 
         * Note: This is generally the thread that ran main(). The thread does
         * not really do any work - it just waits until the server is terminated
         * and then returns to the caller where main() will exit.
         */
        try {

            Thread.currentThread().setName(getClass().getName());
            
        } catch(SecurityException ex) {
            
            // ignore.
            log.warn("Could not set thread name: " + ex);
            
        }
        
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
        
        synchronized (keepAlive) {
            
            try {
                
                keepAlive.wait();
                
            } catch (InterruptedException ex) {
                
                if (INFO)
                    log.info(ex.getLocalizedMessage());

            } finally {
                
                // terminate.
                
                shutdownNow();
                
            }
            
        }
        
        log.fatal("Service is down: class=" + getClass().getName() + ", name="
                + serviceName);
        
    }

    private Object keepAlive = new Object();
    
    /**
     * Runs {@link AbstractServer#shutdownNow()} and terminates all asynchronous
     * processing, including discovery.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ShutdownThread extends Thread {
        
        final AbstractServer server;
        
        public ShutdownThread(final AbstractServer server) {
            
            super("shutdownThread");
            
            if (server == null)
                throw new IllegalArgumentException();
            
            this.server = server;
            
            setDaemon(true);
            
        }
        
        public void run() {
            
            try {

                if (INFO)
                    log.info("Running shutdown.");

                /*
                 * Note: This is the "server" shutdown. It will delegate to the
                 * service shutdown protocol as well as handle unexport of the
                 * service and termination of jini processing.
                 */
                
                server.shutdownNow();
                
            } catch (Exception ex) {

                log.error("While shutting down service: " + ex, ex);

            }

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
    synchronized public void destroy() {

        shutdownNow();
        
        if (INFO)
            log.info("Deleting: " + serviceIdFile);

        if (!serviceIdFile.delete()) {

            log.warn("Could not delete file: " + serviceIdFile);

        }
        
        // wake up so that run() will exit. 
        synchronized(keepAlive) {
            
            keepAlive.notify();
            
        }
        
    }

    /**
     * Runs {@link #destroy()} and logs start and end events.
     */
    public void runDestroy() {

        final Thread t = new Thread("destroyService") {

            public void run() {

                // note: MAY be null.
                final Remote impl = AbstractServer.this.impl;
                
                // note: MAY be null.
                final ServiceID serviceID = AbstractServer.this.serviceID;
                
                // format log message.
                final String msg = "name="
                        + serviceName
                        + (impl == null ? "" : ", class="+impl.getClass())
                        + (serviceID == null ? "" : ", serviceUUID="
                                + JiniUtil.serviceID2UUID(serviceID));
                
                log.warn("will destroy service: " + msg);

                AbstractServer.this.destroy();

                log.warn("service destroyed: " + msg);

            }

        };

        t.setDaemon(true);

        t.start();

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
    
}
