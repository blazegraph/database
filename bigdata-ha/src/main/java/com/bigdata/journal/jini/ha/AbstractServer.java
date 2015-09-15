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
 * Created on Mar 18, 2007
 */

package com.bigdata.journal.jini.ha;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.rmi.Remote;
import java.rmi.server.ExportException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import net.jini.admin.JoinAdmin;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.export.Exporter;
import net.jini.jeri.BasicILFactory;
import net.jini.jeri.BasicJeriExporter;
import net.jini.jeri.tcp.TcpServerEndpoint;
import net.jini.lease.LeaseListener;
import net.jini.lease.LeaseRenewalEvent;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.JoinManager;
import net.jini.lookup.ServiceIDListener;
import net.jini.lookup.entry.Name;

import org.apache.log4j.Logger;

import com.bigdata.Banner;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.PIDUtil;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.RunState;
import com.bigdata.jini.lookup.entry.Hostname;
import com.bigdata.jini.lookup.entry.ServiceUUID;
import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.service.AbstractService;
import com.bigdata.service.IService;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.jini.DataServer.AdministrableDataService;
import com.bigdata.service.jini.FakeLifeCycle;
import com.bigdata.service.jini.JiniClientConfig;
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
 * You must specify the JVM property
 * <code>-Dcom.sun.jini.jeri.tcp.useNIO=true</code> to enable NIO.
 * <p>
 * 
 * Other command line options MAY be recommended depending on the JVM and the
 * service that you are starting, e.g., <code>-server</code>.
 * <p>
 * The service may be <em>terminated</em> by terminating the server process.
 * Termination implies that the server stops execution but that it MAY be
 * restarted. A {@link Runtime#addShutdownHook(Thread) shutdown hook} is
 * installed by the server so that you can also stop the server using ^C
 * (Windows) and <code>kill</code> <i>pid</i> (Un*x). You can record the PID of
 * the process running the server when you start it under Un*x using a shell
 * script. Note that if you are starting multiple services at once with the
 * {@link ServiceStarter} then these methods (^C or kill <i>pid</i>) will take
 * down all servers running in the same VM.
 * <p>
 * Services may be <em>destroyed</em> using {@link DestroyAdmin}, e.g., through
 * the Jini service browser. Note that all persistent data associated with that
 * service is also destroyed!
 * 
 * <p>
 * Note: This class was cloned from the com.bigdata.service.jini package.
 * Zookeeper support was stripped out and the class was made to align with a
 * write replication pipeline for {@link HAJournal} rather than with a
 * federation of bigdata services. However, {@link HAGlue} now extends
 * {@link IService} and we are using zookeeper, so maybe we can line these base
 * classes up again? The timing of service destroy and service shutdown has been
 * modified. The {@link ServiceID} is now properly taken from the file if it was
 * set there (the original code might or might not have been correct in this
 * regard). This needs to be reconciled with the federation. The federation uses
 * ephemeral sequential to create the logical service identifiers. Here they are
 * being assigned manually. This is basically the "flex" versus "static" issue
 * and would also need to be reconciled.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractServer implements Runnable, LeaseListener,
        ServiceIDListener, DiscoveryListener {
    
    final static private Logger log = Logger.getLogger(AbstractServer.class);

    /**
     * Configuration options.
     * <p>
     * Note: The options declared by this interface are in the namespace for the
     * concrete implementation of the {@link AbstractServer} class.
     * <p>
     * Note: The {@link ServiceID} is optional and may be specified using the
     * {@link Entry}[] for the {@link JiniClientConfig} as a {@link ServiceUUID}
     * . If it is not specified, then a random {@link ServiceID} will be
     * assigned. Either way, the {@link ServiceID} is written into the
     * {@link #SERVICE_DIR} and is valid for the life cycle of the persistent
     * service.
     * 
     * @see JiniClientConfig
     * @see ZookeeperClientConfig
     */
    public interface ConfigurationOptions extends HAClient.ConfigurationOptions {

        /**
         * The pathname of the service directory as a {@link File} (required).
         */
        String SERVICE_DIR = "serviceDir";

        /**
         * This object is used to export the service proxy. The choice here
         * effects the protocol that will be used for communications between the
         * clients and the service. The default {@link Exporter} if none is
         * specified is a {@link BasicJeriExporter} using a
         * {@link TcpServerEndpoint}.
         */
        String EXPORTER = "exporter";

    }

//    /**
//     * The timeout in milliseconds to await the discovery of a service if there
//     * is a cache miss (default {@value #DEFAULT_CACHE_MISS_TIMEOUT}).
//     */
//    final protected long cacheMissTimeout;
    
    /**
     * The {@link ServiceID} for this server is either read from a local file,
     * assigned by the registrar (if this is a new service instance), or given
     * in a {@link ServiceUUID} entry in the {@link Configuration} (for either a
     * new service or the restart of a persistent service). If the
     * {@link ServiceID} is assigned by jini, then it is assigned the
     * asynchronously after the service has discovered a
     * {@link ServiceRegistrar}.
     * 
     * @see ConfigurationOptions
     */
    private final AtomicReference<ServiceID> serviceIDRef = new AtomicReference<ServiceID>();
    
    /**
     * The directory for the service. This is the directory within which the
     * {@link #serviceIdFile} exists. A service MAY have its own concept of a
     * data directory, log directory, etc. which can be somewhere else.
     */
    private File serviceDir;
    
    /**
     * The file where the {@link ServiceID} will be written / read.
     */
    private File serviceIdFile;

    /**
     * The PID (best guess).
     * 
     * @see PIDUtil#getPID()
     */
    private final int pid;

    /**
     * The file on which the PID was written.
     * @see #writePIDFile(File)
     */
    private File pidFile;

    /**
     * An attempt is made to obtain an exclusive lock on a file in the same
     * directory as the {@link #serviceIdFile}. If the {@link FileLock} can be
     * obtained then the reference for that {@link RandomAccessFile} is set on
     * this field. If the lock is already held by another process then the
     * server will refuse to start. Since some platforms (NFS volumes, etc.) do
     * not support {@link FileLock} and the server WILL start anyway in those
     * cases. The {@link FileLock} is automatically released if the JVM dies or
     * if the {@link FileChannel} is closed. It is automatically released by
     * {@link #run()} before the server exits or if the ctor fails.
     */
    private RandomAccessFile lockFileRAF = null;
    private FileLock fileLock;
    private File lockFile;

    /**
     * The reference to the {@link HAClient}.
     */
    private final HAClient haClient;

//    private LookupDiscoveryManager lookupDiscoveryManager;
//
//    private ServiceDiscoveryManager serviceDiscoveryManager;

    /**
     * Used to manage the join/leave of the service hosted by this server with
     * Jini service registrar(s).
     */
    private JoinManager joinManager;

    /**
     * Used to suppor the {@link #joinManager}.
     */
    private volatile LookupDiscoveryManager lookupDiscoveryManager = null;

    /**
     * The {@link Configuration} read based on the args[] provided when the
     * server is started.
     */
    protected final Configuration config;

    private final JiniClientConfig jiniClientConfig;

    /**
     * The as-configured entry attributes.
     */
    private final List<Entry> entries;
    
    /**
     * A configured name for the service -or- a default value if no {@link Name}
     * was found in the {@link Configuration}.
     */
    private String serviceName;

    /**
     * The configured name for the service a generated service name if no
     * {@link Name} was found in the {@link Configuration}.
     * <p>
     * Note: Concrete implementations MUST prefer to report this name in the
     * {@link AbstractService#getServiceName()} of their service implementation
     * class. E.g., {@link AdministrableDataService#getServiceName()}.
     */
    final public String getServiceName() {
        
        return serviceName;
        
    }

    /**
     * Responsible for exporting a proxy for the service. Note that the
     * {@link Exporter} is paired to a single service instance. It CAN NOT be
     * used to export more than one object at a time! Therefore the
     * {@link Configuration} entry for the <code>exporter</code> only effects
     * how <em>this</em> server exports its service.
     * 
     * @see ConfigurationOptions#EXPORTER
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
     * The name of the host on which the server is running.
     */
    protected String getHostName() {
        
        return hostname;
        
    }
    private String hostname;
    
    /**
     * The object used to inform the hosting environment that the server is
     * unregistering (terminating). A fake object is used when the server is run
     * from the command line, otherwise the object is supplied by the
     * {@link NonActivatableServiceDescriptor}.
     */
    private LifeCycle lifeCycle;

    /**
     * The {@link RunState} for the server.
     */
    final private AtomicReference<RunState> runState;

    /**
     * {@link AtomicBoolean} serves as both a condition variable and the monitor
     * on which we wait.
     */
    final private AtomicBoolean keepAlive = new AtomicBoolean(true);

    /**
     * The exported proxy for the service implementation object.
     * 
     * @see #getRemoteImpl()
     */
    public Remote getProxy() {
        
        return proxy;
        
    }
    
    /**
     * The service implementation object.
     * 
     * @see #getProxy()
     */
    public Remote getRemoteImpl() {
        
        return impl;
        
    }
    
    /**
     * Return the assigned {@link ServiceID}. If this is a new service and the
     * {@link ServiceUUID} was not specified in the {@link Configuration} then
     * the {@link ServiceID} will be <code>null</code> until it has been
     * assigned by a {@link ServiceRegistrar}.
     */
    public ServiceID getServiceID() {
        
        return serviceIDRef.get();
        
    }
    
    /**
     * Return the service directory.
     * 
     * @see ConfigurationOptions#SERVICE_DIR
     */
    public File getServiceDir() {
        
        return serviceDir;
        
    }
    
    /**
     * The best guess at the process identifier for this process.
     */
    final protected int getPID() {
        
        return pid;
        
    }
    
    /**
     * <code>true</code> iff this is a persistent service (one that you can
     * shutdown and restart).
     */
    protected boolean isPersistent() {
        
        return true;
        
    }
    
    protected JoinManager getJoinManager() {
        
        return joinManager;
        
    }

    /**
     * The {@link HAClient}.
     */
    protected final HAClient getHAClient() {
        
        return haClient;
        
    }
    
    /*
     * DiscoveryListener
     */
    
    /**
     * Lock controlling access to the {@link #discoveryEvent} {@link Condition}.
     */
    private final ReentrantLock discoveryEventLock = new ReentrantLock();

    /**
     * Condition signaled any time there is a {@link DiscoveryEvent} delivered to
     * our {@link DiscoveryListener}.
     */
    private final Condition discoveryEvent = discoveryEventLock
            .newCondition();

    /**
     * Signals anyone waiting on {@link #discoveryEvent}.
     */
    @Override
    public void discarded(final DiscoveryEvent e) {

        try {
            
            discoveryEventLock.lockInterruptibly();
            
            try {
                
                discoveryEvent.signalAll();
            
            } finally {
                
                discoveryEventLock.unlock();
                
            }
            
        } catch (InterruptedException ex) {
            
            return;
            
        }
        
    }

    /**
     * Signals anyone waiting on {@link #discoveryEvent}.
     */
    @Override
    public void discovered(final DiscoveryEvent e) {

        try {

            discoveryEventLock.lockInterruptibly();

            try {

                discoveryEvent.signalAll();

            } finally {

                discoveryEventLock.unlock();

            }

        } catch (InterruptedException ex) {

            return;

        }
        
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
         
            if (log.isInfoEnabled())
                log.info("Set security manager");

        } else {

            if (log.isInfoEnabled())
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
     * This implementation MAY be overridden to invoke {@link System#exit(int)}
     * IFF it is known that the server is being invoked from a command line
     * context. However in no case should execution be allowed to return to the
     * caller.
     */
    protected void fatal(final String msg, final Throwable t) {
       
        log.fatal(msg, t);
        
        try {

            shutdownNow(false/* destroy */);

        } catch (Throwable t2) {
            
            log.error(this, t2);
            
        }
        
        throw new RuntimeException( msg, t );
        
    }

    /**
     * Note: AbstractServer(String[]) is private to ensure that the ctor
     * hierarchy always passes down the variant which accepts the {@link LifeCycle}
     * as well.  This simplifies additional initialization in subclasses. 
     */
    @SuppressWarnings("unused")
    private AbstractServer(final String[] args) {
        
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
        
        runState = new AtomicReference<RunState>(RunState.Start);
        
        /*
         * Display the banner.
         * 
         * Note: This also installs the UncaughtExceptionHandler.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/601
         */
        Banner.banner();

        if (lifeCycle == null)
            throw new IllegalArgumentException();
        
        this.lifeCycle = lifeCycle;

        setSecurityManager();

        // Note the process id (best guess).
        this.pid = PIDUtil.getPID();
        
        /*
         * The runtime shutdown hook appears to be a robust way to handle ^C by
         * providing a clean service termination.
         * 
         * Note: This is setup before we start any async threads, including
         * service discovery.
         */
        Runtime.getRuntime().addShutdownHook(
                new ShutdownThread(false/* destroy */, this));

        /*
         * Read jini configuration & service properties 
         */

        List<Entry> entries = null;
        
        final String COMPONENT = getClass().getName();
        try {

            // Create client.
            haClient = new HAClient(args);

            config = ConfigurationProvider.getInstance(args);
            
//            cacheMissTimeout = (Long) config.getEntry(COMPONENT,
//                    ConfigurationOptions.CACHE_MISS_TIMEOUT, Long.TYPE,
//                    ConfigurationOptions.DEFAULT_CACHE_MISS_TIMEOUT);

            jiniClientConfig = new JiniClientConfig(
                    JiniClientConfig.Options.NAMESPACE, config);

            // convert Entry[] to a mutable list.
            entries = new LinkedList<Entry>(
                    Arrays.asList((Entry[]) jiniClientConfig.entries));
            
            if (log.isInfoEnabled())
                log.info(jiniClientConfig.toString());

            /*
             * Make sure that the parent directory exists.
             * 
             * Note: the parentDir will be null if the serviceIdFile is in the
             * root directory or if it is specified as a filename without any
             * parents in the path expression. Note that the file names a file
             * in the current working directory in the latter case and the root
             * always exists in the former - and in both of those cases we do
             * not have to create the parent directory.
             */
            serviceDir = (File) config.getEntry(COMPONENT,
                    ConfigurationOptions.SERVICE_DIR, File.class); 

            if (serviceDir != null && !serviceDir.exists()) {

                log.warn("Creating: " + serviceDir);

                serviceDir.mkdirs();

            }

            // The file on which the ServiceID will be written.
            serviceIdFile = new File(serviceDir, "service.id");

            if (serviceIdFile.exists()) {

                final ServiceID tmp;
                try {

                    // Read from file.
                    tmp = readServiceId(serviceIdFile);

                } catch (IOException ex) {

                    fatal("Could not read serviceID from existing file: "
                            + serviceIdFile + ": " + this, ex);
                    throw new AssertionError();// keeps compiler happy.

                }

                if (log.isInfoEnabled())
                    log.info("Existing service instance: serviceID=" + tmp);

                // Set the ServiceID that we read from the file.
                setServiceID(tmp);

            } else {

                if (log.isInfoEnabled())
                    log.info("New service instance.");

            }

            // The lock file.
            lockFile = new File(serviceDir, ".lock");

            /*
             * Attempt to acquire an exclusive lock on a file in the same
             * directory as the serviceIdFile.
             */
            acquireFileLock();
            
            writePIDFile(pidFile = new File(serviceDir, "pid"));
            
            /*
             * Make sure that there is a Name and Hostname associated with the
             * service. If a ServiceID was pre-assigned in the Configuration
             * then we will extract that also.
             */
            {
                
                String serviceName = null;
                
                String hostname = null;

                /*
                 * If the ServiceID was read from the serviceId file, then we
                 * want to use that ServiceID and not whatever was in the
                 * Configuration.
                 */
                final ServiceID serviceID = getServiceID();
                UUID serviceUUID = serviceID == null ? null : JiniUtil
                        .serviceID2UUID(serviceID);

                for (Entry e : entries) {

                    if (e instanceof Name && serviceName == null) {

                        // found a name.
                        serviceName = ((Name) e).name;

                    }

                    if (e instanceof Hostname && hostname == null) {

                        hostname = ((Hostname) e).hostname;

                    }

                    if (e instanceof ServiceUUID && serviceUUID == null) {

                        serviceUUID = ((ServiceUUID) e).serviceUUID;

                    }

                }

                // if serviceName not given then set it now.
                if (serviceName == null) {

                    // assign a default service name.

                    final String defaultName = getClass().getName()
                            + "@"
                            + AbstractStatisticsCollector.fullyQualifiedHostName
                            + "#" + hashCode();

                    serviceName = defaultName;

                    // add to the Entry[].
                    entries.add(new Name(serviceName));

                }

                // set the field on the class.
                this.serviceName = serviceName;

                // if hostname not given then set it now.
                if (hostname == null) {

                    /*
                     * @todo This is a best effort during startup and unchanging
                     * thereafter. We should probably report all names for the
                     * host and report the current names for the host. However
                     * there are a number of issues where similar data are not
                     * being updated which could lead to problems if host name
                     * assignments were changed.
                     */
                    
                    hostname = AbstractStatisticsCollector.fullyQualifiedHostName;

                    // add to the Entry[].
                    entries.add(new Hostname(hostname));

                }
            
                // set the field on the class.
                this.hostname = hostname;

                // if serviceUUID assigned then set ServiceID from it now.
                if (serviceUUID != null) {

                    // Convert ServiceID read from Configuration.
                    final ServiceID tmp = JiniUtil.uuid2ServiceID(serviceUUID);

                    // Already assigned ServiceID (may be null).
                    final ServiceID existingServiceID = getServiceID();
                    
                    if (existingServiceID != null) {

                       // Already set.
                       if (!existingServiceID.equals(tmp)) {

                            /*
                             * This is a paranoia check on the Configuration and
                             * the serviceIdFile. The ServiceID should never
                             * change so these values should remain in
                             * agreement.
                             */

                            throw new ConfigurationException(
                                    "ServiceID in Configuration does not agree with the value in "
                                            + serviceIdFile
                                            + " : Configuration=" + tmp
                                            + ", but expected ="
                                            + existingServiceID);

                        }
                        
                    } else {

                        // Set serviceID.
                        setServiceID(JiniUtil.uuid2ServiceID(serviceUUID));

                    }

                    if (!serviceIdFile.exists()) {

                        // write the file iff it does not exist.
                        writeServiceIDOnFile(tmp);

                    } else {
                        /*
                         * The file exists, so verify that it agrees with the
                         * assigned ServiceID.
                         */
                        try {
                            final ServiceID tmp2 = readServiceId(serviceIdFile);
                            if (!tmp.equals(tmp2)) {
                                /*
                                 * The assigned ServiceID and ServiceID written
                                 * on the file do not agree.
                                 */
                                throw new RuntimeException(
                                        "Entry has ServiceID=" + tmp
                                                + ", but file as ServiceID="
                                                + tmp2);
                            }
                        } catch (IOException e1) {
                            throw new RuntimeException(e1);
                        }
                    }

                } else if (!serviceIdFile.exists()) {

                    /*
                     * Since nobody assigned us a ServiceID and since there is
                     * none on record in the [serviceIdFile], we assign one now
                     * ourselves.
                     */
                    
                    // Generate a random ServiceID.
                    final ServiceID tmp = JiniUtil.uuid2ServiceID(UUID
                            .randomUUID());
                    
                    // Set our ServiceID.
                    setServiceID(tmp);
                    
                    // Write the file iff it does not exist.
                    writeServiceIDOnFile(tmp);

                }
                
            }

            // Save a reference to the as-configured Entry[] attributes.
            this.entries = entries;

            /*
             * Extract how the service will provision itself from the
             * Configuration.
             */

            // The exporter used to expose the service proxy.
            exporter = (Exporter) config.getEntry(//
                    getClass().getName(), // component
                    ConfigurationOptions.EXPORTER, // name
                    Exporter.class, // type (of the return object)
                    /*
                     * The default exporter is a BasicJeriExporter using a
                     * TcpServerEndpoint.
                     */
                    new BasicJeriExporter(TcpServerEndpoint.getInstance(0),
                            new BasicILFactory())
                    );

        } catch (ConfigurationException ex) {

            fatal("Configuration error: " + this, ex);
            throw new AssertionError();// keeps compiler happy.
        }
        
    }

    /**
     * Simple representation of state (non-blocking, safe). Some fields reported
     * in the representation may be <code>null</code> depending on the server
     * state.
     */
    @Override
    public String toString() {
        
        // note: MAY be null.
        final ServiceID serviceID = this.serviceIDRef.get();
                
        return getClass().getName()
                + "{serviceName="
                + serviceName
                + ", hostname="
                + hostname
                + ", serviceUUID="
                + (serviceID == null ? "null" : ""
                        + JiniUtil.serviceID2UUID(serviceID)) + "}";
        
    }
    
    /**
     * Attempt to acquire an exclusive lock on a file in the same directory as
     * the {@link #serviceIdFile} (non-blocking). This is designed to prevent
     * concurrent service starts and service restarts while the service is
     * already running.
     * <p>
     * Note: The {@link FileLock} (if acquired) will be automatically released
     * if the process dies. It is also explicitly closed by
     * {@link #shutdownNow()}. DO NOT use advisory locks since they are not
     * automatically removed if the service dies.
     * 
     * @throws RuntimeException
     *             if the file is already locked by another process.
     */
    private void acquireFileLock() {

        try {

            lockFileRAF = new RandomAccessFile(lockFile, "rw");

        } catch (IOException ex) {

            /*
             * E.g., due to permissions, etc.
             */

            throw new RuntimeException("Could not open: file=" + lockFile, ex);

        }

        try {

            fileLock = lockFileRAF.getChannel().tryLock();
            
            if (fileLock == null) {

                /*
                 * Note: A null return indicates that someone else holds the
                 * lock.
                 */

                try {
                    lockFileRAF.close();
                } catch (Throwable t) {
                    // ignore.
                } finally {
                    lockFileRAF = null;
                }

                throw new RuntimeException("Service already running: file="
                        + lockFile);

            }

        } catch (IOException ex) {

            /*
             * Note: This is true of NFS volumes.
             */

            log.warn("FileLock not supported: file=" + lockFile, ex);

        }

    }

    /**
     * Writes the PID on a file in the service directory (best attempt to obtain
     * the PID). If the server is run from the command line, then the pid will
     * be the pid of the server. If you are running multiple servers inside of
     * the same JVM, then the pid will be the same for each of those servers.
     */
    private void writePIDFile(final File file) {

        try {

            // best guess at the PID of the JVM.
            final String pidStr = Integer.toString(this.pid);

            // open the file.
            final FileOutputStream os = new FileOutputStream(file);

            try {

                // discard anything already in the file.
                os.getChannel().truncate(0L);
                
                // write on the PID using ASCII characters.
                os.write(pidStr.getBytes("ASCII"));

                // flush buffers.
                os.flush();

            } finally {

                // and close the file.
                os.close();

            }

        } catch (IOException ex) {

            log.warn("Could not write pid: file=" + file, ex);

        }

    }

    /**
     * Attempt to start lookup discovery.
     * <p>
     * Note: The returned service will perform multicast discovery if ALL_GROUPS
     * is specified and otherwise requires you to specify one or more unicast
     * locators (URIs of hosts running discovery services).
     * 
     * @see JiniClientConfig
     * @see JiniClientConfig#Options
     * 
     * @throws ConfigurationException
     * @throws IOException
     */
    private void startLookupDiscoveryManager(final Configuration config)
            throws ConfigurationException, IOException {

        if (lookupDiscoveryManager == null) {
            
            log.info("Starting lookup discovery.");
            
            final String[] groups = jiniClientConfig.groups;

            final LookupLocator[] lookupLocators = jiniClientConfig.locators;

            this.lookupDiscoveryManager = new LookupDiscoveryManager(groups,
                    lookupLocators, null /* DiscoveryListener */, config);
        
        }
        
    }

    /**
     * Await discovery of at least one {@link ServiceRegistrar}.
     * 
     * @param timeout
     *            The timeout.
     * @param unit
     *            The units for that timeout.
     * 
     * @throws IllegalArgumentException
     *             if minCount is non-positive.
     */
    protected ServiceRegistrar[] awaitServiceRegistrars(final long timeout,
            final TimeUnit unit) throws TimeoutException,
            InterruptedException {

        if (lookupDiscoveryManager == null)
            throw new IllegalStateException();
        
        final long begin = System.nanoTime();
        final long nanos = unit.toNanos(timeout);
        long remaining = nanos;

        ServiceRegistrar[] registrars = null;

        while ((registrars == null || registrars.length == 0)
                && remaining > 0) {

            registrars = lookupDiscoveryManager.getRegistrars();

            Thread.sleep(100/* ms */);

            final long elapsed = System.nanoTime() - begin;

            remaining = nanos - elapsed;

        }

        if (registrars == null || registrars.length == 0) {

            throw new RuntimeException(
                    "Could not discover ServiceRegistrar(s)");

        }

        if (log.isInfoEnabled()) {

            log.info("Found " + registrars.length + " service registrars");

        }

        return registrars;

    }

    /**
     * Export a proxy object for this service instance.
     * 
     * @throws IOException
     * @throws ConfigurationException
     */
    synchronized private void exportProxy(final Remote impl)
            throws ConfigurationException, IOException {

        /*
         * Export a proxy object for this service instance.
         * 
         * Note: This must be done before we start the join manager since the
         * join manager will register the proxy.
         */
        try {

            proxy = exporter.export(impl);
            
            if (log.isInfoEnabled())
                log.info("EXPORTED PROXY: Proxy is " + proxy + "("
                        + proxy.getClass() + ")");

        } catch (ExportException ex) {

            fatal("Export error: " + this, ex);
            throw new AssertionError();// keeps compiler happy.
        }

        /*
         * Start the join manager. 
         */
        try {

            assert proxy != null : "No proxy?";

            // The as-configured Entry[] attributes.
            final Entry[] attributes = entries.toArray(new Entry[0]);

            final ServiceID serviceID = getServiceID();
            
            if (serviceID != null) {

                /*
                 * We read the serviceID from local storage (either the
                 * serviceIDFile and/or the Configuration).
                 */
                
                joinManager = new JoinManager(proxy, // service proxy
                        attributes, // attr sets
                        serviceID, // ServiceID
                        lookupDiscoveryManager, // DiscoveryManager
                        new LeaseRenewalManager(), //
                        config);
                
            } else {
                
                /*
                 * We are requesting a serviceID from the registrar.
                 */
                
                joinManager = new JoinManager(proxy, // service proxy
                        attributes, // attr sets
                        this, // ServiceIDListener
                        lookupDiscoveryManager, // DiscoveryManager
                        new LeaseRenewalManager(), //
                        config);
            
            }

        } catch (Exception ex) {

            fatal("JoinManager: " + this, ex);
            throw new AssertionError();// keeps compiler happy.
        }

//        /*
//         * Note: This is synchronized in case set via listener by the
//         * JoinManager, which would be rather fast action on its part.
//         */
//        synchronized (this) {
//
//            final ServiceID serviceID = getServiceID();
//
//            if (serviceID != null) {
//
//                /*
//                 * Notify the service that it's service UUID has been set.
//                 * 
//                 * @todo Several things currently depend on this notification.
//                 * In effect, it is being used as a proxy for the service
//                 * registration event.
//                 */
//
//                notifyServiceUUID(serviceID);
//
//            }
//
//        }

    }
    
    /**
     * Unexports the {@link #proxy}, halt the {@link JoinManager}, and halt the
     * {@link LookupDiscoveryManager}. This is safe to invoke whether or not the
     * proxy is exported.
     * <p>
     * Note: You can not re-export the proxy. The {@link Exporter} does not
     * support this for a given instance. Also, once un-exported, if you create
     * a new {@link Exporter} and then re-export the proxy, the new proxy will
     * be a distinct instance. Most clients expect the life cycle of the proxy
     * to be the life cycle of the service that is being exposed by the proxy.
     * Unexporting and re-exporting will confuse such clients.
     * 
     * @param force
     *            When true, the object is unexported even if there are pending
     *            or in progress service requests.
     * 
     * @return true iff the object is (or was) unexported.
     * 
     * @see Exporter#unexport(boolean)
     */
    synchronized private boolean unexport(final boolean force) {

        boolean unexported = false;

        if (proxy != null) {

            log.warn("UNEXPORT PROXY: force=" + force + ", proxy=" + proxy);

            try {
                if (exporter.unexport(force)) {

                    unexported = true;

                } else {

                    log.warn("Proxy was not unexported? : " + this);

                }
            } finally {

                proxy = null;

            }

        }

        if (joinManager != null) {

            try {

                joinManager.terminate();

            } catch (Throwable ex) {

                log.error("Could not terminate the join manager: " + this, ex);

            } finally {

                joinManager = null;

            }

        }

        return unexported;

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
    static public ServiceID readServiceId(final File file) throws IOException {

        final FileInputStream is = new FileInputStream(file);

        try {

            final ServiceID serviceID = new ServiceID(new DataInputStream(is));

            if (log.isInfoEnabled())
                log.info("Read ServiceID=" + serviceID + "("
                        + JiniUtil.serviceID2UUID(serviceID) + ") from " + file);

            return serviceID;

        } finally {

            is.close();

        }
        
    }

    private void setServiceID(final ServiceID newValue) {

        if (newValue == null)
            throw new IllegalArgumentException();

        if (!serviceIDRef.compareAndSet(null/* expect */, newValue)) {

            throw new IllegalStateException(
                    "ServiceID may not be changed: ServiceID="
                            + serviceIDRef.get() + ", proposed=" + newValue);

        }

        if (log.isInfoEnabled())
            log.info("serviceID=" + newValue + ", serviceUUID="
                    + JiniUtil.serviceID2UUID(newValue));

    }
    
    /**
     * This method is responsible for saving the {@link ServiceID} on stable
     * storage when it is invoked. It will be invoked iff the {@link ServiceID}
     * was not defined and one was therefore assigned.
     * 
     * @param serviceID
     *            The assigned {@link ServiceID}.
     */
    @Override
    synchronized public void serviceIDNotify(final ServiceID serviceID) {

        setServiceID(serviceID);

        assert serviceIdFile != null : "serviceIdFile not defined?";

        writeServiceIDOnFile(serviceID);

//        notifyServiceUUID(serviceID);
        
        /*
         * Update the Entry[] for the service registrars to reflect the assigned
         * ServiceID.
         */
        {

            final List<Entry> attributes = new LinkedList<Entry>(Arrays
                    .asList(joinManager.getAttributes()));

            final Iterator<Entry> itr = attributes.iterator();

            while (itr.hasNext()) {

                final Entry e = itr.next();

                if (e instanceof ServiceUUID) {

                    itr.remove();

                }

            }

            attributes.add(new ServiceUUID(JiniUtil.serviceID2UUID(serviceID)));

            joinManager.setAttributes(attributes.toArray(new Entry[0]));

        }

    }
    
    synchronized private void writeServiceIDOnFile(final ServiceID serviceID) {

        try {

            final DataOutputStream dout = new DataOutputStream(
                    new FileOutputStream(serviceIdFile));

            try {

                serviceID.writeBytes(dout);

                dout.flush();

                if (log.isInfoEnabled())
                    log.info("ServiceID saved: file=" + serviceIdFile
                            + ", serviceID=" + serviceID + ", serviceUUID="
                            + JiniUtil.serviceID2UUID(serviceID));

            } finally {

                dout.close();

            }

        } catch (Exception ex) {

            log.error("Could not save ServiceID : "+this, ex);

        }

    }
    
//    /**
//     * Notify the {@link AbstractService} that it's service UUID has been set.
//     */
//    final protected void notifyServiceUUID(final ServiceID newValue) {
//
//        setServiceID(newValue);
//        
//    }
    
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
    @Override
    public void notify(final LeaseRenewalEvent event) {
        
        log.warn("Lease could not be renewed: " + this + " : " + event);

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

                    if (log.isInfoEnabled())
                        log.info("Service remains registered with " + a.length
                                + " service registrars");
                    
                }

            }

        } catch (Exception ex) {

            log.error("Problem obtaining joinSet? : " + this, ex);

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
     * resources, taking care to (a) declare the method as
     * <strong>synchronized</strong>, conditionally halt any asynchonrous
     * processing not already halted, conditionally release any resources not
     * already released, and trap, log, and ignored all errors.
     * <p>
     * Note: This is run from within the {@link ShutdownThread} in response to a
     * request to destroy the service.
     * 
     * @param destroy
     *            When <code>true</code> the persistent state associated with
     *            the service is also destroyed.
     */
    final protected void shutdownNow(final boolean destroy) {
        
//        if (log.isTraceEnabled())
//            HAJournalTest.dumpThreads();
        
        // Atomically change RunState.
        if (!runState.compareAndSet(RunState.Start, RunState.ShuttingDown)
                && !runState.compareAndSet(RunState.Running,
                        RunState.ShuttingDown)) {

            /*
             * Break recursion. Already shutting down or already shutdown.
             */
 
            return;

        }

        beforeShutdownHook(destroy);
        
        try {
            
            /*
             * Unexport the proxy, making the service no longer available.
             * 
             * Note: If you do not do this then the client can still make
             * requests even after you have terminated the join manager and the
             * service is no longer visible in the service browser.
             */
            try {

                unexport(true/* force */);

            } catch (Throwable ex) {

                log.error("Problem unexporting service: " + this, ex);

                /* Ignore */

            }

            if (destroy && impl != null && impl instanceof IService) {

                final IService tmp = (IService) impl;

                /*
                 * Delegate to the service to destroy its persistent state.
                 */

                try {

                    tmp.destroy();

                } catch (Throwable ex) {

                    log.error("Problem with service destroy: " + this, ex);

                    // ignore.

                }

            }

            /*
             * Invoke the service's own logic to shutdown its processing.
             */
            if (impl != null && impl instanceof IServiceShutdown) {

                try {

                    final IServiceShutdown tmp = (IServiceShutdown) impl;

                    if (tmp != null && tmp.isOpen()) {

                        /*
                         * Note: The test on isOpen() for the service is
                         * deliberate. The service implementations invoke
                         * server.shutdownNow() from their shutdown() and
                         * shutdownNow() methods in order to terminate the jini
                         * facets of the service. Therefore we test in
                         * service.isOpen() here in order to avoid a recursive
                         * invocation of service.shutdownNow().
                         */

                        tmp.shutdownNow();

                    }

                } catch (Throwable ex) {

                    log.error("Problem with service shutdown: " + this, ex);

                    // ignore.

                }

            }

            // discard reference to the service implementation object.
            impl = null;

            /*
             * Terminate manager threads.
             */

            try {

                terminate();

            } catch (Throwable ex) {

                log.error(
                        "Could not terminate async threads (jini, zookeeper): "
                                + this, ex);

                // ignore.

            }

            /*
             * Hand-shaking with the NonActivableServiceDescriptor.
             */
            if (lifeCycle != null) {

                try {

                    lifeCycle.unregister(this);

                } catch (Throwable ex) {

                    log.error("Could not unregister lifeCycle: " + this, ex);

                    // ignore.

                } finally {

                    lifeCycle = null;

                }

            }

            if (destroy) {

                // Delete any files that we recognize in the service directory.
                recursiveDelete(serviceDir);

                /*
                 * Delete files created by this class.
                 */

                // delete the serviceId file.
                if (serviceIdFile.exists() && !serviceIdFile.delete()) {

                    log.warn("Could not delete: " + serviceIdFile);

                }

                // delete the pid file.
                if (pidFile.exists() && !pidFile.delete()) {

                    log.warn("Could not delete: " + pidFile);

                }

            }

            // release the file lock.
            if (lockFileRAF != null && lockFileRAF.getChannel().isOpen()) {

                /*
                 * If there is a file lock then close the backing channel.
                 */

                try {

                    lockFileRAF.close();

                } catch (IOException ex) {

                    // log and ignore.
                    log.warn(this, ex);

                }

            }

            if (destroy && lockFile.exists()) {

                /*
                 * Note: This will succeed if no one has a lock on the file. You
                 * can not delete the file while you are holding the lock. If
                 * another process gets the file lock after we release it
                 * (immediately above) but before we delete the lock file here,
                 * then the delete will fail and the service directory delete
                 * will also fail.
                 */

                if (!lockFile.delete()) {

                    log.warn("Could not delete: " + serviceDir);

                }

            }

            // delete the service directory after we have released the lock.
            if (destroy && serviceDir.exists() && !serviceDir.delete()) {

                log.warn("Could not delete: " + serviceDir);

            }

            /*
             * Note: keepAlive will be null if this code is invoked from within
             * the constructor on the base class (AbstractServer).
             */

            if (keepAlive != null
                    && keepAlive
                            .compareAndSet(true/* expect */, false/* update */)) {

                // wake up so that run() will exit.
                synchronized (keepAlive) {

                    keepAlive.notifyAll();

                }

            }
            
        } finally {

            runState.set(RunState.Shutdown);
            
        }

    }

    /**
     * Hook invoked at the start of the shutdown procedure.
     * 
     * @param destroy
     *            <code>true</code> iff the service will be destroyed.
     */
    protected void beforeShutdownHook(final boolean destroy) {

        // NOP
        
    }

    /**
     * Return the current {@link RunState}.
     */
    public RunState getRunState() {

        return runState.get();

    }
    
    /**
     * Terminates service management threads.
     * <p>
     * Subclasses which start additional service management threads SHOULD
     * extend this method to terminate those threads. The implementation should
     * be <strong>synchronized</strong>, should conditionally terminate each
     * thread, and should trap, log, and ignore all errors.
     */
    synchronized protected void terminate() {

        if (log.isInfoEnabled())
            log.info("Terminating service management threads.");

        // Note: null reference is possible if ctor fails.
        if (haClient != null && haClient.isConnected()) {
        
            haClient.disconnect(false/* immediateShutdown */);
            
        }
        
        if (lookupDiscoveryManager != null) {

            try {

                lookupDiscoveryManager.terminate();

            } catch (Throwable ex) {

                log.error("Could not terminate the lookup discovery manager: "
                        + this, ex);

            } finally {

                lookupDiscoveryManager = null;

            }

        }
        
    }
    
    /**
     * Start the HAJournalServer and wait for it to terminate.
     * <p>
     * Note: This is invoked from within the constructor of the concrete service
     * class. This ensures that all initialization of the service is complete
     * and is compatible with the Apache River ServiceStarter (doing this in
     * main() is not compatible since the ServiceStarter does not expect the
     * service to implement Runnable).
     */
    @Override
    public void run() {

        if (log.isInfoEnabled())
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

        try {

            /*
             * Note: We run our own LookupDiscoveryManager in order to avoid
             * forcing an unexport of the service proxy when the HAClient is
             * terminated.
             */
            startLookupDiscoveryManager(config);
            
            // Create the service object.
            impl = newService(config);

            // Export the service.
            exportProxy(impl);
            
            // Start the service.
            startUpHook();
            
        } catch (Throwable t) {

            fatal("Startup failure", t);

            throw new AssertionError();
            
        }

        if (runState.compareAndSet(RunState.Start, RunState.Running)) {

            {

                final String msg = "Service is running: class="
                        + getClass().getName() + ", name=" + getServiceName();

                System.out.println(msg);

                if (log.isInfoEnabled())
                    log.info(msg);

            }

            /*
             * Wait until the server is terminated.
             */

            synchronized (keepAlive) {

                while (keepAlive.get()) {

                    try {

                        keepAlive.wait();

                    } catch (InterruptedException ex) {

                        if (log.isInfoEnabled())
                            log.info(ex.getLocalizedMessage());

                    }
                }

            }

        }
        
        System.out.println("Service is down: class=" + getClass().getName()
                + ", name=" + getServiceName());

    }

    /**
     * Hook invoked in {@link #run()}.
     */
    protected void startUpHook() {

        // NOP

    }

    /**
     * Runs {@link AbstractServer#shutdownNow()} and terminates all asynchronous
     * processing, including discovery. This is used for the shutdown hook (^C).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class ShutdownThread extends Thread {

        private final boolean destroy;
        private final AbstractServer server;
        
        public ShutdownThread(final boolean destroy, final AbstractServer server) {

            super("shutdownThread");
            
            if (server == null)
                throw new IllegalArgumentException();

            this.destroy = destroy;
            
            this.server = server;
            
            setDaemon(true);
            
        }

        @Override
        public void run() {

            // format log message.
            final String msg = server.toString();

            log.warn("Will " + (destroy ? "destroy" : "shutdown")
                    + " service: " + msg);

            try {

                /*
                 * Wait long enough for the RMI request to end (in case the
                 * service is shutdown in response to an RMI).
                 */
                Thread.sleep(250/* ms */);

            } catch (InterruptedException e) {
            	
                // Propagate interrupt.
                Thread.currentThread().interrupt();

                return;
            }

            try {

                server.shutdownNow(destroy);

                log.warn("Service " + (destroy ? "destroyed" : "shutdown")
                        + ": " + msg);

            } catch (Throwable t) {

                log.error("Problem "
                        + (destroy ? "destroying" : "shutting down")
                        + " service: " + msg, t);

            }

        }

    }

    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself. Only files recognized by
     * {@link #getFileFilter()} will be deleted.
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(final File f) {

        if (f.isDirectory()) {

            final File[] children = f.listFiles(getFileFilter());

            for (int i = 0; i < children.length; i++) {

                recursiveDelete(children[i]);

            }

        }

        if (log.isInfoEnabled())
            log.info("Removing: " + f);

        if (f.exists() && !f.delete()) {

            log.warn("Could not remove: " + f);

        }

    }

    /**
     * Method may be overriden to recognize files in the service directory so
     * they may be automatically deleted by {@link #destroy()} after the
     * {@link IService#destroy()} has been invoked to destroy any files claimed
     * by the service implementation. The default implementation of this method
     * does not recognize any files.
     */
    protected FileFilter getFileFilter() {
        
        return new FileFilter() {

            @Override
            public boolean accept(final File pathname) {

                return false;
                
            }
            
        };
        
    }

    /**
     * Unless the service is already shutting down (or shutdown), this method
     * runs thread which will destroy the service (asynchronous).
     * <p>
     * Note: By running this is a thread, we avoid closing the service end point
     * during the method call.
     */
    protected void runShutdown(final boolean destroy) {

        final Thread t = new ShutdownThread(destroy, this);
        
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
     * @param config
     *            The {@link Configuration}.
     * @throws Exception 
     */
    abstract protected Remote newService(Configuration config)
            throws Exception;
    
}
