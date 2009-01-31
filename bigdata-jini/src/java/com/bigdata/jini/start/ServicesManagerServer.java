/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jan 2, 2009
 */

package com.bigdata.jini.start;

import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.Properties;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationFile;
import net.jini.config.ConfigurationProvider;
import net.jini.core.discovery.LookupLocator;
import net.jini.export.ServerContext;
import net.jini.io.context.ClientHost;
import net.jini.io.context.ClientSubject;
import net.jini.lookup.entry.Name;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.bigdata.btree.IndexSegment;
import com.bigdata.jini.start.config.IServiceConstraint;
import com.bigdata.jini.start.config.JiniCoreServicesConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.ServicesManagerConfiguration;
import com.bigdata.jini.start.config.ZookeeperServerConfiguration;
import com.bigdata.journal.ITransactionService;
import com.bigdata.service.DataService;
import com.bigdata.service.DefaultServiceFederationDelegate;
import com.bigdata.service.IDataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.MetadataService;
import com.bigdata.service.jini.AbstractServer;
import com.bigdata.service.jini.FakeLifeCycle;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.RemoteAdministrable;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.start.ServiceDescriptor;
import com.sun.jini.start.ServiceStarter;

/**
 * A class for bootstrapping a {@link JiniFederation} across a cluster based on
 * a {@link Configuration} file describing some basic configuration parameters
 * and dynamically managing service configurations. The service configurations
 * are stored in zookeeper and each activated service registers both a
 * persistent znode (for restart) and a variety of transient znodes (to register
 * its claim as an instance of a logical service, for master elections, etc).
 * <p>
 * Each host in a cluster runs <em>ONE (1)</em> of this server. The server
 * will start other services on the local host. Those decisions are based on
 * configuration requirements stored in zookeeper, including the target #of
 * logical services of some service type, the replication count for a logical
 * service, and {@link IServiceConstraint} that are used to decide which host
 * will start the new service.
 * <p>
 * Services are broken down by service type (aka class), by logical service
 * instance, and by physical instances of a logical service. Physical service
 * instances may be transient or persistent. The configuration state required to
 * re-start a persistent physical service is stored in a persistent znode
 * representing that service. For transient services, that znode is ephemeral.
 * <p>
 * The initial configuration for the federation given in a {@link Configuration}
 * file. That file is best located on a shared volume or volume image replicated
 * on the hosts in the cluster, along with the various dependencies required to
 * start and run the federation (java, jini, bigdata, log4j configuration,
 * related JARs, etc). The information in the {@link Configuration} is used to
 * discover and/or start jini and zookeeper instance(s). The
 * {@link ServicesManagerServer} maintains a watch on the
 * {@link ServiceConfiguration} znodes for the various kinds of services and
 * will start or stop services as the state of those nodes changes.
 * 
 * <h2>Service Monitoring</h2>
 * 
 * The {@link ServicesManagerServer} maintains watches on znodes representing
 * {@link ServiceConfiguration}s, logical services, physical service instances,
 * etc. If the number of logical services instances for some service type falls
 * below the {@link ServiceConfiguration#serviceCount} then a new logical
 * service will be created. Likewise, if the number of physical service
 * instances for some logical service falls below the
 * {@link ServiceConfiguration#replicationCount} then a new physical service
 * instance will be started.
 * <p>
 * Physical services which are persistent are represented by persistent znodes
 * within the {@link BigdataZooDefs#PHYSICAL_SERVICES_CONTAINER} for a logical
 * service. This means that the #of physical service instances is NOT decreased
 * when persistent physical service dies or is shutdown normally but ONLY when
 * the physical service instance is destroyed (along with its persistent state).
 * If a persistent physical service instance has died and can not be restarted,
 * then you MUST delete its znode in the
 * {@link BigdataZooDefs#PHYSICAL_SERVICES_CONTAINER} before a new physical
 * service will be created for the corresponding logical service.
 * <p>
 * Physical services which are transient are represented by ephemeral znodes. As
 * soon as one dies and zookeeper notices that its client has timed out the
 * ephemeral znode will be deleted and a watch triggered which will result in a
 * new instance being created on some machine running a
 * {@link ServicesManagerServer} joined with the federation.
 * <p>
 * Most bigdata services have persistent state and may be safely shutdown and
 * restarted. To shutdown a bigdata service instance use
 * {@link RemoteDestroyAdmin#shutdown()} or zap it from the command line (not
 * the preferred approach, but that will only take the service down, but not
 * destroy its data).
 * 
 * <h2>Startup</h2>
 * 
 * Zookeeper service and jini instances are handled somewhat specially. Each is
 * a self-contained system and provides its own means for peers to discover each
 * other and for clients to discover the peers. The remainder of the services
 * use jini to perform service discovery and zookeeper to register ephemeral
 * znodes which represent their existence as part of a logical service and allow
 * them to contend in the master election for a logical service. Only those
 * services listed in the {@link ServicesManagerConfiguration} will be
 * considered for start.
 * 
 * <h3>Zookeeper startup</h3>
 * 
 * A zookeeper service instance will be started if one was identified in the
 * {@link Configuration} as running on this host in the
 * {@link ZookeeperServerConfiguration} and none is found to be running at that
 * time.
 * 
 * <h3>Jini startup</h3>
 * 
 * A jini server instance will be considered for start if one is specified for
 * this host in the {@link JiniCoreServicesConfiguration} using a
 * {@link LookupLocator} or if you are using multicast. In addition, the
 * {@link IServiceConstraint}s for the jini server in the {@link Configuration}
 * must be satisified by this host.
 * 
 * <h3>Configuration push</h3>
 * 
 * During startup, the {@link ServiceConfiguration}s will be pushed to
 * zookeeper. This will trigger a variety of watches on the znodes for the
 * service configurations both in this instance and in other
 * {@link ServicesManagerServer}s running on other hosts which are part of the
 * same federation.
 * <p>
 * Since the {@link ServiceConfiguration}s are pushed each time a
 * {@link ServicesManagerServer} is started it is CRITICAL that all
 * {@link ServicesManagerServer}s use the same configuration. The
 * {@link Configuration} may either be a file on a shared volume or a URL.
 * 
 * <h3>Persistent service restart</h3>
 * 
 * Once we are connected to both jini and zookeeper a scan will be performed of
 * the persistent physical services registered in zookeeper. For each such
 * service, if the service was started on this host and can not be discovered
 * using jini, then an attempt will be made to restart the service. We don't
 * want to do this on an ongoing basis because it would cause any service that
 * was deliberately shutdown to be restarted as soon as we discover that it is
 * no longer discoverable.
 * 
 * <h2>Service replication and failover</h2>
 * 
 * Jini and zookeeper each have their own failover models.
 * 
 * <h3>Jini</h3>
 * 
 * You can run a number of jini peers and any client can connect to any
 * registrar. This is all transparent. You can even start a new jini instance
 * after all have died. Services which have already been discovered will remain
 * reachable (assuming that a network path exists) for some time after the last
 * jini registrar has died.
 * 
 * <h3>Zookeeper</h3>
 * 
 * Zookeeper uses a quorum model. You always want to provision an odd number of
 * zookeeper instances. ONE (1) instance gives you no failover (but the instance
 * may be restarted if it dies). THREE (3) instances give you ONE (1) failure.
 * 
 * <h3>Persistent bigdata services</h3>
 * 
 * <strong>Replication and service failover have NOT been implemented for
 * bigdata</strong>
 * <p>
 * When the physical service supports failover, the shutdown of a physical
 * service instance does not take the logical service offline if there are
 * remaining physical services instances for that logical service. However, the
 * loss of all physical instances of a logical service will result in the loss
 * of the persistent state for the logical service as a whole.
 * <p>
 * The two main services which require failover are the
 * {@link ITransactionService} and the {@link IDataService} (which includes the
 * {@link IMetadataService} as a special case). The loss of the
 * {@link ILoadBalancerService} is normally not critical as only history about
 * the system load over time is lost.
 * 
 * <h4>Transaction service</h4>
 * 
 * tdb
 * 
 * <h4>(Meta)data services</h4>
 * 
 * The basic design is for {@link IDataService} is to pipeline state changes
 * from a master through a failover chain of secondaries. Write can proceed
 * asynchronously on the pipeline until the next commit, at which point the
 * secondaries must be synched with the master. Since all instances have the
 * same state for any commit point, you can read from any instance but you
 * always write on the master.
 * <p>
 * Asynchronous overflow operations are carried out by the master (could be a
 * secondary, but that raises the complexity of the operation) and the generated
 * {@link IndexSegment}s are replicated to the secondaries before atomic
 * updates which redefined index views.
 * 
 * <h2>Destroying services</h2>
 * 
 * <strong>Destroying an arbitrary service instance is dangerous - if it is not
 * replicated then you can loose all your data!</strong>.
 * <p>
 * To <strong>destroy</strong> a bigdata service usie
 * {@link RemoteDestroyAdmin#destroy()}.
 * 
 * <h2>Federation shutdown sequence</h2>
 * 
 * See {@link JiniFederation#distributedFederationShutdown(boolean)}
 * 
 * <h2>Shutdown</h2>
 * 
 * Normal shutdown will kill child processes, using their normal shutdown
 * whenever possible. Once the child processes are dead, the server will
 * terminate as well. Use {@link RemoteDestroyAdmin#shutdown()} and
 * {@link RemoteDestroyAdmin#shutdownNow()} for normal shutdown.
 * 
 * <h2>Signal handling</h2>
 * 
 * <dl>
 * 
 * <dt>HUP</dt>
 * 
 * <dd>This executes the same logic that is describe above for startup.
 * However, it will re-read the {@link Configuration} from whatever source was
 * specified when the server was started. This signal may be used to update the
 * zookeeper configuration, to restart any stopped persistent services, etc.
 * <p>
 * See {@link ServicesManagerStartupTask}.</dd>
 * 
 * <dt>TERM</dt>
 * 
 * <dd>All child processes are terminated, using normal shutdown whenever
 * possible. Once the child processes are dead, the server will terminate as
 * well. (This is normal shutdown.)</dd>
 * 
 * <dt>KILL</dt>
 * 
 * <dd>This signal is NOT trapped. The server will terminate immediately. Any
 * child processes will continue to execute (this is the behavior on at least
 * Windows and linux platforms).</dd>
 * 
 * </dl>
 * 
 * Please note that Java has limited support for signal handlers, so these
 * behaviors might not be available on your deployment platform. When in doubt,
 * TEST FIRST!
 * 
 * @todo document dependencies for performance counter reporting and supported
 *       platforms. perhaps config options for which counters are collected and
 *       which are reported to the LBS.
 * 
 * @todo It is possible to create locks and queues using javaspaces in a manner
 *       similar to zookeeper. Java spaces supports a concept similar to a watch
 *       and supports transactions over operations on the space. Gigaspaces has
 *       defined a variety of extensions that provide FIFO queues.
 * 
 * @todo The {@link MetadataService}, the {@link ILoadBalancerService}, and
 *       the {@link ITransactionService} MUST NOT have more than one logical
 *       instance in a federation. They can (eventually) have failover
 *       instances, but not peers. The {@link DataService} is the only one that
 *       already supports multiple logical service instances (lots!) (but not
 *       failover).
 * 
 * @todo Operator alerts should be generated when persistent physical services
 *       die (this can be noticed either via jini or when the ephemeral znode
 *       for the master election queue goes away).
 * 
 * FIXME remaining big issues are destroying logical and physical services (not
 * implemented yet) and providing failover for the various bigdata services (not
 * implemented yet).
 * 
 * FIXME Start httpd for downloadable code. (contend for lock on node, start
 * instance if insufficient instances are running). The codebase URI should be
 * the concatenation of the URIs for each httpd instance that has been
 * configured. Unlike some other configuration properties, I am not sure that
 * the codebase URI can be changed once a service has been started. We will have
 * to unpack all of the classes into the file system, and then possibly create a
 * single JAR from them, and expose that use the ClassServer. This should be
 * done BEFORE starting jini since jini can then recognize our services in the
 * service browser (the codebase URI needs to be set for that to work).
 * <p>
 * See https://deployutil.dev.java.net/
 * <p>
 * Use class server URL(s) when starting services for their RMI codebase.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BigdataZooDefs
 */
public class ServicesManagerServer extends AbstractServer {

    protected static final Logger log = Logger.getLogger(ServicesManagerServer.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * {@link Configuration} options.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {
       
        /**
         * Namespace for these options.
         */
        String NAMESPACE = ServicesManagerServer.class.getName();
        
    }

    /**
     * The command line arguments from the ctor.
     */
    private final String[] args;
    
    /**
     * Ctor for jini service activation.
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
     */
    public ServicesManagerServer(final String[] args, final LifeCycle lifeCycle) {

        super(args, lifeCycle);
    
        this.args = args;
        
        try {

            /*
             * Note: This signal is not supported under Windows. You can use the
             * sighup() method to accomplish the same ends via RMI.
             */
            new SigHUPHandler("HUP");

        } catch (IllegalArgumentException ex) {

            log.warn("Signal handler not installed: " + ex);
            
        }

    }

    /**
     * SIGHUP Handler.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class SigHUPHandler implements SignalHandler {

        private final SignalHandler oldHandler;

        /**
         * Install handler.
         * 
         * @param signalName
         *            The signal name.
         * @param args
         *            The command line arguments (these identify the
         *            configuration and any overrides).
         * 
         * @see http://www-128.ibm.com/developerworks/java/library/i-signalhandling/
         * 
         * @see http://forum.java.sun.com/thread.jspa?threadID=514860&messageID=2451429
         *      for the use of {@link Runtime#addShutdownHook(Thread)}.
         * 
         * @see http://twit88.com/blog/2008/02/06/java-signal-handling/
         */
        @SuppressWarnings("all") // Signal is in the sun namespace
        protected SigHUPHandler(final String signalName) {

            final Signal signal = new Signal(signalName);

            this.oldHandler = Signal.handle(signal, this);
            
            if (INFO)
                log.info("Installed handler: " + signal + ", oldHandler="
                        + this.oldHandler);

        }

        @SuppressWarnings("all") // Signal is in the sun namespace
        public void handle(final Signal sig) {

            log.warn("Processing signal: " + sig);

            try {
                
                final AbstractServicesManagerService service = (AbstractServicesManagerService) impl;

                if (service != null) {

                    service
                            .sighup(true/* pushConfig */, true/*restartServices*/);
                    
                }

                /*
                 * This appears willing to halt the server so I am not chaining
                 * back to the previous handler!
                 */
                
//                // Chain back to previous handler, if one exists
//                if (oldHandler != SIG_DFL && oldHandler != SIG_IGN) {
//
//                    oldHandler.handle(sig);
//
//                }

            } catch (Throwable t) {

                log.error("Signal handler failed : " + t, t);

            }

        }

    }

    /**
     * Starts and maintains services based on the specified configuration file
     * and/or an existing zookeeper ensemble.
     * 
     * <pre>
     * java -Djava.security.policy=policy.all com.bigdata.jini.start.ServicesManager src/resources/config/bigdata.config
     * </pre>
     * 
     * @param args
     *            The command line arguments.
     */
    public static void main(final String[] args) {

        new ServicesManagerServer(args, new FakeLifeCycle()).run();
        
        System.exit(0);
//      Runtime.getRuntime().halt(0);

    }
    
    @Override
    protected AdministrableServicesManagerService newService(Properties properties) {

        final AdministrableServicesManagerService service = new AdministrableServicesManagerService(
                this, properties);
        
        /*
         * Setup a delegate that let's us customize some of the federation
         * behaviors on the behalf of the service.
         */
        getClient()
                .setDelegate(
                        new DefaultServiceFederationDelegate<AdministrableServicesManagerService>(
                                service));

        return service;

    }
    
    /**
     * Adds jini administration interfaces.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AdministrableServicesManagerService extends
            AbstractServicesManagerService implements
            RemoteAdministrable, RemoteDestroyAdmin {
        
        protected ServicesManagerServer server;
        
        public AdministrableServicesManagerService(ServicesManagerServer server,
                Properties properties) {

            super(properties);
            
            this.server = server;
            
        }
        
        public Object getAdmin() throws RemoteException {

            if (INFO)
                log.info("" + getServiceUUID());

            return server.proxy;
            
        }
        
        /**
         * Adds the following parameters to the {@link MDC}
         * <dl>
         * 
         * <dt>hostname
         * <dt>
         * <dd>The hostname or IP address of this server.</dd>
         * 
         * <dt>clientname
         * <dt>
         * <dd>The hostname or IP address of the client making the request.</dd>
         * 
         * </dl>
         * 
         * Note: {@link InetAddress#getHostName()} is used. This method makes a
         * one-time best effort attempt to resolve the host name from the
         * {@link InetAddress}.
         * 
         * @todo we could pass the class {@link ClientSubject} to obtain the
         *       authenticated identity of the client (if any) for an incoming
         *       remote call.
         */
        protected void setupLoggingContext() {
            
//            super.setupLoggingContext();
            
            try {
                
                final InetAddress clientAddr = ((ClientHost) ServerContext
                        .getServerContextElement(ClientHost.class))
                        .getClientHost();

                MDC.put("clientname", clientAddr.getHostName());

            } catch (ServerNotActiveException e) {

                /*
                 * This exception gets thrown if the client has made a direct
                 * (vs RMI) call so we just ignore it.
                 */

            }

            MDC.put("hostname", server.getHostName());

        }

        protected void clearLoggingContext() {

            MDC.remove("hostname");

            MDC.remove("clientname");

//            super.clearLoggingContext();

        }

        /*
         * DestroyAdmin
         */

        public void destroy() throws RemoteException {

            server.runDestroy();

        }

        synchronized public void shutdown() {
            
            // normal service shutdown (blocks).
            super.shutdown();

            // jini service and server shutdown.
            server.shutdownNow();
            
        }
        
        synchronized public void shutdownNow() {
            
            // immediate service shutdown (blocks).
            super.shutdownNow();
            
            // jini service and server shutdown.
            server.shutdownNow();
            
        }

        @Override
        public JiniFederation getFederation() {

            return server.getClient().getFederation();

        }

        /**
         * Extends the base behavior to return a {@link Name} of the service
         * from the {@link Configuration}. If no name was specified in the
         * {@link Configuration} then the value returned by the base class is
         * returned instead.
         */
        public String getServiceName() {

            String s = server.getServiceName();

            if (s == null)
                s = super.getServiceName();

            return s;

        }

        /**
         * 
         */
        @Override
        public AdministrableServicesManagerService start() {
            
            super.start();
            
            return this;
            
        }

        public void sighup(final boolean pushConfig,
                final boolean restartServices) throws ConfigurationException {

            setupLoggingContext();

            try {

                log.warn("pushConfig=" + pushConfig + ", restartServices="
                        + restartServices);

                final JiniFederation fed = getFederation();

                // Obtain the configuration object (re-read it).
                final ConfigurationFile config = (ConfigurationFile) ConfigurationProvider
                        .getInstance(server.args);

                fed.submitMonitoredTask(new ServicesManagerStartupTask(fed,
                        config, pushConfig, restartServices, this));

            } finally {

                clearLoggingContext();

            }

        }

    }

}
