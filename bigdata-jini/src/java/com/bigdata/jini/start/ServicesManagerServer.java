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
import net.jini.config.ConfigurationFile;
import net.jini.config.ConfigurationProvider;
import net.jini.export.ServerContext;
import net.jini.io.context.ClientHost;
import net.jini.io.context.ClientSubject;
import net.jini.lookup.entry.Name;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.zookeeper.ZooKeeper;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.bigdata.jini.start.config.IServiceConstraint;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.ServicesManagerConfiguration;
import com.bigdata.service.DefaultServiceFederationDelegate;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.AbstractServer;
import com.bigdata.service.jini.FakeLifeCycle;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.RemoteAdministrable;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.start.ServiceDescriptor;
import com.sun.jini.start.ServiceStarter;
import com.sun.jini.start.SharedActivatableServiceDescriptor;

/**
 * A class for bootstrapping a {@link JiniFederation} across a cluster based on
 * a {@link Configuration} file describing some basic configuration parameters
 * and dynamically managing service discovery using <code>jini</code> and
 * master election and other distributed decision making using
 * <code>zookeeper</code>.
 * <p>
 * Each host runs <em>ONE (1)</em> of this server. The server will start other
 * processes on the local host, including: jini, zookeeper, an httpd service for
 * the RMI CODEBASE, and instances of the various services required by an
 * {@link IBigdataFederation}. Those decisions are based on configuration
 * requirements stored in zookeeper, including the target #of logical services
 * of some service type, the replication count for a logical service, and
 * {@link IServiceConstraint} that are used to decide which host will start the
 * new service. Zookeeper service instances are handled somewhat specially.
 * During startup, this server will start a zookeeper instance if one was
 * configured to run on this host and none is found to be running at that time.
 * <p>
 * The initial configuration for the federation is specified in a
 * {@link Configuration} file. That file is best located on a shared volume or
 * volume image replicated on the hosts in the cluster, along with the various
 * dependencies required to start and run the federation (java, jini, bigdata,
 * log4j configuration, related JARs, etc). The {@link Configuration} is used to
 * discover and/or start jini and zookeeper instance(s). Once zookeeper can be
 * discovered (using its own protocol, not jini), {@link ServicesManagerServer}
 * instances will contend for a lock on a node corresponding to the
 * {@link IBigdataFederation} described in the {@link Configuration}. If the
 * node is empty, then it will be populated with the initial configuration by
 * whichever process holds the lock, which is why it is important that all hosts
 * running this class are use an identical {@link Configuration}. Thereafter,
 * zookeeper will be used to manage the services in the federation.
 * <p>
 * Once running, the {@link ServicesManagerServer} will watch the zookeeper
 * nodes for the various kinds of services and will start or stop services as
 * the state of those nodes changes.
 * <p>
 * <strong>Destroying an arbitrary service instance is dangerous - if it is not
 * replicated then you can loose all your data!</strong>. In order to shutdown
 * a bigdata service instance use {@link RemoteDestroyAdmin#shutdown()} or zap
 * it from the command line (not the preferred approach, but that will only take
 * the service down, but not destroy its data). However, if the federation is
 * now undercapacity for that service type (or under the replication count for a
 * service instance), then a new instance will simply be created.
 * <p>
 * SIGHUP may be used to modified {@link ServiceConfiguration}s to zookeeper.
 * The {@link Configuration} will be re-read using the command line arguments,
 * the {@link ServiceConfiguration}s will be re-extracted, and the
 * corresponding znodes in zookeeper will be updated. Live service instances
 * should be administered either using the jini browser or programtically.
 * 
 * FIXME If zookeeper dies (all instances) and is then brought back up there
 * will be a race condition where the physical services will (or should) try to
 * re-assert their ephemeral znodes and the {@link ServicesManagerServer} will
 * (or should) notice that there are no physical services for its logical
 * services.
 * <p>
 * Probably we need to notice the zookeeper reconnect and then have the
 * {@link ServicesManagerServer} wait a bit before taking any decisions,
 * effectively yeilding to the physical services so that they can re-establish
 * their ephemeral znodes.
 * <p>
 * This needs to be evaluated in practice. For example, is the natural behavior
 * of the {@link ZooKeeper} client to re-create any ephemeral znodes owned by it
 * which were in existence at the time of the disconnect?
 * 
 * FIXME I am not convinced that we want to take down the child processes when
 * this service exits. It might be much nicer to leave them online (that is the
 * current behavior).
 * <p>
 * IF we do take them down, then when this service starts up we need to restart
 * any services which are declared in zookeeper but joined in jini. We should do
 * direct service discovery and then re-start any services that we can not find
 * running. [We don't want to do this on an ongoing basis because it would cause
 * any service that was deliberately shutdown to be restarted as soon as we
 * discover that it is no longer discoverable.]
 * <p>
 * IF we don't take them down, then I am not sure if the parent process will
 * exit under various operating systems. We might need to start the processes
 * slightly differently for that to work.
 * 
 * @todo management semantics for deleting znodes are not yet defined. Probably
 *       a physical service should watch its logicalService's znode and compete
 *       to re-create that znode if it is deleted. Likewise, the physical
 *       service should ensure that its own znode is not removed (that can be
 *       done via ACLs as well).
 *       <p>
 *       In particular, deleting a znode SHOULD NOT cause the corresponding
 *       logical or physical service to be terminated. Use the
 *       {@link RemoteDestroyAdmin} API to terminate physical services.
 * 
 * @todo document dependencies for performance counter reporting and supported
 *       platforms. perhaps config options for which counters are collected and
 *       which are reported to the LBS.
 * 
 * @todo if we constrain ourselves to one instance per host then this can be the
 *       process that reports host specific statistics to the LBS.
 * 
 * @todo There is no straightfoward way to re-start a service that has been
 *       shutdown or to differentiate a failed start from a service which could
 *       be started.
 *       <p>
 *       The {@link ServiceConfiguration} of the physical service has all the
 *       necessary information on how to start the service.
 *       <p>
 *       Perhaps we could write an {@link SharedActivatableServiceDescriptor}
 *       into the serviceDir and use that to actually start the service? It
 *       could then be restarted automatically (if we knew where to look for
 *       instances of the service, and we do).
 * 
 * @todo is it possible to create locks and queues using javaspaces in a manner
 *       similar to zookeeper? It does support a concept similar to a watch, but
 *       I am not sure that it has concepts similar to "ephermeral" or
 *       "sequential". Also, I am not clear on its consistency guarentees. It
 *       does support transactions, which could be another way to approach this.
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
 * FIXME remaining big issues are restart of physical services; destroying
 * logical and physical services (not implemented yet) and providing failover
 * for the various bigdata services (not implemented yet).
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

//    /**
//     * Creates a new {@link ServicesManagerServer}.
//     * 
//     */
//    public ServicesManagerServer(final String[] args) {
//
//        this(args, new FakeLifeCycle());
//
//    }

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
        
        installSighupHandler(args);
        
    }

    /**
     * Install SIGHUP (Hang up) handler - the {@link Configuration} will be
     * re-read and pushed to zookeeper.
     * 
     * @param args
     *            The command line arguments (the identify the configuration and
     *            any overrides).
     * 
     * @see http://www-128.ibm.com/developerworks/java/library/i-signalhandling/
     * 
     * @see http://forum.java.sun.com/thread.jspa?threadID=514860&messageID=2451429
     *      for the use of {@link Runtime#addShutdownHook(Thread)}.
     * 
     * @see http://twit88.com/blog/2008/02/06/java-signal-handling/
     */
    protected void installSighupHandler(final String[] args) {

        try {

            new PushConfigurationSignalHandler("HUP", args);

        } catch (IllegalArgumentException ex) {

            log.warn("Signal handler not installed: " + ex);
            
        }

    }

    /**
     * Signal handler shuts down the server politely.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class PushConfigurationSignalHandler implements SignalHandler {

        private final SignalHandler oldHandler;
        
        private final String[] args;

        /**
         * 
         * @param signalName
         *            The signal name.
         * @param args
         *            The command line arguments (the identify the configuration
         *            and any overrides).
         */
        @SuppressWarnings("all") // Signal is in the sun namespace
        protected PushConfigurationSignalHandler(final String signalName,
                final String[] args) {

            final Signal signal = new Signal(signalName);

            this.oldHandler = Signal.handle(signal, this);

            this.args = args;
            
            if (INFO)
                log.info("Installed handler: " + signal + ", oldHandler="
                        + this.oldHandler);

        }

        /**
         * Re-reads the {@link Configuration} and pushes it to zookeeper.
         * <p>
         * Note: This does not change the {@link Configuration} on the service
         * or the {@link JiniFederation}. It is only designed to allow the push
         * of new {@link ServiceConfiguration}s to zookeeper.
         */
        @SuppressWarnings("all") // Signal is in the sun namespace
        public void handle(final Signal sig) {

            log.warn("Processing signal: " + sig);

            try {
                
                final AbstractServicesManagerService service = (AbstractServicesManagerService) impl;

                if (service != null) {

                    // Obtain the configuration object (re-read it).
                    final ConfigurationFile config = (ConfigurationFile) ConfigurationProvider
                            .getInstance(args);
                    
                    // get the service manager's own configuration.
                    final ServicesManagerConfiguration selfConfig = new ServicesManagerConfiguration(
                            config);

                    /*
                     * These are the services that we will start and/or manage.
                     */
                    final ServiceConfiguration[] serviceConfigurations = selfConfig
                            .getServiceConfigurations(config);

                    final ZooKeeper zookeeper = service.getFederation()
                            .getZookeeper();

                    final String zconfig = service.getFederation()
                            .getZooConfig().zroot
                            + "/" + BigdataZooDefs.CONFIG;

                    // push the configuration to zookeeper.
                    service.pushConfiguration(zookeeper, zconfig, service
                            .getFederation().getZooConfig().acl,
                            serviceConfigurations);

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

                log.warn("Pushed configuration.");
                
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
        
//      System.exit(0);
        Runtime.getRuntime().halt(0);

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

        @Override
        protected Configuration getConfiguration() {
            
            return server.config;
            
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

    }

}
