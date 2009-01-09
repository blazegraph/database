package com.bigdata.service.jini;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.Properties;
import java.util.UUID;

import net.jini.config.Configuration;
import net.jini.export.ServerContext;
import net.jini.io.context.ClientHost;
import net.jini.io.context.ClientSubject;
import net.jini.lookup.entry.Name;

import org.apache.log4j.MDC;

import com.bigdata.counters.httpd.CounterSetHTTPDServer;
import com.bigdata.service.DefaultServiceFederationDelegate;
import com.bigdata.service.IFederationDelegate;
import com.bigdata.service.IService;
import com.bigdata.service.LoadBalancerService;

/**
 * The load balancer server.
 * <p>
 * The {@link LoadBalancerServer} starts the {@link LoadBalancerService}. The
 * server and service are configured using a {@link Configuration} file whose
 * name is passed to the {@link LoadBalancerServer#LoadBalancerServer(String[])}
 * constructor or {@link #main(String[])}.
 * <p>
 * 
 * @see src/resources/config for sample configurations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadBalancerServer extends AbstractServer {

    /**
     * Options for this server.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends AdministrableLoadBalancer.Options {
        
    }
    
    /**
     * Creates a new {@link DataServer}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public LoadBalancerServer(String[] args) {

        super(args);
        
    }

    /**
     * Starts a new {@link LoadBalancerServer}. This can be done
     * programmatically by executing
     * 
     * <pre>
     * new LoadBalancerServer(args).run();
     * </pre>
     * 
     * within a {@link Thread}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public static void main(String[] args) {
        
        new LoadBalancerServer(args).run();
        
    }

    @Override
    protected LoadBalancerService newService(final Properties properties) {
        
        LoadBalancerService service = new AdministrableLoadBalancer(this, properties);
        
        /*
         * Setup a delegate that let's us customize some of the federation
         * behaviors on the behalf of the load balancer.
         * 
         * Note: We can't do this with the local or embedded federations since
         * they have only one client per federation and an attempt to set the
         * delegate more than once will cause an exception to be thrown!
         */
        final JiniClient client = getClient();

        if(client.isConnected()) {

            /*
             * Note: We need to set the delegate before the client is connected
             * to the federation. This ensures that the delegate, and hence the
             * load balancer, will see all join/leave events.
             */

            throw new IllegalStateException();
            
        }
        
        client.setDelegate(new LoadBalancerServiceFederationDelegate(service));

        return service;
        
    }
    
    /**
     * Overrides the {@link IFederationDelegate} leave/join behavior to notify
     * the {@link LoadBalancerService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class LoadBalancerServiceFederationDelegate extends
            DefaultServiceFederationDelegate<LoadBalancerService> {

        /**
         * @param service
         */
        public LoadBalancerServiceFederationDelegate(
                LoadBalancerService service) {

            super(service);

        }
        
        /**
         * Notifies the {@link LoadBalancerService}.
         */
        public void serviceJoin(IService service, UUID serviceUUID) {

            try {

                // Note: This is an RMI request!
                final Class serviceIface = service.getServiceIface();
                
                // Note: This is an RMI request!
                final String hostname = service.getHostname();

                if (INFO)
                    log.info("serviceJoin: serviceUUID=" + serviceUUID
                            + ", serviceIface=" + serviceIface + ", hostname="
                            + hostname);
                
                // this is a local method call.
                this.service.join(serviceUUID, serviceIface, hostname);

            } catch (IOException ex) {

                log.error(ex.getLocalizedMessage(), ex);
                
            }
            
        }

        /**
         * Notifies the {@link LoadBalancerService}.
         */
        public void serviceLeave(UUID serviceUUID) {

            if (INFO)
                log.info("serviceUUID=" + serviceUUID);
            
            this.service.leave(serviceUUID);
            
        }

    }
    
    /**
     * Adds jini administration interfaces to the basic {@link LoadBalancerService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AdministrableLoadBalancer extends LoadBalancerService implements
            RemoteAdministrable, RemoteDestroyAdmin {
        
        protected LoadBalancerServer server;
        
        public AdministrableLoadBalancer(LoadBalancerServer server,Properties properties) {
            
            super(properties);
            
            this.server = server;
            
        }
        
        @Override
        public JiniFederation getFederation() {

            return server.getClient().getFederation();
            
        }

        public Object getAdmin() throws RemoteException {

            if (INFO)
                log.info(""+getServiceUUID());

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
         * <dd>The hostname or IP address of the client making the request (at
         * {@link #INFO} or better)</dd>
         * 
         * </dl>
         */
        protected void setupLoggingContext() {

            super.setupLoggingContext();

            if (INFO)
                MDC.put("clientname", getClientHostname());

            MDC.put("hostname", server.getHostName());

        }

        protected void clearLoggingContext() {

            MDC.remove("hostname");

            if (INFO)
                MDC.remove("clientname");

            super.clearLoggingContext();
            
        }
        
        /*
         * DestroyAdmin
         */

        /**
         * Destroy the service and deletes any files containing resources (<em>application data</em>)
         * that was in use by that service.
         * <p>
         * Note: The {@link LoadBalancerService} writes counters into a
         * configured directly but does not otherwise have configured state.
         * Those counters are NOT destroyed so that they may be used for
         * post-mortem analysis. See {@link CounterSetHTTPDServer}.
         * 
         * @throws RemoteException
         */
        public void destroy() throws RemoteException {

            server.runDestroy();

        }

        synchronized public void shutdown() {
            
            // normal service shutdown.
            super.shutdown();
            
            // jini service and server shutdown.
            server.shutdownNow();
            
        }
        
        synchronized public void shutdownNow() {
            
            // immediate service shutdown.
            super.shutdownNow();
            
            // jini service and server shutdown.
            server.shutdownNow();
            
        }
        
        /**
        * Note: {@link InetAddress#getHostName()} is used. This method makes a
        * one-time best effort attempt to resolve the host name from the
        * {@link InetAddress}.
        * 
        * @todo we could pass the class {@link ClientSubject} to obtain the
        *       authenticated identity of the client (if any) for an incoming
        *       remote call.
         */
        protected String getClientHostname() {

            InetAddress clientAddr;

            try {

                clientAddr = ((ClientHost) ServerContext
                        .getServerContextElement(ClientHost.class))
                        .getClientHost();

            } catch (ServerNotActiveException e) {

                /*
                 * This exception gets thrown if the client has made a direct
                 * (vs RMI) call.
                 */

                try {

                    clientAddr = Inet4Address.getLocalHost();

                } catch (UnknownHostException ex) {

                    return "localhost";

                }

            }

            return clientAddr.getCanonicalHostName();

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

     }

}
