package com.bigdata.service.jini;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.Properties;

import net.jini.config.Configuration;
import net.jini.export.Exporter;
import net.jini.export.ServerContext;
import net.jini.io.context.ClientHost;
import net.jini.io.context.ClientSubject;
import net.jini.lookup.entry.Name;

import org.apache.log4j.MDC;

import com.bigdata.counters.httpd.CounterSetHTTPDServer;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.ResourceLockService;
import com.bigdata.service.DefaultServiceFederationDelegate;
import com.bigdata.service.LoadBalancerService;

/**
 * The resource lock manager server.
 * <p>
 * The {@link ResourceLockServer} starts the {@link ResourceLockService}. The
 * server and service are configured using a {@link Configuration} file whose
 * name is passed to the {@link ResourceLockServer#ResourceLockServer(String[])}
 * constructor or {@link #main(String[])}.
 * <p>
 * 
 * @see src/resources/config for sample configurations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ResourceLockServer extends AbstractServer {
    
    /**
     * Options for this server.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends AdministrableResourceLockService.Options {
        
    }
    
    /**
     * Creates a new service.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public ResourceLockServer(String[] args) {

        super(args);
        
    }
    
    /**
     * Starts a new {@link ResourceLockServer}. This can be done
     * programmatically by executing
     * 
     * <pre>
     * new ResourceLockServer(args).run();
     * </pre>
     * 
     * within a {@link Thread}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public static void main(String[] args) {
        
        new ResourceLockServer(args) {
            
            /**
             * Overriden to use {@link System#exit()} since this is the command
             * line interface.
             */
            protected void fatal(String msg, Throwable t) {

                log.fatal(msg, t);

                try {

                    shutdownNow();
                    
                } catch (Throwable t2) {
                    
                    log.error(t2.getMessage(), t2);
                    
                }

                System.exit(1);

            }
            
        }.run();
        
    }

    @Override
    protected ZooResourceLockService newService(Properties properties) {
        
        final ZooResourceLockService service = new AdministrableResourceLockService(this, properties);
        
        /*
         * Setup a delegate that let's us customize some of the federation
         * behaviors on the behalf of the data service.
         */
        getClient().setDelegate(new DefaultServiceFederationDelegate<ZooResourceLockService>(service));

        return service;
        
    }

    /**
     * Adds jini administration interfaces to the basic {@link ResourceLockService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AdministrableResourceLockService extends ZooResourceLockService
            implements RemoteAdministrable, RemoteDestroyAdmin {

        protected ResourceLockServer server;

        public AdministrableResourceLockService(ResourceLockServer server,
                Properties properties) {

            super(properties);
            
            this.server = server;
            
        }
        
        @Override
        public JiniFederation getFederation() {

            return server.getClient().getFederation();
            
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
            
            super.setupLoggingContext();
            
            try {
                
                InetAddress clientAddr = ((ClientHost) ServerContext
                        .getServerContextElement(ClientHost.class))
                        .getClientHost();
                
                MDC.put("clientname",clientAddr.getHostName());
                
            } catch (ServerNotActiveException e) {
                
                /*
                 * This exception gets thrown if the client has made a direct
                 * (vs RMI) call so we just ignore it.
                 */
                
            }
            
            MDC.put("hostname",server.getHostName());
            
        }

        protected void clearLoggingContext() {
            
            MDC.remove("hostname");

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

        /**
         * Wraps the lock as a {@link RemoteLock}, exports the
         * {@link RemoteLock} to obtain its proxy object, and the wraps the
         * {@link RemoteLock}'s proxy with a {@link ClientLock} so that the
         * {@link IResourceLock} API is preserved.
         */
        @Override
        protected IResourceLock getProxy(final ResourceLock lock) {

            final RemoteLock proxy = getFederation().getProxy(
                    new RemoteLockImpl(lock), true/* enableDGC */);

            return new ClientLock(proxy);

        }

        /**
         * Serializable {@link IResourceLock} that wraps the proxy object and hides
         * {@link IOException}s from the API.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        private static class ClientLock implements IResourceLock, Serializable {

            /**
             * 
             */
            private static final long serialVersionUID = -3717335186306277769L;
            
            private final RemoteLock remoteLock;

            public ClientLock(final RemoteLock remoteLock) {

                if (remoteLock == null)
                    throw new IllegalArgumentException();

                this.remoteLock = remoteLock;

            }

            /**
             * FIXME client-service locks are NOT functional! The problem is
             * that the client and service do not operate in the same thread and
             * the lock is based on java locks. Locking is currently disabled in
             * {@link ResourceLockService}.
             */
            public void unlock() {
                
//                try {
//
//                    remoteLock.unlock();
//
//                } catch (IOException ex) {
//
//                    throw new RuntimeException(ex);
//
//                }

            }

        }

        /**
         * Interface provides the API of {@link IResourceLock} but its methods
         * throw {@link IOException} and it extends {@link Remote} and is
         * therefore compatible with RMI and {@link Exporter}.
         */
        private static interface RemoteLock extends Remote {
            
            public void unlock() throws IOException;
            
        }
        
        /**
         * {@link RemoteLock} implementation.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        private static class RemoteLockImpl implements RemoteLock {

            private final ResourceLock lock;

            public RemoteLockImpl(final ResourceLock lock) {

                if (lock == null)
                    throw new IllegalArgumentException();

                this.lock = lock;

            }

            public void unlock() throws IOException {

                this.lock.unlock();

            }

        }

    }

}
