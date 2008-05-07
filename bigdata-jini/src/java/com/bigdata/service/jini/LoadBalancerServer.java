package com.bigdata.service.jini;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.Properties;

import net.jini.config.Configuration;
import net.jini.export.ServerContext;
import net.jini.io.context.ClientHost;
import net.jini.io.context.ClientSubject;

import org.apache.log4j.MDC;

import com.bigdata.counters.httpd.CounterSetHTTPDServer;
import com.bigdata.service.DataService;
import com.bigdata.service.LoadBalancerService;
import com.bigdata.service.MetadataService;

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
     * Handles discovery of the {@link DataService}s and
     * {@link MetadataService}s.
     */
    protected DataServicesClient dataServicesClient = null;
    
    /**
     * Creates a new {@link DataServer}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public LoadBalancerServer(String[] args) {

        super(args);
        
        try {

            dataServicesClient = new DataServicesClient(getDiscoveryManagement());
            
        } catch(Exception ex) {
            
            fatal("Problem initiating service discovery: " + ex.getMessage(), ex);
            
        }
        
    }
    
//    public DataServer(String[] args, LifeCycle lifeCycle) {
//        
//        super( args, lifeCycle );
//        
//    }

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
        
        new LoadBalancerServer(args) {
            
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
    
    protected Remote newService(Properties properties) {
        
        return new AdministrableLoadBalancer(this, properties);
        
    }
    
    protected void terminate() {

        if (dataServicesClient != null) {

            try {

                dataServicesClient.terminate();
                
            } catch(Exception ex) {
                
                log.error("Could not terminate the data services client: "+ex, ex);
                
            } finally {
                
                dataServicesClient = null;
                
            }

        }
        
        super.terminate();

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
//        private UUID serviceUUID;
        
        public AdministrableLoadBalancer(LoadBalancerServer server,Properties properties) {
            
            super(properties);
            
            this.server = server;
            
        }
        
        public Object getAdmin() throws RemoteException {

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

            log.info("" + getServiceUUID());

            new Thread() {

                public void run() {

                    server.destroy();
                    
                    log.info(getServiceUUID()+" - Service stopped.");

                }

            }.start();

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

//        public UUID getServiceUUID() {
//
//            if (serviceUUID == null) {
//
//                serviceUUID = JiniUtil.serviceID2UUID(server.getServiceID());
//
//            }
//
//            return serviceUUID;
//            
//        }
        
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
       
// /*
// * JoinAdmin
// */
//        
// public void addLookupAttributes(Entry[] arg0) throws RemoteException {
//            
// log.info("");
//            
//        }
//
//        public void addLookupGroups(String[] arg0) throws RemoteException {
//
//            log.info("");
//
//        }
//
//        public void addLookupLocators(LookupLocator[] arg0) throws RemoteException {
//
//            log.info("");
//            
//        }
//
//        public Entry[] getLookupAttributes() throws RemoteException {
//
//            log.info("");
//
//            return null;
//        }
//
//        public String[] getLookupGroups() throws RemoteException {
//         
//            log.info("");
//
//            return null;
//        }
//
//        public LookupLocator[] getLookupLocators() throws RemoteException {
//         
//            log.info("");
//
//            return null;
//        }
//
//        public void modifyLookupAttributes(Entry[] arg0, Entry[] arg1) throws RemoteException {
//         
//            log.info("");
//            
//        }
//
//        public void removeLookupGroups(String[] arg0) throws RemoteException {
//            log.info("");
//
//        }
//
//        public void removeLookupLocators(LookupLocator[] arg0) throws RemoteException {
//            log.info("");
//            
//        }
//
//        public void setLookupGroups(String[] arg0) throws RemoteException {
//            log.info("");
//            
//        }
//
//        public void setLookupLocators(LookupLocator[] arg0) throws RemoteException {
//            log.info("");
//            
//        }
        
    }

}
