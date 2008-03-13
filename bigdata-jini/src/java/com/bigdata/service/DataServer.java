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
 * Created on Mar 22, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.Properties;
import java.util.UUID;

import net.jini.config.Configuration;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.export.ServerContext;
import net.jini.io.context.ClientHost;
import net.jini.io.context.ClientSubject;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.log4j.MDC;

/**
 * The bigdata data server.
 * <p>
 * The {@link DataServer} starts the {@link DataService}. The server and
 * service are configured using a {@link Configuration} file whose name is
 * passed to the {@link DataServer#DataServer(String[])} constructor or
 * {@link #main(String[])}.
 * <p>
 * 
 * @see src/resources/config for sample configurations.
 * 
 * @todo identify the minimum set of permissions required to run a
 *       {@link DataServer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServer extends AbstractServer {

    private ServiceDiscoveryManager serviceDiscoveryManager = null;
    private LookupCache metadataServiceLookupCache = null;
    
    /**
     * Provides direct cached lookup of {@link MetadataService}s by their
     * {@link ServiceID}.
     */
    public ServiceCache metadataServiceMap = new ServiceCache();

    protected LookupCache getMetadataServiceLookupCache() {
        
        return metadataServiceLookupCache;
        
    }

    /**
     * Creates a new {@link DataServer}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public DataServer(String[] args) {

        super(args);
        
        /*
         * Setup a helper class that will be notified as services join or leave
         * the various registrars to which the data server is listening.
         */
        try {

            serviceDiscoveryManager = new ServiceDiscoveryManager(getDiscoveryManagement(),
                    new LeaseRenewalManager());
            
        } catch(IOException ex) {
            
            throw new RuntimeException(
                    "Could not initiate service discovery manager", ex);
            
        }

        /*
         * Setup a LookupCache that will be populated with all services that match a
         * filter. This is used to keep track of all metadata services registered
         * with any service registrar to which the data server is listening.
         */
        try {
            
            ServiceTemplate template = new ServiceTemplate(null,
                    new Class[] { IDataService.class }, null);

            metadataServiceLookupCache = serviceDiscoveryManager.createLookupCache(
                    template, new MetadataServiceFilter() /* filter */,
                    metadataServiceMap/* ServiceDiscoveryListener */);

        } catch (RemoteException ex) {
            
            throw new RuntimeException("Could not setup LookupCache", ex);
            
        }
             
    }
    
//    public DataServer(String[] args, LifeCycle lifeCycle) {
//        
//        super( args, lifeCycle );
//        
//    }

    /**
     * Starts a new {@link DataServer}.  This can be done programmatically
     * by executing
     * <pre>
     *    new DataServer(args).run();
     * </pre>
     * within a {@link Thread}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public static void main(String[] args) {
        
        new DataServer(args) {
            
            /**
             * Overriden to use {@link System#exit()} since this is the command
             * line interface.
             */
            protected void fatal(String msg, Throwable t) {

                log.fatal(msg, t);

                System.exit(1);

            }
            
        }.run();
        
    }
    
    protected Remote newService(Properties properties) {
        
        return new AdministrableDataService(this,properties);
        
    }
    
    protected void terminateServiceManagementThreads() {
        
        metadataServiceLookupCache.terminate();
        
        serviceDiscoveryManager.terminate();
        
        super.terminateServiceManagementThreads();

    }
    
    /**
     * Extends the behavior to close and delete the journal in use by the data
     * service.
     */
    public void destroy() {

        DataService service = (DataService)impl;
        
        super.destroy();
        
        // destroy all resources.
        service.getResourceManager().deleteResources();

    }

    /**
     * Adds jini administration interfaces to the basic {@link DataService}.
     * 
     * @todo if there is a jini initialization problem then this class should
     *       not cause a (new) journal to be left behind when the server startup
     *       fails.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AdministrableDataService extends DataService implements
            RemoteAdministrable, RemoteDestroyAdmin {
        
        protected DataServer server;
        private UUID serviceUUID;
        
        public AdministrableDataService(DataServer server,Properties properties) {
            
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
         * 
         * @throws RemoteException
         */
        public void destroy() throws RemoteException {

            log.info(""+getServiceUUID());

            new Thread() {

                public void run() {

                    server.destroy();
                    
                    log.info(getServiceUUID()+" - Service stopped.");

                }

            }.start();

        }

        public UUID getServiceUUID() {

            if (serviceUUID == null) {

                serviceUUID = JiniUtil.serviceID2UUID(server.getServiceID());

            }

            return serviceUUID;
            
        }

        @Override
        public IMetadataService getMetadataService() {

            /*
             * @todo handle one metadata service. right now registering more
             * than one will cause problems since different clients might
             * discover different metadata services and the metadata services
             * are not arranging themselves into a failover chain or a hash
             * partitioned service.
             */

            final ServiceItem item = server.metadataServiceLookupCache
                    .lookup(null);

            if (item == null) {

                // @todo vs throw execption?
                log.warn("No metadata service in cache");

                return null;
                
            }
            
            return (IMetadataService) item.service;

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
