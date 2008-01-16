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
 * Created on Mar 24, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.MDC;

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

/**
 * A metadata server.
 * <p>
 * The metadata server is used to manage the life cycles of scale-out indices
 * and exposes proxies for read and write operations on indices to clients.
 * Clients use index proxies, which automatically direct reads and writes to the
 * {@link IDataService} on which specific index partitions are located.
 * <p>
 * On startup, the metadata service discovers active data services configured in
 * the same group. While running, it tracks when data services start and stop so
 * that it can (re-)allocate index partitions as necessary.
 * 
 * @todo aggregate host load data and service RPC events and report them
 *       periodically so that we can track load and make load balancing
 *       decisions.
 * 
 * @todo note that the service update registration is _persistent_ (assuming
 *       that the service registrar is persistent I suppose) so that will add a
 *       wrinkle to how a bigdata instance must be configured.
 * 
 * @todo should destroy destroy the service instance or the persistent state as
 *       well? Locally, or as replicated?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MetadataServer extends DataServer {

    private ServiceDiscoveryManager serviceDiscoveryManager = null;
    private LookupCache dataServiceLookupCache = null;
    
    /**
     * Provides direct cached lookup of {@link DataService}s by their
     * {@link ServiceID}.
     */
    public ServiceCache dataServiceMap = new ServiceCache();

    protected LookupCache getDataServiceLookupCache() {
        
        return dataServiceLookupCache;
        
    }
    
    /**
     * @param args
     */
    public MetadataServer(String[] args) {
       
        super(args);
        
        /*
         * Setup a helper class that will be notified as services join or leave
         * the various registrars to which the metadata server is listening.
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
         * filter. This is used to keep track of all data services registered
         * with any service registrar to which the metadata server is listening.
         */
        try {
            
            ServiceTemplate template = new ServiceTemplate(null,
                    new Class[] { IDataService.class }, null);

            dataServiceLookupCache = serviceDiscoveryManager
                    .createLookupCache(template,
                            new DataServiceFilter() /* filter */, dataServiceMap/* ServiceDiscoveryListener */);
            
        } catch(RemoteException ex) {
            
            throw new RuntimeException("Could not setup LookupCache", ex);
            
        }
             
    }

    /**
     * Starts a new {@link MetadataServer}.  This can be done programmatically
     * by executing
     * <pre>
     *    new MetadataServer(args).run();
     * </pre>
     * within a {@link Thread}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public static void main(String[] args) {
        
        new MetadataServer(args) {
            
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

        return new AdministrableMetadataService(this,properties);
        
    }
    
    protected void terminateServiceManagementThreads() {
        
        dataServiceLookupCache.terminate();
        
        serviceDiscoveryManager.terminate();
        
        super.terminateServiceManagementThreads();

    }

    /**
     * Adds jini administration interfaces to the basic {@link MetadataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class AdministrableMetadataService extends MetadataService
            implements Remote, RemoteAdministrable, RemoteDestroyAdmin {
        
        protected MetadataServer server;
        private UUID serviceUUID;

        /**
         * @param properties
         */
        public AdministrableMetadataService(MetadataServer server, Properties properties) {

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
         * <dt>hostname
         * <dt>
         * <dd>The hostname or IP address of this server.</dd>
         * 
         * <dt>clientname
         * <dt>
         * <dd>The hostname or IP address of the client making the request.</dd>
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
        
        public UUID getServiceUUID() {

            if (serviceUUID == null) {

                serviceUUID = JiniUtil.serviceID2UUID(server.getServiceID());
                
            }
            
            return serviceUUID;
            
        }

        /**
         * Return the UUID of an under utilized data service.
         * 
         * @todo this is just an arbitrary instance and does not consider
         *       utilization.
         */
        public UUID getUnderUtilizedDataService() throws IOException {

            ServiceItem item = server.dataServiceLookupCache.lookup(null);

            log.info(item.toString());

            return JiniUtil.serviceID2UUID(item.serviceID);
            
        }

        public IDataService getDataServiceByUUID(UUID dataService) throws IOException {
            
            return (IDataService) server.dataServiceMap
                    .getServiceItemByID(JiniUtil.uuid2ServiceID(dataService)).service;
            
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
        
    }

}
