/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Mar 24, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Properties;
import java.util.UUID;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
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

//    /**
//     * @param args
//     * @param lifeCycle
//     */
//    public MetadataServer(String[] args, LifeCycle lifeCycle) {
//        
//        super(args, lifeCycle);
//        
//    }

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
