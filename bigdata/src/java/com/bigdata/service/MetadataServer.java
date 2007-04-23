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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

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
public class MetadataServer extends DataServer implements ServiceDiscoveryListener {

    private ServiceDiscoveryManager serviceDiscoveryManager = null;
    private LookupCache dataServiceLookupCache = null;
    private Map<ServiceID, ServiceItem> serviceIdMap = new ConcurrentHashMap<ServiceID, ServiceItem>();

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
            
            log.error("Could not initiate service discovery manager", ex);
            
            System.exit(1);
            
        }

        /*
         * Setup a dataServiceLookupCache that will be populated with all services that match a
         * filter. This is used to keep track of all data services registered
         * with any service registrar to which the metadata server is listening.
         */
        try {
            
            ServiceTemplate template = new ServiceTemplate(null,
                    new Class[] { IDataService.class }, null);

            dataServiceLookupCache = serviceDiscoveryManager
                    .createLookupCache(template,
                            new DataServiceFilter() /* filter */, this /* ServiceDiscoveryListener */);
            
        } catch(RemoteException ex) {
            
            log.error("Could not setup lookup dataServiceLookupCache", ex);
            
            System.exit(1);
            
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
    
    /**
     * Filter matches a {@link DataService} but not a {@link MetadataService}.
     * <p>
     * 
     * @todo This explicitly filters out service variants that extend
     *       {@link DataService} but which are not tasked as a
     *       {@link DataService} by the {@link MetadataService}. It would be
     *       easier if we refactored the interface hierarchy a bit so that there
     *       was a common interface and abstract class extended by both the
     *       {@link DataService} and the {@link MetadataService} such that we
     *       could match on their specific interfaces without the possibility of
     *       confusion.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class DataServiceFilter implements ServiceItemFilter {

        public boolean check(ServiceItem item) {

            if(item.service==null) {
                
                log.warn("Service is null: "+item);

                return false;
                
            }
            
            if(!(item.service instanceof IMetadataService)) {
               
                log.info("Matched: "+item);
                
                return true;
                
            }

            log.debug("Ignoring: "+item);
            
            return false;
            
        }
        
    };
        
    /*
     * ServiceDiscoveryListener.
     */
    
    /**
     * Adds the {@link ServiceItem} to the internal map to support {@link #getServiceByID()}
     * <p>
     * Note: This event is generated by the {@link LookupCache}. There is an
     * event for each {@link DataService} as it joins any registrar in the set
     * of registrars to which the {@link MetadataServer} is listening. The set
     * of distinct joined {@link DataService}s is accessible via the
     * {@link LookupCache}.
     */
    public void serviceAdded(ServiceDiscoveryEvent e) {
        
        log.info("" + e + ", class="
                + e.getPostEventServiceItem().toString());
        
        serviceIdMap.put(e.getPostEventServiceItem().serviceID, e
                .getPostEventServiceItem());
        
    }

    /**
     * NOP.
     */
    public void serviceChanged(ServiceDiscoveryEvent e) {

        log.info(""+e+", class="
                + e.getPostEventServiceItem().toString());
        
        serviceIdMap.put(e.getPostEventServiceItem().serviceID, e
                .getPostEventServiceItem());

    }

    /**
     * NOP.
     */
    public void serviceRemoved(ServiceDiscoveryEvent e) {

        log.info(""+e+", class="
                + e.getPreEventServiceItem().toString());

        serviceIdMap.remove(e.getPreEventServiceItem().serviceID);

    }

    /**
     * Resolve the {@link ServiceID} for a {@link DataService} to the cached
     * {@link ServiceItem} for that {@link DataService}.
     * 
     * @param serviceID
     *            The {@link ServiceID} for the {@link DataService}.
     * 
     * @return The cache {@link ServiceItem} for that {@link DataService}.
     */
    public ServiceItem getDataServiceByID(ServiceID serviceID) {
        
        return serviceIdMap.get(serviceID);
        
    }
    
    /**
     * Return the #of {@link DataService}s known to this {@link MetadataServer}.
     * 
     * @return The #of {@link DataService}s in the {@link LookupCache}.
     */
    public int getDataServiceCount() {
        
        return serviceIdMap.size();
        
    }
    
    /**
     * Extends the behavior to terminate {@link LookupCache} and
     * {@link ServiceDiscoveryManager} processing.
     */
    public void destroy() {
        
        dataServiceLookupCache.terminate();
        
        serviceDiscoveryManager.terminate();
        
        super.destroy();

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
        
        /**
         * @param properties
         */
        public AdministrableMetadataService(MetadataServer server, Properties properties) {

            super(properties);
            
            this.server = server;
            
        }

        public Object getAdmin() throws RemoteException {

            log.info("");

            return server.proxy;

        }

        /**
         * Return the UUID of an under utilized data service.
         * 
         * @todo this is just an arbitrary instance and does not consider
         *       utilization.
         */
        public ServiceID getUnderUtilizedDataService() throws IOException {

            ServiceItem item = server.dataServiceLookupCache.lookup(null);

            log.info(item.toString());

            return item.serviceID;
            
        }

        public IDataService getDataServiceByID(ServiceID serviceID) throws IOException {
            
            return (IDataService)server.getDataServiceByID(serviceID).service;
            
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

            log.info("");

            new Thread() {

                public void run() {

                    server.destroy();
                    
                    log.info("Service stopped.");

                }

            }.start();

        }
        
    }

}
