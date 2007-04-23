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
    private LookupCache lookupCache = null;
    private Map<ServiceID, ServiceItem> serviceIdMap = new ConcurrentHashMap<ServiceID, ServiceItem>();

    protected LookupCache getLookupCache() {
        
        return lookupCache;
        
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
         * Setup a lookupCache that will be populated with all services that match a
         * filter. This is used to keep track of all data services registered
         * with any service registrar to which the metadata server is listening.
         */
        try {
            
            ServiceTemplate template = new ServiceTemplate(null,
                    new Class[] { IDataService.class }, null);

            lookupCache = serviceDiscoveryManager
                    .createLookupCache(template,
                            new DataServiceFilter() /* filter */, this /* ServiceDiscoveryListener */);
            
        } catch(RemoteException ex) {
            
            log.error("Could not setup lookup lookupCache", ex);
            
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
    
//    /*
//     * DiscoveryListener
//     */
//
//    /**
//     * Extends base implementation to register for notification of
//     * {@link DataService} join/leave events
//     * 
//     * @todo this must be a low-latency handler.
//     * 
//     * @todo pay attention iff registrar is for a group that is used by this
//     *       metadata service as configured.
//     * 
//     * @todo figure out delta in registrars and in group-to-registrar mapping.
//     * 
//     * @todo register for join/leave events for {@link IDataService} on each new
//     *       registrar.
//     * 
//     * @todo task a worker to query the new registrar(s) for any existing
//     *       {@link IDataService}s and continue to query until all such
//     *       services on the registrar have been discovered. This is the pool of
//     *       {@link DataService}s that are available to the
//     *       {@link MetadataService} for management.
//     */
//    public void discovered(DiscoveryEvent e) {
//
//        super.discovered(e);
//        
//        ServiceRegistrar[] registrars = e.getRegistrars();
//        
//        Map<Integer,String[]> registrarToGroups = (Map<Integer,String[]>) e.getGroups();
//        
//        final int nregistrars = registrars.length;
//        
//        log.info("Reported: "+nregistrars+" registrars");
//
//        ServiceTemplate template = new ServiceTemplate(null,
//                new Class[] { IDataService.class }, null);
//        
//        final long leaseDuration = 5000;
//        
//        final MarshalledObject handbackObject;
//        
//        try {
//
//            handbackObject = new MarshalledObject("handback");
//            
//        } catch (IOException ex) {
//            
//            // the string must be serializable....
//            throw new AssertionError(ex);
//            
//        }
//        
//        for(int i=0; i<nregistrars; i++) {
//            
//            ServiceRegistrar registrar = registrars[i];
//            
//            ServiceMatches matches = getDataServices(registrar);
//            
//            log.info("Reported: "+matches.totalMatches+" data services on registrar");
//
//            /*
//             * Note: This is a persistent registration.
//             * 
//             * @todo share a single remote listener object for all registered
//             * events?
//             * 
//             * @todo match all appropriate transitions.
//             * 
//             * @todo explore uses for the handback object - should there be one
//             * per registrar? it can serve as a key that identifies the
//             * registrar....
//             * 
//             * @todo the last argument is a lease duration. we will have to
//             * renew the lease. see what management classes exist to make this
//             * easier ( LeaseRenewalManager ).
//             */
//            try {
//                
//                EventRegistration reg = registrar.notify(template,
//                        ServiceRegistrar.TRANSITION_NOMATCH_MATCH // created.
////                        |ServiceRegistrar.TRANSITION_MATCH_MATCH // modified.
//                        |ServiceRegistrar.TRANSITION_MATCH_NOMATCH // deleted.
//                        , new NotifyListener(), handbackObject, leaseDuration);
//                
//                System.err.println("EventRegistration: "+reg);
//                
//            } catch (RemoteException ex) {
//                
//                log.error("Could not register for notification", ex);
//                
//            }
//            
//        }
//        
//    }
//
//    /**
//     * @todo When a registrar is discarded, do we have to do anything? If the
//     *       registrar was providing redundency, then we can still reach the
//     *       various data services. If the registrar was the sole resolver for
//     *       some services then those services are no longer available -- and
//     *       probably an WARNing should be logged.
//     */
//    public void discarded(DiscoveryEvent e) {
//        
//        super.discarded(e);
//        
//    }
//    
//    /**
//     * RemoteEventListener - events are generated by our registered persistent
//     * notification with one or more service registrars that we use to listen
//     * for join/leave of data services.
//     * <p>
//     * This class extends {@link UnicastRemoteObject} so that it can run in the
//     * {@link ServiceRegistrar}. The class MUST be located in the unpacked JAR
//     * identified by the <em>codebase</em>.
//     * 
//     * @todo I have not figured out yet how to get the events back to the
//     *       {@link MetadataServer} - they are being written in the console in
//     *       which jini is running since they are received remotely and then
//     *       need to be passed back to the {@link MetadataServer} somehow.
//     * 
//     * @todo perhaps pass in the {@link ServiceRegistrar} or the
//     *       {@link MarshalledObject} so that we can identify the service for
//     *       which the event was generated and pass the event to code on the
//     *       {@link MetadataServer} instance (transient reference?) that will
//     *       actually handle the event (notice the join/leave of a data
//     *       service).
//     * 
//     * @see http://archives.java.sun.com/cgi-bin/wa?A2=ind0410&L=jini-users&D=0&P=30410
//     * 
//     * @see http://archives.java.sun.com/cgi-bin/wa?A2=ind0410&L=jini-users&D=0&P=29391
//     * 
//     * @see ServiceDiscoveryManager which can encapsulate the entire problem of
//     *      listening and also enumerating the existing services. However, its
//     *      use is limited by NAT (it will not cross a filewall).
//     */
//    public static class NotifyListener extends UnicastRemoteObject implements RemoteEventListener {
//        
//        /**
//         * 
//         */
//        private static final long serialVersionUID = -5847172051441883860L;
//
//        public NotifyListener() throws RemoteException {
//            super();
//        }
//        
//        public NotifyListener(int port) throws RemoteException {
//            super(port);
//        }
//        
//        /**
//         * 
//         * @param e
//         * @throws UnknownEventException
//         * @throws RemoteException
//         */
//        public void notify(RemoteEvent e) throws UnknownEventException, RemoteException {
//        
//            System.err.println("notify(RemoveEvent:"+e+")");
//            log.info(e.toString());
//            
//        }
//
//    }
    
//    /**
//     * Return an {@link IMetadataService}.
//     * 
//     * @param registrar
//     *            A service registrar to query.
//     *            
//     * @return An {@link IMetadataService} if one was found using that
//     *         registrar.
//     */
//    public IMetadataService getMetadataService(ServiceRegistrar registrar) {
//        
//        Class[] classes = new Class[] {IMetadataService.class};
//        
//        ServiceTemplate template = new ServiceTemplate(null, classes, null);
//        
//        IMetadataService proxy = null;
//        
//        try {
//        
//            proxy = (IMetadataService) registrar.lookup(template);
//            
//        } catch(java.rmi.RemoteException e) {
//
//            log.warn(e);
//            
//        }
//
//        return proxy;
//
//    }
//
//    /**
//     * Return the data service(s) matched on this registrar.
//     * 
//     * @param registrar
//     *            The {@link ServiceRegistrar} to be queried.
//     * 
//     * @return The data service or <code>null</code> if none was matched.
//     * 
//     * @todo we need to describe the services to be discovered by their primary
//     *       interface and only search within a designated group that
//     *       corresponds to the bigdata federation of interest - that group is
//     *       part of the client configuration.
//     * 
//     * @todo we need to filter out matches on {@link MetadataService} since it
//     *       extends {@link DataService}.
//     * 
//     * @todo how do we ensure that we have seen all data services? If we query
//     *       each registrar as it is discovered and then register for updates
//     *       there are two ways in which we could miss some instances: (1) new
//     *       data services register between the query and the registration for
//     *       updates; and (2) the query will not return _ALL_ data services
//     *       registered, but only as match as the match limit.
//     */
//    public ServiceMatches getDataServices(ServiceRegistrar registrar) {
//        
//        Class[] classes = new Class[] {IDataService.class};
//        
//        ServiceTemplate template = new ServiceTemplate(null, classes, null);
//        
//        try {
//        
//            return registrar.lookup(template,0);
//            
//        } catch(java.rmi.RemoteException e) {
//
//            log.warn(e);
//
//            return null;
//            
//        }
//
//    }
//    
    /**
     * Extends the behavior to terminate {@link LookupCache} and
     * {@link ServiceDiscoveryManager} processing.
     */
    public void destroy() {
        
        lookupCache.terminate();
        
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
        
        protected AbstractServer server;
        
        /**
         * @param properties
         */
        public AdministrableMetadataService(AbstractServer server, Properties properties) {

            super(properties);
            
            this.server = server;
            
        }

        public Object getAdmin() throws RemoteException {

            log.info("");

            return server.proxy;

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
