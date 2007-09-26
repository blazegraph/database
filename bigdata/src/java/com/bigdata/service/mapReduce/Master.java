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
 * Created on Mar 17, 2007
 */

package com.bigdata.service.mapReduce;

import java.io.IOException;
import java.rmi.RemoteException;

import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

import com.bigdata.service.BigdataClient;

/**
 * <p>
 * The master for running parallel map/reduce jobs distributed across a cluster.
 * </p>
 * 
 * @todo command line utility that can read an XML job description and run it.
 *       The utility should have a command line option to test against embedded
 *       map, reduce and data services and to limit the #of inputs to process
 *       (just one switch should be enough).
 */
public class Master extends AbstractMaster {

    final MapReduceServiceDiscoveryManager serviceDiscoveryManager;
    
    public Master(MapReduceJob job, BigdataClient client,
            MapReduceServiceDiscoveryManager serviceDiscoveryManager) {

        super(job,client);
        
        // @todo test for active (vs terminated) also since this can be
        // reused across master instances (a master runs a single job,
        // but the discovery stuff can be reused to run many jobs on
        // as many masters).

        if (serviceDiscoveryManager == null)
            throw new IllegalArgumentException();
        
        this.serviceDiscoveryManager = serviceDiscoveryManager;

//        serviceDiscoveryManager.status();

        mapServices = serviceDiscoveryManager.getMapServices();
        
        reduceServices = serviceDiscoveryManager.getReduceServices();
        
        log.info("Discovered "+mapServices.length+" map services");

        log.info("Discovered "+reduceServices.length+" reduce services");

    }

    /**
     * Manages discovery of map/reduce services. An instance of this class can
     * be reused to run multiple map/reduce jobs.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MapReduceServiceDiscoveryManager {
        
        private ServiceDiscoveryManager serviceDiscoveryManager = null;

        private LookupCache mapServiceLookupCache = null;

        private LookupCache reduceServiceLookupCache = null;

        private final ServiceTemplate mapServiceTemplate = new ServiceTemplate(
                null, new Class[] { IMapService.class }, null);

        private final ServiceTemplate reduceServiceTemplate = new ServiceTemplate(
                null, new Class[] { IReduceService.class }, null);

        private ServiceItemFilter mapServiceFilter = null;

        private ServiceItemFilter reduceServiceFilter = null;

        public MapReduceServiceDiscoveryManager(BigdataClient client) {

            /*
             * Setup a helper class that will be notified as services join or
             * leave the various registrars to which the metadata server is
             * listening.
             */
            try {

                serviceDiscoveryManager = new ServiceDiscoveryManager(client
                        .getDiscoveryManagement(), new LeaseRenewalManager());

            } catch (IOException ex) {

                throw new RuntimeException(
                        "Could not initiate service discovery manager", ex);

            }

            /*
             * Setup a LookupCache that is used to keep track of all map
             * services registered with any service registrar to which the
             * client is listening.
             * 
             * @todo provide filtering by attributes identiying the bigdata
             * federation?
             */
            try {

                mapServiceLookupCache = serviceDiscoveryManager
                        .createLookupCache(mapServiceTemplate,
                                mapServiceFilter /* filter */, null);

            } catch (RemoteException ex) {

                terminate();

                throw new RuntimeException(
                        "Could not setup discovery for MapServices", ex);

            }

            /*
             * Setup a LookupCache that is used to keep track of all reduce
             * services registered with any service registrar to which the
             * client is listening.
             * 
             * @todo provide filtering by attributes identiying the bigdata
             * federation?
             */
            try {

                reduceServiceLookupCache = serviceDiscoveryManager
                        .createLookupCache(reduceServiceTemplate,
                                reduceServiceFilter /* filter */, null);

            } catch (RemoteException ex) {

                terminate();

                throw new RuntimeException(
                        "Could not setup discovery for ReduceServices", ex);

            }

        }
        
        /**
         * End discovery, discard caches.
         */
        public void terminate() {

            if(serviceDiscoveryManager!=null) {

                serviceDiscoveryManager.terminate();
             
                serviceDiscoveryManager = null;
                
            }

            if (mapServiceLookupCache != null) {

                mapServiceLookupCache.terminate();
                
                mapServiceLookupCache = null;

            }

            if (reduceServiceLookupCache != null) {

                reduceServiceLookupCache.terminate();
                
                reduceServiceLookupCache = null;

            }

        }

        /**
         * Return an array of all discovered map services.
         */
        public IMapService[] getMapServices() {

            ServiceItem[] serviceItems = mapServiceLookupCache.lookup(
                    mapServiceFilter, Integer.MAX_VALUE);
        
            IMapService[] mapServices = new IMapService[serviceItems.length];
            
            for(int i=0; i<serviceItems.length; i++) {
                
                mapServices[i] = (IMapService)serviceItems[i].service;
                
            }
            
            return mapServices;
            
        }
        
        /**
         * Return an array of all discovered reduce services.
         */
        public IReduceService[] getReduceServices() {

            ServiceItem[] serviceItems = reduceServiceLookupCache.lookup(
                    reduceServiceFilter, Integer.MAX_VALUE);
        
            IReduceService[] reduceServices = new IReduceService[serviceItems.length];
            
            for(int i=0; i<serviceItems.length; i++) {
                
                reduceServices[i] = (IReduceService)serviceItems[i].service;
                
            }
            
            return reduceServices;
            
        }
        

//        /**
//         * @todo remove this.
//         */
//        public void status() {
//
////            // wait a bit for services to be discovered.
////            try {
////                Thread.sleep(1000);
////            } catch (InterruptedException e) {
////                // ignore
////            }
//
//            ServiceItem[] mapServiceItems = mapServiceLookupCache.lookup(
//                    mapServiceFilter, Integer.MAX_VALUE);
//            
//            ServiceItem[] reduceServiceItems = reduceServiceLookupCache.lookup(
//                    reduceServiceFilter, Integer.MAX_VALUE);
//
//            System.err.println("Discovered "+mapServiceItems.length+" map services");
//
//            System.err.println("Discovered "+reduceServiceItems.length+" reduce services");
//
//        }
        
    }
    
//    /**
//     * Filter matches only {@link ServiceItem}s for map services
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    static public class MapServiceFilter implements ServiceItemFilter {
//
//        public boolean check(ServiceItem item) {
//
//            if(item.service==null) {
//                
//                log.warn("Service is null: "+item);
//
//                return false;
//                
//            }
//
//            if (item.service instanceof IMapService) {
//
//                log.info("Matched: " + item);
//
//                return true;
//
//            }
//
//            log.debug("Ignoring: "+item);
//            
//            return false;
//            
//        }
//        
//    }
//    
//    /**
//     * Filter matches only {@link ServiceItem}s for reduce services
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    static public class ReduceServiceFilter implements ServiceItemFilter {
//
//        public boolean check(ServiceItem item) {
//
//            if(item.service==null) {
//                
//                log.warn("Service is null: "+item);
//
//                return false;
//                
//            }
//
//            if (item.service instanceof IReduceService) {
//
//                log.info("Matched: " + item);
//
//                return true;
//
//            }
//
//            log.debug("Ignoring: "+item);
//            
//            return false;
//            
//        }
//        
//    }
    
}
