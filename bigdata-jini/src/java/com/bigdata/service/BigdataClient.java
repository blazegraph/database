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
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.LookupDiscovery;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

import com.bigdata.btree.IIndex;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.CommitRecordIndex.Entry;
import com.bigdata.scaleup.IPartitionMetadata;

/**
 * A client capable of connecting to a distributed bigdata federation using
 * JINI.
 * <p>
 * Clients are configured to perform service lookup with a jini group that
 * identifies the bigdata federation. Clients begin by discovering the
 * {@link IMetadataService}. Clients use the {@link IMetadataService} to manage
 * scale-out indices (add/drop) and to obtain the {@link IDataService} that it
 * must talk to for any given partition of a scale-out index. Once a client has
 * the {@link IDataService} for an index partition, it carries out read and
 * write operations using that {@link IDataService}. An {@link IIndex} factory
 * is provided that hides the {@link IMetadataService} and {@link IDataService}
 * communications from the application. The {@link IIndex} objects provided by
 * the factory are responsible for transparently discovering the
 * {@link IDataService}s on which the index partitions are located and
 * directing read and write operations appropriately.  See {@link ClientIndexView}.
 * <p>
 * A client may discover and use an {@link ITransactionManager} if needs to use
 * transactions as opposed to unisolated reads and writes. When the client
 * requests a transaction, the transaction manager responds with a long integer
 * containing the transaction identifier - this is simply the unique start time
 * assigned to that transaction by the transaction manager. The client then
 * provides that transaction identifier for operations that are isolated within
 * the transaction. When the client is done with the transaction, it must use
 * the transaction manager to either abort or commit the transaction.
 * (Transactions that fail to progress may be eventually aborted.)
 * <p>
 * When using unisolated operations, the client does not need to resolve or use
 * the transaction manager and it simply specifies <code>0L</code> as the
 * transaction identifier for its read and write operations.
 * 
 * @todo write tests where an index is static partitioned over multiple data
 *       services and verify that the {@link ClientIndexView} is consistent.
 *       <p>
 *       Work towards the same guarentee when dynamic partitioning is enabled.
 * 
 * @todo support transactions (there is no transaction manager service yet and
 *       the 2-/3-phase commit protocol has not been implemented on the
 *       journal).
 * 
 * @todo reuse cached information across transactional and non-transactional
 *       views of the same index.
 * 
 * @todo support failover metadata service discovery.
 * 
 * @todo document client configuration, the relationship between jini groups and
 *       a bigdata federation, and whether and how a single client could connect
 *       to more than one bigdata federation.
 * 
 * @todo Use a weak-ref cache with an LRU (or hard reference cache) to retain
 *       cached {@link IPartitionMetadata}. The client needs access by {
 *       indexName, key } to obtain a {@link ServiceID} for a
 *       {@link DataService} and then needs to translate the {@link ServiceID}
 *       to a data service using the {@link #dataServiceMap}.
 * 
 * @see ClientIndexView
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataClient implements IBigdataClient {//implements DiscoveryListener {

    /**
     * The label in the {@link Configuration} file for the client configuration
     * data.
     */
    protected final static transient String CLIENT_LABEL = "ClientDescription";

    private Configuration config;
    private DiscoveryManagement discoveryManager = null;
    private ServiceDiscoveryManager serviceDiscoveryManager = null;
    private LookupCache metadataServiceLookupCache = null;
    private LookupCache dataServiceLookupCache = null;
    
    /**
     * Used to provide {@link DataService} lookup by {@link ServiceID}.
     */
    private ServiceCache dataServiceMap = new ServiceCache();

    private final ServiceTemplate metadataServiceTemplate = new ServiceTemplate(
            null, new Class[] { IMetadataService.class }, null);

    private final ServiceTemplate dataServiceTemplate = new ServiceTemplate(
            null, new Class[] { IDataService.class }, null);
    private ServiceItemFilter metadataServiceFilter = null;

    private ServiceItemFilter dataServiceFilter = new DataServiceFilter();

    public DiscoveryManagement getDiscoveryManagement() {
        
        return discoveryManager;
        
    }
    
    /**
     * Return the {@link IMetadataService} from the cache. If there is a cache
     * miss, then this will wait a bit for a {@link IMetadataService} to show up
     * in the cache before reporting failure.
     * 
     * @return The {@link IMetadataService} or <code>null</code> if none was
     *         found.
     * 
     * @throws InterruptedException
     * 
     * @todo force synchronous lookup on the {@link ServiceDiscoveryManager}
     *       when there is a cache miss?
     * 
     * @todo filter for primary vs failover service? maintain simple field for
     *       return?
     * 
     * @todo parameter for the bigdata federation? (filter)
     */
    public IMetadataService getMetadataService() {
        
        ServiceItem item = metadataServiceLookupCache.lookup(null);
            
        if (item == null) {

            /*
             * cache miss. do a remote query on the managed set of service
             * registrars.
             */

            log.info("Cache miss.");
            
            final long timeout = 1000L; // millis.

            try {

                item = serviceDiscoveryManager.lookup(metadataServiceTemplate,
                        metadataServiceFilter, timeout);

            } catch (RemoteException ex) {
                
                log.error(ex);
                
                return null;
                
            } catch(InterruptedException ex) {
                
                log.info("Interrupted - no match.");
                
                return null;
                
            }

            if( item == null ) {
                
                // Could not discover a metadata service.
                
                log.warn("Could not discover metadata service");
                
                return null;
                
            }
            
        }
        
        log.info("Found: "+item);
        
        return (IMetadataService)item.service;
        
    }

    /**
     * Returns data services known to the client at the time of this request.
     */
    public UUID[] getDataServiceUUIDs(int maxCount) {
        
        ServiceItem[] items = dataServiceMap.getServiceItems(maxCount);
        
        UUID[] uuids = new UUID[items.length];
        
        for(int i=0; i<items.length; i++) {
            
            uuids[i] = JiniUtil.serviceID2UUID(items[i].serviceID);
            
        }

        return uuids;
        
    }
    
    /**
     * Resolve the {@link ServiceID} to an {@link IDataService} using a local
     * cache.
     * 
     * @param serviceUUID
     *            The identifier for a {@link DataService}.
     * 
     * @return The proxy for that {@link DataService} or <code>null</code> iff
     *         the {@link DataService} could not be discovered.
     */
    public IDataService getDataService(UUID serviceUUID) {
        
        ServiceID serviceID = JiniUtil.uuid2ServiceID(serviceUUID);
        
        ServiceItem item = dataServiceMap.getServiceItemByID(serviceID);
        
        if (item == null) {

            /*
             * cache miss. do a remote query on the managed set of service
             * registrars.
             */

            log.info("Cache miss.");
            
            final long timeout = 2000L; // millis.

            try {

                item = serviceDiscoveryManager.lookup(dataServiceTemplate,
                        dataServiceFilter, timeout);

            } catch (RemoteException ex) {
                
                log.error(ex);
                
                return null;
                
            } catch(InterruptedException ex) {
                
                log.info("Interrupted - no match.");
                
                return null;
                
            }

            if( item == null ) {
                
                // Could not discover a data service.
                
                log.warn("Could not discover data service");
                
                return null;
                
            }
            
        }
        
//        log.info("Found: "+item);
        
        return (IDataService)item.service;
        
    }
    
    /**
     * Conditionally install a suitable security manager if there is none in
     * place. This is required before the client can download code. The code
     * will be downloaded from the HTTP server identified by the
     * <code>java.rmi.server.codebase</code> property specified for the VM
     * running the service.
     */
    protected void setSecurityManager() {

        SecurityManager sm = System.getSecurityManager();
        
        if (sm == null) {

            System.setSecurityManager(new SecurityManager());
         
            log.info("Set security manager");

        } else {
            
            log.info("Security manager already in place: "+sm.getClass());
            
        }

    }
    
    /**
     * Client startup reads {@link Configuration} data from the file(s) named by
     * <i>args</i>, starts the client, attempts to discover one or more
     * registrars and establishes a lookup cache for {@link MetadataService}s
     * and {@link DataService}s.
     * 
     * @param args
     *            The command line arguments.
     * 
     * @see #terminate()
     */
    public BigdataClient(String[] args) {

        setSecurityManager();

        LookupLocator[] lookupLocators = null;
        String[] groups = null;

        try {

            config = ConfigurationProvider.getInstance(args);

            /*
             * Extract how the client will discover services from the
             * Configuration.
             */

            groups = (String[]) config.getEntry(CLIENT_LABEL, "groups",
                    String[].class, LookupDiscovery.ALL_GROUPS/* default */);

            /*
             * Note: multicast discovery is used regardless if
             * LookupDiscovery.ALL_GROUPS is selected above. That is why there
             * is no default for the lookupLocators. The default "ALL_GROUPS"
             * means that the lookupLocators are ignored.
             */
            
            lookupLocators = (LookupLocator[]) config
                    .getEntry(CLIENT_LABEL, "unicastLocators",
                            LookupLocator[].class, null/* default */);

        } catch (ConfigurationException ex) {

            throw new RuntimeException("Configuration error: " + ex, ex);
            
        }
        
        try {

            /*
             * Note: This class will perform multicast discovery if ALL_GROUPS
             * is specified and otherwise requires you to specify one or more
             * unicast locators (URIs of hosts running discovery services). As
             * an alternative, you can use LookupDiscovery, which always does
             * multicast discovery.
             */
            discoveryManager = new LookupDiscoveryManager(
                    groups, lookupLocators, null /*DiscoveryListener*/
            );
            
//            discoveryManager = new LookupDiscovery(groups);

        } catch (IOException ex) {

            terminate();

            throw new RuntimeException("Lookup service discovery error: " + ex,
                    ex);

        }

        /*
         * Setup a helper class that will be notified as services join or leave
         * the various registrars to which the client is listening.
         */
        try {

            serviceDiscoveryManager = new ServiceDiscoveryManager(
                    discoveryManager, new LeaseRenewalManager());
            
        } catch(IOException ex) {
            
            terminate();
            
            throw new RuntimeException("Could not initiate service discovery manager", ex);
            
        }

        /*
         * Setup a LookupCache that is used to keep track of all metadata
         * services registered with any service registrar to which the client is
         * listening.
         * 
         * @todo provide filtering by attributes to select the primary vs
         * failover metadata servers? by attributes identiying the bigdata
         * federation?
         */
        try {
            
            metadataServiceLookupCache = serviceDiscoveryManager.createLookupCache(
                    metadataServiceTemplate,
                    null /* filter */, null);
            
        } catch(RemoteException ex) {
            
            terminate();
            
            throw new RuntimeException("Could not setup MetadataService LookupCache", ex);
            
        }

        /*
         * Setup a LookupCache that is used to keep track of all data services
         * registered with any service registrar to which the client is
         * listening.
         * 
         * @todo provide filtering by attributes to select the primary vs
         * failover data servers? by attributes identiying the bigdata
         * federation?
         */
        try {

            dataServiceLookupCache = serviceDiscoveryManager.createLookupCache(
                    dataServiceTemplate,
                    dataServiceFilter /* filter */, dataServiceMap);
            
        } catch(RemoteException ex) {
            
            terminate();
            
            throw new RuntimeException(
                    "Could not setup DataService LookupCache", ex);
            
        }

    }

    public void terminate() {

        if( fed != null ) {

            // disconnect from the federation.
            fed.disconnect();
            
        }
        
        /*
         * Stop various discovery processes.
         */
         
        if (metadataServiceLookupCache != null) {

            metadataServiceLookupCache.terminate();
            
            metadataServiceLookupCache = null;

        }

        if (dataServiceLookupCache != null) {

            dataServiceLookupCache.terminate();
            
            dataServiceLookupCache = null;

        }

        if (serviceDiscoveryManager != null) {

            serviceDiscoveryManager.terminate();

            serviceDiscoveryManager = null;
            
        }

        if (discoveryManager != null) {

            discoveryManager.terminate();

            discoveryManager = null;
            
        }

    }

    /**
     * Connect to a bigdata federation. If the client is already connected, then
     * the existing connection is returned.
     * 
     * @return The federation.
     * 
     * @todo determine how a federation will be identified, e.g., by a name that
     *       is an {@link Entry} on the {@link MetadataServer} and
     *       {@link DataServer} service descriptions and provide that name
     *       attribute here. Note that a {@link MetadataService} can failover,
     *       so the {@link ServiceID} for the {@link MetadataService} is not the
     *       invariant, but rather the name attribute for the federation.
     */
    public IBigdataFederation connect() {

        if (fed == null) {

            fed = new BigdataFederation(this);

        }

        return fed;

    }
    private IBigdataFederation fed = null;
    
    /**
     * Await the availability of an {@link IMetadataService} and the specified
     * minimum #of {@link IDataService}s.
     * 
     * @param minDataServices
     *            The minimum #of data services.
     * @param timeout
     *            The timeout (ms).
     * 
     * @return The #of data services that are available.
     * 
     * @throws InterruptedException
     * @throws TimeoutException
     *             If a timeout occurs.
     */
    public int awaitServices(int minDataServices, long timeout) throws InterruptedException, TimeoutException {
        
        assert minDataServices > 0;
        assert timeout > 0;
        
        final long begin = System.currentTimeMillis();
        
        while ((System.currentTimeMillis() - begin) < timeout) {

            // verify that the client has/can get the metadata service.
            IMetadataService metadataService = getMetadataService();

            UUID[] dataServiceUUIDs = getDataServiceUUIDs(0);
        
            if (metadataService == null
                    || dataServiceUUIDs.length < minDataServices) {
                
                log.info("Waiting...");
                
                Thread.sleep(1000/*ms*/);
                
                continue;
                
            }
            
            log.info("Have metadata service and "+dataServiceUUIDs.length+" data services");
            
            return dataServiceUUIDs.length;
            
        }
        
        throw new TimeoutException();
        
    }

}
