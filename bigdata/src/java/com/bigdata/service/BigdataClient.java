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
import java.rmi.RemoteException;
import java.util.UUID;

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

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.CommitRecordIndex.Entry;
import com.bigdata.scaleup.IPartitionMetadata;

/**
 * Abstract base class for a bigdata client.
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
 * directing read and write operations appropriately. 
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
 * @todo Write or refactor logic to map operations across multiple partitions.
 * 
 * @todo reuse cached information across transactional and non-transactional
 *       views of the same index.
 * 
 * @todo Use lambda expressions (and downloaded code) for server-side logic for
 *       batch operations.
 * 
 * @todo support failover metadata service discovery.
 * 
 * @todo document client configuration, the relationship between jini groups and
 *       a bigdata federation, and whether and how a single client could connect
 *       to more than one bigdata federation.
 * 
 * @todo Use a weak-ref cache with an LRU (or hard reference cache) to retain
 *       cached {@link IPartitionMetadata}.
 * 
 * @todo add a cache for {@link DataService}s. The client needs access by {
 *       indexName, key } to obtain a {@link ServiceID} for a
 *       {@link DataService} and then needs to translate the {@link ServiceID}
 *       to a data service proxy using a cache. The use case is not quite the
 *       same as the {@link MetadataServer} since a client is not interested in
 *       a cache of all {@link DataService}s but only in those to which its
 *       attention has been directed based on the index partitions on which it
 *       is operating. Also, the cache needs to be a weak-ref/LRU combination or
 *       hard reference cache so that we can discard cached services that we are
 *       not interested in tracking. There are other differences to, the client
 *       interest is really based on the index partition so if the index
 *       partition is moved, then the data service may no longer be of interest.
 * 
 * @see ClientIndexView
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataClient {//implements DiscoveryListener {

    public static final transient Logger log = Logger
            .getLogger(BigdataClient.class);

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
    private DataServiceMap dataServiceMap = new DataServiceMap();

    private final ServiceTemplate metadataServiceTemplate = new ServiceTemplate(
            null, new Class[] { IMetadataService.class }, null);

    private final ServiceTemplate dataServiceTemplate = new ServiceTemplate(
            null, new Class[] { IDataService.class }, null);

    private ServiceItemFilter metadataServiceFilter = null;

    private ServiceItemFilter dataServiceFilter = new MetadataServer.DataServiceFilter();

    /**
     * Return the {@link MetadataService} from the cache. If there is a cache
     * miss, then this will wait a bit for a {@link MetadataService} to show up
     * in the cache before reporting failure.
     * 
     * @return The {@link MetadataService} or <code>null</code> if none was
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
     * Resolve the {@link ServiceID} to an {@link IDataService} using a local
     * cache.
     * 
     * @param serviceID
     *            The identifier for a {@link DataService}.
     *            
     * @return The proxy for that {@link DataService} or <code>null</code> iff
     *         the {@link DataService} could not be discovered.
     */
    public IDataService getDataService(ServiceID serviceID) {
        
        ServiceItem item = dataServiceMap.getDataServiceByID(serviceID);
        
        if (item == null) {

            /*
             * cache miss. do a remote query on the managed set of service
             * registrars.
             */

            log.info("Cache miss.");
            
            final long timeout = 1000L; // millis.

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
        
        log.info("Found: "+item);
        
        return (IDataService)item.service;
        
    }
    
    /**
     * Client startup reads {@link Configuration} data from the file(s) named by
     * <i>args</i>, starts the client, attempts to discover one or more
     * registrars and establishes a lookup cache for {@link MetadataService}s.
     * 
     * @param args
     *            The command line arguments.
     * 
     * @see #terminate()
     */
    public BigdataClient(String[] args) {

        // @todo verify that this belongs here.
        System.setSecurityManager(new SecurityManager());

        LookupLocator[] unicastLocators = null;
        String[] groups = null;

        try {

            config = ConfigurationProvider.getInstance(args);

            /*
             * Extract how the client will discover services from the
             * Configuration.
             */

            groups = (String[]) config.getEntry(CLIENT_LABEL, "groups",
                    String[].class, LookupDiscovery.ALL_GROUPS/* default */);

            unicastLocators = (LookupLocator[]) config
                    .getEntry(CLIENT_LABEL, "unicastLocators",
                            LookupLocator[].class, null/* default */);

        } catch (ConfigurationException ex) {

            log.fatal("Configuration error: " + ex, ex);

            System.exit(1);
            
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
                    groups, unicastLocators, null /*DiscoveryListener*/
            );
            
//            discoveryManager = new LookupDiscovery(groups);

        } catch (IOException ex) {

            log.fatal("Lookup service discovery error: " + ex, ex);

            terminate();

            System.exit(1);

        }

        /*
         * Setup a helper class that will be notified as services join or leave
         * the various registrars to which the client is listening.
         */
        try {

            serviceDiscoveryManager = new ServiceDiscoveryManager(
                    discoveryManager, new LeaseRenewalManager());
            
        } catch(IOException ex) {
            
            log.error("Could not initiate service discovery manager", ex);
            
            terminate();
            
            System.exit(1);
            
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
            
            log.error("Could not setup MetadataService LookupCache", ex);

            terminate();
            
            System.exit(1);
            
        }

        /*
         * Setup a LookupCache that is used to keep track of all data services
         * registered with any service registrar to which the client is
         * listening.
         * 
         * @todo provide filtering by attributes to select the primary vs
         * failover metadata servers? by attributes identiying the bigdata
         * federation?
         */
        try {

            dataServiceLookupCache = serviceDiscoveryManager.createLookupCache(
                    dataServiceTemplate,
                    dataServiceFilter /* filter */, dataServiceMap);
            
        } catch(RemoteException ex) {
            
            log.error("Could not setup DataService LookupCache", ex);

            terminate();
            
            System.exit(1);
            
        }

    }

    /**
     * Terminates various background processes.
     */
    public void terminate() {

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
     * Connect to a bigdata federation.
     * 
     * @return
     * 
     * @todo determine how a federation will be identified, e.g., by a name that
     *       is an {@link Entry} on the {@link MetadataServer} and
     *       {@link DataServer} service descriptions and provide that name
     *       attribute here. Note that a {@link MetadataService} can failover,
     *       so the {@link ServiceID} for the {@link MetadataService} is not the
     *       invariant, but rather the name attribute for the federation.
     */
    public IBigdataFederation connect() {

        return new BigdataFederation(this);

    }
    
    /**
     * Interface to a bigdata federation.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo declare public methods intended for application use.
     */
    public static interface IBigdataFederation {

        /**
         * A constant that may be used as the transaction identifier when the
         * operation is <em>unisolated</em> (non-transactional).  The value of
         * this constant is ZERO (0L).
         */
        public static final long UNISOLATED = 0L;

        /**
         * Register a scale-out index with the federation.
         * 
         * @param name
         *            The index name.
         *            
         * @return The UUID that identifies the scale-out index.
         */
        public UUID registerIndex(String name);
        
        /**
         * Obtain a view on a partitioned index.
         * 
         * @param tx
         *            The transaction identifier or zero(0L) iff the index will
         *            be unisolated.
         * 
         * @param name
         *            The index name.
         * 
         * @return The index or <code>null</code> if the index is not
         *         registered with the {@link MetadataService}.
         */
        public IIndex getIndex(long tx, String name);

    }

    /**
     * This class encapsulates access to the metadata and data services for a
     * bigdata federation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BigdataFederation implements IBigdataFederation {
       
        private final BigdataClient client;

        public IMetadataService getMetadataService() {
            
            return client.getMetadataService();
            
        }
        
        public BigdataFederation(BigdataClient client) {
            
            if(client==null) throw new IllegalArgumentException();
            
            this.client = client;
            
        }
        
        public UUID registerIndex(String name) {
            
            try {

                return getMetadataService().registerIndex(name);
                
            } catch(Exception ex) {
                
                log.error(ex);
                
                throw new RuntimeException(ex);
                
            }
            
        }
        
        /**
         * @todo support isolated views, share cached data service information
         *       between isolated and unisolated views.
         */
        public IIndex getIndex(long tx, String name) {

            // @todo if(tx!=0l) validate exists?
            
            return new ClientIndexView(this,tx,name); 
            
        }

        /**
         * @todo setup cache. test cache and lookup on metadata service if a
         *       cache miss. the cache should be based on a lease and the data
         *       service should know whether an index partition has been moved
         *       and notify the client that it needs to re-discover the data
         *       service for an index partition. the cache is basically a
         *       partial copy of the metadata index that is local to the client.
         *       The cache needs to have "fake" entries that are the
         *       left-sibling of each real partition entry so that it can
         *       correctly determine when there is a cache miss.
         *       <p>
         *       Note that index partition definitions will evolve slowly over
         *       time through splits and joins of index segments. again, the
         *       client should presume consistency of its information but the
         *       data service should know when it no longer has information for
         *       a key range in a partition (perhaps passing the partitionId and
         *       a timestamp for the last partition update to the data service
         *       with each request).
         *       <p>
         *       Provide a historical view of the index partition definitions
         *       when transactional isolation is in use by the client. This
         *       should make it possible for a client to not be perturbed by
         *       split/joins of index partitions when executing with
         *       transactional isolation.
         *       <p>
         *       Note that service failover is at least partly orthogonal to the
         *       partition metadata in as much as the index partition
         *       definitions themselves do not evolve (the same separator keys
         *       are in place and the same resources have the consistent data
         *       for a view of the index partition), but it is possible that the
         *       data services have changed. It is an open question how to
         *       maintain isolation with failover while supporting failover
         *       without aborting the transaction. The most obvious thing is to
         *       have a transationally isolated client persue the failover
         *       services already defined in the historical transaction without
         *       causing the partition metadata to be updated on the metadata
         *       service. (Unisolated clients would begin to see updated
         *       partition metadata more or immediately.)
         * 
         * @param tx
         * @param name
         * @param key
         * @return
         */
        public IPartitionMetadata getPartition(long tx, String name, byte[] key) {

            /*
             * Request the index partition metadata for the initial partition of the
             * scale-out index.
             */

            IPartitionMetadata pmd;
            
            try {
             
                byte[] val = getMetadataService().getPartition(name, key);
                
                if(val ==null) return null;
                
                pmd = (IPartitionMetadata) SerializerUtil.deserialize(val);
                
            } catch(Exception ex) {
                
                throw new RuntimeException(ex);
                
            }

            return pmd;

        }
//        
//        private Map<String, Map<Integer, IDataService>> indexCache = new ConcurrentHashMap<String, Map<Integer, IDataService>>(); 
//
//        synchronized(indexCache) {
//      
//          Map<Integer,IDataService> partitionCache = indexCache.get(name);
//       
//          if(partitionCache==null) {
//              
//              partitionCache = new ConcurrentHashMap<Integer, IDataService>();
//              
//              indexCache.put(name, partitionCache);
//              
//          }
//          
//          IDataService dataService = 
//          
//      }

        /**
         * Resolve the data service to which the index partition was mapped.
         * 
         * @todo use lookup cache in a real client.
         */
        public IDataService getDataService(IPartitionMetadata pmd) {

            ServiceID serviceID = JiniUtil
                    .uuid2ServiceID(pmd.getDataServices()[0]);

            final IDataService dataService;
            
            try {

                dataService = client.getDataService(serviceID);
                
            } catch(Exception ex) {
                
                throw new RuntimeException(ex);
                
            }

            return dataService;

        }
        
    }
    
}
