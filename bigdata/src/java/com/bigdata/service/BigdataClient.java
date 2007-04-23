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

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.journal.ITransactionManager;
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
 * @todo Write or refactor logic to map operations across multiple partitions.
 * 
 * FIXME Support factory for indices, reuse cached information across
 * transactional and non-transactional views of the same index.
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
    public IMetadataService getMetadataService() throws InterruptedException {
        
        ServiceItem item = null;
        
        for(int i=0; i<10; i++) {
        
            item = metadataServiceLookupCache.lookup(null);
            
            if (item != null) {

                // found one.
                
                break;
                
            }
        
            Thread.sleep(200);
            
        }
        
        if(item==null) {
            
            log.warn("No metadata service in cache");
            
            return null;
            
        }
        
        log.info("Found: "+item);
        
        return (IMetadataService)item.service;
        
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
         * the various registrars to which the metadata server is listening.
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
            
            ServiceTemplate template = new ServiceTemplate(null,
                    new Class[] { IMetadataService.class }, null);

            metadataServiceLookupCache = serviceDiscoveryManager
                    .createLookupCache(template, null /* filter */, null);
            
        } catch(RemoteException ex) {
            
            log.error("Could not setup lookup metadataServiceLookupCache", ex);

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

        if (serviceDiscoveryManager != null) {

            serviceDiscoveryManager.terminate();

            serviceDiscoveryManager = null;
            
        }

        if (discoveryManager != null) {

            discoveryManager.terminate();

            discoveryManager = null;
            
        }

    }

}
