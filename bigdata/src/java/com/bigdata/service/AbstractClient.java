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

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.LookupDiscovery;
import net.jini.discovery.LookupDiscoveryManager;

import org.apache.log4j.Logger;

import com.bigdata.journal.ITransactionManager;

/**
 * Abstract base class for a bigdata client.
 * <p>
 * Clients are configured to perform service lookup with a jini group that
 * identifies the bigdata federation. Clients begin by discovering the
 * {@link IMetadataService}. Clients use the {@link IMetadataService} to manage
 * indices (add/drop/proxy). Once a client has a proxy for an index, it carries
 * out read and write operations using that proxy. The proxy is responsible for
 * transparently discovering the {@link IDataService}s on which the index
 * partitions are located and directing read and write operations appropriately.
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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractClient implements DiscoveryListener {

    public static final transient Logger log = Logger
            .getLogger(AbstractClient.class);

    /**
     * The label in the {@link Configuration} file for the service
     * description.
     */
    protected final static transient String SERVICE_LABEL = "ServiceDescription";

    /**
     * The label in the {@link Configuration} file for the service advertisment
     * data.
     */
    protected final static transient String ADVERT_LABEL = "AdvertDescription";

    private DiscoveryManagement discoveryManager;

    private Configuration config;

    /**
     * Server startup reads {@link Configuration} data from the file(s) named by
     * <i>args</i>, starts the service, and advertises the service for
     * discovery. Aside from the server class to start, the behavior is more or
     * less entirely parameterized by the {@link Configuration}.
     * 
     * @param args
     *            The command line arguments.
     */
    protected AbstractClient(String[] args) {

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

            groups = (String[]) config.getEntry(ADVERT_LABEL, "groups",
                    String[].class, LookupDiscovery.ALL_GROUPS/* default */);

            unicastLocators = (LookupLocator[]) config
                    .getEntry(ADVERT_LABEL, "unicastLocators",
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
            discoveryManager = new LookupDiscoveryManager(groups,
                    unicastLocators, this
            );
            
//            discoveryManager = new LookupDiscovery(groups);

        } catch (IOException ex) {

            log.fatal("Lookup service discovery error: " + ex, ex);

            try {
                discoveryManager.terminate();
            } catch (Throwable t) {
                /* ignore */
            }

            System.exit(1);

        }

    }

    /**
     * Return the data service matched on this registrar.
     * 
     * @param registrar
     * 
     * @return The data service or <code>null</code> if none was matched.
     * 
     * @todo this belongs in the metadata service since it needs to discover
     *       data services. It also needs to know when data services start and
     *       stop so it needs updates based on the service template.
     * 
     * @todo the client on the other hand needs to discover a single metadata
     *       service and a single transaction manager service. if either the
     *       metadata service or the transaction manager service goes down, then
     *       it needs to discover another service so that it can keep working.
     * 
     * @todo we need to describe the services to be discovered by their primary
     *       interface and only search within a designated group that
     *       corresponds to the bigdata federation of interest - that group is
     *       part of the client configuration.
     */
    public IDataService getDataService(ServiceRegistrar registrar) {
        
        Class[] classes = new Class[] {IDataService.class};
        
        ServiceTemplate template = new ServiceTemplate(null, classes, null);
        
        IDataService proxy = null;
        
        try {
        
            proxy = (IDataService) registrar.lookup(template);
            
        } catch(java.rmi.RemoteException e) {

            log.warn(e);
            
        }

        return proxy;

    }
    
    /**
     * Return an {@link IMetadataService}.
     * 
     * @param registrar
     *            A service registrar to query.
     *            
     * @return An {@link IMetadataService} if one was found using that
     *         registrar.
     */
    public IMetadataService getMetadataService(ServiceRegistrar registrar) {
        
        Class[] classes = new Class[] {IMetadataService.class};
        
        ServiceTemplate template = new ServiceTemplate(null, classes, null);
        
        IMetadataService proxy = null;
        
        try {
        
            proxy = (IMetadataService) registrar.lookup(template);
            
        } catch(java.rmi.RemoteException e) {

            log.warn(e);
            
        }

        return proxy;

    }
    
    /**
     * Return an {@link IMetadataService}.
     * 
     * @param a
     *            An array of registrars to query.
     * 
     * @return An {@link IMetadataService} if one was found.
     * 
     * @todo while the client only needs a single metadata service, the data
     *       services themselves must register with all metadata services
     *       discovered in their group (and I must sort out how the determine
     *       primary vs secondary metadata services, e.g., by a status on the
     *       service or some custom api).
     */
    public IMetadataService getMetadataService(ServiceRegistrar[] a) {
        
        IMetadataService proxy = null;
        
        for(int i=0; i<a.length && proxy == null; i++) {
            
            proxy = getMetadataService(a[i]);
            
        }
        
        return proxy;
        
    }
   
    /**
     * Invoked when a lookup service is discarded.
     */
    public void discarded(DiscoveryEvent arg0) {
        log.info(""+arg0);
        // TODO Auto-generated method stub
        
    }

    /**
     * Invoked when a lookup service is discovered.
     */
    public void discovered(DiscoveryEvent arg0) {
        log.info(""+arg0);
        // TODO Auto-generated method stub
        
    }

}
