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

package com.bigdata.service.jini;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationProvider;
import net.jini.core.discovery.LookupLocator;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.LookupDiscovery;
import net.jini.discovery.LookupDiscoveryManager;

import com.bigdata.Banner;
import com.bigdata.btree.IIndex;
import com.bigdata.service.AbstractBigdataClient;
import com.bigdata.service.BigdataFederation;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.MetadataService;

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
 * directing read and write operations appropriately. See
 * {@link ClientIndexView}.
 * <p>
 * A client may discover and use an {@link ITransactionManagerService} if needs
 * to use transactions as opposed to unisolated reads and writes. When the
 * client requests a transaction, the transaction manager responds with a long
 * integer containing the transaction identifier - this is simply the unique
 * start time assigned to that transaction by the transaction manager. The
 * client then provides that transaction identifier for operations that are
 * isolated within the transaction. When the client is done with the
 * transaction, it must use the transaction manager to either abort or commit
 * the transaction. (Transactions that fail to progress may be eventually
 * aborted.)
 * <p>
 * When using unisolated operations, the client does not need to resolve or use
 * the transaction manager and it simply specifies <code>0L</code> as the
 * transaction identifier for its read and write operations.
 * 
 * @todo document client configuration, the relationship between jini groups and
 *       a bigdata federation, and whether and how a single client could connect
 *       to more than one bigdata federation. the {@link DataServicesClient}
 *       will need to be parameterized to filter for only the federation of
 *       interest.  See {@link IBigdataClient#connect()}.
 * 
 * @see ClientIndexView
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataClient extends AbstractBigdataClient {

    /**
     * The label in the {@link Configuration} file for the client configuration
     * data.
     */
    protected final static transient String CLIENT_LABEL = "ClientDescription";

    private DataServicesClient dataServicesClient;

    private LoadBalancerClient loadBalancerClient;
    
    private DiscoveryManagement discoveryManager;

    public DiscoveryManagement getDiscoveryManagement() {
        
        return discoveryManager;
        
    }
    
    public ILoadBalancerService getLoadBalancerService() {
        
        return loadBalancerClient.getLoadBalancerService();
        
    }
    
    public IMetadataService getMetadataService() {
        
        // Note: allowed before 'connected'.
//      assertConnected();
      
        if(dataServicesClient==null) throw new IllegalStateException();
        
        return dataServicesClient.getMetadataService();
                
    }

    public UUID[] getDataServiceUUIDs(int maxCount) {
        
        // Note: allow before connected.
//        assertConnected();

        if(dataServicesClient==null) throw new IllegalStateException();

        return dataServicesClient.getDataServiceUUIDs(maxCount);
        
    }
    
    public IDataService getDataService(UUID serviceUUID) {
        
        // Note: allow before connected.
//        assertConnected();

        if(dataServicesClient==null) throw new IllegalStateException();

        return dataServicesClient.getDataService(serviceUUID);
                
    }
    
    /**
     * Conditionally install a suitable security manager if there is none in
     * place. This is required before the client can download code. The code
     * will be downloaded from the HTTP server identified by the
     * <code>java.rmi.server.codebase</code> property specified for the VM
     * running the service.
     */
    static protected void setSecurityManager() {

        SecurityManager sm = System.getSecurityManager();
        
        if (sm == null) {

            System.setSecurityManager(new SecurityManager());
         
            log.info("Set security manager");

        } else {
            
            log.info("Security manager already in place: "+sm.getClass());
            
        }

    }
    
    protected BigdataClient(Properties properties) {

        super(properties);
        
        loadBalancerClient = new LoadBalancerClient(discoveryManager);
        
    }
    
    /**
     * Client startup reads {@link Configuration} data from the file(s) named by
     * <i>args</i>, reads the <i>properties</i> file named in the
     * {@value #CLIENT_LABEL} section of the {@link Configuration} file, creates
     * and starts a new client, initiaties discovery for one or more service
     * registrars and establishes a lookup cache for {@link MetadataService}s
     * and {@link DataService}s.
     * 
     * @param args
     *            The command line arguments.
     * 
     * @return The new client.
     * 
     * @throws RuntimeException
     *             if there is a problem: reading the jini configuration for the
     *             client; reading the properties for the client; starting
     *             service discovery, etc.
     */
    public static BigdataClient newInstance(String[] args) {

        // show the copyright banner during statup.
        Banner.banner();

        setSecurityManager();

        /*
         * First read all the configuration data and the properties file.
         */
        final String[] groups;
        final LookupLocator[] lookupLocators;
        final Properties properties;
        try {

            // Obtain the configuration object.
            final Configuration config = ConfigurationProvider
                    .getInstance(args);

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

            /*
             * Extract the name of the properties file used to configure the
             * bigdata client.
             */
            final File propertyFile = (File) config.getEntry(CLIENT_LABEL,
                    "propertyFile", File.class);

            /*
             * Read the properties file.
             */
            properties = getProperties(propertyFile);

        } catch (Exception ex) {

            /*
             * Note: No asynchronous processes have been started so we just wrap
             * the exception and throw it out.
             */

            throw new RuntimeException(ex);

        }
        
        /*
         * Now create the bigdata client object.
         * 
         * Note: once the client has been created we need to invoke
         * shutdownNow() on it if there is a problem.
         */
        final BigdataClient client = new BigdataClient(properties);

        try {

            /*
             * Note: This class will perform multicast discovery if ALL_GROUPS
             * is specified and otherwise requires you to specify one or more
             * unicast locators (URIs of hosts running discovery services). As
             * an alternative, you can use LookupDiscovery, which always does
             * multicast discovery.
             */
            client.discoveryManager = new LookupDiscoveryManager(groups,
                    lookupLocators, null /* DiscoveryListener */
            );

            /*
             * Start discovery for data and metadata services.
             */
            client.dataServicesClient = new DataServicesClient(
                    client.discoveryManager);

        } catch (Exception ex) {

            log.fatal("Could not start client: " + ex.getMessage(), ex);

            client.shutdownNow();

        }

        return client;
        
    }

    /**
     * Read and return the content of the properties file.
     * 
     * @param propertyFile
     *            The properties file.
     * 
     * @throws IOException
     */
    protected static Properties getProperties(File propertyFile)
            throws IOException {

        final Properties properties = new Properties();

        InputStream is = null;

        try {

            is = new BufferedInputStream(new FileInputStream(propertyFile));

            properties.load(is);

            return properties;

        } finally {

            if (is != null)
                is.close();

        }

    }
    
    synchronized public void shutdown() {

        final long begin = System.currentTimeMillis();
        
        log.info("begin");

        super.shutdown();
        
        if( fed != null ) {

            // disconnect from the federation.
            fed.disconnect();
         
            fed = null;
            
        }

        terminateDiscoveryProcesses();

        final long elapsed = System.currentTimeMillis() - begin;
        
        log.info("Done: elapsed="+elapsed+"ms");
        
    }
    
    synchronized public void shutdownNow() {

        final long begin = System.currentTimeMillis();
        
        log.info("begin");
        
        super.shutdownNow();
        
        if( fed != null ) {
            
            // disconnect from the federation.
            fed.disconnect();
         
            fed = null;
            
        }

        terminateDiscoveryProcesses();

        final long elapsed = System.currentTimeMillis() - begin;
        
        log.info("Done: elapsed="+elapsed+"ms");

    }

    /**
     * Stop various discovery processes.
     */
    private void terminateDiscoveryProcesses() {

        if (loadBalancerClient != null) {

            loadBalancerClient.terminate();

            loadBalancerClient = null;
            
        }
        
        if (dataServicesClient != null) {

            dataServicesClient.terminate();

            dataServicesClient = null;
            
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
     */
    synchronized public IBigdataFederation connect() {

        if (fed == null) {

            fed = new BigdataFederation(this);

        }

        return fed;

    }
    
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
        
        // Note: allow before connected.
//        assertConnected();

        assert minDataServices > 0;
        assert timeout > 0;
        
        final long begin = System.currentTimeMillis();
        
        while ((System.currentTimeMillis() - begin) < timeout) {

            // verify that the client has/can get the metadata service.
            IMetadataService metadataService = getMetadataService();

            // find all data services.
            UUID[] dataServiceUUIDs = getDataServiceUUIDs(0/*all*/);
//            // find at most that many data services.
//            UUID[] dataServiceUUIDs = getDataServiceUUIDs(minDataServices);
        
            if (metadataService == null
                    || dataServiceUUIDs.length < minDataServices) {
                
                log.info("Waiting : metadataService="
                        + (metadataService == null ? "not " : "")
                        + " found; #dataServices=" + dataServiceUUIDs.length
                        + " out of " + minDataServices + " required : "
                        + Arrays.toString(dataServiceUUIDs));
                
                Thread.sleep(1000/*ms*/);
                
                continue;
                
            }
            
            log.info("Have metadata service and "+dataServiceUUIDs.length+" data services");
            
            return dataServiceUUIDs.length;
            
        }
        
        throw new TimeoutException();
        
    }

    public IDataService getAnyDataService() {

        // Note: allowed before connected.
        
        if(dataServicesClient==null) return null;
        
        return dataServicesClient.getDataService();
        
    }

}
