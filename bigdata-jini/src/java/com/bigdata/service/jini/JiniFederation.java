/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Mar 28, 2008
 */

package com.bigdata.service.jini;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import net.jini.core.discovery.LookupLocator;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.LookupDiscoveryManager;

import com.bigdata.journal.IResourceLockManager;
import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.TimestampServiceUtil;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.jini.JiniClient.JiniConfig;
import com.sun.jini.admin.DestroyAdmin;

/**
 * Concrete implementation for Jini.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniFederation extends AbstractDistributedFederation {

    protected DataServicesClient dataServicesClient;

    protected LoadBalancerClient loadBalancerClient;

    protected ResourceLockManagerClient resourceLockManagerClient;

    protected TimestampServiceClient timestampServiceClient;
    
    protected DiscoveryManagement discoveryManager;

    public DiscoveryManagement getDiscoveryManagement() {
        
        return discoveryManager;
        
    }
    
    /**
     * Initiaties discovery for one or more service registrars and establishes a
     * lookup caches for various bigdata services.
     * 
     * @param client
     *            The client.
     */
    public JiniFederation(JiniClient client, JiniConfig jiniConfig) {

        super(client);
    
        if(INFO) log.info(jiniConfig.toString());
        
        final String[] groups = jiniConfig.groups;
        
        final LookupLocator[] lookupLocators = jiniConfig.lookupLocators;

        try {

            /*
             * Note: This class will perform multicast discovery if ALL_GROUPS
             * is specified and otherwise requires you to specify one or more
             * unicast locators (URIs of hosts running discovery services). As
             * an alternative, you can use LookupDiscovery, which always does
             * multicast discovery.
             */
            discoveryManager = new LookupDiscoveryManager(groups,
                    lookupLocators, null /* DiscoveryListener */
            );

            // Start discovery for data and metadata services.
            dataServicesClient = new DataServicesClient(discoveryManager);

            // Start discovery for the timestamp service.
            timestampServiceClient = new TimestampServiceClient(
                    discoveryManager);

            // Start discovery for the load balancer service.
            loadBalancerClient = new LoadBalancerClient(discoveryManager);

            // Start discovery for the resource lock manager.
            resourceLockManagerClient = new ResourceLockManagerClient(discoveryManager);

        } catch (Exception ex) {

            log.fatal("Problem initiating service discovery: " + ex.getMessage(), ex);

            try {

                shutdownNow();
                
            } catch (Throwable t) {
                
                log.error(t.getMessage(), t);
                
            }

            throw new RuntimeException(ex);
            
        }
        
    }

    public JiniClient getClient() {
        
        return (JiniClient)super.getClient();
        
    }
    
    public ILoadBalancerService getLoadBalancerService() {

        // Note: return null if service not available/discovered.
        if(loadBalancerClient == null) return null;

        return loadBalancerClient.getLoadBalancerService();
        
    }
    
    public ITimestampService getTimestampService() {
        
        // Note: return null if service not available/discovered.
        if(timestampServiceClient == null) return null;
        
        return timestampServiceClient.getTimestampService();
        
    }
    
    public IResourceLockManager getResourceLockManager() {
        
        // Note: return null if service not available/discovered.
        if(resourceLockManagerClient == null) return null;
        
        return resourceLockManagerClient.getResourceLockManager();
        
    }
    
    public IMetadataService getMetadataService() {

        // Note: return null if service not available/discovered.
        if(dataServicesClient == null) return null;

        return dataServicesClient.getMetadataService();
                
    }

    public UUID[] getDataServiceUUIDs(int maxCount) {
        
        assertOpen();

        return dataServicesClient.getDataServiceUUIDs(maxCount);
        
    }
    
    public IDataService getDataService(UUID serviceUUID) {
        
        // Note: return null if service not available/discovered.
        if(dataServicesClient == null) return null;

        return dataServicesClient.getDataService(serviceUUID);
                
    }
    
    public IDataService getAnyDataService() {

        assertOpen();

        return dataServicesClient.getDataService();
        
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

        assertOpen();

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

    synchronized public void shutdown() {
        
        final long begin = System.currentTimeMillis();
        
        log.info("begin");

        super.shutdown();
        
        terminateDiscoveryProcesses();

        final long elapsed = System.currentTimeMillis() - begin;
        
        log.info("Done: elapsed="+elapsed+"ms");
        
    }
    
    synchronized public void shutdownNow() {

        final long begin = System.currentTimeMillis();
        
        log.info("begin");
        
        super.shutdownNow();
        
        terminateDiscoveryProcesses();

        final long elapsed = System.currentTimeMillis() - begin;
        
        log.info("Done: elapsed="+elapsed+"ms");

    }

    /**
     * Stop various discovery processes.
     */
    private void terminateDiscoveryProcesses() {

        if (resourceLockManagerClient != null) {

            resourceLockManagerClient.terminate();

            resourceLockManagerClient = null;
            
        }
        
        if (timestampServiceClient != null) {

            timestampServiceClient.terminate();

            timestampServiceClient = null;
            
        }
        
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

    public void destroy() {

        assertOpen();
        
        // destroy data services.
        if (dataServicesClient != null) {

            final UUID[] uuids = dataServicesClient.getDataServiceUUIDs(0);

            for (UUID uuid : uuids) {

                final IDataService ds;

                try {

                    ds = getDataService(uuid);

                } catch (Exception ex) {

                    log.error("Could not resolve dataService: uuid" + uuid);

                    continue;

                }

                try {

                    ds.destroy();

                } catch (IOException e) {

                    log.error("Could not destroy dataService: " + ds, e);

                }

            }

        }

        // destroy metadata services.
        if (dataServicesClient != null) {

            final IMetadataService mds = dataServicesClient
                    .getMetadataService();

            if (mds != null) {

                try {

                    mds.destroy();

                } catch (IOException e) {

                    log.error("Could not destroy dataService: " + mds, e);

                }

            }

        }

        // destroy load balancer(s)
        if (loadBalancerClient != null) {

            final ILoadBalancerService loadBalancerService = loadBalancerClient
                    .getLoadBalancerService();

            if (loadBalancerService != null) {

                if ((loadBalancerService instanceof DestroyAdmin)) {

                    try {

                        ((DestroyAdmin) loadBalancerService).destroy();

                    } catch (IOException e) {

                        log.error("Could not destroy loadBalancerService: "
                                + loadBalancerService, e);

                    }

                } else {

                    log
                            .warn("Can not destroy: The load balancer does not implement DestroyAdmin");

                }

            }

        }
        
        // destroy timestamp service(s)
        if(timestampServiceClient!=null) {
            
            final ITimestampService timestampService = timestampServiceClient.getTimestampService(); 

            if (timestampService != null) {

                if ((timestampService instanceof DestroyAdmin)) {

                    try {

                        ((DestroyAdmin) timestampService).destroy();

                    } catch (IOException e) {

                        log.error("Could not destroy timestampService: "
                                + timestampService, e);

                    }

                } else {

                    log
                            .warn("Can not destroy: The timestamp service does not implement DestroyAdmin");

                }


            }
            
        }

    }

    /**
     * 
     * FIXME This is returning the next possible timestamp rather than the
     * timestamp of the last competed commit.
     * <p>
     * The {@link ITimestampService} must be extended to have explicit knowledge
     * of commits (as part of its eventual role as a transaction manager
     * service) and this method should query the {@link ITimestampService} for
     * the timestamp of the last _completed_ commit (rather than the last
     * timestamp assigned by the service). The notification of the commit
     * protocol to the timestamp service can be asynchronous unless the commit
     * is part of a transaction, in which case it needs to be synchronous.
     * (There could be another method for the last transaction commit time. This
     * method reflects commits by unisolated operations).
     */
    public long getLastCommitTime() {

        final ITimestampService timestampService = getTimestampService();

        if (timestampService != null) {

            lastKnownCommitTime = TimestampServiceUtil.nextTimestamp(timestampService);

        }

        return lastKnownCommitTime;

    }

    private long lastKnownCommitTime;

}
