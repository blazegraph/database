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
import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.server.ExportException;
import java.util.UUID;
import java.util.concurrent.Future;

import net.jini.config.Configuration;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.export.Exporter;
import net.jini.jeri.BasicILFactory;
import net.jini.jeri.BasicJeriExporter;
import net.jini.jeri.InvocationLayerFactory;
import net.jini.jeri.tcp.TcpServerEndpoint;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceItemFilter;
import net.jini.lookup.entry.Name;

import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.ISerializer;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.TimestampServiceUtil;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.rule.IRule;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.IService;
import com.bigdata.service.jini.JiniClient.JiniConfig;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.sun.jini.admin.DestroyAdmin;

/**
 * Concrete implementation for Jini.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniFederation extends AbstractDistributedFederation implements
        ServiceDiscoveryListener {

    protected DataServicesClient dataServicesClient;

    protected LoadBalancerClient loadBalancerClient;

    protected ResourceLockClient resourceLockClient;

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
    
        open = true;
        
        if (INFO)
            log.info(jiniConfig.toString());
        
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
            dataServicesClient = new DataServicesClient(discoveryManager, this);

            // Start discovery for the timestamp service.
            timestampServiceClient = new TimestampServiceClient(
                    discoveryManager, this);

            // Start discovery for the load balancer service.
            loadBalancerClient = new LoadBalancerClient(discoveryManager, this);

            // Start discovery for the resource lock manager.
            resourceLockClient = new ResourceLockClient(discoveryManager, this);

        } catch (Exception ex) {

            log.fatal("Problem initiating service discovery: "
                    + ex.getMessage(), ex);

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
    
    public IResourceLockService getResourceLockService() {
        
        // Note: return null if service not available/discovered.
        if(resourceLockClient == null) return null;
        
        return resourceLockClient.getResourceLockService();
        
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

    private boolean open;
    
    synchronized public void shutdown() {
        
        if(!open) return;
        
        open = false;
        
        final long begin = System.currentTimeMillis();

        if (INFO)
            log.info("begin");

        super.shutdown();

        terminateDiscoveryProcesses();

        final long elapsed = System.currentTimeMillis() - begin;

        if (INFO)
            log.info("Done: elapsed=" + elapsed + "ms");

    }

    synchronized public void shutdownNow() {

        if(!open) return;
        
        open = false;

        final long begin = System.currentTimeMillis();

        if (INFO)
            log.info("begin");
        
        super.shutdownNow();
        
        terminateDiscoveryProcesses();

        final long elapsed = System.currentTimeMillis() - begin;
        
        if (INFO)
            log.info("Done: elapsed=" + elapsed + "ms");

    }

    /**
     * Stop various discovery processes.
     */
    private void terminateDiscoveryProcesses() {

        if (resourceLockClient != null) {

            resourceLockClient.terminate();

            resourceLockClient = null;
            
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

    private static final String ERR_RESOLVE = "Could not resolve: ";

    private static final String ERR_DESTROY_ADMIN = "Could not destroy: ";
    
    private static final String ERR_NO_DESTROY_ADMIN = "Does not implement DestroyAdmin: ";
    
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

                    log.error(ERR_RESOLVE + uuid);

                    continue;

                }

                try {

                    ds.destroy();

                } catch (IOException e) {

                    log.error(ERR_DESTROY_ADMIN + ds, e);

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

                    log.error(ERR_DESTROY_ADMIN + mds, e);

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

                        log.error(ERR_DESTROY_ADMIN + loadBalancerService, e);

                    }

                } else {

                    log.warn(ERR_NO_DESTROY_ADMIN + loadBalancerService);

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

                        log.error(ERR_DESTROY_ADMIN + timestampService, e);

                    }

                } else {

                    log.warn(ERR_NO_DESTROY_ADMIN + timestampService);

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

    /**
     * Extended to allow the client to be configured with the <em>name</em> of
     * the {@link DataService} on which a named scale-out index will be
     * registered using the pattern
     * <code>register.<i>name</i> = serviceName</code>, where <i>name</i>
     * is the fully qualified name of the scale-out index and <i>serviceName</i>
     * is the name of a {@link DataService} as reported by
     * {@link IService#getServiceName()} and specified in the configuration file
     * for that service.
     */
    public void registerIndex(IndexMetadata metadata) {
        
        // the name of the scale-out index.
        final String indexName = metadata.getName();

        final String serviceName = getClient().getProperties().getProperty(
                "register." + indexName);

        if (serviceName == null) {
        
            // default behavior.
            super.registerIndex(metadata);
        
            return;
        
        }

        // lookup the named service.
        final ServiceItem[] serviceItems = dataServicesClient.serviceMap
                .getServiceItems(1,

                new ServiceItemFilter() {

                    public boolean check(ServiceItem serviceItem) {

                        final Entry[] a = serviceItem.attributeSets;

                        for (Entry entry : a) {

                            if (!(entry instanceof Name))
                                continue;

                            if (serviceName.equals(((Name) entry).name)) {

                                // found the named service.
                                return true;

                            }

                        }

                        return false;

                    }

                });

        if (serviceItems.length == 0) {

            log.warn("Could not locate service: " + serviceName
                    + " for index: " + indexName);

            // default behavior.
            super.registerIndex(metadata);

            return;

        }

        // there will be only one value in the array (maxCount==1)
        final ServiceItem serviceItem = serviceItems[0];

        final IDataService service;
        try {
            
            service = (IDataService)serviceItem.service;
            
        } catch(ClassCastException ex) {
            
            log.error("Not a data service: "+serviceName);
            
            // default behavior.
            super.registerIndex(metadata);
            
            return;
            
        }

        final UUID uuid;
        try {
            
            uuid = service.getServiceUUID();
            
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

        if (INFO)
            log.info("Registering index=" + indexName + " on service="
                    + serviceName);
        
        // register on the service having that service name.
        registerIndex(metadata, uuid);
        
    }
    
    private InvocationLayerFactory invocationLayerFactory = new BasicILFactory();
    
    /**
     * Export and return a proxy object. The client will have to wrap the proxy
     * object to get back an {@link IChunkedOrderedIterator} interface.
     * <p>
     * Note: This is used for {@link IRule} evaluation in which there is at
     * least one JOIN. It DOES NOT get used for simple {@link IAccessPath}
     * scans. Those use {@link IRangeQuery#rangeIterator()} instead.
     * 
     * @todo Allow {@link Configuration} of the {@link Exporter} for the proxy
     *       iterators and futures.
     *       <p>
     *       Since the {@link Exporter} is paired to a single object the
     *       configuration of the {@link Exporter} will require an additional
     *       level of indirection when compared to the {@link Configuration} of
     *       an {@link AbstractServer}'s {@link Exporter}.
     * 
     * @todo if this is solely used for high-level query then we can probably
     *       drop the {@link IKeyOrder} from the API here and on
     *       {@link RemoteChunk}.
     */
    @Override
    public Object getProxy(
            final IAsynchronousIterator<? extends Object[]> sourceIterator,
            final ISerializer<? extends Object[]> serializer,
            final IKeyOrder<? extends Object> keyOrder) {
        
        if (sourceIterator == null)
            throw new IllegalArgumentException();
        
//        if (sourceIterator.hasNext(1L, TimeUnit.MILLISECONDS)
//                && sourceIterator.isFutureDone()) {
//            
//            /*
//             * @todo If the producer is done we can materialize all of the data
//             * in a ResultSet or RemoteChunk and send that back.
//             */
//            
//            log.warn("Async iterator is done: modify code to send back fully materialized chunk.");
//            
//        }
        
        final long begin = System.currentTimeMillis();
        
        /*
         * Setup the Exporter for the iterator.
         * 
         * Note: This uses TCP Server sockets.
         * 
         * Note: This uses [port := 0], which means a random port is assigned.
         * 
         * Note: Distributed garbage collection is enabled since the proxied
         * iterator CAN become locally weakly reachable sooner than the client
         * can close() the iterator (or perhaps even consume the iterator!)
         * Distributed garbage collection handles this for us and automatically
         * unexports the proxied iterator once it is no longer strongly
         * referenced by the client.
         * 
         * Note: The VM WILL NOT be kept alive by the exported proxy.
         * 
         * Note: The invocation layer factory is reused for each exported proxy
         * (but the exporter itself is paired 1:1 with the exported proxy).
         */
        final Exporter exporter = new BasicJeriExporter(TcpServerEndpoint
                .getInstance(0/* port */), invocationLayerFactory,
                true/* enableDCG */, false/*keepAlive*/);
        
        // wrap the iterator with an exportable object.
        final RemoteChunkedIterator impl = new RemoteChunkedIterator(
                sourceIterator, serializer, keyOrder);
        
        /*
         * Export and return the proxy.
         */
        final Remote proxy;
        try {

            // export proxy.
            proxy = exporter.export(impl);
            
            if (INFO) {

                final long elapsed = System.currentTimeMillis() - begin;

                log.info("Exported proxy: elapsed=" + elapsed + ", proxy="
                        + proxy + "(" + proxy.getClass() + ")");
                
            }

            // return proxy to caller.
            return proxy;

        } catch (ExportException ex) {

            throw new RuntimeException("Export error: " + ex, ex);
            
        }
        
    }

    /**
     * Note that {@link Future}s generated by <code>java.util.concurrent</code>
     * are NOT {@link Serializable}. Futher note the proxy as generated by an
     * {@link Exporter} MUST be encapsulated so that the object returned to the
     * caller can implement {@link Future} without having to declare that the
     * methods throw {@link IOException} (for RMI).
     * 
     * @param future
     *            The future.
     * 
     * @return A proxy for that {@link Future} that masquerades any RMI
     *         exceptions.
     */
    public Future<? extends Object> getProxy(Future<? extends Object> future) {
        
        /*
         * Setup the Exporter for the iterator.
         * 
         * Note: This uses TCP Server sockets.
         * 
         * Note: This uses [port := 0], which means a random port is assigned.
         * 
         * Note: Distributed garbage collection is enabled since the proxied
         * future CAN become locally weakly reachable sooner than the client can
         * get() the result. Distributed garbage collection handles this for us
         * and automatically unexports the proxied iterator once it is no longer
         * strongly referenced by the client.
         * 
         * Note: The VM WILL NOT be kept alive by the exported proxy.
         * 
         * Note: The invocation layer factory is reused for each exported proxy
         * (but the exporter itself is paired 1:1 with the exported proxy).
         */
        final Exporter exporter = new BasicJeriExporter(TcpServerEndpoint
                .getInstance(0/* port */), invocationLayerFactory,
                true/* enableDCG */, false/*keepAlive*/);
        
        // wrap the future in a proxyable object.
        final RemoteFuture impl = new RemoteFutureImpl(future);

        /*
         * Export the proxy.
         */
        final RemoteFuture proxy;
        try {

            // export proxy.
            proxy = (RemoteFuture) exporter.export(impl);

            if (INFO) {

                log.info("Exported proxy: proxy=" + proxy + "("
                        + proxy.getClass() + ")");

            }

        } catch (ExportException ex) {

            throw new RuntimeException("Export error: " + ex, ex);

        }

        // return proxy to caller.
        return new ClientFuture(proxy);

    }

    /**
     * Invokes {@link #serviceJoin(com.bigdata.service.IService, UUID, String)}
     */
    public void serviceAdded(ServiceDiscoveryEvent e) {
        
        final ServiceItem serviceItem = e.getPostEventServiceItem();

        if (serviceItem.service instanceof IService) {

//            System.err.println("serviceAdded: "+serviceItem);
            
            final UUID serviceUUID = JiniUtil.serviceID2UUID(serviceItem.serviceID);

            serviceJoin((IService) serviceItem.service, serviceUUID);

        } else {
            
            log.warn("Not an " + IService.class);
            
        }
        
    }

    /** NOP. */
    public void serviceChanged(ServiceDiscoveryEvent e) {
        
    }

    /**
     * Invokes {@link #serviceLeave(UUID)}
     */
    public void serviceRemoved(ServiceDiscoveryEvent e) {
        
        final ServiceItem serviceItem = e.getPreEventServiceItem();

        final UUID serviceUUID = JiniUtil.serviceID2UUID(serviceItem.serviceID);

//        System.err.println("serviceRemoved: " + serviceUUID);

        serviceLeave(serviceUUID);
        
    }

}
