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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

import net.jini.config.Configuration;
import net.jini.core.discovery.LookupLocator;
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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;

import com.bigdata.btree.IRangeQuery;
import com.bigdata.io.IStreamSerializer;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.ITransactionService;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.IService;
import com.bigdata.service.jini.DataServer.AdministrableDataService;
import com.bigdata.service.jini.JiniClient.JiniConfig;
import com.bigdata.service.jini.LoadBalancerServer.AdministrableLoadBalancer;
import com.bigdata.service.jini.MetadataServer.AdministrableMetadataService;
import com.bigdata.service.jini.ResourceLockServer.AdministrableResourceLockService;
import com.bigdata.service.proxy.ClientAsynchronousIterator;
import com.bigdata.service.proxy.ClientBuffer;
import com.bigdata.service.proxy.ClientFuture;
import com.bigdata.service.proxy.RemoteAsynchronousIterator;
import com.bigdata.service.proxy.RemoteAsynchronousIteratorImpl;
import com.bigdata.service.proxy.RemoteBuffer;
import com.bigdata.service.proxy.RemoteBufferImpl;
import com.bigdata.service.proxy.RemoteFuture;
import com.bigdata.service.proxy.RemoteFutureImpl;
import com.bigdata.zookeeper.ZookeeperClientConfig;
import com.sun.jini.admin.DestroyAdmin;

/**
 * Concrete implementation for Jini.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniFederation extends AbstractDistributedFederation implements
        ServiceDiscoveryListener, Watcher {

    protected DataServicesClient dataServicesClient;

    protected LoadBalancerClient loadBalancerClient;

    protected ResourceLockClient resourceLockClient;

    protected TransactionServiceClient transactionServiceClient;
    
    protected DiscoveryManagement discoveryManager;

    protected ZooKeeper zookeeper;
    
    private final ZookeeperClientConfig zooConfig;
    
    /**
     * Return the zookeeper client configuration.
     */
    public ZookeeperClientConfig getZooConfig() {
        
        return zooConfig;
        
    }
    
    /**
     * Return the zookeeper client.
     */
    public ZooKeeper getZookeeper() {
        
        return zookeeper;
        
    }
    
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
    public JiniFederation(final JiniClient client, final JiniConfig jiniConfig,
            final ZookeeperClientConfig zooConfig) {

        super(client);
    
        open = true;
        
        if (INFO)
            log.info(jiniConfig.toString());
        
        final String[] groups = jiniConfig.groups;
        
        final LookupLocator[] lookupLocators = jiniConfig.lookupLocators;

        try {

            /*
             * Connect to a zookeeper service in the declare ensemble of
             * zookeeper servers.
             * 
             * @todo if the zookeeper integration is to be optional then we can
             * not insist on the zooconfig an this section needs to be
             * conditional on whether or not zookeeper was configured.
             */
            
            this.zooConfig = zooConfig;

            zookeeper = new ZooKeeper(zooConfig.servers,
                    zooConfig.sessionTimeout, this/* watcher */);
            
            try {

                zookeeper.getData(zooConfig.zroot, false/* watch */,
                        new Stat());
                
            } catch (NoNodeException ex) {

                /*
                 * Note: We don't just create the zroot here since there needs
                 * to be an appropriate ACL.
                 * 
                 * @todo Do bigdata integration with zookeeper ACLs for the
                 * purpose of permitting the zookeeper pluggable authentication
                 * schemes to operate when a client connects to zookeeper (NOT
                 * for method or index level authentication of client requires
                 * to bigdata services).
                 */

                log.warn("federation zroot does not exist: " + zooConfig.zroot);

            } catch (ConnectionLossException ex) {

                /*
                 * Zookeeper might not be up. That is ok. We will start anyway
                 * and it might come online later. The ZooKeeper client will
                 * keep looking for a connection and will connection if a server
                 * instance is started.
                 */
                
                log.warn(this, ex);
                
            } catch(KeeperException ex) {
                
                /*
                 * Some other ZooKeeper problem.  We will forge ahead.
                 */
                
                log.warn(this, ex);
                
            }
            
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

            final long cacheMissTimeout = Long.parseLong(jiniConfig.properties
                    .getProperty(JiniClient.Options.CACHE_MISS_TIMEOUT,
                            JiniClient.Options.DEFAULT_CACHE_MISS_TIMEOUT));
            
            // Start discovery for data and metadata services.
            dataServicesClient = new DataServicesClient(discoveryManager, this,
                    cacheMissTimeout);

            // Start discovery for the timestamp service.
            transactionServiceClient = new TransactionServiceClient(
                    discoveryManager, this, cacheMissTimeout);

            // Start discovery for the load balancer service.
            loadBalancerClient = new LoadBalancerClient(discoveryManager, this,
                    cacheMissTimeout);

            // Start discovery for the resource lock manager.
            resourceLockClient = new ResourceLockClient(discoveryManager, this,
                    cacheMissTimeout);

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
        if (loadBalancerClient == null)
            return null;

        return loadBalancerClient.getLoadBalancerService();

    }

    public ITransactionService getTransactionService() {

        // Note: return null if service not available/discovered.
        if (transactionServiceClient == null)
            return null;

        return transactionServiceClient.getTransactionService();

    }
    
    public IResourceLockService getResourceLockService() {
        
        // Note: return null if service not available/discovered.
        if (resourceLockClient == null)
            return null;

        return resourceLockClient.getResourceLockService();

    }
    
    public IMetadataService getMetadataService() {

        // Note: return null if service not available/discovered.
        if (dataServicesClient == null)
            return null;

        return dataServicesClient.getMetadataService();

    }

    public UUID[] getDataServiceUUIDs(int maxCount) {

        assertOpen();

        return dataServicesClient.getDataServiceUUIDs(maxCount);

    }

    public IDataService getDataService(UUID serviceUUID) {

        // Note: return null if service not available/discovered.
        if (dataServicesClient == null)
            return null;

        return dataServicesClient.getDataService(serviceUUID);

    }
    
    public IDataService getAnyDataService() {

        assertOpen();

        return dataServicesClient.getDataService();
        
    }

    public IDataService getDataServiceByName(final String name) {
        
        // Note: no services are available/discovered.
        if (dataServicesClient == null)
            return null;

        return dataServicesClient.getDataServiceByName(name);
        
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
        
        if (transactionServiceClient != null) {

            transactionServiceClient.terminate();

            transactionServiceClient = null;
            
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

        if (zookeeper != null) {
            
            try {

                // close the zookeeper client.
                zookeeper.close();

            } catch (InterruptedException e) {

                throw new RuntimeException(e);

            }

            zookeeper = null;

        }

        // discard all zookeeper watchers.
        watchers.clear();
        
    }

    private static final String ERR_RESOLVE = "Could not resolve: ";

    private static final String ERR_DESTROY_ADMIN = "Could not destroy: ";

    private static final String ERR_NO_DESTROY_ADMIN = "Does not implement DestroyAdmin: ";

    /**
     * Shutdown the services in the distributed federation <strong>NOT</strong>
     * just your client. This method may be used to take the entire federation
     * out of service. All services will be halted and all clients will be
     * disconnected. If you only wish to disconnect from a federation, then use
     * {@link #shutdown()} or {@link #shutdownNow()} instead.
     * <p>
     * The shutdown protocol is as follows:
     * <ol>
     * <li>{@link ITransactionService} (blocks until shutdown).</li>
     * <li>{@link IDataService}s (blocks until all are shutdown).</li>
     * <li>{@link IMetadataService}</li>
     * <li>{@link ILoadBalancerService}</li>
     * <li>{@link IResourceLockService}</li>
     * </ol>
     * 
     * @param immediate
     *            When <code>true</code> the services will be shutdown without
     *            waiting for existing transactions and other tasks to
     *            terminate.
     *            
     * @throws InterruptedException 
     */
    public void distributedFederationShutdown(final boolean immediate)
            throws InterruptedException {

        assertOpen();

        // shutdown the data services.
        {

            final UUID[] a = dataServicesClient
                    .getDataServiceUUIDs(0/* maxCount */);

            final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(a.length);
            
            for (UUID uuid : a) {
                
                final UUID tmp = uuid;
                
                tasks.add(new Callable<Void>() {
                
                    public Void call() throws Exception {
                        
                        final AdministrableDataService service = (AdministrableDataService) getDataService(tmp);

                        if (service != null) {

                            try {

                                if (immediate)
                                    service.shutdownNow();
                                else
                                    service.shutdown();

                            } catch (Throwable t) {

                                log.error(t.getMessage(), t);

                            }

                        }
                        
                        return null;
                        
                    }
                    
                });
                
            }
            
            // blocks until all data services are down.
            getExecutorService().invokeAll(tasks);

        }

        // shutdown the metadata service.
        {

            final AdministrableMetadataService service = (AdministrableMetadataService) getMetadataService();

            try {

                if (immediate)
                    service.shutdownNow();
                else
                    service.shutdown();

            } catch (Throwable t) {

                log.error(t.getMessage(), t);

            }

        }

        // shutdown the load balancer
        {

            final AdministrableLoadBalancer service = (AdministrableLoadBalancer) getLoadBalancerService();

            try {

                if (immediate)
                    service.shutdownNow();
                else
                    service.shutdown();

            } catch (Throwable t) {

                log.error(t.getMessage(), t);

            }

        }

        // shutdown the lock service
        {

            final AdministrableResourceLockService service = (AdministrableResourceLockService) getResourceLockService();

            try {

                if (immediate)
                    service.shutdownNow();
                else
                    service.shutdown();

            } catch (Throwable t) {

                log.error(t.getMessage(), t);

            }

        }

    }

    public void destroy() {

        assertOpen();

        if (zookeeper != null) {

            try {

                // clear out everything in zookeeper for this federation.
                zookeeper.delete(zooConfig.zroot, -1/* version */);
                
            } catch (Exception e) {
                
                throw new RuntimeException(e);
                
            }
            
        }

        // destroy the transaction service(s).
        if (transactionServiceClient != null) {

            final ITransactionService transactionService = transactionServiceClient
                    .getTransactionService();

            if (transactionService != null) {

                if ((transactionService instanceof DestroyAdmin)) {

                    try {

                        ((DestroyAdmin) transactionService).destroy();

                    } catch (IOException e) {

                        log.error(ERR_DESTROY_ADMIN + transactionService, e);

                    }

                } else {

                    log.warn(ERR_NO_DESTROY_ADMIN + transactionService);

                }

            }
            
        }

        // destroy data services.
        if (dataServicesClient != null) {

            final UUID[] uuids = dataServicesClient.getDataServiceUUIDs(0);

            for (UUID uuid : uuids) {

                final IDataService service;

                try {

                    service = getDataService(uuid);

                } catch (Exception ex) {

                    log.error(ERR_RESOLVE + uuid);

                    continue;

                }

                try {

                    service.destroy();

                } catch (IOException e) {

                    log.error(ERR_DESTROY_ADMIN + service, e);

                }

            }

        }

        // destroy metadata services.
        if (dataServicesClient != null) {

            final IMetadataService service = dataServicesClient
                    .getMetadataService();

            if (service != null) {

                try {

                    service.destroy();

                } catch (IOException e) {

                    log.error(ERR_DESTROY_ADMIN + service, e);

                }

            }

        }

        // destroy load balancer(s)
        if (loadBalancerClient != null) {

            final ILoadBalancerService service = loadBalancerClient
                    .getLoadBalancerService();

            if (service != null) {

                if ((service instanceof DestroyAdmin)) {

                    try {

                        ((DestroyAdmin) service).destroy();

                    } catch (IOException e) {

                        log.error(ERR_DESTROY_ADMIN + service, e);

                    }

                } else {

                    log.warn(ERR_NO_DESTROY_ADMIN + service);

                }

            }

        }

        // destroy lock service(s)
        if (resourceLockClient != null) {

            final IResourceLockService service = resourceLockClient
                    .getResourceLockService();

            if (service != null) {

                if ((service instanceof DestroyAdmin)) {

                    try {

                        ((DestroyAdmin) service).destroy();

                    } catch (IOException e) {

                        log.error(ERR_DESTROY_ADMIN + service, e);

                    }

                } else {

                    log.warn(ERR_NO_DESTROY_ADMIN + service);

                }

            }

        }

    }

    public long getLastCommitTime() {

        final ITransactionService transactionService = getTransactionService();

        if (transactionService != null) {

            try {
                
                lastKnownCommitTime = transactionService.getLastCommitTime();
                
            } catch (IOException e) {
                
                log.error("Can not reach the timestampService?", e);
                
                // fall through - report the lastKnownCommitTime.
                
            }

        }

        return lastKnownCommitTime;

    }
    private long lastKnownCommitTime;

    /**
     * Note: The invocation layer factory is reused for each exported proxy (but
     * the exporter itself is paired 1:1 with the exported proxy).
     */
    private InvocationLayerFactory invocationLayerFactory = new BasicILFactory();
    
    /**
     * Return an {@link Exporter} for a single object that implements one or
     * more {@link Remote} interfaces.
     * <p>
     * Note: This uses TCP Server sockets.
     * <p>
     * Note: This uses [port := 0], which means a random port is assigned.
     * <p>
     * Note: The VM WILL NOT be kept alive by the exported proxy (keepAlive is
     * <code>false</code>).
     * 
     * @param enableDGC
     *            if distributed garbage collection should be used for the
     *            object to be exported.
     * 
     * @return The {@link Exporter}.
     */
    protected Exporter getExporter(final boolean enableDGC) {
        
        /*
         * Setup the Exporter for the iterator.
         * 
         * Note: Distributed garbage collection is enabled since the proxied
         * future CAN become locally weakly reachable sooner than the client can
         * get() the result. Distributed garbage collection handles this for us
         * and automatically unexports the proxied iterator once it is no longer
         * strongly referenced by the client.
         */
        
        return new BasicJeriExporter(TcpServerEndpoint
                .getInstance(0/* port */), invocationLayerFactory, enableDGC,
                false/* keepAlive */);
        
    }
    
    /**
     * Export and return a proxy object.
     * <p>
     * Note: This is used for query evaluation in which there is at least one
     * JOIN. It DOES NOT get used for simple {@link IAccessPath} scans. Those
     * use {@link IRangeQuery#rangeIterator()} instead.
     * 
     * @todo Allow {@link Configuration} of the {@link Exporter} for the proxy
     *       iterators and futures.
     *       <p>
     *       Since the {@link Exporter} is paired to a single object the
     *       configuration of the {@link Exporter} will require an additional
     *       level of indirection when compared to the {@link Configuration} of
     *       an {@link AbstractServer}'s {@link Exporter}.
     */
    @Override
    public <E> IAsynchronousIterator<E> getProxy(
            final IAsynchronousIterator<E> sourceIterator,
            final IStreamSerializer<E> serializer,
            final int capacity) {
        
        if (sourceIterator == null)
            throw new IllegalArgumentException();

        if (serializer == null)
            throw new IllegalArgumentException();

        if (capacity <= 0)
            throw new IllegalArgumentException();

//        if (sourceIterator.hasNext(1L, TimeUnit.MILLISECONDS)
//                && sourceIterator.isFutureDone()) {
//            
//            /*
//             * @todo If the producer is done we can materialize all of the data
//             * in a thick iterator, ResultSet or RemoteChunk and send that back.
//             * [probably do not wait at all, but just test isFutureDone].
//             */
//            
//            log.warn("Async iterator is done: modify code to send back fully materialized chunk.");
//            
//        }
        
        /*
         * Setup the Exporter for the iterator.
         * 
         * Note: Distributed garbage collection is enabled since the proxied
         * iterator CAN become locally weakly reachable sooner than the client
         * can close() the iterator (or perhaps even consume the iterator!)
         * Distributed garbage collection handles this for us and automatically
         * unexports the proxied iterator once it is no longer strongly
         * referenced by the client.
         */
        final Exporter exporter = getExporter(true/* enableDCG */);
        
        // wrap the iterator with an exportable object.
        final RemoteAsynchronousIterator<E> impl = new RemoteAsynchronousIteratorImpl<E>(
                sourceIterator
                , serializer
                );
        
        /*
         * Export and return the proxy.
         */
        final RemoteAsynchronousIterator<E> proxy;
        try {

            // export proxy.
            proxy = (RemoteAsynchronousIterator<E>) exporter.export(impl);

        } catch (ExportException ex) {

            throw new RuntimeException("Export error: " + ex, ex);
            
        }

        /*
         * Wrap the proxy object in a serializable object so that it looks like
         * an IChunkedOrderedIterator and return it to the caller.
         */

        return new ClientAsynchronousIterator<E>(proxy, capacity);

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
    public <E> Future<E> getProxy(final Future<E> future) {

        /*
         * Setup the Exporter for the Future.
         * 
         * Note: Distributed garbage collection is enabled since the proxied
         * future CAN become locally weakly reachable sooner than the client can
         * get() the result. Distributed garbage collection handles this for us
         * and automatically unexports the proxied iterator once it is no longer
         * strongly referenced by the client.
         */
        final Exporter exporter = getExporter(true/* enableDGC */);
        
        // wrap the future in a proxyable object.
        final RemoteFuture<E> impl = new RemoteFutureImpl<E>(future);

        /*
         * Export the proxy.
         */
        final RemoteFuture<E> proxy;
        try {

            // export proxy.
            proxy = (RemoteFuture<E>) exporter.export(impl);

            if (INFO) {

                log.info("Exported proxy: proxy=" + proxy + "("
                        + proxy.getClass() + ")");

            }

        } catch (ExportException ex) {

            throw new RuntimeException("Export error: " + ex, ex);

        }

        // return proxy to caller.
        return new ClientFuture<E>(proxy);

    }

    /**
     * A proxy for an {@link IBuffer} that does not extend {@link Remote} and
     * which DOES NOT declare that its methods throw {@link IOException} (for
     * RMI).
     * 
     * @param buffer
     *            The future.
     * 
     * @return A proxy for that {@link IBuffer} that masquerades any RMI
     *         exceptions.
     */
    public <E> IBuffer<E> getProxy(final IBuffer<E> buffer) {
        
        /*
         * Setup the Exporter.
         */
        final Exporter exporter = getExporter(true/* enableDGC */);
        
        // wrap in a proxyable object.
        final RemoteBuffer<E> impl = new RemoteBufferImpl<E>(buffer);

        /*
         * Export the proxy.
         */
        final RemoteBuffer<E> proxy;
        try {

            // export proxy.
            proxy = (RemoteBuffer<E>) exporter.export(impl);

            if (INFO) {

                log.info("Exported proxy: proxy=" + proxy + "("
                        + proxy.getClass() + ")");

            }

        } catch (ExportException ex) {

            throw new RuntimeException("Export error: " + ex, ex);

        }

        // return proxy to caller.
        return new ClientBuffer<E>(proxy);

    }

    @SuppressWarnings("unchecked")
    public <E> E getProxy(final E obj, final boolean enableDGC) {

        try {

            return (E) getExporter(enableDGC).export((Remote) obj);
            
        } catch (ExportException e) {
            
            throw new RuntimeException(e);
            
        }
        
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

    public void process(WatchedEvent event) {
        
        if(INFO)
            log.info(event.toString());
        
        for(Watcher w : watchers) {
            
            w.process(event);
            
        }
        
    }
    
    /**
     * Adds a {@link Watcher} which will receive {@link WatchedEvent}s until it
     * is removed.
     * 
     * @param w
     *            The watcher.
     * 
     * @todo we can specify the watcher for a watch, so we don't really need the
     *       addWatcher API on the JiniFederation.
     */
    public void addWatcher(final Watcher w) {

        if (w == null)
            throw new IllegalArgumentException();

        if(INFO)
            log.info("watcher="+w);
        
        watchers.add(w);

    }

    /**
     * Remove a {@link Watcher}.
     * 
     * @param w
     *            The watcher.
     */
    public void removeWatcher(final Watcher w) {

        if (w == null)
            throw new IllegalArgumentException();

        if(INFO)
            log.info("watcher="+w);

        watchers.remove(w);

    }

    private final CopyOnWriteArrayList<Watcher> watchers = new CopyOnWriteArrayList<Watcher>();
    
}
