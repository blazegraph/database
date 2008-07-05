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

package com.bigdata.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.cache.ICacheEntry;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IServiceCounters;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.QueueStatisticsTask;
import com.bigdata.journal.TaskCounters;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.relation.DefaultRelationLocator;
import com.bigdata.relation.IRelationLocator;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.InnerCause;
import com.bigdata.util.NT;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.httpd.AbstractHTTPD;

/**
 * Abstract base class for common functionality for {@link IBigdataFederation}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo implement {@link IServiceShutdown}. When it is declared here it messes
 *       up the Options interface hierarchy. What appears to be happening is
 *       that the IServiceShutdown.Options interface is flattened into
 *       IServiceShutdown and it shadows the Options that are being used.
 */
abstract public class AbstractFederation implements IBigdataFederation {

    private IBigdataClient client;
    
    public IBigdataClient getClient() {
        
        assertOpen();
        
        return client;
        
    }
    
    public boolean isOpen() {
        
        return client != null;
        
    }
    
    /**
     * Normal shutdown allows any existing client requests to federation
     * services to complete but does not schedule new requests, disconnects from
     * the federation, and then terminates any background processing that is
     * being performed on the behalf of the client (service discovery, etc).
     * <p>
     * Note: concrete implementations MUST extend this method.
     * <p>
     * Note: Clients use {@link IBigdataClient#disconnect(boolean)} to
     * disconnect from a federation. The federation implements that disconnect
     * using either {@link #shutdown()} or {@link #shutdownNow()}.
     * <p>
     * The implementation must be a NOP if the federation is already shutdown.
     */
    synchronized public void shutdown() {

        if(!isOpen()) return;

        final long begin = System.currentTimeMillis();

        log.info("begin");

        // allow client requests to finish normally.
        threadPool.shutdown();

        try {

            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {

                log.warn("Timeout awaiting thread pool termination.");

            }

        } catch (InterruptedException e) {

            log.warn("Interrupted awaiting thread pool termination.", e);

        }
        
        if (statisticsCollector != null) {

            statisticsCollector.stop();

            statisticsCollector = null;

        }

        // terminate sampling and reporting tasks.
        sampleService.shutdown();

        // optional httpd service for the local counters. 
        if( httpd != null) {
            
            httpd.shutdown();
            
        }
        
        // @todo run a final ReportTask?
        // @todo send leave() notice to the LBS?

        log.info("done: elapsed=" + (System.currentTimeMillis() - begin));

        client = null;
        
    }

    /**
     * Immediate shutdown terminates any client requests to federation services,
     * disconnects from the federation, and then terminate any background
     * processing that is being performed on the behalf of the client (service
     * discovery, etc).
     * <p>
     * Note: concrete implementations MUST extend this method to either
     * disconnect from the remote federation or close the embedded federation
     * and then clear the {@link #fed} reference so that the client is no longer
     * "connected" to the federation.
     * <p>
     * Note: Clients use {@link IBigdataClient#disconnect(boolean)} to disconnect
     * from a federation.  The federation implements that disconnect using either
     * {@link #shutdown()} or {@link #shutdownNow()}.
     * <p>
     * The implementation must be a NOP if the federation is already shutdown.
     */
    synchronized public void shutdownNow() {
        
        if(!isOpen()) return;
        
        final long begin = System.currentTimeMillis();
        
        log.info("begin");
        
        // stop client requests.
        threadPool.shutdownNow();
        
        if (statisticsCollector != null) {

            statisticsCollector.stop();

            statisticsCollector = null;

        }

        // terminate sampling and reporting tasks immediately.
        sampleService.shutdownNow();

        // terminate the optional httpd service for the client's live counters.
        if( httpd != null) {
            
            httpd.shutdownNow();
            
        }
        
        log.info("done: elapsed="+(System.currentTimeMillis()-begin));
        
        client = null;
        
    }
    
    /**
     * @throws IllegalStateException
     *                if the client has disconnected from the federation.
     */
    protected void assertOpen() {

        if (client == null) {

            throw new IllegalStateException();

        }

    }

    /**
     * Used to run application tasks.
     */
    private final ThreadPoolExecutor threadPool;
    
    /**
     * Used to sample and report on the queue associated with the
     * {@link #threadPool}.
     */
    private final ScheduledExecutorService sampleService = Executors
            .newSingleThreadScheduledExecutor(DaemonThreadFactory
                    .defaultThreadFactory());
    
    /**
     * httpd reporting the live counters for the client while it is connected to
     * the federation.
     */
    private AbstractHTTPD httpd;
    
    /**
     * Locator for relations.
     */
    private final IRelationLocator relationLocator;
    
    public IRelationLocator getRelationLocator() {
        
        return relationLocator;
    
    }
    
    /**
     * Counters that aggregate across all tasks submitted by the client against
     * the connected federation. Those counters are sampled by a
     * {@link QueueStatisticsTask} and reported by the client to the
     * {@link ILoadBalancerService}.
     */
    private final TaskCounters taskCounters = new TaskCounters();

    /**
     * Returns the {@link TaskCounters}s that aggregate across all operations
     * performed by the client against the connected federation. The
     * {@link TaskCounters} will be sampled by a {@link QueueStatisticsTask} and
     * the sampled data reported by the client to the
     * {@link ILoadBalancerService}.
     */
    public TaskCounters getTaskCounters() {

        return taskCounters;

    }
 
//    /**
//     * Counters that aggregate across all instances of the same task submitted
//     * by the client against the connected federation. Those counters are
//     * sampled by a {@link QueueStatisticsTask} and reported by the client to
//     * the {@link ILoadBalancerService}.
//     */
//    private final Map<Class, TaskCounters> taskCountersByProc = new HashMap<Class, TaskCounters>();
//
//    /**
//     * Counters that aggregate across all tasks submitted by the client against
//     * a given scale-out index for the connected federation. Those counters are
//     * sampled by a {@link QueueStatisticsTask} and reported by the client to
//     * the {@link ILoadBalancerService}.
//     */
//    private final Map<String, TaskCounters> taskCountersByIndex = new HashMap<String, TaskCounters>();
//    
//    /**
//     * Returns the {@link TaskCounters} instance that is used to track
//     * statistics for the {@link #getThreadPool()} for instances of the given
//     * procedure. The {@link TaskCounters} will be sampled by a
//     * {@link QueueStatisticsTask} and the sampled data reported by the client
//     * to the {@link ILoadBalancerService}.
//     * 
//     * @param procedure
//     *            A procedure.
//     */
//    synchronized public TaskCounters getTaskCounters(IIndexProcedure procedure) {
//
//        if (procedure == null)
//            throw new IllegalArgumentException();
//
//        final Class cls = procedure.getClass();
//
//        TaskCounters taskCounters = taskCountersByProc.get(cls);
//
//        if (taskCounters == null) {
//
//            taskCounters = new TaskCounters();
//
//            taskCountersByProc.put(cls, taskCounters);
//            
//            final long initialDelay = 0; // initial delay in ms.
//            final long delay = 1000; // delay in ms.
//            final TimeUnit unit = TimeUnit.MILLISECONDS;
//            
//            final String relpath = "Thread Pool" + ICounterSet.pathSeparator
//                    + "procedure" + ICounterSet.pathSeparator + cls.getName();
//    
//            if(log.isInfoEnabled()) log.info("Adding counters: relpath="+relpath);
//            
//            final QueueStatisticsTask queueStatisticsTask = new QueueStatisticsTask(
//                    relpath, threadPool, taskCounters);
//            
//            addScheduledStatisticsTask(queueStatisticsTask, initialDelay,
//                    delay, unit);
//
//            assert clientRoot != null;
//            
//            queueStatisticsTask.addCounters(clientRoot.makePath(relpath));
//
//        }
//
//        return taskCounters;
//
//    }
//    
//    /**
//     * Returns the {@link TaskCounters} instance that is used to track
//     * statistics for the {@link #getThreadPool()} the given scale-out index.
//     * The {@link TaskCounters} will be sampled by a {@link QueueStatisticsTask}
//     * and the sampled data reported by the client to the
//     * {@link ILoadBalancerService}.
//     * <p>
//     * Note: This aggregates across all operations against the scale-out index
//     * by this client regardless of whether they are {@link ITx#UNISOLATED},
//     * {@link ITx#READ_COMMITTED}, historical reads, or tasks isolated by a
//     * transaction.
//     * 
//     * @param ndx
//     *            A procedure.
//     * 
//     * @todo it might be nice to break this out into the three services against
//     *       which those tasks will be run by the data service (the unisolated,
//     *       read-only, and transaction services).
//     */
//    synchronized public TaskCounters getTaskCounters(ClientIndexView ndx) {
//
//        if (ndx == null)
//            throw new IllegalArgumentException();
//
//        final String name = ndx.getName();
//
//        TaskCounters taskCounters = taskCountersByIndex.get(name);
//
//        if (taskCounters == null) {
//
//            taskCounters = new TaskCounters();
//
//            taskCountersByIndex.put(name, taskCounters);
//            
//            final long initialDelay = 0; // initial delay in ms.
//            final long delay = 1000; // delay in ms.
//            final TimeUnit unit = TimeUnit.MILLISECONDS;
//
//            final String relpath = "Thread Pool" + ICounterSet.pathSeparator
//                    + "index" + ICounterSet.pathSeparator + name;
//            
//            if(log.isInfoEnabled()) log.info("Adding counters: relpath="+relpath);
//            
//            final QueueStatisticsTask queueStatisticsTask = new QueueStatisticsTask(
//                    relpath, threadPool, taskCounters);
//            
//            addScheduledStatisticsTask(queueStatisticsTask, initialDelay,
//                    delay, unit);
//
//            assert clientRoot != null;
//
//            queueStatisticsTask.addCounters(clientRoot.makePath(relpath));
//
//        }
//
//        return taskCounters;
//
//    }
    
    /**
     * Collects interesting statistics on the client's host and process
     * for reporting to the {@link ILoadBalancerService}.
     */
    private AbstractStatisticsCollector statisticsCollector;
    
    /**
     * Adds a task which will run until cancelled, until it throws an exception,
     * or until the federation is {@link #shutdown()}.
     * <p>
     * Note: Tasks run on this service generally update sampled values on
     * {@link ICounter}s reported to the {@link ILoadBalancerService}. Basic
     * information on the {@link #getThreadPool()} is reported automatically.
     * Clients may add additional tasks to report on client-side aspects of
     * their application.
     * <p>
     * Note: Non-sampled counters are automatically conveyed to the
     * {@link ILoadBalancerService} once added to the basic {@link CounterSet}
     * returned by {@link #getCounterSet()}.
     * 
     * @param task
     *            The task.
     * @param initialDelay
     *            The initial delay.
     * @param delay
     *            The delay between invocations.
     * @param unit
     *            The units for the delay parameters.
     * 
     * @return The {@link ScheduledFuture} for that task.
     */
    public ScheduledFuture addScheduledStatisticsTask(Runnable task,
            long initialDelay, long delay, TimeUnit unit) {

        if (task == null)
            throw new IllegalArgumentException();

        log.info("Adding task: " + task.getClass());

        return sampleService.scheduleWithFixedDelay(task, initialDelay, delay,
                unit);

    }

    synchronized public CounterSet getCounterSet() {

        if (countersRoot == null) {

            countersRoot = new CounterSet();

            if (statisticsCollector != null) {

                countersRoot.attach(statisticsCollector.getCounters());

            }

            clientRoot = countersRoot.makePath(getClientCounterPathPrefix());

            /*
             * Basic counters.
             */

            AbstractStatisticsCollector.addBasicServiceOrClientCounters(
                    clientRoot, getClient(), client.getProperties());

        }

        return countersRoot;
        
    }
    private CounterSet countersRoot;
    private CounterSet clientRoot;
    
    public String getClientCounterPathPrefix() {

        final UUID clientUUID = getClient().getClientUUID();

        final String ps = ICounterSet.pathSeparator;

        final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;

        final String pathPrefix = ps + hostname + ps + "service" + ps
                + IBigdataClient.class.getName() + ps + clientUUID + ps;

        return pathPrefix;

    }
    
    public ExecutorService getThreadPool() {
        
        assertOpen();
        
        return threadPool;
        
    }

    protected AbstractFederation(IBigdataClient client) {

        if (client == null)
            throw new IllegalArgumentException();

        this.client = client;

        final int threadPoolSize = client.getThreadPoolSize();

        if (threadPoolSize == 0) {

            threadPool = (ThreadPoolExecutor) Executors
                    .newCachedThreadPool(DaemonThreadFactory
                            .defaultThreadFactory());

        } else {

            threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    threadPoolSize, DaemonThreadFactory.defaultThreadFactory());

        }
        
        /*
         * indexCache
         */
        {

            indexCache = new WeakValueCache<NT, IClientIndex>(
                    new LRUCache<NT, IClientIndex>(client.getIndexCacheCapacity()));
            
        }
        
        /*
         * Start collecting performance counters from the OS.
         */
        {

            final Properties p = client.getProperties();
            
            final boolean collectPlatformStatistics = Boolean
                    .parseBoolean(p
                            .getProperty(
                                    com.bigdata.service.IBigdataClient.Options.COLLECT_PLATFORM_STATISTICS,
                                    com.bigdata.service.IBigdataClient.Options.DEFAULT_COLLECT_PLATFORM_STATISTICS));

            log.info(com.bigdata.service.IBigdataClient.Options.COLLECT_PLATFORM_STATISTICS
                            + "=" + collectPlatformStatistics);

            if (collectPlatformStatistics) {
                
                final UUID clientUUID = client.getClientUUID();

                log.info("Starting performance counter collection: uuid="
                        + clientUUID);

                p.setProperty(AbstractStatisticsCollector.Options.PROCESS_NAME,
                        "service" + ICounterSet.pathSeparator
                                + IBigdataClient.class.getName()
                                + ICounterSet.pathSeparator
                                + clientUUID.toString());

                statisticsCollector = AbstractStatisticsCollector
                        .newInstance(p);

                statisticsCollector.start();

            }
        
        }

        /*
         * Setup sampling on the client's thread pool. This collects interesting
         * statistics about the thread pool for reporting to the load balancer
         * service.
         */
        {

            final long initialDelay = 0; // initial delay in ms.
            final long delay = 1000; // delay in ms.
            final TimeUnit unit = TimeUnit.MILLISECONDS;

            final String relpath = "Thread Pool";

            final QueueStatisticsTask queueStatisticsTask = new QueueStatisticsTask(
                    relpath, threadPool, taskCounters);

            addScheduledStatisticsTask(queueStatisticsTask, initialDelay,
                    delay, unit);

            // make sure the counter set is constructed.
            getCounterSet();

            assert clientRoot != null;

            queueStatisticsTask.addCounters(clientRoot.makePath(relpath));

        }
        
        /*
         * Setup reporting to the load balancer service.
         * 
         * Note: the short initial delay means that we will run the ReportTask
         * immediately after which has the effect of notify()ing the load
         * balancer service that this client is now running.
         * 
         * Note: the initialDelay is not zero because we may have to discover
         * the load balancer service and that is done within a subclass and is
         * an asynchronous process in anycase.
         */
        {
            
            addScheduledStatisticsTask(new ReportTask(), 3/* initialDelay */,
                    60/* delay */, TimeUnit.SECONDS);
            
        }

        /*
         * HTTPD service reporting out statistics on a randomly assigned
         * port. The port is reported to the load balancer and also written
         * into the file system. The httpd service will be shutdown with the
         * connection to the federation.
         * 
         * @todo write port into the [serviceDir], but serviceDir needs to
         * be declared!
         * 
         * @todo option to disable this service (and also for the data service).
         */
        {
            
            try {

                final CounterSet counterSet = (CounterSet) getCounterSet();
                
                httpd = new CounterSetHTTPD(
                        0/* random port */, counterSet );
                
                // the URL that may be used to access the local httpd.
                final String url = "http://"
                        + AbstractStatisticsCollector.fullyQualifiedHostName
                        + ":" + httpd.getPort()
                        + "?path="+URLEncoder.encode(getClientCounterPathPrefix(),"UTF-8");
                
                // add counter reporting that url to the load balancer.
                clientRoot.addCounter(IServiceCounters.LOCAL_HTTPD,
                        new OneShotInstrument<String>(url));
                
            } catch (IOException e) {
                
                log.error("Could not start httpd", e);
                
            }
            
        }

        /*
         * Setup locator for relations. The default resolves relations using the
         * global row store.
         */
        relationLocator = new DefaultRelationLocator(getThreadPool(), this);
        
    }
    
    public void registerIndex(IndexMetadata metadata) {

        assertOpen();

        registerIndex(metadata, null);

    }

    public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID) {

        assertOpen();

        if (dataServiceUUID == null) {
            
            final ILoadBalancerService loadBalancerService = getLoadBalancerService();

            if (loadBalancerService == null) {

                try {

                    /*
                     * As a failsafe (or at least a failback) we ask the client
                     * for ANY data service that it knows about and use that as
                     * the data service on which we will register this index.
                     * This lets us keep going if the load balancer is dead when
                     * this request comes through.
                     */

                    dataServiceUUID = getAnyDataService().getServiceUUID();

                } catch (Exception ex) {

                    log.error(ex);

                    throw new RuntimeException(ex);

                }
                
            } else {

                try {

                    dataServiceUUID = loadBalancerService
                            .getUnderUtilizedDataService();

                } catch (Exception ex) {

                    throw new RuntimeException(ex);

                }

            }
            
        }

        return registerIndex(//
                metadata, //
                new byte[][] { new byte[] {} },//
                new UUID[] { dataServiceUUID } //
            );

    }

    public UUID registerIndex(IndexMetadata metadata, byte[][] separatorKeys,
            UUID[] dataServiceUUIDs) {

        assertOpen();

        try {

            UUID indexUUID = getMetadataService().registerScaleOutIndex(
                    metadata, separatorKeys, dataServiceUUIDs);

            return indexUUID;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * A canonicalizing cache for the client's {@link IIndex} proxy objects. The
     * keys are {@link NT} objects which represent both the name of the index
     * and the timestamp for the index view. The values are the {@link IIndex}
     * proxy objects.
     * <p>
     * Note: The "dirty" flag associated with the object in this cache is
     * ignored.
     */
    final protected WeakValueCache<NT, IClientIndex> indexCache;
    
    /**
     * @todo synchronization should be limited to the index resource and the
     *       cache when we actually touch the cache. synchronizing the entire
     *       method limits concurrency for access to other named resources or
     *       the same named resource as of a different timestamp.
     */
    synchronized public IIndex getIndex(String name,long timestamp) {

        log.info("name="+name+" @ "+timestamp);
        
        assertOpen();

        final NT nt = new NT(name,timestamp);
        
        IClientIndex ndx = indexCache.get(nt);

        if (ndx == null) {

            final MetadataIndexMetadata mdmd = getMetadataIndexMetadata(name,
                    timestamp);

            // No such index.
            if (mdmd == null) {

                log.info("name="+name+" @ "+timestamp+" : is not registered");
                
                return null;
                
            }

            // Index exists.
            ndx = new ClientIndexView(this, name, timestamp, mdmd);

            indexCache.put(nt, ndx, false/* dirty */);

            log.info("name="+name+" @ "+timestamp+" : index exists.");
            
        } else {
            
            log.info("name="+name+" @ "+timestamp+" : cache hit.");
            
        }

        return ndx;

    }

    public void dropIndex(String name) {

        log.info("name="+name);
        
        assertOpen();

        try {
            
            getMetadataService().dropScaleOutIndex(name);

            log.info("dropped scale-out index.");
            
            dropIndexFromCache(name);

        } catch (Exception e) {

            throw new RuntimeException( e );
            
        }

    }

    /**
     * Drops the entry for the named index from the {@link #indexCache}.
     * <p>
     * Historical and transactional reads are still allowed, but we remove the
     * the read-committed or unisolated views from the cache once the index has
     * been dropped. If a client wants them, it needs to re-request. If they
     * have been re-registered on the metadata service then they will become
     * available again.
     * <p>
     * Note: Operations against unisolated or read-committed indices will throw
     * exceptions if they execute after the index was dropped.
     */
    protected void dropIndexFromCache(String name) {
        
        synchronized(indexCache) {
            
            Iterator<ICacheEntry<NT,IClientIndex>> itr = indexCache.entryIterator();
            
            while(itr.hasNext()) {
                
                final ICacheEntry<NT,IClientIndex> entry = itr.next();
                
                final IClientIndex ndx = entry.getObject(); 
                
                if(name.equals(ndx.getName())) {
                    
                    final long timestamp = ndx.getTimestamp();

                    if (timestamp == ITx.UNISOLATED
                            || timestamp == ITx.READ_COMMITTED) {
                    
                        log.info("dropped from cache: "+name+" @ "+timestamp);
                        
                        // remove from the cache.
                        indexCache.remove(entry.getKey());

                    }
                    
                }
                
            }
            
        }
        
    }
    
    /**
     * Return the metadata for the metadata index itself.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @param timestamp
     * 
     * @return The metadata for the metadata index or <code>null</code> iff no
     *         scale-out index is registered by that name at that timestamp.
     */
    protected MetadataIndexMetadata getMetadataIndexMetadata(String name, long timestamp) {

        assertOpen();

        final MetadataIndexMetadata mdmd;
        try {

            // @todo test cache for this object as of that timestamp?
            mdmd = (MetadataIndexMetadata) getMetadataService()
                    .getIndexMetadata(
                            MetadataService.getMetadataIndexName(name),
                            timestamp);
            
            assert mdmd != null;

        } catch( NoSuchIndexException ex ) {
            
            return null;
        
        } catch (ExecutionException ex) {
            
            if(InnerCause.isInnerCause(ex, NoSuchIndexException.class)) return null;
            
            throw new RuntimeException(ex);
            
        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }
        
        if (mdmd == null) {

            // No such index.
            
            return null;

        }
        
        return mdmd;

    }
    
    /*
     * thread-local key builder. 
     */

    /**
     * A {@link ThreadLocal} variable providing access to thread-specific
     * instances of a configured {@link IKeyBuilder}.
     * <p>
     * Note: this {@link ThreadLocal} is not static since we need configuration
     * properties from the constructor - those properties can be different for
     * different {@link IBigdataClient}s on the same machine.
     */
    private ThreadLocal<IKeyBuilder> threadLocalKeyBuilder = new ThreadLocal<IKeyBuilder>() {

        protected synchronized IKeyBuilder initialValue() {

            return KeyBuilder.newUnicodeInstance(client.getProperties());

        }

    };

    /**
     * Return a {@link ThreadLocal} {@link IKeyBuilder} instance configured
     * using the properties specified for the {@link IBigdataClient}.
     */
    public IKeyBuilder getKeyBuilder() {
        
        assertOpen();
        
        return threadLocalKeyBuilder.get();
        
    }
    
    /*
     * named records
     */

    private final GlobalRowStoreHelper globalRowStoreHelper = new GlobalRowStoreHelper(this);
    
    public SparseRowStore getGlobalRowStore() {
        
        return globalRowStoreHelper.getGlobalRowStore();
        
    }
    
//    private final String GLOBAL_ROW_STORE_INDEX = "__global_namespace_index";
//
//    synchronized public SparseRowStore getGlobalRowStore() {
//        
//        log.info("");
//
//        if (globalRowStore == null) {
//
//            IIndex ndx = getIndex(GLOBAL_ROW_STORE_INDEX, ITx.UNISOLATED);
//
//            if (ndx == null) {
//
//                log.info("Global row store does not exist - will try to register now");
//                
//                try {
//
//                    registerIndex(new IndexMetadata(GLOBAL_ROW_STORE_INDEX,
//                            UUID.randomUUID()));
//
//                } catch (Exception ex) {
//
//                    throw new RuntimeException(ex);
//
//                }
//
//                ndx = getIndex(GLOBAL_ROW_STORE_INDEX, ITx.UNISOLATED);
//
//                if (ndx == null) {
//
//                    throw new RuntimeException("Could not find index?");
//
//                }
//
//            }
//
//            globalRowStore = new SparseRowStore(ndx);
//
//        }
//        
//        return globalRowStore;
//
//    }
//    private SparseRowStore globalRowStore;
    
//    public Object getNamedRecord(final String primaryKey) {
//
//        if (primaryKey == null)
//            throw new IllegalArgumentException();
//
//        final ITPS tps = getGlobalRowStore().read(getKeyBuilder(),//
//                GlobalRowStoreSchema.INSTANCE, //
//                primaryKey,//
//                Long.MAX_VALUE, // most recent value only
//                null // filter
//                );
//        
//        final ITPV tpv = tps.get(GlobalRowStoreSchema.VALUE);
//
//        if (tpv.getValue() == null) {
//
//            // No value for that name.
//            return null;        
//            
//        }
//        
//        return tpv;
//        
//    }
//
//    public Object putNamedRecord(String primaryKey, Object value) {
//
//        if (primaryKey == null)
//            throw new IllegalArgumentException();
//
//        final Map<String,Object> row = new HashMap<String,Object>();
//        
//        row.put(primaryKey,value);
//        
//        final ITPS tps = getGlobalRowStore().write(//
//                getKeyBuilder(), //
//                GlobalRowStoreSchema.INSTANCE, //
//                row,//
//                SparseRowStore.AUTO_TIMESTAMP_UNIQUE,//
//                null // filter
//                );
//
//        final ITPV tpv = tps.get(GlobalRowStoreSchema.VALUE);
//
//        if (tpv.getValue() == null) {
//
//            // No value for that name.
//            return null;
//
//        }
//
//        return tpv;
//
//    }

    /**
     * Periodically send performance counter data to the
     * {@link ILoadBalancerService}.
     * 
     * @see DataService.ReportTask
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class ReportTask implements Runnable {

        /**
         * Note: The logger is named for this class, but since it is an inner
         * class the name uses a "$" delimiter (vs a ".") between the outer and
         * the inner class names.
         */
        final protected Logger log = Logger.getLogger(ReportTask.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
                .toInt();

        public ReportTask() {
        }

        /**
         * Note: Don't throw anything here since we don't want to have the task
         * suppressed!
         */
        public void run() {

            try {

                reportPerformanceCounters();
                
            } catch (Throwable t) {

                log.warn("Problem in report task?", t);

            }

        }
        
        /**
         * Send performance counters to the load balancer.
         * 
         * @throws IOException 
         */
        protected void reportPerformanceCounters() throws IOException {

            final UUID clientUUID = getClient().getClientUUID();

            final ILoadBalancerService loadBalancerService = getLoadBalancerService();

            if (loadBalancerService == null) {

                log.warn("Could not discover load balancer service.");

                return;

            }

            /*
             * @todo this is probably worth compressing as there will be a lot
             * of redundency.
             * 
             * @todo allow filter on what gets sent to the load balancer?
             */
            ByteArrayOutputStream baos = new ByteArrayOutputStream(
                    Bytes.kilobyte32 * 2);

            getCounterSet().asXML(baos, "UTF-8", null/* filter */);

            loadBalancerService.notify("Hello", clientUUID,
                    IBigdataClient.class.getName(), baos.toByteArray());

            log.info("Notified the load balancer.");
            
        }

    }

    /**
     * Forces the immediate reporting of the {@link CounterSet} to the
     * {@link ILoadBalancerService}.
     */
    public void reportCounters() {

        new ReportTask().run();
        
    }

}
