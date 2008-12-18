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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bfs.GlobalFileSystemHelper;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IServiceCounters;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.TemporaryStoreFactory;
import com.bigdata.rawstore.Bytes;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.service.IBigdataClient.Options;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.QueueStatisticsTask;
import com.bigdata.util.concurrent.TaskCounters;
import com.bigdata.util.httpd.AbstractHTTPD;

/**
 * Abstract base class for {@link IBigdataFederation} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo implement {@link IServiceShutdown}. When it is declared here it messes
 *       up the Options interface hierarchy. What appears to be happening is
 *       that the IServiceShutdown.Options interface is flattened into
 *       IServiceShutdown and it shadows the Options that are being used.
 */
abstract public class AbstractFederation implements IBigdataFederation, IFederationDelegate {

    protected static final Logger log = Logger.getLogger(IBigdataFederation.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    private AbstractClient client;
    
    public AbstractClient getClient() {
        
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

        if (INFO)
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
            
            httpd = null;
            
            httpdURL = null;
            
        }
        
        if (INFO)
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
        
        if(INFO)
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
            
            httpd = null;
            
            httpdURL = null;
            
        }

        if (INFO)
            log.info("done: elapsed=" + (System.currentTimeMillis() - begin));
        
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
            .newSingleThreadScheduledExecutor(new DaemonThreadFactory
                    (getClass().getName()+".sampleService"));
    
    /**
     * httpd reporting the live counters for the client while it is connected to
     * the federation.
     */
    private AbstractHTTPD httpd;
    
    /**
     * The URL that may be used to access the httpd service exposed by this
     * client.
     */
    private String httpdURL;

    final public String getHttpdURL() {
        
        return httpdURL;
        
    }
    
    /**
     * Locator for relations, etc.
     */
    private final DefaultResourceLocator resourceLocator;
    
    public DefaultResourceLocator getResourceLocator() {
        
        assertOpen();
        
        return resourceLocator;
    
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
     * information on the {@link #getExecutorService()} is reported
     * automatically. Clients may add additional tasks to report on client-side
     * aspects of their application.
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
    public ScheduledFuture addScheduledTask(Runnable task,
            long initialDelay, long delay, TimeUnit unit) {

        if (task == null)
            throw new IllegalArgumentException();

        if (INFO)
            log.info("Scheduling task: task=" + task.getClass()
                    + ", initialDelay=" + initialDelay + ", delay=" + delay
                    + ", unit=" + unit);

        return sampleService.scheduleWithFixedDelay(task, initialDelay, delay,
                unit);

    }

    synchronized public CounterSet getCounterSet() {

        if (countersRoot == null) {

            countersRoot = new CounterSet();

            if (statisticsCollector != null) {

                countersRoot.attach(statisticsCollector.getCounters());

            }

            serviceRoot = countersRoot.makePath(getServiceCounterPathPrefix());

            /*
             * Basic counters.
             */

            AbstractStatisticsCollector.addBasicServiceOrClientCounters(
                    serviceRoot, getServiceName(), getServiceIface(), client
                            .getProperties());

        }

        return countersRoot;
        
    }
    private CounterSet countersRoot;
    private CounterSet serviceRoot;
    
    public CounterSet getServiceCounterSet() {

        // note: defines [serviceRoot] as side effect.
        getCounterSet();
        
        return serviceRoot;
        
    }

    public String getServiceCounterPathPrefix() {

        final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;

        return getServiceCounterPathPrefix(getServiceUUID(), getServiceIface(),
                hostname);

    }

    /**
     * The path prefix under which all of the client or service's counters are
     * located. The returned path prefix is terminated by an
     * {@link ICounterSet#pathSeparator}.
     * 
     * @param serviceUUID
     *            The service {@link UUID}.
     * @param serviceIface
     *            The primary interface or class for the service.
     * @param hostname
     *            The fully qualified name of the host on which the service is
     *            running.
     */
    static public String getServiceCounterPathPrefix(UUID serviceUUID,
            Class serviceIface, String hostname) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();
        
        if (serviceIface == null)
            throw new IllegalArgumentException();
        
        if (hostname == null)
            throw new IllegalArgumentException();
        
        final String ps = ICounterSet.pathSeparator;

        final String pathPrefix = ps + hostname + ps + "service" + ps
                + serviceIface.getName() + ps + serviceUUID + ps;

        return pathPrefix;

    }
    
    public ExecutorService getExecutorService() {
        
        assertOpen();
        
        return threadPool;
        
    }

    protected AbstractFederation(final IBigdataClient client) {

        if (client == null)
            throw new IllegalArgumentException();

        this.client = (AbstractClient) client;

        if (this.client.getDelegate() == null) {

            /*
             * If no service has set the delegate by this point then we setup a
             * default delegate.
             */

            this.client.setDelegate(new DefaultClientDelegate(this));
            
        }
        
        final int threadPoolSize = client.getThreadPoolSize();

        if (threadPoolSize == 0) {

            threadPool = (ThreadPoolExecutor) Executors
                    .newCachedThreadPool(new DaemonThreadFactory
                            (getClass().getName()+".executorService"));

        } else {

            threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    threadPoolSize, new DaemonThreadFactory
                    (getClass().getName()+".executorService"));

        }
        
        tempStoreFactory = new TemporaryStoreFactory(this.client
                .getTempStoreMaxExtent());
        
        addScheduledTask(//
                new StartDeferredTasksTask(),// task to run.
                150, // initialDelay (ms)
                150, // delay
                TimeUnit.MILLISECONDS // unit
                );

        // Setup locator.
        resourceLocator = new DefaultResourceLocator(this,
                null, // delegate
                ((AbstractClient) client).getLocatorCacheCapacity(),
                ((AbstractClient) client).getLocatorCacheTimeout());
        
    }
    
    public void registerIndex(IndexMetadata metadata) {

        assertOpen();

        registerIndex(metadata, null);

    }

    public UUID registerIndex(final IndexMetadata metadata, UUID dataServiceUUID) {

        assertOpen();

        if (dataServiceUUID == null) {
            
            // see if there is an override.
            dataServiceUUID = metadata.getInitialDataServiceUUID();
            
            if (dataServiceUUID == null) {

                final ILoadBalancerService loadBalancerService = getLoadBalancerService();

                if (loadBalancerService == null) {

                    try {

                        /*
                         * As a failsafe (or at least a failback) we ask the
                         * client for ANY data service that it knows about and
                         * use that as the data service on which we will
                         * register this index. This lets us keep going if the
                         * load balancer is dead when this request comes
                         * through.
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
            
        }

        return registerIndex(//
                metadata, //
                new byte[][] { new byte[] {} },//
                new UUID[] { dataServiceUUID } //
            );

    }

    public UUID registerIndex(final IndexMetadata metadata,
            final byte[][] separatorKeys, final UUID[] dataServiceUUIDs) {

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
     * Return the cache for {@link IIndex} objects.
     */
    abstract protected AbstractIndexCache<? extends IClientIndex> getIndexCache();
    
    /**
     * Applies an {@link AbstractIndexCache} and strengthens the return type.
     */
    public IClientIndex getIndex(String name, long timestamp) {

        if (INFO)
            log.info("name="+name+" @ "+timestamp);
        
        assertOpen();

        return getIndexCache().getIndex(name, timestamp);
        
    }

    public void dropIndex(String name) {

        if (INFO)
            log.info("name=" + name);

        assertOpen();

        try {

            getMetadataService().dropScaleOutIndex(name);

            if (INFO)
                log.info("dropped scale-out index.");
            
            getIndexCache().dropIndexFromCache(name);

        } catch (Exception e) {

            throw new RuntimeException( e );
            
        }

    }

    public SparseRowStore getGlobalRowStore() {
        
        return globalRowStoreHelper.getGlobalRowStore();

    }

    private final GlobalRowStoreHelper globalRowStoreHelper = new GlobalRowStoreHelper(
            this);

    public BigdataFileSystem getGlobalFileSystem() {

        return globalFileSystemHelper.getGlobalFileSystem();

    }

    private final GlobalFileSystemHelper globalFileSystemHelper = new GlobalFileSystemHelper(
            this);

    public TemporaryStore getTempStore() {

        return tempStoreFactory.getTempStore();

    }

    private final TemporaryStoreFactory tempStoreFactory;

    /**
     * Forces the immediate reporting of the {@link CounterSet} to the
     * {@link ILoadBalancerService}. Any errors will be logged, not thrown.
     */
    public void reportCounters() {

        new ReportTask(this).run();
        
    }

    /**
     * Delegated.
     */
    public String getServiceName() {
    
        return client.getDelegate().getServiceName();
        
    }
    
    /**
     * Delegated.
     */
    public Class getServiceIface() {
    
        return client.getDelegate().getServiceIface();
        
    }
    
    /**
     * Delegated.
     */
    public UUID getServiceUUID() {
        
        return client.getDelegate().getServiceUUID();
        
    }
    
    /**
     * Delegated.
     */
    public boolean isServiceReady() {
        
        return client.getDelegate().isServiceReady();
        
    }
    
    /**
     * Delegated.
     */
    public void reattachDynamicCounters() {
        
        client.getDelegate().reattachDynamicCounters();
        
    }
    
    /**
     * Delegated.
     */
    public void didStart() {
        
        client.getDelegate().didStart();
        
    }
    
    public void serviceJoin(IService service, UUID serviceUUID) {
        
        if(INFO) {
            
            log.info("service=" + service + ", serviceUUID" + serviceUUID);
            
        }
        
        client.getDelegate().serviceJoin(service, serviceUUID);
        
    }

    public void serviceLeave(UUID serviceUUID) {
        
        if(INFO) {
            
            log.info("serviceUUID="+serviceUUID);
            
        }
        
        final AbstractClient client = this.client;

        if (client != null && client.isConnected()) {

            client.getDelegate().serviceLeave(serviceUUID);

        }

    }
    
    /**
     * This task runs periodically. Once {@link #getServiceUUID()} reports a
     * non-<code>null</code> value, it will start an (optional)
     * {@link AbstractStatisticsCollector}, an (optional) httpd service, and
     * the (required) {@link ReportTask}.
     * <p>
     * Note: The {@link ReportTask} will relay any collected performance
     * counters to the {@link ILoadBalancerService}, but it also lets the
     * {@link ILoadBalancerService} know which services exist, which is
     * important for some of its functions.
     * <p>
     * Once these task(s) have been started, this task will throw an exception
     * in order to prevent it from being re-executed.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class StartDeferredTasksTask implements Runnable {

        /**
         * Note: The logger is named for this class, but since it is an inner
         * class the name uses a "$" delimiter (vs a ".") between the outer and
         * the inner class names.
         */
        final protected Logger log = Logger.getLogger(StartDeferredTasksTask.class);

        final protected boolean INFO = log.isInfoEnabled();
        
        public StartDeferredTasksTask() {
        
        }

        /**
         * @throws RuntimeException
         *             once the deferred task(s) are running to prevent
         *             re-execution of this startup task.
         */
        public void run() {

            final boolean started;
            
            try {
                
                started = startDeferredTasks();
                
            } catch (Throwable t) {

                log.warn("Problem in report task?", t);

                return;
                
            }

            if (started) {

                /*
                 * Note: This exception is thrown once this task has executed
                 * successfully.
                 */
                
                throw new RuntimeException("Normal completion.");
                
            }
            
        }

        /**
         * Starts performance counter collection once the service {@link UUID}
         * is known.
         * 
         * @return <code>true</code> iff performance counter collection was
         *         started.
         * 
         * @throws IOException
         *             if {@link IDataService#getServiceUUID()} throws this
         *             exception (it never should since it is a local method
         *             call).
         */
        protected boolean startDeferredTasks() throws IOException {

            if (getServiceUUID() == null) {

                log.warn("Service UUID is not assigned yet.");

                return false;

            }
            
            if(!isServiceReady()) {
            
                log.warn("Service not ready yet.");

                return false;
                
            }
            
            /*
             * start collection on various work queues.
             * 
             * Note: The data service starts collection for its queues
             * (actually, the concurrency managers queues) once it is up and
             * running.
             * 
             * @todo have it collect using the same scheduled thread pool.
             */
            startQueueStatisticsCollection();
            
            // start collecting performance counters (if enabled).
            startPerformanceCounterCollection();

//            // notify the load balancer of this service join.
//            notifyJoin();
            
            // start reporting to the load balancer.
            startReportTask();

            // start the local httpd service reporting on this service.
            startHttpdService();
            
            // notify delegates that deferred startup has occurred.
            AbstractFederation.this.didStart();

            return true;

        }


        /**
         * Setup sampling on the client's thread pool. This collects interesting
         * statistics about the thread pool for reporting to the load balancer
         * service.
         */
        protected void startQueueStatisticsCollection() {

            if (!client.getCollectQueueStatistics()) {

                if (INFO)
                    log.info("Queue statistics collection disabled: "
                            + getServiceIface());

                return;

            }

            final long initialDelay = 0; // initial delay in ms.
            final long delay = 1000; // delay in ms.
            final TimeUnit unit = TimeUnit.MILLISECONDS;

            final String relpath = "Thread Pool";

            final QueueStatisticsTask queueStatisticsTask = new QueueStatisticsTask(
                    relpath, threadPool, taskCounters);

            addScheduledTask(queueStatisticsTask, initialDelay,
                    delay, unit);

            queueStatisticsTask.addCounters(getServiceCounterSet().makePath(relpath));

        }
        
        /**
         * Start collecting performance counters from the OS (if enabled).
         */
        protected void startPerformanceCounterCollection() {

            final UUID serviceUUID = getServiceUUID();

            final Properties p = getClient().getProperties();

            if (getClient().getCollectPlatformStatistics()) {

                p.setProperty(AbstractStatisticsCollector.Options.PROCESS_NAME,
                        "service" + ICounterSet.pathSeparator
                                + getServiceIface().getName()
                                + ICounterSet.pathSeparator
                                + serviceUUID.toString());

                statisticsCollector = AbstractStatisticsCollector
                        .newInstance(p);

                statisticsCollector.start();

                /*
                 * Attach the counters that will be reported by the statistics
                 * collector service.
                 */
                ((CounterSet) getCounterSet()).attach(statisticsCollector
                        .getCounters());

                if (INFO)
                    log.info("Collecting platform statistics: uuid="
                            + serviceUUID);

            }

        }

        /**
         * Start task to report service and counters to the load balancer.
         */
        protected void startReportTask() {

            final Properties p = getClient().getProperties();

            final long delay = Long.parseLong(p.getProperty(
                    Options.REPORT_DELAY, Options.DEFAULT_REPORT_DELAY));

            if (INFO)
                log.info(Options.REPORT_DELAY + "=" + delay);

            final TimeUnit unit = TimeUnit.MILLISECONDS;

            final long initialDelay = delay;

            addScheduledTask(new ReportTask(AbstractFederation.this),
                    initialDelay, delay, unit);

            if (INFO)
                log.info("Started ReportTask.");

        }

        /**
         * Start the local httpd service (if enabled). The service is started on
         * the {@link IBigdataClient#getHttpdPort()}, on a randomly assigned
         * port if the port is <code>0</code>, or NOT started if the port is
         * <code>-1</code>. If the service is started, then the URL for the
         * service is reported to the load balancer and also written into the
         * file system. When started, the httpd service will be shutdown with
         * the federation.
         * 
         * @throws UnsupportedEncodingException
         */
        protected void startHttpdService() throws UnsupportedEncodingException {

            final String path = getServiceCounterPathPrefix();
            
            final int httpdPort = client.getHttpdPort();

            if (httpdPort == -1) {

                if (INFO)
                    log.info("httpd disabled: "+path);
                
                return;

            }

            try {

                AbstractFederation.this.httpd = newHttpd(httpdPort,
                        getCounterSet());

            } catch (IOException e) {

                log.error("Could not start httpd : "+path, e);

                return;
                
            }

            // the URL that may be used to access the local httpd.
            httpdURL = "http://"
                    + AbstractStatisticsCollector.fullyQualifiedHostName + ":"
                    + httpd.getPort() + "?path="
                    + URLEncoder.encode(path, "UTF-8");

            if (INFO)
                log.info("start:\n" + httpdURL);

            // add counter reporting that url to the load balancer.
            serviceRoot.addCounter(IServiceCounters.LOCAL_HTTPD,
                    new OneShotInstrument<String>(httpdURL));

        }
        
        /**
         * Create a new {@link AbstractHTTPD} instance.
         * 
         * @param port
         *            The port, or zero for a random port.
         * @param counterSet
         *            The root {@link CounterSet} that will be served up.
         * 
         * @throws IOException
         */
        protected final AbstractHTTPD newHttpd(final int httpdPort,
                CounterSet counterSet) throws IOException {

            return new CounterSetHTTPD(httpdPort, counterSet) {

                public Response doGet(String uri, String method,
                        Properties header, Map<String, Vector<String>> parms)
                        throws Exception {

                    try {

                        reattachDynamicCounters();

                    } catch (Exception ex) {

                        /*
                         * Typically this is because the live journal has been
                         * concurrently closed during the request.
                         */

                        log.warn("Could not re-attach dynamic counters: " + ex,
                                ex);

                    }

                    return super.doGet(uri, method, header, parms);

                }

            };

        }

    }
    
    /**
     * Periodically report performance counter data to the
     * {@link ILoadBalancerService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ReportTask implements Runnable {

        /**
         * Note: The logger is named for this class, but since it is an inner
         * class the name uses a "$" delimiter (vs a ".") between the outer and
         * the inner class names.
         */
        final protected Logger log = Logger.getLogger(ReportTask.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        final protected boolean INFO = log.isInfoEnabled();

        private final AbstractFederation fed;
        
        public ReportTask(AbstractFederation fed) {

            if (fed == null)
                throw new IllegalArgumentException();
            
            this.fed = fed;
            
        }

        /**
         * Note: Don't throw anything here since we don't want to have the task
         * suppressed!
         */
        public void run() {

            try {
                
                fed.reattachDynamicCounters();

            } catch (Throwable t) {

                log.error("Could not update performance counter view : " + t, t);

            }
            
            try {

                /*
                 * Report the performance counters to the load balancer.
                 */
                
                reportPerformanceCounters();
                
            } catch (Throwable t) {

                log.error("Could not report performance counters : " + t, t);

            }

        }
        
        /**
         * Send performance counters to the load balancer.
         * 
         * @throws IOException 
         */
        protected void reportPerformanceCounters() throws IOException {

            // Note: This _is_ a local method call.
            final UUID serviceUUID = fed.getServiceUUID();

            // Will be null until assigned by the service registrar.
            if (serviceUUID == null) {

                if(INFO)
                    log.info("Service UUID not assigned yet.");

                return;

            }

            final ILoadBalancerService loadBalancerService = fed.getLoadBalancerService();

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
            final ByteArrayOutputStream baos = new ByteArrayOutputStream(
                    Bytes.kilobyte32 * 2);

            fed.getCounterSet().asXML(baos, "UTF-8", null/* filter */);

            loadBalancerService.notify(serviceUUID, baos.toByteArray());

            if (INFO)
                log.info("Notified the load balancer.");
            
        }

    }

}
