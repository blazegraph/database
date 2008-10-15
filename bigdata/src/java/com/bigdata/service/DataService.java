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
 * Created on Mar 14, 2007
 */

package com.bigdata.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.Banner;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.DropIndexTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ILocalTransactionManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StoreManager;

/**
 * An implementation of a network-capable {@link IDataService}. The service is
 * started using the {@link DataServer} class. Operations are submitted using an
 * {@link IConcurrencyManager#submit(AbstractTask)} and will run with the
 * appropriate concurrency controls as imposed by that method.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see DataServer, which is used to start this service.
 * 
 * @todo Participate in 1-phase (local) and 2-/3- phrase (distributed) commits
 *       with an {@link ITransactionManagerService} service. The data service
 *       needs to notify the {@link ITransactionManagerService} each time an
 *       isolated writer touches a named index so that the transaction manager
 *       can build up the set of resources that must be locked during the
 *       validate/commit phrase.
 * 
 * @todo Write benchmark test to measure interhost transfer rates. Should be
 *       100Mbits/sec (~12M/sec) on a 100BaseT switched network. With full
 *       duplex in the network and the protocol, that rate should be
 *       bidirectional. Can that rate be sustained with a fully connected
 *       bi-directional transfer?
 * 
 * @todo RPC requests are currently made via RPC using JERI. While you can elect
 *       to use the TCP/NIO server via configuration options (see
 *       http://java.sun.com/products/jini/2.0.1/doc/api/net/jini/jeri/tcp/package-summary.html),
 *       there will still be a thread allocated per concurrent RPC and no
 *       throttling will be imposed by JERI.
 *       <p>
 *       The present design of the {@link IDataService} API requires that a
 *       server thread be dedicated to each request against that interface - in
 *       this way it exactly matches the RPC semantics supported by JERI. The
 *       underlying reason is that the RPC calls are all translated into
 *       {@link Future}s when the are submitted via
 *       {@link ConcurrencyManager#submit(AbstractTask)}. The
 *       {@link DataService} itself then invokes {@link Future#get()} in order
 *       to await the completion of the request and return the response (object
 *       or thrown exception).
 *       <p>
 *       A re-design based on an asynchronous response from the server could
 *       remove this requirement, thereby allowing a handful of server threads
 *       to handle a large volume of concurrent client requests. The design
 *       would use asynchronous callback to the client via JERI RPC calls to
 *       return results, indications that the operation was complete, or
 *       exception information. A single worker thread on the server could
 *       monitor the various futures and RPC clients when responses become
 *       available or on request timeout.
 *       <p>
 *       See {@link NIODataService}, which contains some old code that can be
 *       refactored for an NIO interface to the data service.
 *       <p>
 *       Another option to throttle requests is to use a blocking queue to
 *       throttle the #of tasks that are submitted to the data service. Latency
 *       should be imposed on threads submitting tasks as the queue grows in
 *       order to throttle clients. If the queue becomes full
 *       {@link RejectedExecutionException} will be thrown, and the client will
 *       have to handle that. In contrast, if the queue never blocks and never
 *       imposes latency on clients then it is possible to flood the data
 *       service with requests, even through they will be processed by no more
 *       than {@link ConcurrentManager.Options#WRITE_SERVICE_MAXIMUM_POOL_SIZE}
 *       threads.
 * 
 * @todo Review JERI options to support secure RMI protocols. For example, using
 *       SSL or an SSH tunnel. For most purposes I expect bigdata to operate on
 *       a private network, but replicate across gateways is also a common use
 *       case. Do we have to handle it specially?
 */
abstract public class DataService extends AbstractService
    implements IDataService, IServiceShutdown //IWritePipeline
{

    public static final Logger log = Logger.getLogger(DataService.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.isDebugEnabled();
    
    /**
     * Options understood by the {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.journal.Options,
            com.bigdata.journal.ConcurrencyManager.Options,
            com.bigdata.resources.ResourceManager.Options,
            com.bigdata.counters.AbstractStatisticsCollector.Options
            // @todo local tx manager options?
            {

    }
    
    /**
     * @todo improve reporting here and for block write as well (goes through
     *       unisolated tasks at the present).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class ReadBlockCounters {
        
        /** #of block read requests. */
        long readBlockCount, readBlockErrorCount, readBlockBytes, readBlockNanos;
        
        public ReadBlockCounters() {
        
        }
        
    }
    
    /**
     * Counters for the block read API.
     */
    final private ReadBlockCounters readBlockApiCounters = new ReadBlockCounters();

    private ResourceManager resourceManager;
    private ConcurrencyManager concurrencyManager;
    private AbstractLocalTransactionManager localTransactionManager;
    
    /**
     * The object used to manage the local resources.
     */
    public ResourceManager getResourceManager() {
        
        return resourceManager;
        
    }

    /**
     * The object used to control access to the local resources.
     */
    public ConcurrencyManager getConcurrencyManager() {
        
        return concurrencyManager;
        
    }

    /**
     * The object used to coordinate transactions executing against local
     * resources.
     */
    public ILocalTransactionManager getLocalTransactionManager() {
        
        return localTransactionManager; 
        
    }
    
    /**
     * Returns the {@link IResourceManager}.
     * 
     * @param properties
     *            Properties to configure that object.
     * 
     * @return The {@link IResourceManager}.
     */
    protected IResourceManager newResourceManager(Properties properties) {

        return new ResourceManager(properties) {

            public IBigdataFederation getFederation() {
                
                return DataService.this.getFederation();
                                
            }
            
            public UUID getDataServiceUUID() {

                return DataService.this.getServiceUUID();
                
            }
            
            /**
             * @todo this must report the entire service failover chain.
             */
            public UUID[] getDataServiceUUIDs() {

                return new UUID[] {
                        
                        getDataServiceUUID()
                        
                };
                
            }
            
        };

    }

    /**
     * A clone of properties specified to the ctor.
     */
    private final Properties properties;

    /**
     * An object wrapping the properties specified to the ctor.
     */
    public Properties getProperties() {

        return new Properties(properties);
        
    }
    
    /**
     * Core constructor - you MUST {@link #start()} the {@link DataService}
     * before it can be used.
     * 
     * @param properties
     *            The configuration properties.
     * 
     * @see Options
     * 
     * @see #start()
     */
    protected DataService(Properties properties) {
        
        // show the copyright banner during statup.
        Banner.banner();

        this.properties = (Properties) properties.clone();
        
    }
    
    /**
     * Note: "open" is judged by the {@link ConcurrencyManager#isOpen()} but the
     * {@link DataService} is not usable until {@link StoreManager#isStarting()}
     * returns <code>false</code> (there is asynchronous processing involved
     * in reading the existing store files or creating the first store file and
     * you can not use the {@link DataService} until that processing has been
     * completed). The {@link ConcurrencyManager} will block for a while waiting
     * for the {@link StoreManager} startup to complete and will reject tasks if
     * startup processing does not complete within a timeout.
     */
    public boolean isOpen() {
        
        final ConcurrencyManager tmp = this.concurrencyManager;

        return tmp != null && tmp.isOpen();
        
    }
    
    /**
     * Starts the {@link DataService}.
     * 
     * @todo it would be nice if {@link #start()} could restart after
     *       {@link #shutdown()} but that is hardly necessary.
     */
    @Override
    synchronized public DataService start() {
        
        if(isOpen()) {
            
            throw new IllegalStateException(); 
            
        }
        
        resourceManager = (ResourceManager) newResourceManager(properties);

        localTransactionManager = new AbstractLocalTransactionManager(resourceManager) {

            public long nextTimestamp() throws IOException {

                // resolve the timestamp service.
                final ITimestampService timestampService = DataService.this
                        .getFederation().getTimestampService();

                if (timestampService == null)
                    throw new NullPointerException(
                            "TimestampService not discovered");

                // request the next distinct timestamp (robust).
                return timestampService.nextTimestamp();
                
            }

        };

        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, resourceManager);

        localTransactionManager.setConcurrencyManager(concurrencyManager);

        if (resourceManager instanceof ResourceManager) {

            /*
             * Startup the resource manager.
             */

            ((ResourceManager) resourceManager)
                    .setConcurrencyManager(concurrencyManager);

        }
        
        return this;
        
    }
    
    /**
     * Delegate handles custom counters for the {@link ResourceManager}, local
     * {@link TransactionService} and the {@link ConcurrencyManager}, dynamic
     * re-attachment of counters, etc. This delegate must be set on the
     * {@link AbstractClient} for those additional features to work.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class DataServiceFederationDelegate extends
            DefaultServiceFederationDelegate<DataService> {

        public DataServiceFederationDelegate(DataService service) {

            super(service);
            
        }
        
        /**
         * Dynamically detach and attach the counters for the named indices
         * underneath of the {@link IndexManager}.
         * <p>
         * Note: This method limits the frequency of update to no more than once per
         * second.
         */
        synchronized public void reattachDynamicCounters() {

            final long now = System.currentTimeMillis();

            final long elapsed = now - lastReattachMillis;

            if (service.isOpen() && service.resourceManager.isRunning()
                    && elapsed > 1000/* ms */) {

                final CounterSet tmp = service.resourceManager
                        .getIndexManagerCounters();

                assert tmp != null;

                synchronized (tmp) {

                    tmp.detach("indices");

                    tmp.makePath("indices").attach(
                            service.concurrencyManager.getIndexCounters()
                    // resourceManager.getLiveJournal().getNamedIndexCounters()
                            );

                }

                lastReattachMillis = now;

            }

        }
        private long lastReattachMillis = 0L;

        public boolean isServiceReady() {
            
            if(!service.resourceManager.isOpen()) {
                
                /*
                 * This will happen if the store manager is unable to discover
                 * the timestamp service. It will halt its startup process and
                 * report that it is closed. At that point the data service can
                 * not start and will shutdown.
                 */

                log.fatal("Store manager not open - will shutdown.");

                // shutdown the data service.
                service.shutdownNow();

                // collection was not started.
                return false;

            }

            if (!service.resourceManager.isRunning()) {

                log.warn("Resource manager is not running yet.");

                return false;

            }

            return true;

        }

        /**
         * Extended to setup {@link DataService} specific counters and to write
         * the client URL onto a file in the service's data directory.
         */
        public void didStart() {

            super.didStart();

            setupCounters();

            logHttpdURL();

        }

        /**
         * Sets up {@link DataService} specific counters.
         * 
         * @todo Add some counters providing a histogram of the index partitions
         *       that have touched or that are "hot"?
         * 
         * @see IDataServiceCounters
         */
        protected void setupCounters() {

            if (getServiceUUID() == null) {

                throw new IllegalStateException(
                        "The ServiceUUID is not available yet");

            }
            
            if(!service.isOpen()) {
                
                /*
                 * The service has already been closed.
                 */
                
                log.warn("Service is not open.");
                
                return;
                
            }

            /*
             * Service specific counters.
             */

            final CounterSet serviceRoot = service.getFederation()
                    .getServiceCounterSet();

            serviceRoot.makePath(IDataServiceCounters.resourceManager).attach(
                    service.resourceManager.getCounters());

            serviceRoot.makePath(IDataServiceCounters.concurrencyManager)
                    .attach(service.concurrencyManager.getCounters());

            serviceRoot.makePath(IDataServiceCounters.transactionManager)
                    .attach(service.localTransactionManager.getCounters());

            // block API.
            {

                CounterSet tmp = serviceRoot.makePath("Block API");

                tmp.addCounter("Blocks Read", new Instrument<Long>() {
                    public void sample() {
                        setValue(service.readBlockApiCounters.readBlockCount);
                    }
                });

                tmp.addCounter("Blocks Read Per Second",
                        new Instrument<Double>() {
                            public void sample() {

                                // @todo encapsulate this logic.

                                long secs = TimeUnit.SECONDS
                                        .convert(
                                                service.readBlockApiCounters.readBlockNanos,
                                                TimeUnit.NANOSECONDS);

                                final double v;

                                if (secs == 0L)
                                    v = 0d;
                                else
                                    v = service.readBlockApiCounters.readBlockCount
                                            / secs;

                                setValue(v);

                            }
                        });

            }

        }

        /**
         * Writes the URL of the local httpd service for the {@link DataService}
         * onto a file named <code>httpd.url</code> in the data directory that
         * was configured for the {@link DataService}.
         */
        protected void logHttpdURL() {
            
            final File dir = service.getResourceManager().getDataDir();
            
            final File httpdURLFile = new File(dir, "httpd.url");
            
            // delete in case old version exists.
            httpdURLFile.delete();
            
            final String httpdURL = service.getFederation().getHttpdURL();

            if (httpdURL != null) {

                try {

                    final Writer w = new BufferedWriter(new FileWriter(
                            httpdURLFile));

                    try {

                        w.write(httpdURL);

                    } finally {

                        w.close();

                    }

                } catch (IOException ex) {

                    log.warn("Problem writing httpdURL on file: "
                            + httpdURLFile);
                    
                }

            }

        }
        
    }
    
    /**
     * Polite shutdown does not accept new requests and will shutdown once the
     * existing requests have been processed.
     */
    synchronized public void shutdown() {

        if(!isOpen()) return;
        
        if (concurrencyManager != null) {
            concurrencyManager.shutdown();
            concurrencyManager = null;
        }
        
        if(localTransactionManager!=null) {
            localTransactionManager.shutdown();
            localTransactionManager = null;
        }

        if(resourceManager!=null) {
            resourceManager.shutdown();
            resourceManager = null;
        }

        super.shutdown();
        
    }

    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon as
     * possible.
     */
    synchronized public void shutdownNow() {

        if(!isOpen()) return;

        if (concurrencyManager != null) {
            concurrencyManager.shutdownNow();
            concurrencyManager = null;
        }

        if (localTransactionManager != null) {
            localTransactionManager.shutdownNow();
            localTransactionManager = null;
        }

        if (resourceManager != null) {
            resourceManager.shutdownNow();
            resourceManager = null;
        }

        super.shutdownNow();

    }

    /**
     * Interface defines and documents the counters and counter namespaces
     * reported by the {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IDataServiceCounters {
       
        /**
         * The namespace for the counters pertaining to the {@link ConcurrencyManager}.
         */
        String concurrencyManager = "Concurrency Manager";

        /**
         * The namespace for the counters pertaining to the {@link ITransactionManager}.
         */
        String transactionManager = "Transaction Manager";
        
        /**
         * The namespace for the counters pertaining to the {@link ResourceManager}.
         */
        String resourceManager = "Resource Manager";
        
    }
        
    /*
     * ITxCommitProtocol.
     */
    
    public long commit(long tx) throws IOException {
        
        setupLoggingContext();
        
        try {
        
            // will place task on writeService and block iff necessary.
            return localTransactionManager.commit(tx);
        
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    public void abort(long tx) throws IOException {

        setupLoggingContext();

        try {

            // will place task on writeService iff read-write tx.
            localTransactionManager.abort(tx);
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    /*
     * IDataService.
     */
    
    /**
     * Forms the name of the index corresponding to a partition of a named
     * scale-out index as <i>name</i>#<i>partitionId</i>.
     * <p>
     * Another advantage of this naming scheme is that index partitions are just
     * named indices and all of the mechanisms for operating on named indices
     * and for concurrency control for named indices apply automatically. Among
     * other things, this means that different tasks can write concurrently on
     * different partitions of the same named index on a given
     * {@link DataService}.
     * 
     * @return The name of the index partition.
     */
    public static final String getIndexPartitionName(final String name,
            final int partitionId) {

        if (name == null) {

            throw new IllegalArgumentException();
            
        }

        if (partitionId == -1) {

            // Not a partitioned index.
            return name;
            
        }
        
        return name + "#" + partitionId;

    }

    /**
     * Returns either {@link IDataService} or {@link IMetadataService} as
     * appropriate.
     */
    public Class getServiceIface() {

        final Class serviceIface;
        
        if(DataService.this instanceof IMetadataService) {
        
            serviceIface = IMetadataService.class;
            
        } else {
            
            serviceIface = IDataService.class;
            
        }
        
        return serviceIface;

    }
    
    /**
     * Sets up the {@link MDC} logging context. You should do this on every
     * client facing point of entry and then call {@link #clearLoggingContext()}
     * in a <code>finally</code> clause. You can extend this method to add
     * additional context.
     * <p>
     * This implementation adds the following parameters to the {@link MDC}.
     * <dl>
     * <dt>serviceName</dt>
     * <dd> The serviceName is typically a configuration property for the
     * service. This datum can be injected into log messages using
     * <em>%X{serviceName}</em> in your log4j pattern layout.</dd>
     * <dt>serviceUUID</dt>
     * <dd>The serviceUUID is, in general, assigned asynchronously by the
     * service registrar. Once the serviceUUID becomes available it will be
     * added to the {@link MDC}. This datum can be injected into log messages
     * using <em>%X{serviceUUID}</em> in your log4j pattern layout.</dd>
     * </dl>
     */
    protected void setupLoggingContext() {

        try {
            
            // Note: This _is_ a local method call.
            
            UUID serviceUUID = getServiceUUID();
            
            // Will be null until assigned by the service registrar.
            
            if (serviceUUID == null) {

                return;
                
            }
            
            // Add to the logging context for the current thread.
            
            MDC.put("serviceName", getServiceName());

            MDC.put("serviceUUID", serviceUUID);

        } catch(Throwable t) {

            /*
             * Ignore.
             */
            
        }
        
    }

    /**
     * Clear the logging context.
     */
    protected void clearLoggingContext() {
        
        MDC.remove("serviceName");

        MDC.remove("serviceUUID");
        
    }
    
    public void registerIndex(String name, IndexMetadata metadata)
            throws IOException, InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            if (metadata == null)
                throw new IllegalArgumentException();

            final AbstractTask task = new RegisterIndexTask(concurrencyManager,
                    name, metadata);
            
            concurrencyManager.submit(task).get();
        
        } finally {
            
            clearLoggingContext();
            
        }

    }
    
    public void dropIndex(String name) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {
        
            final AbstractTask task = new DropIndexTask(concurrencyManager,
                    name);
            
            concurrencyManager.submit(task).get();

        } finally {
            
            clearLoggingContext();
            
        }

    }
   
    public IndexMetadata getIndexMetadata(String name, long timestamp)
            throws IOException, InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            // Choose READ_COMMITTED iff UNISOLATED was requested.
            final long startTime = (timestamp == ITx.UNISOLATED
                    ? ITx.READ_COMMITTED
                    : timestamp);

            final AbstractTask task = new GetIndexMetadataTask(
                    concurrencyManager, startTime, name);

            return (IndexMetadata) concurrencyManager.submit(task).get();

        } finally {

            clearLoggingContext();

        }
        
    }

    /**
     * Retrieves the {@link IndexMetadata} for the named index as of the
     * specified timestamp.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class GetIndexMetadataTask extends AbstractTask {

        public GetIndexMetadataTask(ConcurrencyManager concurrencyManager,
                long startTime, String name) {

            super(concurrencyManager, startTime, name);
            
        }

        @Override
        protected IndexMetadata doTask() throws Exception {
            
            return getIndex(getOnlyResource()).getIndexMetadata();
            
        }
        
    }
    
    /**
     * Note: This chooses {@link ITx#READ_COMMITTED} if the the index has
     * {@link ITx#UNISOLATED} isolation and the {@link IIndexProcedure} is an
     * read-only operation. This provides better concurrency on the
     * {@link DataService} by moving read-only operations off of the
     * {@link WriteExecutorService}.
     */
    public Object submit(long tx, String name, IIndexProcedure proc)
            throws InterruptedException, ExecutionException {

        setupLoggingContext();

        try {
    
            if (name == null)
                throw new IllegalArgumentException();

            if (proc == null)
                throw new IllegalArgumentException();
            
            // Choose READ_COMMITTED iff proc is read-only and UNISOLATED was requested.
            final long startTime = (tx == ITx.UNISOLATED
                        && proc.isReadOnly() ? ITx.READ_COMMITTED
                        : tx);

            // wrap the caller's task.
            final AbstractTask task = new IndexProcedureTask(
                    concurrencyManager, startTime, name, proc);
            
            if(proc instanceof IDataServiceAwareProcedure) {

                if(log.isInfoEnabled()) {
                    
                    log.info("Data service aware procedure: "+proc.getClass().getName());
                    
                }

                // set the data service on the task.
                ((IDataServiceAwareProcedure)proc).setDataService( this );
                
            }
            
            // submit the procedure and await its completion.
            return concurrencyManager.submit(task).get();
        
        } finally {
            
            clearLoggingContext();
            
        }

    }

    /**
     * The task will be run on the
     * {@link IBigdataFederation#getExecutorService()}.
     * <p>
     * The {@link Callable} MAY implement {@link IDataServiceAwareProcedure} to
     * obtain the {@link DataService} reference, which can be used to obtain a
     * local {@link IBigdataClient} reference or to submit additional tasks to
     * the {@link ConcurrencyManager}.
     * <p>
     * Note: When the {@link DataService} is accessed via RMI the {@link Future}
     * MUST be a proxy.
     * 
     * @see AbstractDistributedFederation#getProxy(Future)
     * 
     * @todo Map/reduce can be handled in the this manner.
     *       <p>
     *       Note that we have excellent locators for the best data service when
     *       the map/reduce input is the scale-out repository since the task
     *       should run on the data service that hosts the file block(s). When
     *       failover is supported, the task can run on the service instance
     *       with the least load. When the input is a networked file system,
     *       then additional network topology smarts would be required to make
     *       good choices.
     */
    public Future<? extends Object> submit(Callable<? extends Object> task)
            throws InterruptedException, ExecutionException {
     
        setupLoggingContext();

        try {
    
            if (task == null)
                throw new IllegalArgumentException();
            
            if(task instanceof IDataServiceAwareProcedure) {
         
                if(log.isInfoEnabled()) {
                    
                    log.info("Data service aware procedure: "+task.getClass().getName());
                    
                }
                
                // set the data service on the task.
                ((IDataServiceAwareProcedure)task).setDataService( this );
                
            }
            
            // submit the task and await its completion.
            return getFederation().getExecutorService().submit(task);
        
        } finally {
            
            clearLoggingContext();
            
        }
        
    }
    
    /**
     * Encapsulate the {@link Future} within a proxy that may be marshalled by
     * RMI and sent to a remote client. The client will interact with the
     * unmarshalled {@link Future}, which in turn will use RMI to control the
     * original {@link Future} within the {@link DataService}.
     * <p>
     * The default implementation simply returns the <i>future</i> and MUST be
     * overriden when remote clients will use RMI to execute methods on the
     * {@link DataService}.
     * 
     * @param future
     *            The future.
     * 
     * @return The encapsulated future.
     */
    protected Future wrapFuture(Future future) {
        
        return future;
        
    }
    
    public ResultSet rangeIterator(long tx, String name, byte[] fromKey,
            byte[] toKey, int capacity, int flags, IFilterConstructor filter)
            throws InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {

            if (name == null)
                throw new IllegalArgumentException();
            
            /*
             * Figure out if the iterator is read-only for the time that it
             * executes on the data service. For this case, we ignore the CURSOR
             * flag since modifications during iterator execution on the data
             * service can only be introduced via a filter or the REMOVEALL
             * flag. The caller will be used a chunked iterator. Therefore if
             * they choose to delete tuples while visiting the elements in the
             * ResultSet then the deletes will be issued as separate requests.
             */
            final boolean readOnly = ((flags & IRangeQuery.READONLY) != 0)
                    || (filter == null &&
//                       ((flags & IRangeQuery.CURSOR) == 0) &&
                       ((flags & IRangeQuery.REMOVEALL) == 0)
                       );

            long timestamp = tx;

            if (timestamp == ITx.UNISOLATED && readOnly) {

                /*
                 * If the iterator is readOnly then READ_COMMITTED has the same
                 * semantics as UNISOLATED and provides better concurrency since
                 * it reduces contention for the writeService.
                 */

                timestamp = ITx.READ_COMMITTED;

            }

//            final long startTime = (tx == ITx.UNISOLATED
//                        && ((flags & IRangeQuery.REMOVEALL)==0)? ITx.READ_COMMITTED
//                        : tx);

            final RangeIteratorTask task = new RangeIteratorTask(
                    concurrencyManager, tx, name, fromKey, toKey, capacity,
                    flags, filter);
    
            // submit the task and wait for it to complete.
            return (ResultSet) concurrencyManager.submit(task).get();
        
        } finally {
            
            clearLoggingContext();
            
        }
            
    }

    /**
     * @todo this operation should be able to abort an
     *       {@link IBlock#inputStream() read} that takes too long or if there
     *       is a need to delete the resource.
     * 
     * @todo this should be run on the read service.
     * 
     * @todo coordinate close out of stores.
     * 
     * @todo efficient (stream-based) read from the journal (IBlockStore API).
     *       This is a fully buffered read and will cause heap churn.
     */
    public IBlock readBlock(IResourceMetadata resource, final long addr) {

        if (resource == null)
            throw new IllegalArgumentException();

        if (addr == 0L)
            throw new IllegalArgumentException();

        setupLoggingContext();

        final long begin = System.nanoTime();
        
        try {
            
            final IRawStore store = resourceManager.openStore(resource.getUUID());
    
            if (store == null) {
    
                log.warn("Resource not available: " + resource);
    
                readBlockApiCounters.readBlockErrorCount++;

                throw new IllegalStateException("Resource not available");
    
            }
    
            final int byteCount = store.getByteCount(addr);
            
            return new IBlock() {
    
                public long getAddress() {
                    
                    return addr;
                    
                }
    
                // @todo reuse buffers
                public InputStream inputStream() {
    
                    // this is when it actually reads the data.
                    final ByteBuffer buf = store.read(addr);

                    // #of bytes buffered.
                    readBlockApiCounters.readBlockBytes += byteCount;

                    // caller will read from this object.
                    return new ByteBufferInputStream(buf);
    
                }
    
                public int length() {
    
                    return byteCount;
    
                }
    
            };
            
        } finally {
            
            readBlockApiCounters.readBlockCount++;

            readBlockApiCounters.readBlockNanos = System.nanoTime() - begin;

            clearLoggingContext();
            
        }
                 
    }
    
    /**
     * Task for running a rangeIterator operation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class RangeIteratorTask extends AbstractTask {

        private final byte[] fromKey;
        private final byte[] toKey;
        private final int capacity;
        private final int flags;
        private final IFilterConstructor filter;
        
        public RangeIteratorTask(ConcurrencyManager concurrencyManager,
                long startTime, String name, byte[] fromKey, byte[] toKey,
                int capacity, int flags, IFilterConstructor filter) {

            super(concurrencyManager, startTime, name);

            this.fromKey = fromKey;
            this.toKey = toKey;
            this.capacity = capacity;
            this.flags = flags;
            this.filter = filter; // MAY be null.

        }

        public ResultSet doTask() throws Exception {

            final IIndex ndx = getIndex(getOnlyResource());
            
            /*
             * Figure out the upper bound on the #of tuples that could be
             * materialized.
             * 
             * Note: the upper bound on the #of key-value pairs in the range is
             * truncated to an [int].
             */
            
            final int rangeCount = (int) ndx.rangeCount(fromKey, toKey);

            final int limit = (rangeCount > capacity ? capacity : rangeCount);

            /*
             * Iterator that will visit the key range.
             * 
             * Note: We always visit the keys regardless of whether we pass them
             * on to the caller. This is necessary in order for us to set the
             * [lastKey] field on the result set and that is necessary to
             * support continuation queries.
             */
            
            final ITupleIterator itr = ndx.rangeIterator(fromKey, toKey, limit,
                    flags | IRangeQuery.KEYS, filter);
            
            /*
             * Populate the result set from the iterator.
             */

            return new ResultSet(ndx, capacity, flags, itr);

        }
        
    }

    /*
     * 
     */
    
    public void forceOverflow() throws IOException {
    
        setupLoggingContext();

        try {

            if (!(resourceManager instanceof ResourceManager)) {

                throw new UnsupportedOperationException();

            }

            final WriteExecutorService writeService = concurrencyManager
                    .getWriteService();

            final ResourceManager resourceManager = (ResourceManager) this.resourceManager;

            if (resourceManager.isOverflowAllowed()) {

                log.info("Setting flag to force overflow processing");

                // trigger overflow on the next group commit.
                writeService.forceOverflow.set(true);

            }

        } finally {

            clearLoggingContext();

        }
        
    }
    
    public long getOverflowCounter() throws IOException {
    
        setupLoggingContext();

        try {

            if (!(resourceManager instanceof ResourceManager)) {

                throw new UnsupportedOperationException();

            }

            return resourceManager.getOverflowCount();

        } finally {

            clearLoggingContext();

        }
        
    }
    
}
