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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.Banner;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.AbstractTx;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.DropIndexTask;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ILocalTransactionManager;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.journal.JournalTransactionService.SinglePhaseCommit;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StoreManager;
import com.bigdata.resources.StoreManager.ManagedJournal;
import com.bigdata.util.concurrent.DaemonThreadFactory;

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
    protected IResourceManager newResourceManager(final Properties properties) {

        return new ResourceManager(properties) {

            public IBigdataFederation getFederation() {
                
                return DataService.this.getFederation();
                                
            }
            
            public DataService getDataService() {
                
                return DataService.this;
                
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
     * The dynamic property set associated with the data service instance.
     */
    private final Session session = new Session();
    
    /**
     * The dynamic property set (aka session) associated with the
     * {@link DataService} instance. The state of the {@link Session} is NOT
     * persistent.
     * <p>
     * <strong>This is an experimental feature</strong>
     * <p>
     * Note: These {@link Session} properties are transient and local to a
     * specific {@link DataService} instance. if failover support is desired,
     * then you should probably use the {@link IResourceLockService} so that the
     * updates can be atomic across the replicated instances of the data
     * service.
     */
    public Session getSession() {

        return session;
        
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
     * Invoked periodically to clear stale entries from a variety of LRU caches.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class ClearStaleCacheEntries implements Runnable {

        public void run() {

            if (!resourceManager.isRunning()) {

                log.warn("Halting task : resource manager is not running.");
                
                throw new RuntimeException();
                
            }
            
            resourceManager.clearStaleCacheEntries();
            
        }
        
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

        /*
         * Schedule tasks that will clear stale references from the index cache,
         * the index segment cache, and the store cache. This ensures that the
         * LRU references in these caches will become weakly reachable after a
         * timeout even in the event that there are no touched on the cache.
         * 
         * @todo config params for initialDelay and delay.
         * 
         * @todo do we need to do this for the Journal as well? Probably else
         * these indices will still be strongly references. Also the resource
         * locator cache, etc. All instances of ConcurrentWeakReferenceCache.
         * 
         * @todo one consequence of this is that you can shutdown heavily
         * buffered indices, which you might not want to do.  In that case
         * the delay should be ZERO and the task should not be run.
         */
        {

            final long initialDelay = 5000;
            
            final long delay = 5000;
            
            /*
             * Note: The task will self-cancel by throwing an exception once the
             * resource manager is no longer running.
             */

            getFederation().addScheduledTask(new ClearStaleCacheEntries(),
                    initialDelay, delay, TimeUnit.MILLISECONDS);
            
        }
        
        localTransactionManager = new AbstractLocalTransactionManager() {

            public ITransactionService getTransactionService() {
                
                return DataService.this.getFederation().getTransactionService();
                
            }
                        
        };

        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, resourceManager);

//        localTransactionManager.setConcurrencyManager(concurrencyManager);

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
     * {@link AbstractTransactionService} and the {@link ConcurrencyManager}, dynamic
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

            serviceRoot.makePath(IDataServiceCounters.transactionService)
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
     * reported by the {@link DataService} and the various services which it
     * uses.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IDataServiceCounters extends
            ConcurrencyManager.IConcurrencyManagerCounters,
//            ...TransactionManager.XXXCounters,
            ResourceManager.IResourceManagerCounters
            {
       
        /**
         * The namespace for the counters pertaining to the {@link ConcurrencyManager}.
         */
        String concurrencyManager = "Concurrency Manager";

        /**
         * The namespace for the counters pertaining to the {@link ITransactionService}.
         */
        String transactionService = "Transaction Manager";
        
        /**
         * The namespace for the counters pertaining to the {@link ResourceManager}.
         */
        String resourceManager = "Resource Manager";
        
    }
        
    /*
     * ITxCommitProtocol.
     * 
     * FIXME The data service MUST notify the transaction service when a task
     * writes on an index partition isolated by a transactions on the data
     * service. The local state of the transaction should include the fact that
     * notice was generated for the data service so we do not reissue notices
     * for each write operation. The notice itself can be sent by the ITx object
     * when it isolates an index for the tx for the first time (or better yet,
     * when it observes that there has been a write on an isolated index by a
     * task - so maybe in AbstractTask), but it will need to be able to access
     * the data service's UUID in order to send that notice.
     */
    
    public void setReleaseTime(final long releaseTime) {
        
        setupLoggingContext();
        
        try {
            
            getResourceManager().setReleaseTime(releaseTime);
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    /**
     * Note: This is basically identical to the standalone journal case.
     * 
     * @see JournalTransactionService#commitImpl(long)}.
     */
    public long singlePhaseCommit(final long tx) throws ExecutionException,
            InterruptedException, IOException {
        
        setupLoggingContext();
        
        try {

            if(TimestampUtility.isReadOnly(tx)) {
                
                /*
                 * A read-only transaction.
                 * 
                 * Note: We do not maintain state on the client for read-only
                 * transactions. The state for a read-only transaction is captured
                 * by its transaction identifier and by state on the transaction
                 * service, which maintains a read lock.
                 */
                
                throw new IllegalArgumentException();
                
            }
            
            final AbstractTx state = (AbstractTx) getLocalTransactionManager()
                    .getTx(tx);

            if (state == null) {

                /*
                 * This is not an active transaction.
                 */

                throw new IllegalStateException();

            }

            final AbstractTask task = new SinglePhaseCommit(
                    getConcurrencyManager(), getLocalTransactionManager(),
                    state);

            // submit commit task and await its future.
            getConcurrencyManager().submit(task).get();

            /*
             * Note: The assigned commit time is on the AbstractTask itself.
             */
            return task.getCommitTime();
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    public Future<Void> twoPhasePrepare(final long tx, final long revisionTime)
            throws ExecutionException, InterruptedException, IOException {
        
        setupLoggingContext();
        
        try {

            if(TimestampUtility.isReadOnly(tx)) {
                
                /*
                 * A read-only transaction.
                 * 
                 * Note: We do not maintain state on the client for read-only
                 * transactions. The state for a read-only transaction is captured
                 * by its transaction identifier and by state on the transaction
                 * service, which maintains a read lock.
                 */
                
                throw new IllegalArgumentException();
                
            }
            
            final AbstractTx state = (AbstractTx) getLocalTransactionManager()
                    .getTx(tx);

            if (state == null) {

                /*
                 * This is not an active transaction.
                 */

                throw new IllegalStateException();

            }
            
            /*
             * Submit commit task and _return_ its Future.
             * 
             * Note: The caller can use the Future to cancel the 2-phase commit.
             */

            return commitService
                    .submit(new TwoPhaseCommit(state, revisionTime));

        } finally {

            clearLoggingContext();

        }

    }

    /*
     * FIXME shutdown with the dataservice!
     */
    private ExecutorService commitService = Executors
            .newSingleThreadExecutor(new DaemonThreadFactory(getServiceName()
                    + "-commitService"));
    
    public void twoPhaseCommit(final long tx, final long commitTime)
            throws ExecutionException, InterruptedException, IOException {

        setupLoggingContext();

        try {
            
            if (commitTime <= getResourceManager().getLiveJournal()
                    .getLastCommitTime()) {

                /*
                 * The commit times must strictly advance.
                 */
                
                throw new IllegalArgumentException();
                
            }

            if(TimestampUtility.isReadOnly(tx)) {
                
                /*
                 * A read-only transaction.
                 * 
                 * Note: We do not maintain state on the client for read-only
                 * transactions. The state for a read-only transaction is captured
                 * by its transaction identifier and by state on the transaction
                 * service, which maintains a read lock.
                 */
                
                throw new IllegalArgumentException();
                
            }
            
            final AbstractTx state = (AbstractTx) getLocalTransactionManager()
                    .getTx(tx);

            if (state == null) {

                /*
                 * This is not an active transaction.
                 */

                throw new IllegalStateException();

            }

            final TwoPhaseCommit task = commitTasks.get(state);
            
            if (task == null) {

                /*
                 * There is no commit task for that transaction.
                 */

                throw new IllegalStateException();
                
            }
            
            if (!state.isPrepared()) {
                
                /*
                 * @todo We might want to be holding [tx.lock] here, in which
                 * case the [task] and the [tx] should probably use the same
                 * lock object -- [tx.lock].
                 */
                
                throw new IllegalStateException();
                
            }
            
            task.lock.lock();

            try {

                // set the commitTime to be used for the tx.
                task.commitTime = commitTime;

                /*
                 * Signal the [ready] condition on the task so that it will
                 * commit the journal and release the write lock.
                 */

                task.ready.signal();

                /*
                 * Wait until [done] is signaled.
                 */
                
                task.done.await();
                
            } finally {
                
                task.lock.unlock();

            }
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    /**
     * Map containing {@link TwoPhaseCommit} tasks for transactions that have
     * been issued a {@link #twoPhasePrepare(long, long)} message but not yet
     * either cancelled (via their {@link Future}) or successfully committed.
     */
    private final ConcurrentHashMap<ITx, TwoPhaseCommit> commitTasks = new ConcurrentHashMap<ITx, TwoPhaseCommit>();

    /**
     * A 2-phase commit protocol used when the transaction write set is
     * distributed across more than one {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class TwoPhaseCommit implements Callable<Void> {

        /**
         * The local state for the transaction.
         */
        protected final AbstractTx state;

        /**
         * The revision time that will be used for {@link ITuple}s when the
         * write set of the transaction is merged down onto the unisolated
         * indices.
         */
        protected final long revisionTime;

        /**
         * Lock used to coordinate access to the {@link #ready} and
         * {@link #done} signals.
         */
        protected final ReentrantLock lock;

        /**
         * Volatile flag is set if this task is aborted. You MUST hold the
         * {@link #lock} to inspect the state of this flag so that decisions
         * made on its state will be atomic.
         */
//        protected volatile boolean aborted = false;
        
        /**
         * Condition is signalled when all participants in the distributed
         * commit protocol are ready.
         * <p>
         * Note that the {@link #commitTime} MUST be set before you signal this
         * {@link Condition}.
         */
        protected final Condition ready;

        /**
         * Once this task receives the {@link #ready} signal it will begin the
         * atomic commit phase and it will send a {@link #done} signal when the
         * commit is done.
         * <p>
         * Note: This {@link Condition} is used to await the completion of the
         * 2-phase commit protocol on this {@link DataService} before returning
         * to the caller. However, {@link #done} WILL NOT be signalled unless
         * this task has noticed the {@link #ready} signal.
         */
        protected final Condition done;

        /**
         * The commit time assigned by the {@link ITransactionService}.
         */
        protected long commitTime = 0L;

        /**
         * @param state
         * @param revisionTime
         */
        protected TwoPhaseCommit(final AbstractTx state, final long revisionTime) {

            if (state == null)
                throw new IllegalArgumentException();

            this.state = state;

            this.revisionTime = revisionTime;

            this.lock = new ReentrantLock(); // @todo ? vs state.lock;

            this.ready = lock.newCondition();
            
            this.done = lock.newCondition();
            
        }

        public Void call() throws Exception {

            if (!state.lock.tryLock()
                    && !state.lock.tryLock(10L, TimeUnit.MILLISECONDS)) {

                /*
                 * This idiom tries barging in and then waits for a short moment
                 * before deciding that the transaction has ongoing work on this
                 * data service. It then aborts the commit protocol so that we
                 * don't wind up deadlocked while holding the exclusive lock on
                 * the write service.
                 */

                throw new RuntimeException("Transaction is still being used?");

            }
            
            try {
                
                lock.lock();

                try {

                    // add to the set of running commits.
                    commitTasks.put(state, this);

                    final WriteExecutorService writeService = getConcurrencyManager()
                            .getWriteService();

                    /*
                     * Wait for the exclusive lock on the write service.
                     * 
                     * Note: There WILL NOT be any UNISOLATED tasks running on
                     * this data service while we prepare, mergeDown or wait for
                     * the signal to commit the transaction!
                     * 
                     * Note: Read tasks are not blocked by this protocol.
                     */

                    writeService.tryLock(Long.MAX_VALUE, TimeUnit.SECONDS);

                    try {

                        /*
                         * Prepare the transaction (validate and merge down onto
                         * the unisolated indices and then checkpoints those
                         * indices).
                         */
                        state.prepare(revisionTime);

                        /*
                         * Note: Since we are holding the exclusive write lock
                         * it is not possible for the live journal to overflow
                         * and therefore this reference will remain valid until
                         * we release that lock.
                         */
                        final ManagedJournal liveJournal = resourceManager
                                .getLiveJournal();

                        ensureMinFree(liveJournal);

                        /*
                         * Wait until signaled -or- interrupted.
                         * 
                         * Note: throws InterruptedException.
                         */
                        ready.await();

                        /*
                         * Once we receive the [ready] signal we do the atomic
                         * commit and we MUST signal [done] regardless of the
                         * outcome of that commit.
                         * 
                         * Note: Any failure at this point will result in a bad
                         * commit for the transaction as some data services may
                         * succeed while at least this one will fail.
                         */
                        try {

                            if (commitTime == 0) {

                                /*
                                 * The commitTime must be set before you signal
                                 * [ready].
                                 */

                                throw new AssertionError();

                            }

                            // commit using the caller's commit time.
                            liveJournal.commitNow(commitTime);

                            return null;

                        } finally {

                            done.signal();

                        }

                    } catch (Throwable t) {

//                        aborted = true;
                        
                        state.abort();

                        throw new RuntimeException(t);

                    } finally {

                        // release the exclusive lock on the write service.
                        writeService.unlock();

                    }

                } finally {

                    // remove from the set of running commits.
                    commitTasks.remove(state);

                    lock.unlock();

                }

            } finally {

                state.lock.unlock();

            }

        }
        
        /**
         * Make sure that the live journal is not completely full so that we can
         * avoid the possibility of a "disk full" error.
         * <p>
         * Note: You need an exclusive write lock on the journal to extend it,
         * but we have one.
         */
        protected void ensureMinFree(AbstractJournal liveJournal) {
        
            final IBufferStrategy buf = liveJournal.getBufferStrategy();

            final long remaining = buf.getUserExtent() - buf.getNextOffset();

            final long MIN_FREE = Bytes.kilobyte * 10;

            if (remaining < MIN_FREE) {

                buf.truncate(buf.getExtent() + MIN_FREE);

            }

        }
        
    }

    public void abort(long tx) throws IOException {

        setupLoggingContext();

        try {

            final ITx state = getLocalTransactionManager().getTx(tx);

            if (state == null)
                throw new IllegalArgumentException();
            
            state.abort();
            
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
            final long timestamp = (tx == ITx.UNISOLATED
                        && proc.isReadOnly() ? ITx.READ_COMMITTED
                        : tx);

            // wrap the caller's task.
            final AbstractTask task = new IndexProcedureTask(
                    concurrencyManager, timestamp, name, proc);
            
            if(proc instanceof IDataServiceAwareProcedure) {

                if(INFO) {
                    
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
                    concurrencyManager, timestamp, name, fromKey, toKey,
                    capacity, flags, filter);

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
     * overflow processing API 
     */

    public void forceOverflow(final boolean immediate,
            final boolean compactingMerge) throws IOException,
            InterruptedException, ExecutionException {
    
        setupLoggingContext();

        try {

            if (!(resourceManager instanceof ResourceManager)) {

                throw new UnsupportedOperationException();

            }
            
            final Callable task = new ForceOverflowTask(compactingMerge);
            
            log.warn("Will force overflow: immediate=" + immediate
                    + ", compactingMerge=" + compactingMerge);
            
            if (immediate) {

                /*
                 * Run the task on the write service. The task writes a small
                 * record on the journal in order to make sure that it is dirty
                 * and then sets the flag to force overflow with the next
                 * commit. Since the task runs on the write service and since
                 * the journal is dirty, a group commit will occur and
                 * synchronous overflow processing will occur before this method
                 * returns.
                 * 
                 * Note: the resource itself is arbitrary - there is no index
                 * by that name.
                 */

                getConcurrencyManager().submit(
                        new AbstractTask(getConcurrencyManager(),
                                ITx.UNISOLATED,
                                new String[] { "__forceOverflow" }) {

                    @Override
                    protected Object doTask() throws Exception {

                        // write a one byte record on the journal.
                        getJournal().write(ByteBuffer.wrap(new byte[]{1}));
                        
                        // run task that will set the overflow flag.
                        return task.call();
                        
                    }
                    
                }).get();
                
            } else {

                /*
                 * Provoke overflow with the next group commit. All this does is
                 * set the flag that will cause overflow to occur with the next
                 * group commit. Since the task does not run on the write
                 * service it will return immediately.
                 */
                
                try {

                    task.call();
                    
                } catch (Exception e) {
                    
                    throw new RuntimeException(e);
                    
                }

            }

        } finally {

            clearLoggingContext();

        }
        
    }

    public boolean purgeOldResources(final long timeout,
            final boolean truncateJournal) throws InterruptedException {

        // delegate all the work.
        return getResourceManager().purgeOldResources(timeout, truncateJournal);
        
    }
    
    /**
     * Task sets the flag that will cause overflow processing to be triggered on
     * the next group commit.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class ForceOverflowTask implements Callable<Void> {

        private final boolean compactingMerge;
        
        public ForceOverflowTask(final boolean compactingMerge) {
            
            this.compactingMerge = compactingMerge;
            
        }
        
        public Void call() throws Exception {

            final WriteExecutorService writeService = concurrencyManager
                    .getWriteService();

            final ResourceManager resourceManager = (ResourceManager) DataService.this.resourceManager;

            if (resourceManager.isOverflowAllowed()) {

                if (compactingMerge) {

                    resourceManager.compactingMerge.set(true);

                }

                // trigger overflow on the next group commit.
                writeService.forceOverflow.set(true);

            }

            return null;

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
    
    public boolean isOverflowActive() throws IOException {
        
        setupLoggingContext();

        try {

            if (!(resourceManager instanceof ResourceManager)) {

                throw new UnsupportedOperationException();

            }

            /*
             * overflow processing is enabled but not allowed, which means that
             * overflow processing is occurring right now.
             */
            return resourceManager.isOverflowEnabled()
                    && !resourceManager.isOverflowAllowed();

        } finally {

            clearLoggingContext();

        }
        
    }
    
}
