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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
import com.bigdata.concurrent.LockManager;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.DropIndexTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ILocalTransactionManager;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.journal.RunState;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.journal.Tx;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.journal.ConcurrencyManager.IConcurrencyManagerCounters;
import com.bigdata.journal.JournalTransactionService.SinglePhaseCommit;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.OverflowManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StoreManager;
import com.bigdata.resources.IndexManager.IIndexManagerCounters;
import com.bigdata.resources.ResourceManager.IResourceManagerCounters;
import com.bigdata.resources.StoreManager.ManagedJournal;

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
 * @todo Startup should be broken into two aspects: local startup and service
 *       connect and disconnect events. For example, we on the tx service
 *       connect the store manager should notify the tx service of the last
 *       commit time on the live journal. On disconnect, the data service needs
 *       to go offline. The metadata service is required only for overflow
 *       processing, but if it remains down then we will eventually need to
 *       bring the data service offline when the buffered writes would cause the
 *       live journal to no longer be fully buffered as the overflow processing
 *       time will be increased if we need to read through to the disk during
 *       overflow.
 * 
 * @todo Write benchmark test to measure interhost transfer rates. Should be
 *       100Mbits/sec (~12M/sec) on a 100BaseT switched network. With full
 *       duplex in the network and the protocol, that rate should be
 *       bidirectional. Can that rate be sustained with a fully connected
 *       bi-directional transfer?
 * 
 * FIXME LockManager and FutureCompletionService
 * <p>
 * I have figured out the cause of the concurrency bottleneck. We can patch it
 * by increasing the thread pool size, but the cause is the lock manager which
 * requires a thread for a task to await a lock rather than being purely state
 * based. E.g., it is based on a queue of threads executing tasks awaiting
 * access to resources rather than a queue of tasks. I can refactor to fix this
 * issue, but I think that we can defer that for now. There is a similar thread
 * consumption issue arising in the RMI interface. Even through RMI is using NIO
 * for communications, each remote request is attached to a worker thread. So
 * each task submitted to a data service is immediately attached to a thread and
 * then to another thread if it needs to await a resource lock. The way to fix
 * this is to change the API to always return a Future for the operation
 * immediately (so we don't have as many threads awaiting RMI outcomes) and then
 * use a future completion service on the bigdata federation class that accepts
 * asynchronous outcomes for the submitted events. This will correspond nicely
 * to the Java ExecutorService and Future APIs, will simplify the API, will
 * consume fewer resources on the services, and will allow greater concurrency.
 * <p>
 * The following notes in fact describe very much the same problem:
 * <p>
 * RPC requests are currently made via RPC using JERI. While you can elect to
 * use the TCP/NIO server via configuration options (see
 * http://java.sun.com/products/jini/2.0.1/doc/api/net/jini/jeri/tcp/package-summary.html),
 * there will still be a thread allocated per concurrent RPC and no throttling
 * will be imposed by JERI.
 * <p>
 * The present design of the {@link IDataService} API requires that a server
 * thread be dedicated to each request against that interface - in this way it
 * exactly matches the RPC semantics supported by JERI. The underlying reason is
 * that the RPC calls are all translated into {@link Future}s when the are
 * submitted via {@link ConcurrencyManager#submit(AbstractTask)}. The
 * {@link DataService} itself then invokes {@link Future#get()} in order to
 * await the completion of the request and return the response (object or thrown
 * exception).
 * <p>
 * A re-design based on an asynchronous response from the server could remove
 * this requirement, thereby allowing a handful of server threads to handle a
 * large volume of concurrent client requests. The design would use asynchronous
 * callback to the client via JERI RPC calls to return results, indications that
 * the operation was complete, or exception information. A single worker thread
 * on the server could monitor the various futures and RPC clients when
 * responses become available or on request timeout. (This would be the
 * FutureCompletionService).
 * 
 * FIXME It is possible that a deadlock could arise due to the current design of
 * the {@link LockManager} as outlined above. The deadlock situation would occur
 * when a running task (A) needs to run an unisolated operation (B) on another
 * index partition on the same data service. If all threads in the write service
 * for the target data service are blocked awaiting access to a resource that is
 * being created by (A) then (A) will block since (B) will not be executed
 * because the thread pool is already saturated by tasks awaiting access to (A).
 * A variant across data services might also be possible.
 * <p>
 * This situation is only likely to arise for asynchronous overflow tasks since
 * they can introduce this kind of dependency during their atomic update phase.
 * The workaround is to increase the core thread pool size for the
 * {@link ConcurrencyManager}. The issue will be resolved by a re-design of the
 * {@link LockManager}.
 * 
 * FIXME Probably ALL of the methods {@link IDataService} should be subsumed
 * under {@link #submit(Callable)} or
 * {@link #submit(long, String, IIndexProcedure)} so they do not block on the
 * {@link DataService} and thereby absorb a thread.
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

    /**
     * Object manages the resources hosted by this {@link DataService}.
     */
    private ResourceManager resourceManager;

    /**
     * Object provides concurrency control for the named resources (indices).
     */
    private ConcurrencyManager concurrencyManager;

    /**
     * Object supports local transactions and does handshaking with the
     * {@link DistributedTransactionService}.
     */
    private DataServiceTransactionManager localTransactionManager;
    
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
     * Concrete implementation manages the local state of transactions executing
     * on a {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class DataServiceTransactionManager extends
            AbstractLocalTransactionManager {

        public ITransactionService getTransactionService() {

            return DataService.this.getFederation().getTransactionService();

        }

        /**
         * Exposed to {@link DataService#singlePhaseCommit(long)}
         */
        public void deactivateTx(final Tx localState) {

            super.deactivateTx(localState);

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

        if (isOpen()) {

            throw new IllegalStateException();

        }

        resourceManager = (ResourceManager) newResourceManager(properties);
       
        localTransactionManager = new DataServiceTransactionManager();
        
        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, resourceManager);

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
         * Note: This method limits the frequency of update to no more than once
         * per second.
         * <p>
         * Note: {@link OverflowManager#overflow()} is responsible for
         * reattaching the counters for the live {@link ManagedJournal} during
         * synchronous overflow.
         */
        synchronized public void reattachDynamicCounters() {

            final long now = System.currentTimeMillis();

            final long elapsed = now - lastReattachMillis;

            if (service.isOpen() && service.resourceManager.isRunning()
                    && elapsed > 1000/* ms */) {

                // The service's counter set hierarchy.
                final CounterSet serviceRoot = service.getFederation()
                        .getServiceCounterSet();

                // The lock manager
                {
                    
                    // the lock manager is a direct child of this node.
                    final CounterSet tmp = (CounterSet) serviceRoot
                            .getPath(IDataServiceCounters.concurrencyManager
                                    + ICounterSet.pathSeparator
                                    + IConcurrencyManagerCounters.writeService);

                    synchronized (tmp) {

                        /*
                         * Note: We detach and then attach since that wipes out
                         * any counter set nodes for queues which no longer
                         * exist. Otherwise they will build up forever.
                         */

                        // detach the old counters.
                        tmp.detach(IConcurrencyManagerCounters.LockManager);

                        // attach the the new counters.
                        ((CounterSet) tmp
                                .makePath(IConcurrencyManagerCounters.LockManager))
                                .attach(service.concurrencyManager
                                        .getWriteService().getLockManager()
                                        .getCounters());

                    }

                }
                
                // The live indices.
                {
                
                    /*
                     * The counters for the index manager within the service's
                     * counter hierarchy.
                     * 
                     * Note: The indices are a direct child of this node.
                     */
                    final CounterSet tmp = (CounterSet) serviceRoot
                            .getPath(IDataServiceCounters.resourceManager
                                    + ICounterSet.pathSeparator
                                    + IResourceManagerCounters.IndexManager);

                    synchronized (tmp) {

                        /*
                         * Note: We detach and then attach since that wipes out
                         * any counter set nodes for index partitions which no
                         * longer exist. Otherwise they will build up forever.
                         */
                        final boolean exists = tmp
                                .getPath(IIndexManagerCounters.Indices) != null;

                        // detach the index partition counters.
                        tmp.detach(IIndexManagerCounters.Indices);

                        // attach the current index partition counters.
                        ((CounterSet) tmp
                                .makePath(IIndexManagerCounters.Indices))
                                .attach(service.resourceManager
                                        .getIndexCounters());

                        if (INFO)
                            log
                                    .info("Attached index partition counters: preexisting="
                                            + exists
                                            + ", path="
                                            + tmp.getPath());

                    }

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

            logHttpdURL(service.getResourceManager().getDataDir());

        }

        /**
         * Sets up {@link DataService} specific counters.
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

    }
    
    /**
     * Polite shutdown does not accept new requests and will shutdown once the
     * existing requests have been processed.
     */
    synchronized public void shutdown() {

        if (!isOpen())
            return;

        if (concurrencyManager != null) {
            concurrencyManager.shutdown();
            concurrencyManager = null;
        }

        if (localTransactionManager != null) {
            localTransactionManager.shutdown();
            localTransactionManager = null;
        }

        if (resourceManager != null) {
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

        if (!isOpen())
            return;

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
         * The namespace for the counters pertaining to the {@link ILocalTransactionService}.
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
                 * transactions. The state for a read-only transaction is
                 * captured by its transaction identifier and by state on the
                 * transaction service, which maintains a read lock.
                 * 
                 * Note: Thrown exception since this method will not be invoked
                 * by the txService for a read-only tx.
                 */

                throw new IllegalArgumentException();
                
            }
            
            final Tx localState = (Tx) getLocalTransactionManager().getTx(tx);

            if (localState == null) {

                /*
                 * This is not an active transaction.
                 */

                throw new IllegalStateException();

            }
            
            /*
             * Note: This code is shared (copy-by-value) by the
             * JournalTransactionService commitImpl(...)
             */
            final ManagedJournal journal = getResourceManager().getLiveJournal();
            
            {

                /*
                 * A transaction with an empty write set can commit immediately
                 * since validation and commit are basically NOPs (this is the same
                 * as the read-only case.)
                 * 
                 * Note: We lock out other operations on this tx so that this
                 * decision will be atomic.
                 */

                localState.lock.lock();

                try {

                    if (localState.isEmptyWriteSet()) {

                        /*
                         * Sort of a NOP commit. 
                         */
                        
                        localState.setRunState(RunState.Committed);

                        ((DataServiceTransactionManager) journal
                                .getLocalTransactionManager())
                                .deactivateTx(localState);
                        
//                        state.setRunState(RunState.Committed);
                        
                        return 0L;

                    }

                } finally {

                    localState.lock.unlock();

                }

            }

            final IConcurrencyManager concurrencyManager = /*journal.*/getConcurrencyManager();

            final AbstractTask<Void> task = new SinglePhaseCommit(
                    concurrencyManager, journal.getLocalTransactionManager(),
                    localState);

            try {
                
                /*
                 * FIXME This is not working yet. If we submit directly to the
                 * concurrency manager, then there is a ClassCastException on
                 * the DirtyListener. If we submit directly to the WriteService
                 * then the task does not hold its locks. None of these options
                 * work. The write service really needs a refactor (to be state
                 * based rather like the new lock service) before I finish the
                 * distributed commit protocol.
                 */
                // submit and wait for the result.
                concurrencyManager
                .submit(task).get();
//                .getWriteService().submit(task).get();
//                .getWriteService().getLockManager().submit(task.getResource(), task).get();

                /*
                 * FIXME The state changes for the local tx should be atomic across
                 * this operation. In order to do that we have to make those changes
                 * inside of SinglePhaseTask while it is holding the lock, but after
                 * it has committed. Perhaps the best way to do this is with a pre-
                 * and post- call() API since we can not hold the lock across the
                 * task otherwise (it will deadlock).
                 */

                localState.lock.lock();
                
                try {
                
                    localState.setRunState(RunState.Committed);

                    ((DataServiceTransactionManager) journal
                            .getLocalTransactionManager())
                            .deactivateTx(localState);
                
//                    state.setRunState(RunState.Committed);

                } finally {
                    
                    localState.lock.unlock();
                    
                }

            } catch (Throwable t) {

//                log.error(t.getMessage(), t);

                localState.lock.lock();

                try {

                    localState.setRunState(RunState.Aborted);

                    ((DataServiceTransactionManager) journal
                            .getLocalTransactionManager())
                            .deactivateTx(localState);

//                    state.setRunState(RunState.Aborted);

                    throw new RuntimeException(t);
                    
                } finally {
                    
                    localState.lock.unlock();

                }

            }

            /*
             * Note: This is returning the commitTime set on the task when it was
             * committed as part of a group commit.
             */
            
//            log.warn("\n" + state + "\n" + localState);

            return task.getCommitTime();

        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    public void prepare(final long tx, final long revisionTime)
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
                 * 
                 * Note: Thrown exception since this method will not be invoked
                 * by the txService for a read-only tx.
                 */
                
                throw new IllegalArgumentException();
                
            }
            
            final Tx state = (Tx) getLocalTransactionManager().getTx(tx);

            if (state == null) {

                /*
                 * This is not an active transaction.
                 */

                throw new IllegalStateException();

            }
            
            /*
             * Submit the task and await its future
             */

            concurrencyManager.submit(
                    new DistributedCommitTask(concurrencyManager,
                            resourceManager, getServiceUUID(), state,
                            revisionTime)).get();

            // Done.
            
        } finally {

            clearLoggingContext();

        }

    }

    /**
     * Task handling the distributed commit protocol for the
     * {@link IDataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class DistributedCommitTask extends AbstractTask<Void> {

        // ctor arg.
        private final ResourceManager resourceManager;
        private UUID dataServiceUUID;
        private final Tx state;
        private final long revisionTime;
        
        // derived.
        private final long tx;
        
        /**
         * @param concurrencyManager
         * @param resourceManager
         * @param dataServiceUUID
         * @param localState
         * @param revisionTime
         */
        public DistributedCommitTask(
                final ConcurrencyManager concurrencyManager,//
                final ResourceManager resourceManager,//
                final UUID dataServiceUUID,//
                final Tx localState,//
                final long revisionTime//
        ) {

            super(concurrencyManager, ITx.UNISOLATED, localState
                    .getDirtyResource());

            if (resourceManager == null)
                throw new IllegalArgumentException();

            if (localState == null)
                throw new IllegalArgumentException();
            
            if (revisionTime == 0L)
                throw new IllegalArgumentException();
            
            if (revisionTime <= localState.getStartTimestamp())
                throw new IllegalArgumentException();

            this.resourceManager = resourceManager;

            this.dataServiceUUID = dataServiceUUID;

            this.state = localState;

            this.revisionTime = revisionTime;

            this.tx = localState.getStartTimestamp();

        }

        /**
         * FIXME Finish, write tests and debug.
         */
        @Override
        protected Void doTask() throws Exception {

            final ITransactionService txService = resourceManager
                    .getLiveJournal().getLocalTransactionManager()
                    .getTransactionService();

            prepare();

            final long commitTime = txService.prepared(tx, dataServiceUUID);

            // obtain the exclusive write lock on journal.
            lockJournal();
            try {

                // Commit using the specified commit time.
                commit(commitTime);

                boolean success = false;
                try {

                    /*
                     * Wait until the entire distributed transaction is
                     * committed.
                     */
                    success = txService.committed(tx, dataServiceUUID);

                } finally {

                    if (!success) {

                        // Rollback the journal.
                        rollback();

                    }

                }
                
            } finally {

                // release the exclusive write lock on journal.
                unlockJournal();

            }

            return null;

        }

        /**
         * Prepare the transaction (validate and merge down onto the unisolated
         * indices and then checkpoints those indices).
         * <p>
         * Note: This presumes that we are already holding exclusive write locks
         * on the named indices such that the pre-conditions for validation and
         * its post-conditions can not change until we either commit or discard
         * the transaction.
         * <p>
         * Note: The indices need to be isolated as by {@link AbstractTask} or
         * they will be enrolled onto {@link Name2Addr}'s commitList when they
         * become dirty and then checkpointed and included with the NEXT commit.
         * <p>
         * For this reason, the {@link DistributedCommitTask} is an UNISOLATED
         * task so that we can reuse the existing mechanisms as much as
         * possible.
         * 
         * FIXME This will work if we can grab the write service lock from
         * within the task (which will mean changing that code to allow the lock
         * with the caller only still running or simply waiting until we are
         * signaled by the txService that all participants are either go
         * (continue execution and will commit at the next group commit, but
         * then we need a protocol to impose the correct commit time, e.g., by
         * passing it on the task and ensuring that there is no other tx ready
         * in the commit group) or abort (just throw an exception).
         */
        protected void prepare() {
            
            state.prepare(revisionTime);
            
        }

        /**
         * Obtain the exclusive lock on the write service. This will prevent any
         * other tasks using the concurrency API from writing on the journal.
         */
        protected void lockJournal() {

            throw new UnsupportedOperationException();
            
        }
        
        protected void unlockJournal() {
            
            throw new UnsupportedOperationException();
            
        }
        
        /**
         * Commit the transaction using the specified <i>commitTime</i>.
         * <p>
         * Note: There are no persistent side-effects unless this method returns
         * successfully.
         * 
         * @param commitTime
         *            The commit time that must be used.
         */
        protected void commit(final long commitTime) {

            /*
             * @todo enroll the named indices onto Name2Addr's commitList (this
             * basically requires breaking the isolation imposed by the
             * AbstractTask).
             */

            if (true)
                throw new UnsupportedOperationException();

            final ManagedJournal journal = resourceManager.getLiveJournal();
            
            // atomic commit.
            journal.commitNow(commitTime);

        }
        
        /**
         * Discard the last commit, restoring the journal to the previous commit
         * point.
         */
        protected void rollback() {
            
            final ManagedJournal journal = resourceManager.getLiveJournal();
            
            journal.rollback();
            
        }
        
    }

    public void abort(final long tx) throws IOException {

        setupLoggingContext();

        try {

            final Tx localState = (Tx) getLocalTransactionManager().getTx(tx);

            if (localState == null)
                throw new IllegalArgumentException();

            localState.lock.lock();

            try {

                localState.setRunState(RunState.Aborted);

            } finally {

                localState.lock.unlock();

            }
            
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
     * <p>
     * Note: When the {@link DataService} is accessed via RMI the {@link Future}
     * MUST be a proxy. This gets handled by the concrete server implementation.
     */
    public Future submit(final long tx, final String name,
            final IIndexProcedure proc) {

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
            return concurrencyManager.submit(task);
        
        } finally {
            
            clearLoggingContext();
            
        }

    }

    /**
     * Note: When the {@link DataService} is accessed via RMI the {@link Future}
     * MUST be a proxy. This gets handled by the concrete server implementation.
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
     * 
     * @todo we should probably put the federation object in a sandbox in order
     *       to prevent various operations by tasks running in the
     *       {@link DataService} using the {@link IDataServiceAwareProcedure}
     *       interface to gain access to the {@link DataService}'s federation.
     *       for example, if they use {@link AbstractFederation#shutdownNow()}
     *       then the {@link DataService} itself would be shutdown.
     */
    public Future<? extends Object> submit(final Callable<? extends Object> task) {

        setupLoggingContext();

        try {

            if (task == null)
                throw new IllegalArgumentException();

            /*
             * Submit to the ExecutorService for the DataService's federation
             * object. This is used for tasks which are not associated with a
             * timestamp and hence not linked to any specific view of the named
             * indices.
             */

            if (task instanceof IDataServiceAwareProcedure) {

                if (log.isInfoEnabled()) {

                    log.info("Data service aware procedure: "
                            + task.getClass().getName());

                }

                // set the data service on the task.
                ((IDataServiceAwareProcedure) task).setDataService(this);

            }

            // submit the task and return its Future.
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
     * Overflow processing API 
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

    public long getAsynchronousOverflowCounter() throws IOException {

        setupLoggingContext();

        try {

            if (!(resourceManager instanceof ResourceManager)) {

                throw new UnsupportedOperationException();

            }

            return resourceManager.getAsynchronousOverflowCount();

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
