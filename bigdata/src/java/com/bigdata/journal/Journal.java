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
package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bfs.GlobalFileSystemHelper;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.ReadCommittedView;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.config.IntegerValidator;
import com.bigdata.config.LongValidator;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.quorum.Quorum;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StaleLocatorReason;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.LoadBalancerService;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.LatchedExecutor;
import com.bigdata.util.concurrent.ShutdownHelper;
import com.bigdata.util.concurrent.TaskCounters;
import com.bigdata.util.concurrent.ThreadPoolExecutorStatisticsTask;
import com.bigdata.util.httpd.AbstractHTTPD;

/**
 * Concrete implementation suitable for a local and unpartitioned database.
 * <p>
 * Note: This implementation does NOT not support partitioned indices. Because
 * all data must reside on a single journal resource there is no point to a
 * view. Views are designed to have data on a mixture of the live journal, one
 * or more historical journals, and one or more {@link IndexSegment}s.
 * 
 * @see ResourceManager, which supports views.
 */
public class Journal extends AbstractJournal implements IConcurrencyManager,
        /*ILocalTransactionManager,*/ IResourceManager {

    /**
     * Object used to manage local transactions. 
     */
    private final AbstractLocalTransactionManager localTransactionManager; 

    /**
     * Object used to manage tasks executing against named indices.
     */
    private final ConcurrencyManager concurrencyManager;

    /**
     * Options understood by the {@link Journal}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends com.bigdata.journal.Options,
            com.bigdata.journal.ConcurrencyManager.Options,
            com.bigdata.journal.TemporaryStoreFactory.Options {

        /**
         * The capacity of the {@link HardReferenceQueue} backing the
         * {@link IResourceLocator} maintained by the {@link Journal}. The
         * capacity of this cache indirectly controls how many
         * {@link ILocatableResource}s the {@link Journal} will hold open.
         * <p>
         * The effect of this parameter is indirect owning to the semantics of
         * weak references and the control of the JVM over when they are
         * cleared. Once an {@link ILocatableResource} becomes weakly reachable,
         * the JVM will eventually GC the object. Since objects which are
         * strongly reachable are never cleared, this provides our guarantee
         * that resources are never closed if they are in use.
         * 
         * @see #DEFAULT_LOCATOR_CACHE_CAPACITY
         */
        String LOCATOR_CACHE_CAPACITY = Journal.class.getName()
                + ".locatorCacheCapacity";

        String DEFAULT_LOCATOR_CACHE_CAPACITY = "20";
        
        /**
         * The timeout in milliseconds for stale entries in the
         * {@link IResourceLocator} cache -or- ZERO (0) to disable the timeout
         * (default {@value #DEFAULT_LOCATOR_CACHE_TIMEOUT}). When this timeout
         * expires, the reference for the entry in the backing
         * {@link HardReferenceQueue} will be cleared. Note that the entry will
         * remain in the {@link IResourceLocator} cache regardless as long as it
         * is strongly reachable.
         */
        String LOCATOR_CACHE_TIMEOUT = Journal.class.getName()
                + ".locatorCacheTimeout";

        String DEFAULT_LOCATOR_CACHE_TIMEOUT = "" + (60 * 1000);

        /**
         * The #of threads that will be used to read on the local disk.
         * 
         * @see Journal#getReadExecutor()
         */
        String READ_POOL_SIZE = Journal.class.getName() + ".readPoolSize";

        String DEFAULT_READ_POOL_SIZE = "0";
        
        /*
         * Performance counters options.
         */
        
        /**
         * Boolean option for the collection of statistics from the underlying
         * operating system (default
         * {@value #DEFAULT_COLLECT_PLATFORM_STATISTICS}).
         * 
         * @see AbstractStatisticsCollector#newInstance(Properties)
         */
        String COLLECT_PLATFORM_STATISTICS = Journal.class.getName()
                + ".collectPlatformStatistics";

        String DEFAULT_COLLECT_PLATFORM_STATISTICS = "false"; 

        /**
         * Boolean option for the collection of statistics from the various
         * queues using to run tasks (default
         * {@link #DEFAULT_COLLECT_QUEUE_STATISTICS}).
         * 
         * @see ThreadPoolExecutorStatisticsTask
         */
        String COLLECT_QUEUE_STATISTICS = Journal.class.getName()
                + ".collectQueueStatistics";

        String DEFAULT_COLLECT_QUEUE_STATISTICS = "false";

        /**
         * Integer option specifies the port on which an httpd service will be
         * started that exposes the {@link CounterSet} for the client (default
         * {@value #DEFAULT_HTTPD_PORT}). When ZERO (0), a random port will be
         * used. The httpd service may be disabled by specifying <code>-1</code>
         * as the port.
         * <p>
         * Note: The httpd service for the {@link LoadBalancerService} is
         * normally run on a known port in order to make it easy to locate that
         * service, e.g., port 80, 8000 or 8080, etc. This MUST be overridden for
         * the {@link LoadBalancerService} it its configuration since
         * {@link #DEFAULT_HTTPD_PORT} will otherwise cause a random port to be
         * assigned.
         */
        String HTTPD_PORT = Journal.class.getName() + ".httpdPort";

        /**
         * The default http service port is <code>-1</code>, which means
         * performance counter reporting is disabled by default.
         */
        String DEFAULT_HTTPD_PORT = "-1";
        
    }
    
    /**
     * Create or re-open a journal.
     * 
     * @param properties
     *            See {@link com.bigdata.journal.Options}.
     */
    public Journal(final Properties properties) {
        
        this(properties, null/* quorum */);
    
    }

    public Journal(final Properties properties,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum) {

        super(properties, quorum);

        tempStoreFactory = new TemporaryStoreFactory(properties);
        
        executorService = (ThreadPoolExecutor) Executors
                .newCachedThreadPool(new DaemonThreadFactory(getClass()
                        .getName()
                        + ".executorService"));

        if (Boolean.valueOf(properties.getProperty(
                Options.COLLECT_QUEUE_STATISTICS,
                Options.DEFAULT_COLLECT_QUEUE_STATISTICS))) {
            
            scheduledExecutorService = Executors
                    .newSingleThreadScheduledExecutor(new DaemonThreadFactory(
                            getClass().getName() + ".sampleService"));
            
        } else {
         
            scheduledExecutorService = null;
            
        }
        
        {
            
            final int readPoolSize = Integer.valueOf(properties.getProperty(
                    Options.READ_POOL_SIZE, Options.DEFAULT_READ_POOL_SIZE));
            
            if (readPoolSize > 0) {

                readService = new LatchedExecutor(executorService,
                        readPoolSize);

            } else {

                readService = null;
                
            }

        }

        {

            final int cacheCapacity = getProperty(
                    Options.LOCATOR_CACHE_CAPACITY,
                    Options.DEFAULT_LOCATOR_CACHE_CAPACITY,
                    IntegerValidator.GT_ZERO);

            final long cacheTimeout = getProperty(
                    Options.LOCATOR_CACHE_TIMEOUT,
                    Options.DEFAULT_LOCATOR_CACHE_TIMEOUT,
                    LongValidator.GTE_ZERO);

            resourceLocator = new DefaultResourceLocator(this, null/*delegate*/,
                    cacheCapacity, cacheTimeout);
            
        }

        resourceLockManager = new ResourceLockService();

        localTransactionManager = newLocalTransactionManager();

        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, this);
        
        getExecutorService().execute(new StartDeferredTasksTask());
        
    }

    protected AbstractLocalTransactionManager newLocalTransactionManager() {

        final JournalTransactionService abstractTransactionService = new JournalTransactionService(
                properties, this) {

            {
                
                final long lastCommitTime = Journal.this.getLastCommitTime();
                
                if (lastCommitTime != 0L) {

                    /*
                     * Notify the transaction service on startup so it can set
                     * the effective release time based on the last commit time
                     * for the store.
                     */
                    updateReleaseTimeForBareCommit(lastCommitTime);
                    
                }
                
            }
            
            protected void activateTx(final TxState state) {
                final IBufferStrategy bufferStrategy = Journal.this.getBufferStrategy();
                if(bufferStrategy instanceof RWStrategy) {
                    ((RWStrategy)bufferStrategy).getRWStore().activateTx();
                }
                super.activateTx(state);
            }

            protected void deactivateTx(final TxState state) {
                super.deactivateTx(state);
                final IBufferStrategy bufferStrategy = Journal.this.getBufferStrategy();
                if(bufferStrategy instanceof RWStrategy) {
                    ((RWStrategy)bufferStrategy).getRWStore().deactivateTx();
                }
            }
            
        }.start();

        return new AbstractLocalTransactionManager() {

            public AbstractTransactionService getTransactionService() {
                
                return abstractTransactionService;
                
            }

            /**
             * Extended to shutdown the embedded transaction service.
             */
            @Override
            public void shutdown() {

                ((JournalTransactionService) getTransactionService())
                        .shutdown();

                super.shutdown();

            }

            /**
             * Extended to shutdown the embedded transaction service.
             */
            @Override
            public void shutdownNow() {

                ((JournalTransactionService) getTransactionService())
                        .shutdownNow();

                super.shutdownNow();

            }
        
        };

    }
    
    public AbstractLocalTransactionManager getLocalTransactionManager() {

        return localTransactionManager;

    }

    /**
     * Interface defines and documents the counters and counter namespaces
     * reported by the {@link Journal} and the various services which it uses.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static interface IJournalCounters extends
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
         * The namespace for counters pertaining to the
         * {@link Journal#getExecutorService()}.
         */
        String executorService = "Executor Service";
        
        /**
         * Performance counters for the query engine associated with this
         * journal (if any).
         */
        String queryEngine = "Query Engine";
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to attach additional performance counters.
     */
    @Override
    public CounterSet getCounters() {

        final CounterSet root = new CounterSet();

        // Host wide performance counters (collected from the OS).
        if (platformStatisticsCollector != null) {

            root.attach(platformStatisticsCollector.getCounters());

        }

        // JVM wide performance counters.
        {
            
            final CounterSet tmp = root.makePath("JVM");
            
            tmp.attach(AbstractStatisticsCollector.getMemoryCounterSet());
            
        }

        // Journal performance counters.
        {

            final CounterSet tmp = root.makePath("Journal");
            
            tmp.attach(super.getCounters());

            tmp.makePath(IJournalCounters.concurrencyManager)
                    .attach(concurrencyManager.getCounters());

            tmp.makePath(IJournalCounters.transactionManager)
                    .attach(localTransactionManager.getCounters());

            if (threadPoolExecutorStatisticsTask != null) {

                tmp.makePath(IJournalCounters.executorService)
                        .attach(threadPoolExecutorStatisticsTask.getCounters());

            }

        }
        
        // Lookup an existing query engine, but do not cause one to be created.
        final QueryEngine queryEngine = QueryEngineFactory
                .getExistingQueryController(this);

        if (queryEngine != null) {

            final CounterSet tmp = root.makePath(IJournalCounters.queryEngine);

            tmp.attach(queryEngine.getCounters());
            
        }

        return root;
        
    }
    
    /*
     * IResourceManager
     */

    public File getTmpDir() {
        
        return tmpDir;
        
    }
    
    /**
     * The directory in which the journal's file is located -or-
     * <code>null</code> if the journal is not backed by a file.
     */
    public File getDataDir() {
        
        final File file = getFile();
        
        if (file == null) {

            return null;

        }
        
        return file.getParentFile();
        
    }

    /**
     * Note: This will only succeed if the <i>uuid</i> identifies <i>this</i>
     * journal.
     */
    public IRawStore openStore(final UUID uuid) {
    
        if(uuid == getRootBlockView().getUUID()) {
            
            return this;
            
        }

        throw new UnsupportedOperationException();
        
    }
        
    /**
     * Always returns an array containing a single {@link BTree} which is the
     * {@link BTree} loaded from the commit record whose commit timestamp is
     * less than or equal to <i>timestamp</i> -or- <code>null</code> if there
     * are no {@link ICommitRecord}s that satisfy the probe or if the named
     * index was not registered as of that timestamp.
     * 
     * @param name
     * @param timestamp
     * 
     * @throws UnsupportedOperationException
     *             If the <i>timestamp</i> is {@link ITx#READ_COMMITTED}. You
     *             MUST use {@link #getIndex(String, long)} in order to obtain a
     *             view that has {@link ITx#READ_COMMITTED} semantics.
     */
    public AbstractBTree[] getIndexSources(final String name,
            final long timestamp) {

        final BTree btree;
        
        if (timestamp == ITx.UNISOLATED) {
        
            /*
             * Unisolated operation on the live index.
             */
            
            // MAY be null.
            btree = getIndex(name);

        } else if (timestamp == ITx.READ_COMMITTED) {

            /*
             * BTree does not know how to update its view with intervening
             * commits. Further, for a variety of reasons including the
             * synchronization problems that would be imposed, there are no
             * plans for BTree to be able to provide read-committed semantics.
             * Instead a ReadCommittedView is returned by
             * getIndex(name,timestamp) when ITx#READ_COMMITTED is requested and
             * this method is not invoked.
             */
            throw new UnsupportedOperationException("Read-committed view");
            
//            /*
//             * Read committed operation against the most recent commit point.
//             * 
//             * Note: This commit record is always defined, but that does not
//             * mean that any indices have been registered.
//             */
//
//            final ICommitRecord commitRecord = getCommitRecord();
//
//            final long ts = commitRecord.getTimestamp();
//
//            if (ts == 0L) {
//
//                log.warn("Nothing committed: name="+name+" - read-committed operation.");
//
//                return null;
//
//            }
//
//            // MAY be null.
//            btree = getIndex(name, commitRecord);
//
//            if (btree != null) {
//
////                /*
////                 * Mark the B+Tree as read-only.
////                 */
////                
////                btree.setReadOnly(true);
//
//                assert ((BTree) btree).getLastCommitTime() != 0;
////                btree.setLastCommitTime(commitRecord.getTimestamp());
//                
//            }
            
        } else {

            /*
             * A specified historical index commit point.
             */
            
            final long ts = Math.abs(timestamp);

            final ICommitRecord commitRecord = getCommitRecord(ts);

            if (commitRecord == null) {

                log.warn("No commit record: name="+name+", timestamp="+ts);
                
                return null;
                
            }

            // MAY be null
            btree = getIndex(name, commitRecord);
        
            if (btree != null) {

//                /*
//                 * Mark the B+Tree as read-only.
//                 */
//                
//                btree.setReadOnly(true);
                
                assert btree.getLastCommitTime() != 0;
//                btree.setLastCommitTime(commitRecord.getTimestamp());
                
            }

        }
        
        /* 
         * No such index as of that timestamp.
         */

        if (btree == null) {

            if (log.isInfoEnabled())
                log.info("No such index: name=" + name + ", timestamp="
                        + timestamp);

            return null;

        }

        return new AbstractBTree[] {

                btree

        };

    }

    /**
     * Always returns <i>this</i>.
     */
    final public AbstractJournal getLiveJournal() {

        return this;

    }
    
    /**
     * Always returns <i>this</i>.
     */
    final public AbstractJournal getJournal(final long timestamp) {
        
        return this;
        
    }

    /**
     * Compacts the named indices found on this journal as of the most recent
     * commit point, writing their view onto a new Journal. This method MAY be
     * used concurrently with the {@link Journal} but writes after the selected
     * commit point WILL NOT be reflected in the output file. Typical uses are
     * to reduce the space required by the backing store, to improve locality in
     * the backing store, and to make a backup of the most recent commit point.
     * 
     * @param outFile
     *            The file on which the new journal will be created.
     * 
     * @return The {@link Future} on which you must {@link Future#get() wait}
     *         for the {@link CompactTask} to complete. The already open journal
     *         is accessible using {@link Future#get()}. If you are backing up
     *         data, then be sure to shutdown the returned {@link Journal} so
     *         that it can release its resources.
     */
    public Future<Journal> compact(final File outFile) {

        return executorService.submit(new CompactTask(this, outFile,
                getLastCommitTime()));
        
    }

    /**
     * Note: {@link ITx#READ_COMMITTED} views are given read-committed semantics
     * using a {@link ReadCommittedView}.  This means that they can be cached
     * since the view will update automatically as commits are made against
     * the {@link Journal}.
     *  
     * @see IndexManager#getIndex(String, long)
     */
    public ILocalBTreeView getIndex(final String name, final long timestamp) {
        
        if (name == null) {

            throw new IllegalArgumentException();

        }

        final boolean isReadWriteTx = TimestampUtility.isReadWriteTx(timestamp);

        final Tx tx = (Tx) (isReadWriteTx ? getConcurrencyManager()
                .getTransactionManager().getTx(timestamp) : null);

        if (isReadWriteTx) {

            if (tx == null) {

                log.warn("Unknown transaction: name=" + name + ", tx="
                        + timestamp);

                return null;

            }

            tx.lock.lock();

            try {

                if (!tx.isActive()) {

                    // typically this means that the transaction has already
                    // prepared.
                    log.warn("Transaction not active: name=" + name + ", tx="
                            + timestamp + ", prepared=" + tx.isPrepared()
                            + ", complete=" + tx.isComplete() + ", aborted="
                            + tx.isAborted());

                    return null;

                }

            } finally {

                tx.lock.unlock();

            }
                                
        }
        
        if( isReadWriteTx && tx == null ) {
        
            /*
             * Note: This will happen both if you attempt to use a transaction
             * identified that has not been registered or if you attempt to use
             * a transaction manager after the transaction has been either
             * committed or aborted.
             */
            
            log.warn("No such transaction: name=" + name + ", tx=" + timestamp);

            return null;
            
        }
        
        final boolean readOnly = TimestampUtility.isReadOnly(timestamp);
//        final boolean readOnly = (timestamp < ITx.UNISOLATED)
//                || (isReadWriteTx && tx.isReadOnly());

        final ILocalBTreeView tmp;

        if (isReadWriteTx) {

            /*
             * Isolated operation.
             * 
             * Note: The backing index is always a historical state of the named
             * index.
             */

            final ILocalBTreeView isolatedIndex = tx.getIndex(name);

            if (isolatedIndex == null) {

                log.warn("No such index: name="+name+", tx="+timestamp);
                
                return null;

            }

            tmp = isolatedIndex;

        } else {
            
            /*
             * Non-transactional view.
             */

            if (readOnly) {

                if (timestamp == ITx.READ_COMMITTED) {

                    // read-committed
                    
                    tmp = new ReadCommittedView(this, name);

                } else {
                    
                    // historical read

                    final AbstractBTree[] sources = getIndexSources(name,
                            timestamp);

                    if (sources == null) {

                        log.warn("No such index: name=" + name + ", timestamp="
                                + timestamp);

                        return null;

                    }

                    assert sources[0].isReadOnly();

                    tmp = (BTree) sources[0];

                }
                
            } else {
                
                /*
                 * Writable unisolated index.
                 * 
                 * Note: This is the "live" mutable index. This index is NOT
                 * thread-safe. A lock manager is used to ensure that at most
                 * one task has access to this index at a time.
                 */

                assert timestamp == ITx.UNISOLATED;
                
                final AbstractBTree[] sources = getIndexSources(name, ITx.UNISOLATED);
                
                if (sources == null) {

                    if (log.isInfoEnabled())
                        log.info("No such index: name="+name+", timestamp="+timestamp);
                    
                    return null;
                    
                }

                assert ! sources[0].isReadOnly();

                tmp = (BTree) sources[0];

            }

        }
        
        return tmp;

    }

    /**
     * Always returns the {@link BTree} as the sole element of the array since
     * partitioned indices are not supported.
     */
    public AbstractBTree[] getIndexSources(final String name,
            final long timestamp, final BTree btree) {
        
        return new AbstractBTree[] { btree };
        
    }

    /**
     * Create a new transaction on the {@link Journal}.
     * 
     * @param timestamp
     *            A positive timestamp for a historical read-only transaction as
     *            of the first commit point LTE the given timestamp,
     *            {@link ITx#READ_COMMITTED} for a historical read-only
     *            transaction as of the most current commit point on the
     *            {@link Journal} as of the moment that the transaction is
     *            created, or {@link ITx#UNISOLATED} for a read-write
     *            transaction.
     * 
     * @return The transaction identifier.
     * 
     * @see ITransactionService#newTx(long)
     */
    public long newTx(final long timestamp) {
        
        try {

			return getTransactionService().newTx(timestamp);

        } catch (IOException e) {

            /*
             * Note: IOException is declared for RMI but will not be thrown
             * since the transaction service is in fact local.
             */

            throw new RuntimeException(e);

        }
        
    }

    /**
     * Abort a transaction.
     * 
     * @param tx
     *            The transaction identifier.
     *            
     * @see ITransactionService#abort(long)
     */
    public void abort(final long tx) {

        try {

            /*
             * Note: TransactionService will make call back to the
             * localTransactionManager to handle the client side of the
             * protocol.
             */
            
            localTransactionManager.getTransactionService().abort(tx);

        } catch (IOException e) {
            
            /*
             * Note: IOException is declared for RMI but will not be thrown
             * since the transaction service is in fact local.
             */
            
            throw new RuntimeException(e);
            
        }

    }

    /**
     * Commit a transaction.
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @return The commit time assigned to that transaction.
     * 
     * @see ITransactionService#commit(long)
     */
    public long commit(final long tx) throws ValidationError {

        try {

            /*
             * Note: TransactionService will make call back to the
             * localTransactionManager to handle the client side of the
             * protocol.
             */

            return localTransactionManager.getTransactionService().commit(tx);

        } catch (IOException e) {

            /*
             * Note: IOException is declared for RMI but will not be thrown
             * since the transaction service is in fact local.
             */

            throw new RuntimeException(e);

        }

    }

//    /**
//     * @deprecated This method in particular should be hidden from the
//     *             {@link Journal} as it exposes the {@link ITx} which really
//     *             deals with the client-side state of a transaction and which
//     *             should not be visible to applications - they should just use
//     *             the [long] transaction identifier.
//     */
//    public ITx getTx(long startTime) {
//    
//        return localTransactionManager.getTx(startTime);
//        
//    }

    /**
     * Returns the next timestamp from the {@link ILocalTransactionManager}.
     * 
     * @deprecated This is here for historical reasons and is only used by the
     *             test suite.  Use {@link #getLocalTransactionManager()} and
     *             {@link ITransactionService#nextTimestamp()}.
     */
    public long nextTimestamp() {
    
        return localTransactionManager.nextTimestamp();
    
    }

    /*
     * IConcurrencyManager
     */
    
    public ConcurrencyManager getConcurrencyManager() {
        
        return concurrencyManager;
        
    }
    
    /**
     * Note: The transaction service si shutdown first, then the
     * {@link #executorService}, then the {@link IConcurrencyManager}, the
     * {@link ITransactionService} and finally the {@link IResourceLockService}.
     */
    synchronized public void shutdown() {
        
        if (!isOpen())
            return;

        /*
         * Shutdown the transaction service. This will not permit new
         * transactions to start and will wait until running transactions either
         * commit or abort.
         */
        localTransactionManager.shutdown();

        if (platformStatisticsCollector != null) {

            platformStatisticsCollector.stop();

            platformStatisticsCollector = null;

        }

        if (scheduledExecutorService != null) {

            scheduledExecutorService.shutdown();
            
        }
        
        // optional httpd service for the local counters.
        if (httpd != null) {

            httpd.shutdown();

            httpd = null;

            httpdURL = null;

        }
        
        /*
         * Shutdown the executor service. This will wait for any tasks being run
         * on that service by the application to complete.
         */
        try {

            new ShutdownHelper(executorService, 1000/* logTimeout */,
                    TimeUnit.MILLISECONDS) {
               
                protected void logTimeout() {

                    log.warn("Waiting on task(s)"
                            + ": elapsed="
                            + TimeUnit.NANOSECONDS.toMillis(elapsed())
                            + "ms, #active="
                            + ((ThreadPoolExecutor) executorService)
                                    .getActiveCount());
                    
                }

            };

        } catch (InterruptedException ex) {

            log.warn("Immediate shutdown: "+ex);
            
            // convert to immediate shutdown.
            shutdownNow();
            
            return;

        }

        /*
         * Shutdown the concurrency manager - this will allow existing
         * non-transactional operations to complete but prevent additional
         * operations from starting.
         */
        concurrencyManager.shutdown();
        
        super.shutdown();
        
    }

    /**
     * Note: The {@link IConcurrencyManager} is shutdown first, then the
     * {@link ITransactionService} and finally the {@link IResourceManager}.
     */
    synchronized public void shutdownNow() {

        if (!isOpen())
            return;

        if (platformStatisticsCollector != null) {

            platformStatisticsCollector.stop();

            platformStatisticsCollector = null;

        }

        if (scheduledExecutorService != null)
            scheduledExecutorService.shutdownNow();
        
        // optional httpd service for the local counters.
        if (httpd != null) {

            httpd.shutdown();

            httpd = null;

            httpdURL = null;

        }

        // Note: can be null if error in ctor.
        if (executorService != null)
            executorService.shutdownNow();

        // Note: can be null if error in ctor.
        if (concurrencyManager != null)
            concurrencyManager.shutdownNow();

        // Note: can be null if error in ctor.
        if (localTransactionManager != null)
            localTransactionManager.shutdownNow();

        super.shutdownNow();
        
    }
    
    public void deleteResources() {
        
        super.deleteResources();
        
        // Note: can be null if error in ctor.
        if (tempStoreFactory != null)
            tempStoreFactory.closeAll();

    }

    public <T> Future<T> submit(AbstractTask<T> task) {

        return concurrencyManager.submit(task);
        
    }

    public List<Future> invokeAll(
            Collection<? extends AbstractTask> tasks, long timeout,
            TimeUnit unit) throws InterruptedException {
        
        return concurrencyManager.invokeAll(tasks, timeout, unit);
        
    }

    public <T> List<Future<T>> invokeAll(
            Collection<? extends AbstractTask<T>> tasks)
            throws InterruptedException {
        
        return concurrencyManager.invokeAll(tasks);
        
    }

    public IResourceManager getResourceManager() {
        
        return concurrencyManager.getResourceManager();
        
    }

    public ILocalTransactionManager getTransactionManager() {

        return concurrencyManager.getTransactionManager();
        
    }
    
    public ITransactionService getTransactionService() {
    	
    	return getTransactionManager().getTransactionService();

    }

    public WriteExecutorService getWriteService() {

        return concurrencyManager.getWriteService();
        
    }

    /*
     * IResourceManager
     */
    
    /**
     * Note: This implementation always returns <code>false</code>. As a
     * consequence the journal capacity will simply be extended by
     * {@link #write(ByteBuffer)} until the available disk space is exhausted.
     * 
     * @return This implementation returns <code>false</code> since overflow
     *         is NOT supported.
     */
    public boolean shouldOverflow() {

        return false;

    }
    
    /**
     * Note: This implementation always returns <code>false</code>.
     */
    public boolean isOverflowEnabled() {
        
        return false;
        
    }
    
    public Future<Object> overflow() {
        
        throw new UnsupportedOperationException();
        
    }

//    /**
//     * This request is always ignored for a {@link Journal} since it does not
//     * have any resources to manage.
//     */
//    public void setReleaseTime(final long releaseTime) {
//
//        if (releaseTime < 0L) {
//
//            // Not a timestamp.
//            throw new IllegalArgumentException();
//            
//        }
//
//        // ignored.
//        
//    }

    /**
     * @throws UnsupportedOperationException
     *             since {@link #overflow()} is not supported.
     */
    public File getIndexSegmentFile(IndexMetadata indexMetadata) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    public IBigdataFederation<?> getFederation() {

        throw new UnsupportedOperationException();
        
    }
    
    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    public DataService getDataService() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    public UUID getDataServiceUUID() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * Always returns <code>null</code> since index partition moves are not
     * supported.
     */
    public StaleLocatorReason getIndexPartitionGone(String name) {
        
        return null;
        
    }

    /*
     * global row store.
     */
    public SparseRowStore getGlobalRowStore() {

        return getGlobalRowStoreHelper().getGlobalRowStore();

    }

    /**
     * Return a view of the global row store as of the specified timestamp. This
     * is mainly used to provide access to historical views. 
     * 
     * @param timestamp
     *            The specified timestamp.
     * 
     * @return The global row store view -or- <code>null</code> if no view
     *         exists as of that timestamp.
     */
    public SparseRowStore getGlobalRowStore(final long timestamp) {

        return getGlobalRowStoreHelper().get(timestamp);

    }

    /**
     * Return the {@link GlobalRowStoreHelper}.
     * <p>
     * Note: An atomic reference provides us with a "lock" object which doubles
     * as a reference. We are not relying on its CAS properties.
     */
    private final GlobalRowStoreHelper getGlobalRowStoreHelper() {
        
        GlobalRowStoreHelper t = globalRowStoreHelper.get();

        if (t == null) {

            synchronized (globalRowStoreHelper) {

                /*
                 * Note: Synchronized to avoid race conditions when updating
                 * (this allows us to always return our reference if we create a
                 * new helper instance).
                 */

                t = globalRowStoreHelper.get();

                if (t == null) {

                    globalRowStoreHelper
                            .set(t = new GlobalRowStoreHelper(this));

                }

            }

        }

        return globalRowStoreHelper.get();
    }

    final private AtomicReference<GlobalRowStoreHelper> globalRowStoreHelper = new AtomicReference<GlobalRowStoreHelper>();

    /*
     * global file system.
     * 
     * Note: An atomic reference provides us with a "lock" object which doubles
     * as a reference. We are not relying on its CAS properties.
     */
    public BigdataFileSystem getGlobalFileSystem() {

        GlobalFileSystemHelper t = globalFileSystemHelper.get();
        
        if (t == null) {

            synchronized (globalFileSystemHelper) {

                /*
                 * Note: Synchronized to avoid race conditions when updating
                 * (this allows us to always return our reference if we create a
                 * new helper instance).
                 */

                t = globalFileSystemHelper.get();

                if (t == null) {

                    globalFileSystemHelper
                            .set(t = new GlobalFileSystemHelper(this));

                }
                
            }

        }

        return globalFileSystemHelper.get().getGlobalFileSystem();

    }
    final private AtomicReference<GlobalFileSystemHelper> globalFileSystemHelper = new AtomicReference<GlobalFileSystemHelper>();

    protected void discardCommitters() {

        super.discardCommitters();

        synchronized (globalRowStoreHelper) {

            /*
             * Note: Synchronized even though atomic. We are using this as an
             * mutable lock object without regard to its CAS behavior.
             */

            globalRowStoreHelper.set(null);

        }
        
        synchronized (globalFileSystemHelper) {

            /*
             * Note: Synchronized even though atomic. We are using this as an
             * mutable lock object without regard to its CAS behavior.
             */

            globalFileSystemHelper.set(null);
            
        }

    }
    
    public TemporaryStore getTempStore() {
        
        return tempStoreFactory.getTempStore();
        
    }
    private final TemporaryStoreFactory tempStoreFactory;

    public DefaultResourceLocator getResourceLocator() {

        assertOpen();
        
        return resourceLocator;
        
    }
    private final DefaultResourceLocator<?> resourceLocator;
    
    public IResourceLockService getResourceLockService() {
        
        assertOpen();
        
        return resourceLockManager;
        
    }
    private final ResourceLockService resourceLockManager;

    public ExecutorService getExecutorService() {
        
        assertOpen();
        
        return executorService;
        
    }
    private final ThreadPoolExecutor executorService;

    /**
     * Used to sample and report on the queue associated with the
     * {@link #executorService} and <code>null</code> if we will not be
     * collecting data on task execution.
     */
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * Collects interesting statistics on the {@link #executorService}.
     * 
     * @see Options#COLLECT_QUEUE_STATISTICS
     */
    private ThreadPoolExecutorStatisticsTask threadPoolExecutorStatisticsTask = null;

    /**
     * Counters that aggregate across all tasks submitted to the Journal's
     * {@link ExecutorService}. Those counters are sampled by a
     * {@link ThreadPoolExecutorStatisticsTask}.
     * 
     * @see Options#COLLECT_QUEUE_STATISTICS
     */
    private final TaskCounters taskCounters = new TaskCounters();

    /**
     * Collects interesting statistics on the host and process.
     * 
     * @see Options#COLLECT_PLATFORM_STATISTICS
     */
    private AbstractStatisticsCollector platformStatisticsCollector = null;

    /**
     * httpd reporting the live counters -or- <code>null</code> if not enabled.
     * 
     * @see Options#HTTPD_PORT
     */
    private AbstractHTTPD httpd = null;
    
    /**
     * The URL that may be used to access the httpd service exposed by this
     * client -or- <code>null</code> if not enabled.
     */
    private String httpdURL = null;

    /**
     * The URL that may be used to access the httpd service exposed by this
     * client -or- <code>null</code> if not enabled.
     */
    final public String getHttpdURL() {
        
        return httpdURL;
        
    }
    
    /**
     * An executor service used to read on the local disk.
     * 
     * @todo This is currently used by prefetch. We should generalize this
     *       mechanism, probably moving it to the {@link IResourceManager}, and
     *       use it to do all IO, ideally using the JSR 166 fork/join
     *       mechanisms.
     *       <p>
     *       This should be reconciled with the {@link ConcurrencyManager},
     *       which has distinct {@link ExecutorService}s for readers and writers
     *       which control the per-task concurrency while this controls the disk
     *       read concurrency.
     *       <p>
     *       We could use the same pool for readers and writers on the disk.
     */
    public LatchedExecutor getReadExecutor() {
        
//        assertOpen();
        
        return readService;
        
    }
    private final LatchedExecutor readService;

    /**
     * This task runs once starts an (optional)
     * {@link AbstractStatisticsCollector} and an (optional) httpd service.
     * <p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     *         FIXME Make sure that we disable this by default for the unit
     *         tests or we will have a bunch of sampling processes running!
     */
    private class StartDeferredTasksTask implements Runnable {

        /**
         * Note: The logger is named for this class, but since it is an inner
         * class the name uses a "$" delimiter (vs a ".") between the outer and
         * the inner class names.
         */
        final private Logger log = Logger.getLogger(StartDeferredTasksTask.class);

        private StartDeferredTasksTask() {
        }

        public void run() {

            try {
                
                startDeferredTasks();
                
            } catch (Throwable t) {

                log.error(t, t);

                return;
                
            }

        }

        /**
         * Starts performance counter collection.
         */
        protected void startDeferredTasks() throws IOException {

            // start collection on various work queues.
            startQueueStatisticsCollection();
            
            // start collecting performance counters (if enabled).
            startPlatformStatisticsCollection();

            // start the local httpd service reporting on this service.
            startHttpdService();

        }

        /**
         * Setup sampling on the client's thread pool. This collects interesting
         * statistics about the thread pool for reporting to the load balancer
         * service.
         */
        protected void startQueueStatisticsCollection() {

            final boolean collectQueueStatistics = Boolean.valueOf(getProperty(
                    Options.COLLECT_QUEUE_STATISTICS,
                    Options.DEFAULT_COLLECT_QUEUE_STATISTICS));

            if (log.isInfoEnabled())
                log.info(Options.COLLECT_QUEUE_STATISTICS + "="
                        + collectQueueStatistics);

            if (!collectQueueStatistics) {

                return;

            }

            final long initialDelay = 0; // initial delay in ms.
            final long delay = 1000; // delay in ms.
            final TimeUnit unit = TimeUnit.MILLISECONDS;

            final String relpath = "Thread Pool";

            threadPoolExecutorStatisticsTask = new ThreadPoolExecutorStatisticsTask(
                    relpath, executorService, taskCounters);

            scheduledExecutorService
                    .scheduleWithFixedDelay(threadPoolExecutorStatisticsTask,
                            initialDelay, delay, unit);

        }
        
        /**
         * Start collecting performance counters from the OS (if enabled).
         */
        protected void startPlatformStatisticsCollection() {

            final boolean collectPlatformStatistics = Boolean
                    .valueOf(getProperty(Options.COLLECT_PLATFORM_STATISTICS,
                            Options.DEFAULT_COLLECT_PLATFORM_STATISTICS));

            if (log.isInfoEnabled())
                log.info(Options.COLLECT_PLATFORM_STATISTICS + "="
                        + collectPlatformStatistics);

            if (!collectPlatformStatistics) {

                return;

            }

            final Properties p = getProperties();

            if (p.getProperty(AbstractStatisticsCollector.Options.PROCESS_NAME) == null) {

                // Set default name for this process.
                p.setProperty(AbstractStatisticsCollector.Options.PROCESS_NAME,
                        "service" + ICounterSet.pathSeparator
                                + Journal.class.getName());

            }

            try {

                final AbstractStatisticsCollector tmp = AbstractStatisticsCollector
                        .newInstance(p);

                tmp.start();

                // Note: synchronized(Journal.this) keeps find bugs happy.
                synchronized(Journal.this) {
                    
                    Journal.this.platformStatisticsCollector = tmp;
                    
                }
                
                if (log.isInfoEnabled())
                    log.info("Collecting platform statistics.");

            } catch (Throwable t) {

                log.error(t, t);
                
            }

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
            
            final int httpdPort = Integer.valueOf(getProperty(
                    Options.HTTPD_PORT, Options.DEFAULT_HTTPD_PORT));

            if (log.isInfoEnabled())
                log.info(Options.HTTPD_PORT + "=" + httpdPort
                        + (httpdPort == -1 ? " (disabled)" : ""));

            if (httpdPort == -1) {

                return;

            }

            final AbstractHTTPD httpd;
            try {

                httpd = new CounterSetHTTPD(httpdPort, Journal.this);

            } catch (IOException e) {

                log.error("Could not start httpd: port=" + httpdPort, e);

                return;
                
            }

            if (httpd != null) {

                // Note: synchronized(Journal.this) keeps findbugs happy.
                synchronized (Journal.this) {

                    // save reference to the daemon.
                    Journal.this.httpd = httpd;

                    // the URL that may be used to access the local httpd.
                    Journal.this.httpdURL = "http://"
                            + AbstractStatisticsCollector.fullyQualifiedHostName
                            + ":" + httpd.getPort() + "/?path="
                            + URLEncoder.encode("", "UTF-8");

                    if (log.isInfoEnabled())
                        log.info("start:\n" + httpdURL);
                
                }

            }

        }
        
    } // class StartDeferredTasks

}
