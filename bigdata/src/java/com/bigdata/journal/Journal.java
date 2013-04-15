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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
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
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.ReadCommittedView;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.config.IntegerValidator;
import com.bigdata.config.LongValidator;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.jini.ha.HAJournal;
import com.bigdata.quorum.Quorum;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StaleLocatorReason;
import com.bigdata.rwstore.IRWStrategy;
import com.bigdata.rwstore.IRawTx;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.LatchedExecutor;
import com.bigdata.util.concurrent.ShutdownHelper;
import com.bigdata.util.concurrent.ThreadPoolExecutorBaseStatisticsTask;

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
     * Logger.
     */
    private static final Logger log = Logger.getLogger(Journal.class);

    /**
     * @see http://sourceforge.net/apps/trac/bigdata/ticket/443 (Logger for
     *      RWStore transaction service and recycler)
     */
    private static final Logger txLog = Logger.getLogger("com.bigdata.txLog");

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
     */
    public interface Options extends com.bigdata.journal.Options,
            com.bigdata.journal.ConcurrencyManager.Options,
            com.bigdata.journal.TemporaryStoreFactory.Options,
            com.bigdata.journal.QueueStatsPlugIn.Options,
            com.bigdata.journal.PlatformStatsPlugIn.Options,
            com.bigdata.journal.HttpPlugin.Options
            // Note: Do not import. Forces bigdata-ganglia dependency.
            // com.bigdata.journal.GangliaPlugIn.Options
            {

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

//        if (Boolean.valueOf(properties.getProperty(
//                Options.COLLECT_QUEUE_STATISTICS,
//                Options.DEFAULT_COLLECT_QUEUE_STATISTICS))) {
            
            scheduledExecutorService = Executors
                    .newSingleThreadScheduledExecutor(new DaemonThreadFactory(
                            getClass().getName() + ".sampleService"));
            
//        } else {
//         
//            scheduledExecutorService = null;
//            
//        }
        
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

        resourceLocator = newResourceLocator();
        
        resourceLockManager = new ResourceLockService();

        localTransactionManager = newLocalTransactionManager();

        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, this);
        
        getExecutorService().execute(new StartDeferredTasksTask());
        
    }

    /**
     * Ensure that the WORM mode of the journal always uses
     * {@link Long#MAX_VALUE} for
     * {@link AbstractTransactionService.Options#MIN_RELEASE_AGE}.
     * 
     * @param properties
     *            The properties.
     *            
     * @return The argument, with the minReleaseAge overridden if necessary.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/391
     */
    private Properties checkProperties(final Properties properties) {
        if (getBufferStrategy() instanceof WORMStrategy) {
            properties.setProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE, ""
                            + Long.MAX_VALUE);
        }
        return properties;
    }

    /**
     * Factory for the {@link IResourceLocator} for the {@link Journal}.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected IResourceLocator<?> newResourceLocator() {

        final int cacheCapacity = getProperty(Options.LOCATOR_CACHE_CAPACITY,
                Options.DEFAULT_LOCATOR_CACHE_CAPACITY,
                IntegerValidator.GT_ZERO);

        final long cacheTimeout = getProperty(Options.LOCATOR_CACHE_TIMEOUT,
                Options.DEFAULT_LOCATOR_CACHE_TIMEOUT, LongValidator.GTE_ZERO);

        return new DefaultResourceLocator(this, null/* delegate */,
                cacheCapacity, cacheTimeout);

    }
    
    protected AbstractLocalTransactionManager newLocalTransactionManager() {

        final JournalTransactionService abstractTransactionService = new JournalTransactionService(
                checkProperties(properties), this) {

            /*
             * @see http://sourceforge.net/apps/trac/bigdata/ticket/445 (RWStore
             * does not track tx release correctly)
             */
            final private ConcurrentHashMap<Long, IRawTx> m_rawTxs = new ConcurrentHashMap<Long, IRawTx>();

            // Note: This is the implicit constructor call.
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

            /*
             * HA Quorum Overrides.
             * 
             * Note: The basic pattern is that the quorum must be met, a leader
             * executes the operation directly, and a follower delegates the
             * operation to the leader. This centralizes the decisions about the
             * open transactions, and the read locks responsible for pinning
             * commit points, on the leader.
             * 
             * If the journal is not highly available, then the request is
             * passed to the base class (JournalTransactionService, which
             * extends AbstractTransactionService).
             */
			
			@Override
            public long newTx(final long timestamp) {

                final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();

                if (quorum == null) {

                    // Not HA. 
                    return this._newTx(timestamp);

                }

                final long token = getQuorumToken();

                if (quorum.getMember().isLeader(token)) {

                    // HA and this is the leader.
                    return this._newTx(timestamp);

                }
                
                /*
                 * The transaction needs to be allocated by the leader.
                 * 
                 * Note: Heavy concurrent query on a HAJournal will pin history
                 * on the leader. However, the lastReleaseTime will advance
                 * since clients will tend to read against the then current
                 * lastCommitTime, so we will still recycle the older commit
                 * points once there is no longer an active reader for those
                 * commit points.
                 */
                
                final HAGlue leaderService = quorum.getMember()
                        .getLeader(token);
                
                final long tx;
                try {

                    // delegate to the quorum leader.
                    tx = leaderService.newTx(timestamp);

                } catch (IOException e) {
                    
                    throw new RuntimeException(e);
                    
                }

                // Make sure the quorum is still valid.
                quorum.assertQuorum(token);

                return tx;

            }
			
            /**
             * Core impl.
             * <p>
             * This code pre-increments the active transaction count within the
             * RWStore before requesting a new transaction from the transaction
             * service. This ensures that the RWStore does not falsely believe
             * that there are no open transactions during the call to
             * AbstractTransactionService#newTx().
             * <p>
             * Note: This code was moved into the inner class extending the
             * {@link JournalTransactionService} in order to ensure that we
             * follow this pre-incremental pattern for an {@link HAJournal} as
             * well.
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/440#comment:13">
             *      BTree can not be case to Name2Addr </a>
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/530">
             *      Journal HA </a>
             */
			private final long _newTx(final long timestamp) {

			    IRawTx tx = null;
		        try {
		        
                    if (getBufferStrategy() instanceof IRWStrategy) {

                        // pre-increment the active tx count.
                        tx = ((IRWStrategy) getBufferStrategy()).newTx();
                    }

                    return super.newTx(timestamp);

                } finally {

                    if (tx != null) {

                        /*
                         * If we had pre-incremented the transaction counter in
                         * the RWStore, then we decrement it before leaving this
                         * method.
                         */

                        tx.close();

                    }

                }

			}
			
            @Override
            public long commit(final long tx) {

                final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();

                if (quorum == null) {

                    // Not HA. 
                    return super.commit(tx);

                }

                final long token = getQuorumToken();

                if (quorum.getMember().isLeader(token)) {

                    // HA and this is the leader.
                    return super.commit(tx);

                }
                
                /*
                 * Delegate to the quorum leader.
                 */
                
                final HAGlue leaderService = quorum.getMember()
                        .getLeader(token);
                
                final long commitTime;
                try {

                    // delegate to the quorum leader.
                    commitTime = leaderService.commit(tx);

                } catch (IOException e) {
                    
                    throw new RuntimeException(e);
                    
                }

                // Make sure the quorum is still valid.
                quorum.assertQuorum(token);

                return commitTime;

            }
            
            @Override
            public void abort(final long tx) {

                final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();

                if (quorum == null) {

                    // Not HA. 
                    super.abort(tx);
                    
                    return;

                }

                final long token = getQuorumToken();

                if (quorum.getMember().isLeader(token)) {

                    // HA and this is the leader.
                    super.abort(tx);
                    
                    return;

                }
                
                /*
                 * Delegate to the quorum leader.
                 */
                
                final HAGlue leaderService = quorum.getMember()
                        .getLeader(token);
                
                try {

                    // delegate to the quorum leader.
                    leaderService.abort(tx);

                } catch (IOException e) {
                    
                    throw new RuntimeException(e);
                    
                }

                // Make sure the quorum is still valid.
                quorum.assertQuorum(token);

                return;

            }

            @Override
            public void notifyCommit(final long commitTime) {
                
                final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();

                if (quorum == null) {

                    // Not HA. 
                    super.notifyCommit(commitTime);
                    
                    return;

                }

                final long token = getQuorumToken();

                if (quorum.getMember().isLeader(token)) {

                    // HA and this is the leader.
                    super.notifyCommit(commitTime);
                    
                    return;

                }
                
                /*
                 * Delegate to the quorum leader.
                 */
                
                final HAGlue leaderService = quorum.getMember()
                        .getLeader(token);
                
                try {

                    // delegate to the quorum leader.
                    leaderService.notifyCommit(commitTime);

                } catch (IOException e) {
                    
                    throw new RuntimeException(e);
                    
                }

                // Make sure the quorum is still valid.
                quorum.assertQuorum(token);

                return;

            }

            @Override
            public long getReleaseTime() {
                
                final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();

                if (quorum == null) {

                    // Not HA. 
                    return super.getReleaseTime();

                }

                final long token = getQuorumToken();

                if (quorum.getMember().isLeader(token)) {

                    // HA and this is the leader.
                    return super.getReleaseTime();
                    
                }
                
                /*
                 * Delegate to the quorum leader.
                 */
                
                final HAGlue leaderService = quorum.getMember()
                        .getLeader(token);

                final long releaseTime;
                try {

                    // delegate to the quorum leader.
                    releaseTime = leaderService.getReleaseTime();

                } catch (IOException e) {
                    
                    throw new RuntimeException(e);
                    
                }

                // Make sure the quorum is still valid.
                quorum.assertQuorum(token);

                return releaseTime;

            }

            @Override
            public long nextTimestamp(){
            
                final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();

                if (quorum == null) {

                    // Not HA. 
                    return super.nextTimestamp();

                }

                final long token = getQuorumToken();

                if (quorum.getMember().isLeader(token)) {

                    // HA and this is the leader.
                    return super.nextTimestamp();
                    
                }
                
                /*
                 * Delegate to the quorum leader.
                 */
                
                final HAGlue leaderService = quorum.getMember()
                        .getLeader(token);

                final long nextTimestamp;
                try {

                    // delegate to the quorum leader.
                    nextTimestamp = leaderService.nextTimestamp();

                } catch (IOException e) {
                    
                    throw new RuntimeException(e);
                    
                }

                // Make sure the quorum is still valid.
                quorum.assertQuorum(token);

                return nextTimestamp;

            }
            
            protected void activateTx(final TxState state) {
                if (txLog.isInfoEnabled())
                    txLog.info("OPEN : txId=" + state.tx
                            + ", readsOnCommitTime=" + state.readsOnCommitTime);
                final IBufferStrategy bufferStrategy = Journal.this.getBufferStrategy();
                if (bufferStrategy instanceof IRWStrategy) {
                    final IRawTx tx = ((IRWStrategy)bufferStrategy).newTx();
                    if (m_rawTxs.put(state.tx, tx) != null) {
                        throw new IllegalStateException(
                                "Unexpected existing RawTx");
                    }
                }
                super.activateTx(state);
            }

            protected void deactivateTx(final TxState state) {
                if (txLog.isInfoEnabled())
                    txLog.info("CLOSE: txId=" + state.tx
                            + ", readsOnCommitTime=" + state.readsOnCommitTime);
                /*
                 * Note: We need to deactivate the tx before RawTx.close() is
                 * invoked otherwise the activeTxCount will never be zero inside
                 * of RawTx.close() and the session protection mode of the
                 * RWStore will never be able to release storage.
                 */
                super.deactivateTx(state);
                
                final IRawTx tx = m_rawTxs.remove(state.tx);
                if (tx != null) {
                    tx.close();
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
         * The namespace for the counters pertaining to the named indices.
         */
        String indexManager = "Index Manager";

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
        {

            final AbstractStatisticsCollector t = getPlatformStatisticsCollector();

            if (t != null) {

                root.attach(t.getCounters());

            }
            
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

			// Live index counters iff available.
            {

                final CounterSet indexCounters = getIndexCounters();

                if (indexCounters != null) {
                    
                    tmp.makePath(IJournalCounters.indexManager).attach(
                            indexCounters);
                    
                }

            }

			tmp.makePath(IJournalCounters.concurrencyManager)
                    .attach(concurrencyManager.getCounters());

            tmp.makePath(IJournalCounters.transactionManager)
                    .attach(localTransactionManager.getCounters());

            {

                final IPlugIn<Journal, ThreadPoolExecutorBaseStatisticsTask> plugin = pluginQueueStats
                        .get();

                if (plugin != null) {

                    final ThreadPoolExecutorBaseStatisticsTask t = plugin
                            .getService();

                    if (t != null) {

                        tmp.makePath(IJournalCounters.executorService).attach(
                                t.getCounters());

                    }

                }
                
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

    @Override
    public File getTmpDir() {
        
        return tmpDir;
        
    }
    
    /**
     * The directory in which the journal's file is located -or-
     * <code>null</code> if the journal is not backed by a file.
     */
    @Override
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
             * 
             * @see <a
             * href="http://sourceforge.net/apps/trac/bigdata/ticket/546" > Add
             * cache for access to historical index views on the Journal by name
             * and commitTime. </a>
             */
            
            final long ts = Math.abs(timestamp);

//            final ICommitRecord commitRecord = getCommitRecord(ts);
//
//            if (commitRecord == null) {
//
//                log.warn("No commit record: name=" + name + ", timestamp=" + ts);
//
//                return null;
//                
//            }
//
//            // MAY be null
//            btree = getIndex(name, commitRecord);

            // MAY be null
            btree = (BTree) super.getIndex(name, ts);

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

    @Override
	public void dropIndex(final String name) {

		final BTreeCounters btreeCounters = getIndexCounters(name);

		super.dropIndex(name);

		if (btreeCounters != null) {

			// Conditionally remove the counters for the old index.
			indexCounters.remove(name, btreeCounters);

		}
    	
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Note: {@link ITx#READ_COMMITTED} views are given read-committed semantics
     * using a {@link ReadCommittedView}.  This means that they can be cached
     * since the view will update automatically as commits are made against
     * the {@link Journal}.
     *  
     * @see IndexManager#getIndex(String, long)
     */
    @Override
    public ILocalBTreeView getIndex(final String name, final long timestamp) {
        
        if (name == null) {

            throw new IllegalArgumentException();

        }

        final boolean isReadWriteTx = TimestampUtility.isReadWriteTx(timestamp);

//        final Tx tx = (Tx) (isReadWriteTx ? getConcurrencyManager()
//                .getTransactionManager().getTx(timestamp) : null);
        final Tx tx = (Tx) getConcurrencyManager().getTransactionManager()
                .getTx(timestamp);

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
             * 
             * Note: Tx.getIndex() will pass through the actual commit time of
             * the ground state against which the transaction is reading (if it
             * is available, which it is on the local Journal).
             * 
             * @see <a
             * href="https://sourceforge.net/apps/trac/bigdata/ticket/266">
             * Refactor native long tx id to thin object</a>
             */

            final ILocalBTreeView isolatedIndex = tx.getIndex(name);

            if (isolatedIndex == null) {

                log.warn("No such index: name=" + name + ", tx=" + timestamp);

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

                    if (tx != null) {
  
                        /*
                         * read-only transaction
                         * 
                         * @see <a href=
                         * "http://sourceforge.net/apps/trac/bigdata/ticket/546"
                         * > Add cache for access to historical index views on
                         * the Journal by name and commitTime. </a>
                         */

                        final AbstractBTree[] sources = getIndexSources(name,
                                tx.getReadsOnCommitTime());

                        if (sources == null) {

                            log.warn("No such index: name=" + name
                                    + ", timestamp=" + timestamp);

                            return null;

                        }

                        assert sources[0].isReadOnly();

                        tmp = (BTree) sources[0];
                        
                    } else {

                        // historical read not protected by a transaction

                        final AbstractBTree[] sources = getIndexSources(name,
                                timestamp);

                        if (sources == null) {

                            log.warn("No such index: name=" + name
                                    + ", timestamp=" + timestamp);

                            return null;

                        }

                        assert sources[0].isReadOnly();

                        tmp = (BTree) sources[0];

                    }

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
        
        /*
         * Make sure that it is using the canonical counters for that index.
         * 
         * Note: AbstractTask also does this for UNISOLATED indices which it
         * loads by itself as part of providing ACID semantics for add/drop
         * of indices.
         */

        tmp.getMutableBTree().setBTreeCounters(getIndexCounters(name));

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
     * <p>
     * Note: This is a convenience method. The implementation of this method is
     * delegated to the object returned by {@link #getTransactionService()}.
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
    final public long newTx(final long timestamp) {

//        IRawTx tx = null;
//        try {
//            if (getBufferStrategy() instanceof IRWStrategy) {
//                
//                /*
//                 * This code pre-increments the active transaction count within
//                 * the RWStore before requesting a new transaction from the
//                 * transaction service. This ensures that the RWStore does not
//                 * falsely believe that there are no open transactions during
//                 * the call to AbstractTransactionService#newTx().
//                 * 
//                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/440#comment:13
//                 */
//                tx = ((IRWStrategy) getBufferStrategy()).newTx();
//            }
//            try {
//
//                return getTransactionService().newTx(timestamp);
//
//            } catch (IOException ioe) {
//
//                /*
//                 * Note: IOException is declared for RMI but will not be thrown
//                 * since the transaction service is in fact local.
//                 */
//
//                throw new RuntimeException(ioe);
//
//            }
//
//        } finally {
//        
//            if (tx != null) {
//            
//                /*
//                 * If we had pre-incremented the transaction counter in the
//                 * RWStore, then we decrement it before leaving this method.
//                 */
//
//                tx.close();
//
//            }
//            
//        }

        /*
         * Note: The RWStore native tx pre-increment logic is now handled by
         * _newTx() in the inner class that extends JournalTransactionService.
         */
        try {

            return getTransactionService().newTx(timestamp);

        } catch (IOException ioe) {

            /*
             * Note: IOException is declared for RMI but will not be thrown
             * since the transaction service is in fact local.
             */

            throw new RuntimeException(ioe);

        }

    }

    /**
     * Abort a transaction.
     * <p>
     * Note: This is a convenience method. The implementation of this method is
     * delegated to the object returned by {@link #getTransactionService()}.
     * 
     * @param tx
     *            The transaction identifier.
     *            
     * @see ITransactionService#abort(long)
     */
    final public void abort(final long tx) {

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
     * <p>
     * Note: This is a convenience method. The implementation of this method is
     * delegated to the object returned by {@link #getTransactionService()}.
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @return The commit time assigned to that transaction.
     * 
     * @see ITransactionService#commit(long)
     */
    final public long commit(final long tx) throws ValidationError {

        try {

            /*
             * Note: TransactionService will make call back to the
             * localTransactionManager to handle the client side of the
             * protocol.
             */

            return getTransactionService().commit(tx);

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
     * <p>
     * Note: This is a convenience method. The implementation of this method is
     * delegated to the object returned by {@link #getTransactionService()}.
     * 
     * @deprecated This is here for historical reasons and is only used by the
     *             test suite. Use {@link #getLocalTransactionManager()} and
     *             {@link ITransactionService#nextTimestamp()}.
     * 
     * @see ITransactionService#nextTimestamp()
     */
    final public long nextTimestamp() {
    
        return localTransactionManager.nextTimestamp();
    
    }

    /*
     * IConcurrencyManager
     */
    
    public ConcurrencyManager getConcurrencyManager() {
        
        return concurrencyManager;
        
    }
    
    /**
     * Note: The transaction service is shutdown first, then the
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

        {

            final IPlugIn<?, ?> plugIn = pluginGanglia.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(false/* immediateShutdown */);

            }

        }
        
        {
        
            final IPlugIn<?, ?> plugIn = pluginQueueStats.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(false/* immediateShutdown */);

            }
            
        }

        {
         
            final IPlugIn<?, ?> plugIn = pluginPlatformStats.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(false/* immediateShutdown */);

            }
            
        }
        
        if (scheduledExecutorService != null) {

            scheduledExecutorService.shutdown();
            
        }
        
        // optional httpd service for the local counters.
        {
            
            final IPlugIn<?, ?> plugIn = pluginHttpd.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(false/* immediateShutdown */);

            }
            
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

        /*
         * Note: The ganglia plug in is executed on the main thread pool. We
         * need to terminate it in order for the thread pool to shutdown.
         */
        {

            final IPlugIn<?, ?> plugIn = pluginGanglia.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(true/* immediateShutdown */);

            }
            
        }

        {

            final IPlugIn<?, ?> plugIn = pluginQueueStats.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(true/* immediateShutdown */);

            }
            
        }

        {
         
            final IPlugIn<?, ?> plugIn = pluginPlatformStats.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(true/* immediateShutdown */);

            }
            
        }

        if (scheduledExecutorService != null)
            scheduledExecutorService.shutdownNow();
        
        // optional httpd service for the local counters.
        {
            
            final IPlugIn<?, ?> plugIn = pluginHttpd.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(false/* immediateShutdown */);

            }
            
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
    
//    public void deleteResources() {
//        
//        super.deleteResources();
//        
//        // Note: can be null if error in ctor.
//        if (tempStoreFactory != null)
//            tempStoreFactory.closeAll();
//
//    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the {@link TemporaryStoreFactory}.
     */
    @Override
    protected void _close() {

        super._close();

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

//        return concurrencyManager.getTransactionManager();
        return localTransactionManager;
        
    }
    
    public ITransactionService getTransactionService() {
    	
//       return getTransactionManager().getTransactionService();
        return localTransactionManager.getTransactionService();

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

//    /**
//     * Return a view of the global row store as of the specified timestamp. This
//     * is mainly used to provide access to historical views. 
//     * 
//     * @param timestamp
//     *            The specified timestamp.
//     * 
//     * @return The global row store view -or- <code>null</code> if no view
//     *         exists as of that timestamp.
//     */
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

    public IResourceLocator<?> getResourceLocator() {

        assertOpen();
        
        return resourceLocator;
        
    }
    private final IResourceLocator<?> resourceLocator;
    
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
     * {@link #executorService}. May be used to schedule other tasks as well.
     */
    private final ScheduledExecutorService scheduledExecutorService;

    /*
     * plugins.
     */
    
    private final AtomicReference<IPlugIn<Journal, ThreadPoolExecutorBaseStatisticsTask>> pluginQueueStats = new AtomicReference<IPlugIn<Journal,ThreadPoolExecutorBaseStatisticsTask>>();
    private final AtomicReference<IPlugIn<Journal, AbstractStatisticsCollector>> pluginPlatformStats = new AtomicReference<IPlugIn<Journal, AbstractStatisticsCollector>>();
    private final AtomicReference<IPlugIn<Journal, ?>> pluginHttpd = new AtomicReference<IPlugIn<Journal, ?>>();
    
    /**
     * An optional plug in for Ganglia.
     * <p>
     * Note: The plug in concept was introduced to decouple the ganglia
     * component. Do not introduce imports into the {@link Journal} class that
     * would make the ganglia code a required dependency!
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/609">
     *      bigdata-ganglia is required dependency for Journal </a>
     */
    private final AtomicReference<IPlugIn<Journal, ?>> pluginGanglia = new AtomicReference<IPlugIn<Journal, ?>>();

    /**
     * Host wide performance counters (collected from the OS) (optional).
     * 
     * @see PlatformStatsPlugIn
     */
    protected AbstractStatisticsCollector getPlatformStatisticsCollector() {

        final IPlugIn<Journal, AbstractStatisticsCollector> plugin = pluginPlatformStats
                .get();

        if (plugin == null)
            return null;

        final AbstractStatisticsCollector t = plugin.getService();

        return t;

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
            {

                final IPlugIn<Journal, ThreadPoolExecutorBaseStatisticsTask> tmp = new QueueStatsPlugIn();
                
                tmp.startService(Journal.this);
                
                // Save reference iff started.
                pluginQueueStats.set(tmp);
                
            }
            
            // start collecting performance counters (if enabled).
            {

                final IPlugIn<Journal, AbstractStatisticsCollector> tmp = new PlatformStatsPlugIn();
                
                tmp.startService(Journal.this);
                
                pluginPlatformStats.set(tmp);
                
            }

            // start the local httpd service reporting on this service.
            {

                final IPlugIn<Journal, CounterSetHTTPD> tmp = new HttpPlugin();
                
                tmp.startService(Journal.this);
                
                pluginHttpd.set(tmp);
                
            }

            /**
             * Start embedded ganglia peer. It will develop a snapshot of the
             * metrics in memory for all nodes reporting in the ganglia network
             * and will self-report metrics from the performance counter
             * hierarchy to the ganglia network.
             * 
             * Note: Do NOT invoke this plug in unless it will start and run to
             * avoid a CLASSPATH dependency on bigdata-ganglia when it is not
             * used. The plugin requires platform statistics collection to run,
             * so if you do not want to have a CLASSPATH dependency on ganglia,
             * you need to disable the PlatformStatsPlugIn.
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/609">
             *      bigdata-ganglia is required dependency for Journal </a>
             */
            if (getPlatformStatisticsCollector() != null) {

                final IPlugIn<Journal, ?> tmp = new GangliaPlugIn();

                tmp.startService(Journal.this);

                if (tmp.isRunning()) {

                    // Save reference iff started.
                    pluginGanglia.set(tmp);

                }

            }

        }

    } // class StartDeferredTasks

    public ScheduledFuture<?> addScheduledTask(final Runnable task,
            final long initialDelay, final long delay, final TimeUnit unit) {

        if (task == null)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled())
            log.info("Scheduling task: task=" + task.getClass()
                    + ", initialDelay=" + initialDelay + ", delay=" + delay
                    + ", unit=" + unit);

		return scheduledExecutorService.scheduleWithFixedDelay(task,
				initialDelay, delay, unit);

    }

	/**
	 * {@inheritDoc}
	 * 
	 * @see Options#COLLECT_PLATFORM_STATISTICS
	 */
    final public boolean getCollectPlatformStatistics() {
		return Boolean.valueOf(properties.getProperty(
				Options.COLLECT_PLATFORM_STATISTICS,
				Options.DEFAULT_COLLECT_PLATFORM_STATISTICS));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see Options#COLLECT_QUEUE_STATISTICS
	 */
	final public boolean getCollectQueueStatistics() {
		return Boolean.valueOf(properties.getProperty(
				Options.COLLECT_QUEUE_STATISTICS,
				Options.DEFAULT_COLLECT_QUEUE_STATISTICS));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see Options#HTTPD_PORT
	 */
	final public int getHttpdPort() {
		return Integer.valueOf(properties.getProperty(Options.HTTPD_PORT,
				Options.DEFAULT_HTTPD_PORT));
	}

    /*
     * Per index counters.
     */

    /**
     * Canonical per-index {@link BTreeCounters}. These counters are set on each
     * {@link AbstractBTree} that is materialized by
     * {@link #getIndexOnStore(String, long, IRawStore)}. The same
     * {@link BTreeCounters} object is used for the unisolated, read-committed,
     * read-historical and isolated views of the index and for each source in
     * the view regardless of whether the source is a mutable {@link BTree} on
     * the live journal or a read-only {@link BTree} on a historical journal.
     * 
     * @see #getIndexCounters(String)
     * @see #dropIndex(String)
     */
    final private ConcurrentHashMap<String/* name */, BTreeCounters> indexCounters = new ConcurrentHashMap<String, BTreeCounters>();

    public BTreeCounters getIndexCounters(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        // first test for existence.
        BTreeCounters t = indexCounters.get(name);

        if (t == null) {

            // not found.  create a new instance.
            t = new BTreeCounters();

            // put iff absent.
            final BTreeCounters oldval = indexCounters.putIfAbsent(name, t);

            if (oldval != null) {

                // someone else got there first so use their instance.
                t = oldval;

            } else {
                
                if (log.isInfoEnabled())
                    log.info("New counters: indexPartitionName=" + name);
                
            }
            
        }

        assert t != null;
        
        return t;
        
    }

    /**
	 * A Journal level semaphore used to restrict applications to a single
	 * unisolated connection. The "unisolated" connection is an application
	 * level construct which supports highly scalable ACID operations but only a
	 * single such "connection" can exist at a time for a Journal. This
	 * constraint arises from the need for the application to coordinate
	 * operations on the low level indices and commit/abort processing while it
	 * holds the permit.
	 * <p>
	 * Note: If by some chance the permit has become "lost" it can be rebalanced
	 * by {@link Semaphore#release()}. However, uses of this {@link Semaphore}
	 * should ensure that it is release along all code paths, including a
	 * finalizer if necessary.
	 */   
	private final Semaphore unisolatedSemaphore = new Semaphore(1/* permits */,
			false/* fair */);

	/**
	 * Acquire a permit for the UNISOLATED connection.
	 * 
	 * @throws InterruptedException
	 */
	public void acquireUnisolatedConnection() throws InterruptedException {

		unisolatedSemaphore.acquire();

		if (log.isDebugEnabled())
			log.debug("acquired semaphore: availablePermits="
					+ unisolatedSemaphore.availablePermits());

		if (unisolatedSemaphore.availablePermits() != 0) {
			/*
			 * Note: This test can not be made atomic with the Semaphore API. It
			 * is possible unbalanced calls to release() could drive the #of
			 * permits in the Semaphore above ONE (1) since the Semaphore
			 * constructor does not place an upper bound on the #of permits, but
			 * rather sets the initial #of permits available. An attempt to
			 * acquire a permit which has a post-condition with additional
			 * permits available will therefore "eat" a permit.
			 */
			throw new IllegalStateException();
		}

	}

	/**
	 * Release the permit for the UNISOLATED connection.
	 * 
	 * @throws IllegalStateException
	 *             unless the #of permits available is zero.
	 */
	public void releaseUnisolatedConnection() {

		if (log.isDebugEnabled())
			log.debug("releasing semaphore: availablePermits="
					+ unisolatedSemaphore.availablePermits());

		if (unisolatedSemaphore.availablePermits() != 0) {
			/*
			 * Note: This test can not be made atomic with the Semaphore API. It
			 * is possible that a concurrent call could drive the #of permits in
			 * the Semaphore above ONE (1) since the Semaphore constructor does
			 * not place an upper bound on the #of permits, but rather sets the
			 * initial #of permits available.
			 */
			throw new IllegalStateException();
		}

		unisolatedSemaphore.release();

	}

}
