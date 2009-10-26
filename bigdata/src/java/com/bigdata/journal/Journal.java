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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bfs.GlobalFileSystemHelper;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.ReadCommittedView;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.config.IntegerValidator;
import com.bigdata.config.LongValidator;
import com.bigdata.counters.CounterSet;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.StaleLocatorReason;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.ShutdownHelper;

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

    }
    
    /**
     * Create or re-open a journal.
     * 
     * @param properties
     *            See {@link com.bigdata.journal.Options}.
     */
    public Journal(final Properties properties) {
        
        super(properties);
     
        tempStoreFactory = new TemporaryStoreFactory(properties);
        
        executorService = Executors.newCachedThreadPool(new DaemonThreadFactory
                (getClass().getName()+".executorService"));

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
        
        final JournalTransactionService abstractTransactionService = new JournalTransactionService(
                properties, this).start();

        localTransactionManager = new AbstractLocalTransactionManager() {

            public AbstractTransactionService getTransactionService() {
                
                return abstractTransactionService;
                
            }

            /**
             * Extended to shutdown the embedded transaction service.
             */
            public void shutdown() {

                ((JournalTransactionService) getTransactionService())
                        .shutdown();

                super.shutdown();

            }

            /**
             * Extended to shutdown the embedded transaction service.
             */
            public void shutdownNow() {

                ((JournalTransactionService) getTransactionService())
                        .shutdownNow();

                super.shutdownNow();

            }
        
        };

        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, this);

    }

    public AbstractLocalTransactionManager getLocalTransactionManager() {

        return localTransactionManager;

    }

    synchronized public CounterSet getCounters() {

        if (counters == null) {

            counters = super.getCounters();
            
            counters.attach(concurrencyManager.getCounters());
    
            counters.attach(localTransactionManager.getCounters());
            
        }
        
        return counters;
        
    }
    private CounterSet counters;
    
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
     * are no {@link ICommitRecord}s that satisify the probe or if the named
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
    final public AbstractJournal getJournal(long timestamp) {
        
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

            return localTransactionManager.getTransactionService().newTx(
                    timestamp);

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
    
    private final ConcurrencyManager concurrencyManager;

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

        if(!isOpen()) return;

        executorService.shutdownNow();
        
        concurrencyManager.shutdownNow();
        
        localTransactionManager.shutdownNow();

        super.shutdownNow();
        
    }
    
    public void deleteResources() {
        
        super.deleteResources();
        
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
    public IBigdataFederation getFederation() {

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

//    /**
//     * @throws UnsupportedOperationException
//     *             always.
//     */
//    public UUID[] getDataServiceUUIDs() {
//
//        throw new UnsupportedOperationException();
//        
//    }

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
    synchronized public SparseRowStore getGlobalRowStore() {

        if (globalRowStoreHelper == null) {

            globalRowStoreHelper = new GlobalRowStoreHelper(this);

        }

        return globalRowStoreHelper.getGlobalRowStore();

    }
    private GlobalRowStoreHelper globalRowStoreHelper;

    /*
     * global file system.
     */
    synchronized public BigdataFileSystem getGlobalFileSystem() {

        if (globalFileSystemHelper == null) {

            globalFileSystemHelper = new GlobalFileSystemHelper(this);

        }

        return globalFileSystemHelper.getGlobalFileSystem();

    }
    private GlobalFileSystemHelper globalFileSystemHelper;

    protected void discardCommitters() {

        super.discardCommitters();

        globalRowStoreHelper = null;

    }
    
    public TemporaryStore getTempStore() {
        
        return tempStoreFactory.getTempStore();
        
    }
    private final TemporaryStoreFactory tempStoreFactory;

    public DefaultResourceLocator getResourceLocator() {

        assertOpen();
        
        return resourceLocator;
        
    }
    private final DefaultResourceLocator resourceLocator;
    
    public IResourceLockService getResourceLockService() {
        
        assertOpen();
        
        return resourceLockManager;
        
    }
    private ResourceLockService resourceLockManager;

    public ExecutorService getExecutorService() {
        
        assertOpen();
        
        return executorService;
        
    }
    private final ExecutorService executorService;

}
