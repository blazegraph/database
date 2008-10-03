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
import java.util.concurrent.TimeUnit;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bfs.GlobalFileSystemHelper;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.ReadCommittedView;
import com.bigdata.counters.CounterSet;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.StaleLocatorReason;
import com.bigdata.service.AbstractEmbeddedResourceLockManager;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.MillisecondTimestampFactory;
import com.bigdata.util.concurrent.DaemonThreadFactory;

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
        ILocalTransactionManager, IResourceManager {

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
            com.bigdata.journal.ConcurrencyManager.Options {

    }
    
    /**
     * Create or re-open a journal.
     * 
     * @param properties
     *            See {@link com.bigdata.journal.Options}.
     */
    public Journal(Properties properties) {
        
        super(properties);
     
        executorService = Executors.newCachedThreadPool(DaemonThreadFactory
                .defaultThreadFactory());

        resourceLocator = new DefaultResourceLocator(this, null/*delegate*/);

        resourceLockManager = new AbstractEmbeddedResourceLockManager(UUID
                .randomUUID(), properties) {

            public AbstractFederation getFederation() {

                throw new UnsupportedOperationException();

            }

        }.start();

        localTransactionManager = new AbstractLocalTransactionManager(this/* resourceManager */) {

            public long nextTimestamp() {

                return MillisecondTimestampFactory.nextMillis();

            }

        };
        
        concurrencyManager = new ConcurrencyManager(properties, this, this);

        localTransactionManager.setConcurrencyManager(concurrencyManager);

    }

    public ILocalTransactionManager getLocalTransactionManager() {

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
        
        File file = getFile();
        
        if (file == null) {

            return null;

        }
        
        return file.getParentFile();
        
    }

    /**
     * Note: This will only succeed if the <i>uuid</i> identifies <i>this</i>
     * journal.
     */
    public IRawStore openStore(UUID uuid) {
    
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
                
                assert ((BTree) btree).getLastCommitTime() != 0;
//                btree.setLastCommitTime(commitRecord.getTimestamp());
                
            }

        }
        
        /* 
         * No such index as of that timestamp.
         */

        if (btree == null) {

            log.warn("No such index: name="+name+", timestamp="+timestamp);
            
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
    
    /*
     * ILocalTransactionManager
     */

//    public int getActiveTxCount() {
//        return transactionManager.getActiveTxCount();
//    }
//    
//    public int getPreparedTxCount() {
//        return transactionManager.getPreparedTxCount();
//    }
    
    public void activateTx(ITx tx) throws IllegalStateException {
        localTransactionManager.activateTx(tx);
    }

    public void completedTx(ITx tx) throws IllegalStateException {
        localTransactionManager.completedTx(tx);
    }

    /**
     * Note: {@link ITx#READ_COMMITTED} views are given read-committed semantics
     * using a {@link ReadCommittedView}.  This means that they can be cached
     * since the view will update automatically as commits are made against
     * the {@link Journal}.
     *  
     * @see IndexManager#getIndex(String, long)
     */
    public IIndex getIndex(final String name, final long timestamp) {
        
        if (name == null) {

            throw new IllegalArgumentException();

        }

        final boolean isTransaction = timestamp > ITx.UNISOLATED;
        
        final ITx tx = (isTransaction ? getConcurrencyManager()
                .getTransactionManager().getTx(timestamp) : null); 
        
        if(isTransaction) {

            if(tx == null) {
                
                log.warn("Unknown transaction: name="+name+", tx="+timestamp);
                
                return null;
                    
            }
            
            if(!tx.isActive()) {
                
                // typically this means that the transaction has already prepared.
                log.warn("Transaction not active: name=" + name + ", tx="
                        + timestamp + ", prepared=" + tx.isPrepared()
                        + ", complete=" + tx.isComplete() + ", aborted="
                        + tx.isAborted());

                return null;
                
            }
                                
        }
        
        if( isTransaction && tx == null ) {
        
            /*
             * Note: This will happen both if you attempt to use a transaction
             * identified that has not been registered or if you attempt to use
             * a transaction manager after the transaction has been either
             * committed or aborted.
             */
            
            log.warn("No such transaction: name=" + name + ", tx=" + tx);

            return null;
            
        }
        
        final boolean readOnly = (timestamp < ITx.UNISOLATED)
                || (isTransaction && tx.isReadOnly());

        final IIndex tmp;

        if (isTransaction) {

            /*
             * Isolated operation.
             * 
             * Note: The backing index is always a historical state of the named
             * index.
             */

            final IIndex isolatedIndex = tx.getIndex(name);

            if (isolatedIndex == null) {

                log.warn("No such index: name="+name+", tx="+timestamp);
                
                return null;

            }

            tmp = isolatedIndex;

        } else {
            
            /*
             * historical read -or- unisolated read operation.
             */

            if (readOnly) {

                if (timestamp == ITx.READ_COMMITTED) {

                    tmp = new ReadCommittedView(this, name);

                } else {

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

                    log.warn("No such index: name="+name+", timestamp="+timestamp);
                    
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
    public AbstractBTree[] getIndexSources(String name, long timestamp,
            BTree btree) {
        
        return new AbstractBTree[] { btree };
        
    }

    public ITx getTx(long startTime) {
        return localTransactionManager.getTx(startTime);
    }

    public long nextTimestamp() throws IOException {
        return localTransactionManager.nextTimestamp();
    }

    public long nextTimestampRobust() {
        return localTransactionManager.nextTimestampRobust();
    }

    public void prepared(ITx tx) throws IllegalStateException {
        localTransactionManager.prepared(tx);
    }

    public long newTx(IsolationEnum level) {
        
        return localTransactionManager.newTx(level);
        
    }
    
    public void abort(long startTime) {
        
        localTransactionManager.abort(startTime);
        
    }

    public long commit(long startTime) throws ValidationError {

        return localTransactionManager.commit(startTime);

    }

    public void wroteOn(long startTime, String[] resource) {

        localTransactionManager.wroteOn(startTime, resource);
        
    }

    /*
     * IConcurrencyManager
     */
    
    private final ConcurrencyManager concurrencyManager;

    public ConcurrencyManager getConcurrencyManager() {
        
        return concurrencyManager;
        
    }
    
    /**
     * Note: The {@link IConcurrencyManager} is shutdown first, then the
     * {@link ITransactionManager} and finally the {@link IResourceManager}.
     */
    synchronized public void shutdown() {
        
        if (!isOpen())
            return;

        executorService.shutdown();

        try {

            final long shutdownTimeout = 2;

            final TimeUnit unit = TimeUnit.SECONDS;

            final long begin = System.currentTimeMillis();

            long elapsed = System.currentTimeMillis() - begin;

            if (!executorService.awaitTermination(shutdownTimeout - elapsed,
                    unit)) {

                log.warn("timeout: elapsed=" + elapsed);

            }

        } catch (InterruptedException ex) {

            log.warn("Awaiting executor service: "+ex);

        }

        concurrencyManager.shutdown();
       
        localTransactionManager.shutdown();
        
        if (resourceLockManager != null) {

            resourceLockManager.shutdown();

            resourceLockManager = null;

        }
        
        super.shutdown();
        
    }

    /**
     * Note: The {@link IConcurrencyManager} is shutdown first, then the
     * {@link ITransactionManager} and finally the {@link IResourceManager}.
     */
    synchronized public void shutdownNow() {

        if(!isOpen()) return;

        executorService.shutdownNow();
        
        concurrencyManager.shutdownNow();
        
        localTransactionManager.shutdownNow();

        resourceLockManager.shutdownNow();

        super.shutdownNow();
        
    }

    public Future<? extends Object> submit(AbstractTask task) {

        return concurrencyManager.submit(task);
        
    }

    public List<Future<? extends Object>> invokeAll(Collection<AbstractTask> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        
        return concurrencyManager.invokeAll(tasks, timeout, unit);
        
    }

    public List<Future<? extends Object>> invokeAll(Collection<AbstractTask> tasks) throws InterruptedException {
        
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
    
    public Future<Object> overflow() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * This request is always ignored for a {@link Journal} since it does not
     * have any resources to manage.
     */
    public void setReleaseTime(long timestamp) {

        if (INFO)
            log.info("Request ignored for Journal: timestamp="+timestamp);
        
    }

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
    public UUID getDataServiceUUID() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    public UUID[] getDataServiceUUIDs() {

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
    private final TemporaryStoreFactory tempStoreFactory = new TemporaryStoreFactory();

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
