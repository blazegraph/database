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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.concurrent.LockManager;
import com.bigdata.counters.CounterSet;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.IDataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.IMetadataService;
import com.bigdata.util.MillisecondTimestampFactory;

/**
 * Concrete implementation suitable for a local and unpartitioned database.
 */
public class Journal extends AbstractJournal implements IConcurrencyManager,
        ILocalTransactionManager, IResourceManager {

    /**
     * Object used to manage local transactions. 
     */
    private final AbstractLocalTransactionManager localTransactionManager; 
    
    /**
     * A static instance is used so that different journals on the same JVM will
     * all use the same underlying time source.
     */
    private static final MillisecondTimestampFactory timestampFactory = new MillisecondTimestampFactory();

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
     
        localTransactionManager = new AbstractLocalTransactionManager(this/* resourceManager */) {
         
            public long nextTimestamp() {

                return timestampFactory.nextMillis();

            }
            
        };
        
        concurrencyManager = new ConcurrencyManager(properties, this, this);
        
        localTransactionManager.setConcurrencyManager(concurrencyManager);
        
    }
    
    public long commit() {

        return commitNow(nextTimestamp());

    }
    
    synchronized public CounterSet getCounters() {
        
        if(counters==null) {

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
     */
    public AbstractBTree[] getIndexSources(String name, long timestamp) {

        final BTree btree;
        
        if (timestamp == ITx.UNISOLATED) {
        
            /*
             * Unisolated operation on the live index.
             */
            
            // MAY be null.
            btree = getIndex(name);

        } else if (timestamp == ITx.READ_COMMITTED) {

            /*
             * Read committed operation against the most recent commit point.
             * 
             * Note: This commit record is always defined, but that does not
             * mean that any indices have been registered.
             */

            final ICommitRecord commitRecord = getCommitRecord();

            final long ts = commitRecord.getTimestamp();

            if (ts == 0L) {

                log.warn("Nothing committed: read-committed operation.");

                return null;

            }

            // MAY be null.
            btree = getIndex(name, commitRecord);

            if (btree != null) {

                /*
                 * Mark the B+Tree as read-only.
                 */
                
                btree.setReadOnly(true);

                assert ((BTree) btree).getLastCommitTime() != 0;
//                btree.setLastCommitTime(commitRecord.getTimestamp());
                
            }
            
        } else {

            /*
             * A specified historical index commit point.
             */
            
            final long ts = Math.abs(timestamp);

            final ICommitRecord commitRecord = getCommitRecord(ts);

            if (commitRecord == null) {

                log.warn("No commit record: timestamp="+ts);
                
                return null;
                
            }

            // MAY be null
            btree = getIndex(name, commitRecord);
        
            if (btree != null) {

                /*
                 * Mark the B+Tree as read-only.
                 */
                
                btree.setReadOnly(true);
                
                assert ((BTree) btree).getLastCommitTime() != 0;
//                btree.setLastCommitTime(commitRecord.getTimestamp());
                
            }

        }
        
        /* 
         * No such index as of that timestamp.
         */

        if (btree == null) {

            log.warn("No such index: timestamp="+timestamp);
            
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
     * Note: logic copied from {@link ResourceManager#getIndex(String, long)}.
     */
    public IIndex getIndex(String name, long timestamp) {
        
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

                final AbstractBTree[] sources = getIndexSources(name, timestamp);

                if (sources == null) {

                    log.warn("No such index: name="+name+", timestamp="+timestamp);
                    
                    return null;

                }

                assert sources.length > 0;
                
                assert sources[0].isReadOnly();

                if (sources.length == 1) {
                    
                    tmp = sources[0];
                    
                } else {
                    
                    tmp = new FusedView(sources);
                    
                }
                
//                if (sources.length == 1) {
//
//                    tmp = new ReadOnlyIndex(sources[0]);
//
//                } else {
//
//                    tmp = new ReadOnlyFusedView(sources);
//
//                }
                            
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

                if (sources.length == 1) {

                    tmp = sources[0];
                    
                } else {
                    
                    tmp = new FusedView( sources );
                    
                }

            }

        }
        
        return tmp;

    }

    public ITx getTx(long startTime) {
        return localTransactionManager.getTx(startTime);
    }

    public long nextTimestamp() {
        return localTransactionManager.nextTimestamp();
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
    public void shutdown() {
        
        concurrencyManager.shutdown();
       
        localTransactionManager.shutdown();
        
        super.shutdown();
        
    }

    /**
     * Note: The {@link IConcurrencyManager} is shutdown first, then the
     * {@link ITransactionManager} and finally the {@link IResourceManager}.
     */
    public void shutdownNow() {
        
        concurrencyManager.shutdownNow();
        
        localTransactionManager.shutdownNow();

        super.shutdownNow();
        
    }

    public Future<Object> submit(AbstractTask task) {

        return concurrencyManager.submit(task);
        
    }

    public List<Future<Object>> invokeAll(Collection<AbstractTask> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        
        return concurrencyManager.invokeAll(tasks, timeout, unit);
        
    }

    public List<Future<Object>> invokeAll(Collection<AbstractTask> tasks) throws InterruptedException {
        
        return concurrencyManager.invokeAll(tasks);
        
    }

    public LockManager<String> getLockManager() {

        return concurrencyManager.getLockManager();
        
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
    public ILoadBalancerService getLoadBalancerService() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    public IMetadataService getMetadataService() {

        throw new UnsupportedOperationException();
        
    }
    
    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    public IDataService getDataService(UUID uuid) {
        
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

    public String getStatistics(String name, long timestamp) {

        IIndex ndx = getIndex(name, timestamp);
        
        if(ndx==null) return null;
        
        return ndx.getStatistics();
        
    }

}
