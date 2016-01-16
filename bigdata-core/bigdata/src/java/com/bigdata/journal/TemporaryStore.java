/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Feb 21, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bfs.GlobalFileSystemHelper;
import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.DaemonThreadFactory;

/**
 * A temporary store that supports named indices but no concurrency controls.
 * <p>
 * If you want a temporary store that supports named indices and concurrency
 * controls then choose a {@link Journal} with {@link BufferMode#Temporary}.
 * This has the advantage of full concurrency support, group commit, and
 * low-latency startup (since the file is not created until the store attempts
 * to write through to the disk). However, {@link TemporaryStore} is lighter
 * weight precisely because it does not provide concurrency control.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/585" > GIST </a>
 */
//* {@link #checkpoint()} may be used to checkpoint the indices and
//* {@link #restoreLastCheckpoint()} may be used to revert to the last
//* checkpoint. If you note the checkpoint addresses from {@link #checkpoint()}
//* then you can restore any checkpoint with {@link #restoreCheckpoint(long)}
public class TemporaryStore extends TemporaryRawStore implements IBTreeManager {

    private static final Logger log = Logger.getLogger(TemporaryStore.class);

    /**
     * The size of the live index cache for the {@link Name2Addr} instance.
     * 
     * @todo this should be a configuration property once the temporary store
     *       accepts a {@link Properties} object in its ctor.
     * 
     * @see Options#DEFAULT_LIVE_INDEX_CACHE_CAPACITY
     */
    private final int liveIndexCacheCapacity = 20;

    /**
     * The timeout in milliseconds for stale entries in the live index cache for
     * the {@link Name2Addr} instance.
     * 
     * @todo this should be a configuration property once the temporary store
     *       accepts a {@link Properties} object in its ctor.
     * 
     * @see Options#DEFAULT_LIVE_INDEX_CACHE_TIMEOUT
     */
    private final long liveIndexCacheTimeout = Long
            .parseLong(Options.DEFAULT_LIVE_INDEX_CACHE_TIMEOUT);
    
    /**
     * BTree mapping index names to the last metadata record committed for the
     * named index. The keys are index names (unicode strings). The values are
     * the last known address of the named btree.
     * <p>
     * Note: This is a mutable {@link BTree} so it is NOT thread-safe. We always
     * synchronize on this object before accessing it.
     */
    private final Name2Addr name2Addr;

    /**
     * A {@link TemporaryStore} that can scale-up. The backing file will be
     * created using the Java temporary file mechanism.
     * 
     * @see WormAddressManager#SCALE_UP_OFFSET_BITS
     * @see #getTempFile()
     */
    public TemporaryStore() {

        this(WormAddressManager.SCALE_UP_OFFSET_BITS);
        
    }

    /**
     * A {@link TemporaryStore} provisioned with the specified <i>offsetBits</i>.
     * The backing file will be created using the Java temporary file mechanism.
     * 
     * @param offsetBits
     *            This determines the capacity of the store file and the maximum
     *            length of a record. The value is passed through to
     *            {@link WormAddressManager#WormAddressManager(int)}.
     */
    public TemporaryStore(final int offsetBits) {

        this(offsetBits, getTempFile());
        
    }

    /**
     * A {@link TemporaryStore} provisioned with the specified <i>offsetBits</i>
     * and backed by the specified file.
     * 
     * @param offsetBits
     *            This determines the capacity of the store file and the maximum
     *            length of a record. The value is passed through to
     *            {@link WormAddressManager#WormAddressManager(int)}.
     * @param file
     *            The backing file (may exist, but must be empty if it exists).
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public TemporaryStore(final int offsetBits, final File file) {

        super(0L/* maximumExtent */, offsetBits, file);

        name2Addr = Name2Addr.create(this);

        name2Addr.setupCache(liveIndexCacheCapacity, liveIndexCacheTimeout);

        executorService = Executors.newCachedThreadPool(new DaemonThreadFactory
                (getClass().getName()+".executorService"));
        
        resourceLocator = new DefaultResourceLocator(//
                this,//
                null // delegate
        );
        
    }
    
//    /**
//     * Setup the btree that resolved named btrees.
//     */
//    private void setupName2AddrBTree() {
//
//        assert name2Addr == null;
//        
//        name2Addr = Name2Addr.create(this);
//
//        name2Addr.setupCache(liveIndexCacheCapacity, liveIndexCacheTimeout);
//        
//    }
    
//    /**
//     * The address of the last checkpoint written. When ZERO(0L) no checkpoint
//     * has been written and {@link #name2Addr} is simple discarded on
//     * {@link #abort()}.
//     */
//    private long lastCheckpointAddr = 0L;
//
//    /**
//     * Reverts to the last checkpoint, if any. If there is no last checkpoint,
//     * then the post-condition is as if the store had never been written on
//     * (except that the storage on the backing file is not reclaimed).
//     */
//    public void restoreLastCheckpoint() {
//        
//        restoreCheckpoint(lastCheckpointAddr);
//        
//    }
//    
//    /**
//     * Reverts to the checkpoint associated with the given <i>checkpointAddr</i>.
//     * When ZERO(0L), the post-condition is as if the store had never been
//     * written on (except that the storage on the backing file is not
//     * reclaimed). The <i>checkpointAddr</i> is noted as the current
//     * {@link #restoreLastCheckpoint()} point.
//     */
//    public void restoreCheckpoint(final long checkpointAddr) {
//
//        assertOpen();
//
//        name2Addr = null;
//        
//        if (checkpointAddr != 0L) {
//
//            name2Addr = (Name2Addr) Name2Addr
//                    .load(this, checkpointAddr, false/* readOnly */);
//
//        } else {
//            
//            setupName2AddrBTree();
//            
//        }
//        
//        // note the restore point.
//        lastCheckpointAddr = checkpointAddr;
//
//    }
//
//    /**
//     * Checkpoints the dirty indices and notes the new
//     * {@link #restoreLastCheckpoint()} point. You can revert to the last
//     * written checkpoint using {@link #restoreLastCheckpoint()} or to an
//     * arbitrary checkpoint using {@link #restoreCheckpoint(long)}.
//     * <p>
//     * Note: {@link ITx#READ_COMMITTED} views of indices become available after
//     * a {@link #checkpoint()}. If the store has not been checkpointed, then
//     * the read committed views are unavailable for an index. After a checkpoint
//     * in which a given index was dirty, a new read-committed view is available
//     * for that index and checkpoint.
//     * <p>
//     * Note: This is NOT an atomic commit protocol, but the restore point will
//     * be updated iff the checkpoint succeeds.
//     * 
//     * @return The checkpoint address.
//     */
//    public long checkpoint() {
//
//        assertOpen();
//
//        // checkpoint the indices and note the restore point.
//        return lastCheckpointAddr = name2Addr.handleCommit(System
//                .currentTimeMillis());
//        
//    }

    @Override
    public void registerIndex(final IndexMetadata metadata) {
        
        registerIndex(metadata.getName(), metadata);
        
    }
    
    @Override
    public BTree registerIndex(final String name, final IndexMetadata metadata) {

        return (BTree) register(name, metadata);

    }

    /**
     * Variant method creates and registered a named persistence capable data
     * structure but does not assume that the data structure will be a
     * {@link BTree}.
     * 
     * @param store
     *            The backing store.
     * @param metadata
     *            The metadata that describes the data structure to be created.
     * 
     * @return The persistence capable data structure.
     * 
     * @see Checkpoint#create(IRawStore, IndexMetadata)
     */
    @Override
    public ICheckpointProtocol register(final String name,
            final IndexMetadata metadata) {

        final ICheckpointProtocol ndx = Checkpoint.create(this, metadata);

        register(name, ndx);

        return ndx;

    }

    @Override
    final public BTree registerIndex(final String name, final BTree btree) {

        registerIndex(name, btree);

        return btree;
        
    }
    
    /**
     * Register a named persistence capable data structure (core impl).
     * 
     * @param name
     *            The name.
     * @param ndx
     *            The data structure.
     */
    private final void register(final String name, final ICheckpointProtocol ndx) {

        synchronized (name2Addr) {

            assertOpen();

            // add to the persistent name map.
            name2Addr.registerIndex(name, ndx);

        }

    }
    
    @Override
    public void dropIndex(final String name) {
        
        synchronized(name2Addr) {

            assertOpen();

            // drop from the persistent name map.
            name2Addr.dropIndex(name);
            
        }
        
    }

    @Override
    public Iterator<String> indexNameScan(final String prefix,
            final long timestampIsIgnored) {

        final List<String> names = new LinkedList<String>();

        synchronized (name2Addr) {

            final Iterator<String> itr = Name2Addr.indexNameScan(prefix,
                    name2Addr);

            while (itr.hasNext()) {

                names.add(itr.next());

            }

        }

        return names.iterator();

    }
    
    @Override
    public ICheckpointProtocol getUnisolatedIndex(final String name) {

        synchronized(name2Addr) {

            assertOpen();

            return name2Addr.getIndex(name);
            
        }
        
    }
    
//    /**
//     * Return an {@link ITx#UNISOLATED} view of the named index -or-
//     * <code>null</code> if there is no registered index by that name.
//     */
    @Override
    public BTree getIndex(final String name) {

        return (BTree) getUnisolatedIndex(name);

    }

//    /**
//     * Return an {@link ITx#UNISOLATED} view of the named index -or-
//     * <code>null</code> if there is no registered index by that name.
//     */
//    public HTree getHTree(final String name) {
//
//        return (HTree) getUnisolatedIndex(name);
//
//    }

    @Override
    public BTree getIndex(final String name, final long timestamp) {
        
        return (BTree) getIndexLocal(name, timestamp);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: Requests for historical reads or read-only tx views will result
     * return the {@link ITx#UNISOLATED} index view.
     * <p>
     * Note: If {@link ITx#READ_COMMITTED} is requested, then the returned
     * {@link BTree} will reflect the state of the named index as of the last
     * {@link #checkpoint()}. This view will be read-only and is NOT updated by
     * {@link #checkpoint()}. You must actually {@link #checkpoint()} before an
     * {@link ITx#READ_COMMITTED} view will be available. When there is not a
     * checkpoint available, the {@link ITx#UNISOLATED} index view will be
     * returned.
     */// Note: core impl for all index access methods.
    @Override
    public ICheckpointProtocol getIndexLocal(final String name,
            final long commitTime) {
        
        assertOpen();

        if (commitTime == ITx.READ_COMMITTED) {
            
            final long checkpointAddr;
            
            synchronized(name2Addr) {
                
                final Entry entry = name2Addr.getEntry(name);
                
                if (entry == null) {

                    log.warn("No such index: name=" + name + ", timestamp="
                            + TimestampUtility.toString(commitTime));
                    
                    return null;
                    
                }

                checkpointAddr = entry.checkpointAddr;
                
            }

            /*
             * Load from the store.
             * 
             * TODO There is no canonicalizaing mapping for read-only indices
             * for the TemporaryStore. Each such request will produce a distinct
             * view of that index. This could be very wasteful of resources
             * (RAM).
             */
            final ICheckpointProtocol ndx = Checkpoint.loadFromCheckpoint(this,
                    checkpointAddr, true/* readOnly */);
            
//            final BTree btree = BTree
//                    .load(this, checkpointAddr, true/*readOnly*/);
            
//            btree.setReadOnly(true);
            
            return ndx;
            
        }
        
        if (commitTime == ITx.UNISOLATED) {

            return getUnisolatedIndex(name);

        }

        /**
         * FIXME The RWStore uses a read-only transaction to protect against
         * recycling of the B+Tree revisions associated with the commit point on
         * which it is reading. The temporary store only supports unisolated
         * reads, so this is just ignoring the tx specified by the mutation rule
         * for reading on the temporary store and going with the unisolated
         * index anyway.
         * <p>
         * As a work around, I have modified the TemporaryStore in the
         * JOURNAL_HA_BRANCH to use the unisolated view of the index. This
         * allows the RWStore to be used in combination with incremental truth
         * maintenance.
         * <p>
         * In the BOP model, the timestamp is associated with the predicate.
         * This means that we can correctly annotate the predicate reading on
         * the RWStore with the read-only tx identifier and the predicate
         * reading on the TemporaryStore with the UNISOLATED timestamp (0L).
         * Therefore, once the RWStore is brought into the QUADS_QUERY_BRANCH we
         * can modify the rules to specify UNISOLATED for the reads on the
         * TemporaryStore and the tx for the reads on the RWStore and then roll
         * back the change to TemporaryStore? to force the use of the unisolated
         * index when a read-only tx was specified.
         * 
         * @see <a hreg="https://sourceforge.net/apps/trac/bigdata/ticket/215" >
         *      RWStore / TemporaryStore interaction when performing incremental
         *      truth maintenance </a>
         */
        
//        throw new UnsupportedOperationException(
//                "Not supported: timestamp="
//                + TimestampUtility.toString(timestamp));

        return getUnisolatedIndex(name);

    }

    @Override
    public SparseRowStore getGlobalRowStore() {

        assertOpen();
        
        return globalRowStoreHelper.getGlobalRowStore();
        
    }
    final private GlobalRowStoreHelper globalRowStoreHelper = new GlobalRowStoreHelper(this); 

    @Override
    public SparseRowStore getGlobalRowStore(final long timestamp) {

        // TODO Perhaps only allowed for UNISOLATED on a TemporaryStore? 
        return globalRowStoreHelper.get(timestamp);
        
    }
    
    @Override
    public BigdataFileSystem getGlobalFileSystem() {

        assertOpen();
        
        return globalFileSystemHelper.getGlobalFileSystem();
        
    }
    final private GlobalFileSystemHelper globalFileSystemHelper = new GlobalFileSystemHelper(this); 

    @Override
    public DefaultResourceLocator<?> getResourceLocator() {

        assertOpen();
        
        return resourceLocator;
        
    }
    private final DefaultResourceLocator<?> resourceLocator;
    
    @Override
    public ExecutorService getExecutorService() {
    
        assertOpen();
        
        return executorService;
        
    }
    private final ExecutorService executorService;
    
    @Override
    final public IResourceLockService getResourceLockService() {

        return resourceLockManager;
        
    }
    final private ResourceLockService resourceLockManager = new ResourceLockService();

    @Override
    public void close() {

        // immediate shutdown.
        executorService.shutdownNow();
        
        super.close();
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation always returns ZERO (0L) since you can not perform a
     * commit on a {@link TemporaryRawStore} (it supports checkpoints but not
     * commits).
     */
    @Override
	public long getLastCommitTime() {

		return 0L;
		
	}
    
    /**
     * The {@link TemporaryStore} does not support group commit.
     */
    @Override
    public boolean isGroupCommit() {
       
       return false;
       
    }

    /**
     * Always returns <i>this</i> {@link TemporaryStore}.
     */
    @Override
    public TemporaryStore getTempStore() {
        
        return this;
        
    }

    /**
     * Not supported, returns <code>null</code>.
     */
    @Override
	public ScheduledFuture<?> addScheduledTask(Runnable task,
			long initialDelay, long delay, TimeUnit unit) {
		return null;
	}

    /**
     * Not supported, returns <code>false</code>.
     */
    @Override
	public boolean getCollectPlatformStatistics() {
		return false;
	}

    /**
     * Not supported, returns <code>false</code>.
     */
    @Override
	public boolean getCollectQueueStatistics() {
		return false;
	}

    /**
     * Not supported, returns <code>false</code>.
     */
    @Override
	public int getHttpdPort() {
		return -1;
	}

}
