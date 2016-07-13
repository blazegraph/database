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
package com.bigdata.btree;

import java.io.IOException;

import com.bigdata.btree.AbstractBTreeTupleCursor.AbstractCursorPosition;
import com.bigdata.btree.IndexSegment.ImmutableNodeFactory.ImmutableLeaf;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.data.INodeData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.FixedByteArrayBuffer;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;
import com.bigdata.service.EventType;

/**
 * An index segment is read-only btree corresponding to some key range of a
 * potentially distributed index. The file format of the index segment includes
 * a metadata record, the leaves of the segment in key order, and the nodes of
 * the segment in an arbitrary order. It is possible to map or buffer the part
 * of the file containing the index nodes or the entire file depending on
 * application requirements.
 * <p>
 * Note: iterators returned by this class do not support removal (the nodes and
 * leaves will all refuse mutation operations).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegment extends AbstractBTree {//implements ILocalBTreeView {

    /**
     * Type safe reference to the backing store.
     */
    private final IndexSegmentStore fileStore;

    /**
     * Iff events are being reported.
     */
    private volatile Event openCloseEvent = null;

//    /**
//     * An LRU for {@link ImmutableLeaf}s. This cache takes advantage of the
//     * fact that the {@link LeafIterator} can read leaves without navigating
//     * down the node hierarchy.  This reference is set to <code>null</code> 
//     * if the {@link IndexSegment} is {@link #close()}d.
//     */
//    private ConcurrentWeakValueCacheWithTimeout<Long, ImmutableLeaf> leafCache;

//    /**
//     * Return the approximate #of open leaves and zero if the
//     * {@link IndexSegment} is not open.
//     */
//    final public int getOpenLeafCount() {
//
//        final ConcurrentWeakValueCacheWithTimeout<Long, ImmutableLeaf> tmp = leafCache;
//
//        if (tmp == null) {
//
//            return 0;
//            
//        }
//        
//        return tmp.size();
//        
//    }
//
//    /**
//     * The approximate #of bytes in the in-memory {@link IndexSegment} leaves
//     * -or- ZERO (0) if the {@link IndexSegment} is closed.
//     */
//    public long getOpenLeafByteCount() {
//
//        final ConcurrentWeakValueCacheWithTimeout<Long, ImmutableLeaf> tmp = leafCache;
//
//        if (tmp == null)
//            return 0L;
//
//        final Iterator<WeakReference<ImmutableLeaf>> itr = tmp.iterator();
//
//        long leafByteCount = 0;
//
//        while (itr.hasNext()) {
//
//            final ImmutableLeaf leaf = itr.next().get();
//
//            if (leaf != null) {
//
//                leafByteCount += fileStore.getByteCount(leaf.identity);
//
//            }
//
//        }
//
//        return leafByteCount;
//        
//    }

    @Override
    final public int getHeight() {
        // Note: fileStore.checkpoint is now final. reopen() is not required.
//        reopen();

        return fileStore.getCheckpoint().height;

    }

    @Override
    final public long getNodeCount() {
        // Note: fileStore.checkpoint is now final. reopen() is not required.
//        reopen();

        return fileStore.getCheckpoint().nnodes;

    }

    @Override
    final public long getLeafCount() {
        // Note: fileStore.checkpoint is now final. reopen() is not required.
//        reopen();

        return fileStore.getCheckpoint().nleaves;

    }

    @Override
    final public long getEntryCount() {
        // Note: fileStore.checkpoint is now final. reopen() is not required.
//        reopen();

        return fileStore.getCheckpoint().nentries;

    }

    @Override
    final public ICheckpoint getCheckpoint() {
        
        return fileStore.getCheckpoint();
        
    }

    @Override
    public long getRecordVersion() {
        return getCheckpoint().getRecordVersion();
    }

    @Override
    public long getMetadataAddr() {
        return getCheckpoint().getMetadataAddr();
    }

    @Override
    public long getRootAddr() {
        return getCheckpoint().getRootAddr();
    }

    @Override
    public void setLastCommitTime(long lastCommitTime) {
        throw new UnsupportedOperationException(ERROR_READ_ONLY);
    }

    @Override
    public long writeCheckpoint() {
        throw new UnsupportedOperationException(ERROR_READ_ONLY);
    }

    @Override
    public Checkpoint writeCheckpoint2() {
        throw new UnsupportedOperationException(ERROR_READ_ONLY);
    }

    @Override
    public IDirtyListener getDirtyListener() {
        throw new UnsupportedOperationException(ERROR_READ_ONLY);
    }

    @Override
    public void setDirtyListener(IDirtyListener listener) {
        throw new UnsupportedOperationException(ERROR_READ_ONLY);
    }

    @Override
    public long handleCommit(long commitTime) {
        throw new UnsupportedOperationException(ERROR_READ_ONLY);
    }

    @Override
    public void invalidate(Throwable t) {
        // NOP. Not a mutable index.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to be thread-safe without requiring any locks.
     */
    public String toString() {

        /*
         * I moved to code to generate the human friendly representation into
         * this class in order to avoid any locking. Taking a lock in toString()
         * just makes me nervous. Most of the information is in the
         * IndexSegmentCheckpoint, which is a final reference on the
         * IndexSegmentStore. The rest of the information is on the
         * IndexMetadata object, which is also final on the IndexSegmentStore.
         * 
         * We could probably rely on the overrides of the relevant access
         * methods on IndexSegment, e.g., getEntryCount(), etc. and just use
         * super.toString(). However, it seems safer and more future proof to
         * directly access the relevant fields from within this implementation.
         */
        final StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getSimpleName());
        
        sb.append("{ ");
        
        // Note: Guaranteed available on the IndexSegmentStore.
        final IndexMetadata metadata = fileStore.getIndexMetadata();

        if (metadata.getName() != null) {

            sb.append("name=" + metadata.getName());

        } else {

            sb.append("uuid=" + metadata.getIndexUUID());

        }
        
        // Note: Guaranteed available on the IndexSegmentStore.
        final IndexSegmentCheckpoint chk = fileStore.getCheckpoint();
        
        // Note: [branchingFactor is final on AbstractBTree
        sb.append(", m=" + branchingFactor);//getBranchingFactor());

        sb.append(", height=" + chk.height);

        sb.append(", entryCount=" + chk.nentries);

        sb.append(", nodeCount=" + chk.nnodes);

        sb.append(", leafCount=" + chk.nleaves);

        sb.append(", lastCommitTime=" + chk.commitTime);

        sb.append("}");
        
        return sb.toString();

    }

//    /**
//     * Returns ZERO (0) in order to disable the read-retention queue.
//     * <p>
//     * Note: The read-retention queue is <em>disabled</em> for the
//     * {@link IndexSegment}. Instead the {@link IndexSegment} relies on the
//     * write-retention queue (all touched nodes and leaves are placed on that
//     * queue) and its {@link #leafCache}. The capacity of the write-retention
//     * queue is the right order of magnitude to fully buffer the visited nodes
//     * of an {@link IndexSegment} while the {@link #leafCache} capacity and
//     * whether or not the nodes region are fully buffered may be used to control
//     * the responsiveness for leaves.
//     */
//    @Override
//    final protected int getReadRetentionQueueCapacity() {
//        
//        return 0;
//        
//    }
//    
//    @Override
//    final protected int getReadRetentionQueueScan() {
//        
//        return 0;
//        
//    }

    /**
     * Open a read-only index segment.
     * 
     * @param fileStore
     *            The store containing the {@link IndexSegment}.
     * 
     * @see IndexSegmentStore#loadIndexSegment()
     */
    public IndexSegment(final IndexSegmentStore fileStore) {

        super(fileStore,//
                ImmutableNodeFactory.INSTANCE,//
                true, // always read-only
                fileStore.getIndexMetadata(),//
                fileStore.getIndexMetadata().getIndexSegmentRecordCompressorFactory()
                );

        // Type-safe reference to the backing store.
        this.fileStore = (IndexSegmentStore) fileStore;

        _reopen();

    }

    /**
     * Extended to also close the backing file.
     */
    @Override
    public void close() {

        if (root == null) {

            throw new IllegalStateException(ERROR_CLOSED);

        }

        fileStore.lock.lock();
        try {

//            // release the leaf cache.
//            leafCache.clear();
//            leafCache = null;

            // release buffers and hard reference to the root node.
            super.close();

            // close the backing file (can be re-opened).
            fileStore.close();

            if (openCloseEvent != null) {

                openCloseEvent.end();

                openCloseEvent = null;

            }
            
        } finally {

            fileStore.lock.unlock();
            
        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to a more constrained type.
     */
    @Override
    final public IndexSegmentStore getStore() {
        
        return fileStore;
        
    }

    /**
     * Extended to explicitly close the {@link IndexSegment} and the backing
     * {@link IndexSegmentStore}. A finalizer is necessary for this class
     * because we maintain {@link IndexSegment}s in a weak value cache and do
     * not explicitly close then before their reference is cleared. This leads
     * to the {@link IndexSegmentStore} being left open. The finalizer fixes
     * that.
     */
    @Override
    protected void finalize() throws Throwable {

        if (isOpen()) {

            if (INFO)
                log.info("Closing IndexSegment: " + fileStore.getFile());
            
            close();

        }

        super.finalize();

    }
    
    @Override
    protected void _reopen() {
        
        // prevent concurrent close.
        fileStore.lock.lock();
        try {

            if (fileStore.fed != null) {

                openCloseEvent = new Event(fileStore.fed, new EventResource(
                        fileStore.getIndexMetadata(), fileStore.file),
                        EventType.IndexSegmentOpenClose);

            }
            
            if (!fileStore.isOpen()) {

                // reopen the store.
                fileStore.reopen();

            }

//            this.leafCache = new ConcurrentWeakValueCacheWithTimeout<Long, ImmutableLeaf>(
//                    fileStore.getIndexMetadata()
//                            .getIndexSegmentLeafCacheCapacity(), fileStore
//                            .getIndexMetadata()
//                            .getIndexSegmentLeafCacheTimeout());

            // the checkpoint record (read when the backing store is opened).
            final IndexSegmentCheckpoint checkpoint = fileStore.getCheckpoint();

            // true iff we should fully buffer the nodes region of the index
            // segment.
            final boolean bufferNodes = metadata.getIndexSegmentBufferNodes();

            if (checkpoint.nnodes > 0 && bufferNodes) {

                try {

                    /*
                     * Read the index nodes from the file into a buffer. If
                     * there are no index nodes (that is if everything fits in
                     * the root leaf of the index) then we skip this step.
                     * 
                     * Note: We always read in the root in IndexSegment#_open()
                     * and hold a hard reference to the root while the
                     * IndexSegment is open.
                     */

                    fileStore.bufferIndexNodes();

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }

            /*
             * Read the root node.
             * 
             * Note: if this is an empty index segment then we actually create a
             * new empty root leaf since there is nothing to be read from the
             * file.
             */
            final long addrRoot = checkpoint.addrRoot;
            if (addrRoot == 0L) {

                // empty index segment; create an empty root leaf.
                this.root = new ImmutableLeaf(this);

            } else {

                // read the root node/leaf from the file.
                this.root = readNodeOrLeaf(addrRoot);

            }

            if (checkpoint.addrBloom != 0L) {

                try {

                    /*
                     * Read the optional bloom filter from the backing store.
                     */
                    bloomFilter = fileStore.readBloomFilter();

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }

//            // report on the event.
//            ResourceManager.openIndexSegment(null/* name */, fileStore
//                    .getFile().toString(), fileStore.size());

        } finally {

            fileStore.lock.unlock();

        }
        
    }

//    /**
//     * Extended to not place hard references to leaves into the
//     * {@link AbstractBTree#writeRetentionQueue} since {@link IndexSegment}
//     * leaves are normally held by the {@link #leafCache} for iterators and
//     * single-tuple operations do not require that we hold a hard reference to
//     * the leaf.
//     */
//    @Override
//    protected void touch(final AbstractNode<?> node) {
//
//        if (node.isLeaf())
//            return;
//
//        super.touch(node);
//
//    }
    
    @Override
    final public BloomFilter getBloomFilter() {
        
        // make sure the index is open.
        reopen();
        
        /*
         * return the reference. The bloom filter is read when the file is
         * (re-)opened. if the reference is null then there is no bloom filter.
         */
        
        return bloomFilter;
        
    }

//    final public boolean isReadOnly() {
//        
//        return true;
//        
//    }
    
    /**
     * The value of the {@link IndexSegmentCheckpoint#commitTime} field.
     */
    final public long getLastCommitTime() {
        
        return fileStore.getCheckpoint().commitTime;
        
    }
    
    /**
     * @throws UnsupportedOperationException
     *             always since the {@link IndexSegment} is read-only.
     */
    final public long getRevisionTimestamp() {
        
        throw new UnsupportedOperationException(ERROR_READ_ONLY);
        
    }
    
    
    /*
     * ISimpleBTree (disallows mutation operations, applies the optional bloom
     * filter when present).
     */

    /**
     * Operation is disallowed - the counter is only stored in the mutable
     * {@link BTree}.
     */
    final public ICounter getCounter() {
        
        throw new UnsupportedOperationException(ERROR_READ_ONLY);
        
    }
    
    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    @Override
    final public void removeAll() {
        
        throw new UnsupportedOperationException(ERROR_READ_ONLY);
        
    }

    /**
     * Type-safe method reads the leaf from the backing file given the address
     * of that leaf. This method is suitable for ad-hoc examination of the leaf
     * or for a low-level iterator based on the prior/next leaf addresses.
     * <p>
     * Note: The parent is NOT set on the leaf but it MAY be defined if the leaf
     * was read from cache.
     * 
     * @param addr
     *            The address of a leaf.
     * 
     * @return That leaf.
     */
    public ImmutableLeaf readLeaf(final long addr) {

        final ImmutableLeaf leaf = (ImmutableLeaf) readNodeOrLeaf(addr);

        // Note: not necessary. Was already touched on the global LRU by
        // readNodeOrLeaf().
//        /*
//         * This method is called in the context of the direct access methods for
//         * the first and last leaves and the sequential scans of linked leaves.
//         * Normally, the leaf would have been touched during top-down
//         * navigation. However, the contexts in which this is invoked do not
//         * involve top-down navigation so we touch the leaf on the LRU so it
//         * will be strongly held for a while.
//         */
//        globalLRU.add(leaf.getDelegate());
        
        return leaf;
        
    }

    /**
     * Extended to transparently re-open the backing {@link IndexSegmentStore}.
     */
    @Override
    protected AbstractNode<?> readNodeOrLeaf(final long addr) {

//        final Long tmp = Long.valueOf(addr);
//
//        {
//
//            // test the leaf cache.
//            final ImmutableLeaf leaf = leafCache.get(tmp);
//
//            if (leaf != null) {
//
//                // cache hit.
//                return leaf;
//                
//            }
//        
//        }

        if (!fileStore.isOpen()) {

            /*
             * Make sure the backing store is open.
             * 
             * Note: DO NOT call IndexSegment#reopen() here since it invokes
             * this method in order to read in the root node!
             */
            fileStore.reopen();
            
        }
        
        // read the node or leaf
        final AbstractNode<?> node = super.readNodeOrLeaf(addr);

//        if (node.isLeaf()) {
//
//            /*
//             * Put the leaf in the LRU cache.
//             */
//            
//            final ImmutableLeaf leaf = (ImmutableLeaf) node;
//
//            /*
//             * Synchronize on the cache first to make sure that the leaf has not
//             * been concurrently entered into the cache.
//             */
//
//            synchronized (leafCache) {
//
//                if (leafCache.get(tmp) == null) {
//
//                    leafCache.put(tmp, leaf);//, false/* dirty */);
//
//                }
//
//            }
//
//        }

        return node;
        
    }

    public ImmutableLeaf findLeaf(final byte[] key) {
        
        if (key == null)
            throw new IllegalArgumentException();
        
        assert rangeCheck(key, true/*allowUpperBound*/);
        
        if (getStore().getCheckpoint().height == 0) {

            /*
             * When only the root leaf exists, any key is spanned by that node.
             */

            return (ImmutableLeaf) getRoot();
            
        }
        
        return readLeaf( findLeafAddr( key ) );
        
    }

    /**
     * Find the address of the leaf that would span the key.
     * 
     * @param key
     *            The key
     * 
     * @return The address of the leaf which would span that key.
     * 
     * @throws IllegalArgumentException
     *             if the <i>key</i> is <code>null</code>.
     * @throws RuntimeException
     *             if the key does not lie within the optional key-range
     *             constraints for an index partition.
     */
    public long findLeafAddr(final byte[] key) {

        if (key == null)
            throw new IllegalArgumentException();

        assert rangeCheck(key, true/*allowUpperBound*/);

        final int height = getStore().getCheckpoint().height; 
        
        if (height == 0) {

            /*
             * When only the root leaf exists, any key is spanned by that node.
             */

            return getStore().getCheckpoint().addrRoot;
            
        }

        final IndexSegmentAddressManager am = getStore().getAddressManager();
        
        AbstractNode<?> node = getRootOrFinger(key);
        
        int i = 0;
        
        while(true) {
            
            final int childIndex = ((Node) node).findChild(key);

            final long childAddr = ((Node) node).getChildAddr(childIndex);
            
            if(am.isLeafAddr(childAddr)) {
        
                // This is a leaf address so we are done.
                return childAddr;
                
            }

            // descend the node hierarchy.
            node = (Node)((Node)node).getChild(childIndex);
            
            i++;
            
            assert i <= height : "Exceeded tree height";
            
        }
        
    }

    /*
     * INodeFactory
     */

    /**
     * Factory for immutable nodes and leaves used by the {@link NodeSerializer}.
     */
    protected static class ImmutableNodeFactory implements INodeFactory {

        public static final INodeFactory INSTANCE = new ImmutableNodeFactory();

        private ImmutableNodeFactory() {
        }

        public Leaf allocLeaf(final AbstractBTree btree, final long addr,
                final ILeafData data) {

            return new ImmutableLeaf(btree, addr, data);

        }

        public Node allocNode(final AbstractBTree btree, final long addr,
                final INodeData data) {

            return new ImmutableNode(btree, addr, data);

        }

        /**
         * Immutable node throws {@link UnsupportedOperationException} for the
         * public mutator API but does not try to override all low-level
         * mutation behaviors.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        public static class ImmutableNode extends Node {

            /**
             * @param btree
             * @param addr
             * @param data
             */
            protected ImmutableNode(final AbstractBTree btree, final long addr,
                    final INodeData data) {

                super(btree, addr, data);

            }

            @Override
            public void delete() {

                throw new UnsupportedOperationException(ERROR_READ_ONLY);

            }

            @Override
            public Tuple insert(byte[] key, byte[] val, boolean deleted, boolean putIfAbsent,
                    long timestamp, Tuple tuple) {

                throw new UnsupportedOperationException(ERROR_READ_ONLY);

            }

            @Override
            public Tuple remove(byte[] key, Tuple tuple) {

                throw new UnsupportedOperationException(ERROR_READ_ONLY);

            }

        }

        /**
         * A double-linked, read-only, empty leaf. This is used for the root
         * leaf of an empty {@link IndexSegment}.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id: IndexSegment.java 2265 2009-10-26 12:51:06Z thompsonbry
         *          $
         */
        private static class EmptyReadOnlyLeafData implements ILeafData {

            final private boolean deleteMarkers;
            final private boolean versionTimestamps;
            final private boolean rawRecords;

			public EmptyReadOnlyLeafData(final boolean deleteMarkers,
					final boolean versionTimestamps, final boolean rawRecords) {

				this.deleteMarkers = deleteMarkers;

				this.versionTimestamps = versionTimestamps;

				this.rawRecords = rawRecords;
                
            }
            
            final public boolean hasDeleteMarkers() {
                return deleteMarkers;
            }

            final public boolean hasVersionTimestamps() {
                return versionTimestamps;
            }

            final public boolean hasRawRecords() {
                return rawRecords;
            }

            final public int getValueCount() {
                return 0;
            }

            final public IRaba getValues() {
                return ReadOnlyValuesRaba.EMPTY;
            }

            final public boolean getDeleteMarker(int index) {
                throw new IndexOutOfBoundsException();
            }

            final public long getVersionTimestamp(int index) {
                throw new IndexOutOfBoundsException();
            }

            final public long getRawRecord(int index) {
                throw new IndexOutOfBoundsException();
            }

            /** Yes. */
            final public boolean isDoubleLinked() {
                return true;
            }

            /** Returns ZERO (0) since there is no next leaf. */
            final public long getNextAddr() {
                return 0L;
            }

            /** Returns ZERO (0) since there is no prior leaf. */
            final public long getPriorAddr() {
                return 0L;
            }

            final public int getKeyCount() {
                return 0;
            }

            final public IRaba getKeys() {
                return ReadOnlyKeysRaba.EMPTY;
            }

            final public long getMaximumVersionTimestamp() {
                return Long.MIN_VALUE;
            }

            final public long getMinimumVersionTimestamp() {
                return Long.MAX_VALUE;
            }

//            final public int getSpannedTupleCount() {
//                return 0;
//            }

            final public boolean isLeaf() {
                return true;
            }

            final public boolean isReadOnly() {
                return true;
            }

            final public boolean isCoded() {
                return true;
            }

            final public AbstractFixedByteArrayBuffer data() {
                return FixedByteArrayBuffer.EMPTY;
            }

        } // class EmptyReadOnlyLeaf
        
        /**
         * Immutable leaf throws {@link UnsupportedOperationException} for the
         * public mutator API but does not try to override all low-level
         * mutation behaviors.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        public static class ImmutableLeaf extends Leaf {

            /**
             * Ctor used when the {@link IndexSegment} is empty (no tuples) to
             * create an empty (and immutable) root leaf.
             * 
             * @param btree
             */
            protected ImmutableLeaf(final AbstractBTree btree) {

                // Note: This creates a _mutable_ leaf.
                super(btree);

                /*
                 * So we replace the data record with a special immutable leaf
                 * data object that is double-linked.  This is only used for
                 * the empty, immutable root leaf of an IndexSegment.
                 */
                this.data = new EmptyReadOnlyLeafData(//
                        btree.getIndexMetadata().getDeleteMarkers(), //
                        btree.getIndexMetadata().getVersionTimestamps(),//
                        btree.getIndexMetadata().getRawRecords()//
                        );

//                super(btree);
                
//                priorAddr = nextAddr = 0L;
                
            }
            
            /**
             * @param btree
             * @param addr
             * @param keys
             * @param values
             */
            protected ImmutableLeaf(final AbstractBTree btree, final long addr,
                    final ILeafData data
//                    , final long priorAddr,
//                    final long nextAddr
                    ) {

                super(btree, addr, data);

//                // prior/next addrs must be known.
//                assert priorAddr != -1L;
//                assert nextAddr != -1L;
//
//                this.priorAddr = priorAddr;
//                
//                this.nextAddr = nextAddr;
                
            }

            @Override
            public void delete() {

                throw new UnsupportedOperationException(ERROR_READ_ONLY);

            }

            @Override
            public Tuple insert(byte[] key, byte[] val, boolean deleted, boolean putIfAbsent,
                    long timestamp, Tuple tuple) {

                throw new UnsupportedOperationException(ERROR_READ_ONLY);

            }

            @Override
            public Tuple remove(byte[] key, Tuple tuple) {

                throw new UnsupportedOperationException(ERROR_READ_ONLY);

            }

            public ImmutableLeaf nextLeaf() {

                final long nextAddr = getNextAddr();
                
                if (nextAddr == 0L) {

                    // no more leaves.

                    if (DEBUG)
                        log.debug("No more leaves");

                    return null;

                }

                // return the next leaf in the key order.
                return ((IndexSegment) btree).readLeaf(nextAddr);

            }

            public ImmutableLeaf priorLeaf() {

                final long priorAddr = getPriorAddr();

                if (priorAddr == 0L) {

                    // no more leaves.

                    if (DEBUG)
                        log.debug("No more leaves");

                    return null;

                }

                // return the previous leaf in the key order.
                return ((IndexSegment) btree).readLeaf(priorAddr);

            }

        } // class ImmutableLeaf

//        /**
//         * An immutable empty leaf used as the right-most child of an
//         * {@link IndexSegment} {@link Node} when the right-most child was not
//         * emitted by the {@link IndexSegmentBuilder}. Normally the builder will
//         * assign tuples to the nodes and leaves in the {@link IndexSegmentPlan}
//         * such that each separatorKey in a {@link Node} has a left and a right
//         * child. When the {@link IndexSegmentPlan} was based on an overestimate
//         * of the actual range count, then the right child for a separatorKey in
//         * a {@link Node} is sometimes not populated and will have the address
//         * 0L. When that address is dereferenced an instance of this class is
//         * transparently materialized which gives the {@link Node} the
//         * appearence of having both a left- and right-child for that
//         * separatorKey.
//         * <p>
//         * Nodes other than those immediately dominating leaves can have a mock
//         * rightmost leaf as a child. This means that the rightSibling of a Node
//         * can be a Leaf!
//         * 
//         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//         *         Thompson</a>
//         * @version $Id$
//         */
//        static public class ImmutableEmptyLastLeaf extends ImmutableLeaf {
//
//            /**
//             * @param btree
//             *            The owning {@link IndexSegment}.
//             * @param priorAddr
//             *            The address of the previous leaf in the natural
//             *            traversal order or 0L if there is no prior leaf.
//             */
//            public ImmutableEmptyLastLeaf(final AbstractBTree btree,
//                    final long priorAddr) {
//
//                super(  (IndexSegment) btree,
//                        0L/* selfAddr */,
//                        new EmptyReadOnlyLeafData(//
//                                btree.getIndexMetadata().getDeleteMarkers(), //
//                                btree.getIndexMetadata().getVersionTimestamps()) {
//                            /**
//                             * Overridden to use the priorAddr field passed to
//                             * the constructor.
//                             */
//                    @Override
//                    public long getPriorAddr() {
//                        return priorAddr;
//                    }
//                });
//
//            }
//
//        } // class ImmutableEmptyLastLeaf
        
    } // class ImmutableNodeFactory

    /**
     * A position for the {@link IndexSegmentTupleCursor}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     *            The generic type for objects de-serialized from the values in
     *            the index.
     */
    static private class CursorPosition<E> extends AbstractCursorPosition<ImmutableLeaf,E> {
        
        @SuppressWarnings("unchecked")
        public IndexSegmentTupleCursor<E> getCursor() {
            
            return (IndexSegmentTupleCursor)cursor;
            
        }
        
        /**
         * Create position on the specified key, or on the successor of the
         * specified key if that key is not found in the index.
         * 
         * @param cursor
         *            The {@link ITupleCursor}.
         * @param leafCursor
         *            The leaf cursor.
         * @param index
         *            The index of the tuple in the <i>leaf</i>.
         * @param key
         *            The key.
         */
        public CursorPosition(IndexSegmentTupleCursor<E> cursor,
                ILeafCursor<ImmutableLeaf> leafCursor, int index, byte[] key) {

            super(cursor, leafCursor, index, key);

        }

        /**
         * Copy constructor.
         * 
         * @param p
         */
        public CursorPosition(CursorPosition<E> p) {
            
            super( p );
            
        }

    }
    
    /**
     * Implementation for an immutable {@link IndexSegment}. This
     * implementation uses the prior/next leaf references for fast forward and
     * reference scans of the {@link IndexSegment}.
     * <p>
     * Note: Since the {@link IndexSegment} is immutable it does not maintain
     * listeners for concurrent modifications.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     *            The generic type for the objects de-serialized from the index.
     */
    public static class IndexSegmentTupleCursor<E> extends
            AbstractBTreeTupleCursor<IndexSegment, ImmutableLeaf, E> {

        public IndexSegmentTupleCursor(final IndexSegment btree,
                final Tuple<E> tuple, final byte[] fromKey, final byte[] toKey) {

            super(btree, tuple, fromKey, toKey);

        }

        @Override
        final protected CursorPosition<E> newPosition(
                final ILeafCursor<ImmutableLeaf> leafCursor, final int index,
                final byte[] key) {

            final CursorPosition<E> pos = new CursorPosition<E>(this,
                    leafCursor, index, key);

            return pos;

        }

        @Override
        protected CursorPosition<E> newTemporaryPosition(
                final ICursorPosition<ImmutableLeaf, E> p) {

            return new CursorPosition<E>((CursorPosition<E>)p );
            
        }

        /*
         * The methods below are apparently never used. BBT 12/29/2009.
         */
        
//        /**
//         * The {@link IndexSegmentStore} backing the {@link IndexSegment} for
//         * that is being traversed by the {@link ITupleCursor}.
//         */
//        protected IndexSegmentStore getStore() {
//            
//            return btree.getStore();
//            
//        }
//
//        /**
//         * Return the leaf that spans the optional {@link #getFromKey()}
//         * constraint and the first leaf if there is no {@link #getFromKey()}
//         * constraint.
//         * 
//         * @return The leaf that spans the first tuple that can be visited by
//         *         this cursor.
//         * 
//         * @see Leaf#getKeys()
//         * @see IKeyBuffer#search(byte[])
//         */
//        protected ImmutableLeaf getLeafSpanningFromKey() {
//            
//            final long addr;
//            
//            if (fromKey == null) {
//
//                addr = getStore().getCheckpoint().addrFirstLeaf;
//
//            } else {
//
//                // find leaf for fromKey
//                addr = getIndex().findLeafAddr(fromKey);
//
//            }
//
//            assert addr != 0L;
//            
//            final ImmutableLeaf leaf = getIndex().readLeaf(addr);
//
//            assert leaf != null;
//            
//            return leaf;
//
//        }
//
////        /**
////         * Return the leaf that spans the optional {@link #getToKey()}
////         * constraint and the last leaf if there is no {@link #getFromKey()}
////         * constraint.
////         * 
////         * @return The leaf that spans the first tuple that can NOT be visited
////         *         by this cursor (exclusive upper bound).
////         * 
////         * @see Leaf#getKeys()
////         * @see IKeyBuffer#search(byte[])
////         */
////        protected ImmutableLeaf getLeafSpanningToKey() {
////            
////            final long addr;
////            
////            if (toKey == null) {
////
////                addr = getStore().getCheckpoint().addrLastLeaf;
////
////            } else {
////
////                // find leaf for toKey
////                addr = getIndex().findLeafAddr(toKey);
////
////            }
////
////            assert addr != 0L;
////            
////            final ImmutableLeaf leaf = getIndex().readLeaf(addr);
////
////            assert leaf != null;
////            
////            return leaf;
////
////        }
//
//        /**
//         * Return the leaf that spans the key.  The caller must check to see
//         * whether the key actually exists in the leaf.
//         * 
//         * @param key
//         *            The key.
//         *            
//         * @return The leaf spanning that key.
//         */
//        protected ImmutableLeaf getLeafSpanningKey(final byte[] key) {
//            
//            final long addr = getIndex().findLeafAddr(key);
//
//            assert addr != 0L;
//
//            final ImmutableLeaf leaf = getIndex().readLeaf(addr);
//
//            assert leaf != null;
//
//            return leaf;
//
//        }

    }

    @Override
    public ImmutableLeafCursor newLeafCursor(final SeekEnum where) {
        
        return new ImmutableLeafCursor(where);
        
    }
    
    @Override
    public ImmutableLeafCursor newLeafCursor(final byte[] key) {
        
        return new ImmutableLeafCursor(key);
        
    }
    
    /**
     * Cursor using the double-linked leaves for efficient scans.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class ImmutableLeafCursor implements ILeafCursor<ImmutableLeaf> {

        private ImmutableLeaf leaf;
        
        public ImmutableLeaf leaf() {
            
            return leaf;
            
        }

        public IndexSegment getBTree() {
            
            return IndexSegment.this;
            
        }
        
        public ImmutableLeafCursor clone() {
            
            return new ImmutableLeafCursor(this);
            
        }
        
        /**
         * Copy constructor.
         * 
         * @param src
         */
        private ImmutableLeafCursor(final ImmutableLeafCursor src) {
            
            if (src == null)
                throw new IllegalArgumentException();
            
            this.leaf = src.leaf;
            
        }
        
        public ImmutableLeafCursor(final SeekEnum where) {

            switch (where) {

            case First:
                
                first();
                
                break;
                
            case Last:
                
                last();
                
                break;
                
            default:
                
                throw new AssertionError("Unknown seek directive: " + where);
            
            }
            
        }
        
        public ImmutableLeafCursor(final byte[] key) {
            
            seek(key);
            
        }

        public ImmutableLeaf seek(final byte[] key) {
            
            leaf = findLeaf(key);
            
            return leaf;
            
        }
        
        public ImmutableLeaf seek(final ILeafCursor<ImmutableLeaf> src) {
            
            if (src == null)
                throw new IllegalArgumentException();
            
            if (src == this) {

                // NOP
                return leaf;
                
            }
            
            if (src.getBTree() != IndexSegment.this) {
                
                throw new IllegalArgumentException();
                
            }
            
            return leaf = src.leaf();
            
        }

        public ImmutableLeaf first() {
            
            final long addr = getStore().getCheckpoint().addrFirstLeaf;

            leaf = readLeaf(addr);
            
            return leaf;
            
        }

        public ImmutableLeaf last() {
            
            final long addr = getStore().getCheckpoint().addrLastLeaf;

            leaf = readLeaf(addr);
            
            return leaf;
            
        }

        public ImmutableLeaf next() {

            final ImmutableLeaf l = leaf.nextLeaf();

            if (l == null)
                return null;

            leaf = l;

            return l;
            
        }

        public ImmutableLeaf prior() {
            
            final ImmutableLeaf l = leaf.priorLeaf();

            if (l == null)
                return null;

            leaf = l;

            return l;
            
        }
        
    }

    public BTree getMutableBTree() {
        throw new UnsupportedOperationException();
    }

    public int getSourceCount() {
        return 1;
    }

    public AbstractBTree[] getSources() {
        return new AbstractBTree[]{this};
    }

//    @Override
//	ByteBuffer readRawRecord(final long addr) {
//
//		// read from the backing store.
//		return getStore().read(addr);
//
//	}

}
