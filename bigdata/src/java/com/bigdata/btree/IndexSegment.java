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
package com.bigdata.btree;

import java.util.NoSuchElementException;

import it.unimi.dsi.mg4j.util.BloomFilter;

import com.bigdata.btree.IndexSegment.ImmutableNodeFactory.ImmutableLeaf;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.resources.ResourceManager;

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
public class IndexSegment extends AbstractBTree {

    /**
     * Type safe reference to the backing store.
     */
    private final IndexSegmentStore fileStore;

    /**
     * An optional bloom filter that will be used to filter point tests. Since
     * bloom filters do not support removal of keys the option to use a filter
     * is restricted to {@link IndexSegment}s since they are read-only data
     * structures.
     */
    protected BloomFilter bloomFilter;

    /**
     * An LRU for {@link ImmutableLeaf}s. This cache takes advantage of the
     * fact that the {@link LeafIterator} can read leaves without navigating
     * down the node hierarchy.
     */
    private WeakValueCache<Long, ImmutableLeaf> leafCache;

    final public int getHeight() {

        reopen();

        return fileStore.getCheckpoint().height;

    }

    final public int getLeafCount() {

        reopen();

        return fileStore.getCheckpoint().nleaves;

    }

    final public int getNodeCount() {

        reopen();

        return fileStore.getCheckpoint().nnodes;

    }

    final public int getEntryCount() {

        reopen();

        return fileStore.getCheckpoint().nentries;

    }

    /**
     * Open a read-only index segment.
     * 
     * @param fileStore
     *            The store containing the {@link IndexSegment}.
     * 
     * @todo explore good defaults for the hard reference queue, which should
     *       probably be much smaller as the branching factor grows larger.
     *       <p>
     *       The index segment is read only so we do not need to do IO on
     *       eviction. All the listener needs to do is count queue evictions to
     *       collect statistics on the index performance. The capacity should be
     *       relatively low and the #of entries to scan should be relatively
     *       high since each entry is relatively large, e.g., try with 100 and
     *       20 respectively.
     *       <p>
     *       Consider whether we can use only a read-retention queue for an
     *       index segment.
     * 
     * @see IndexSegmentStore#loadIndexSegment()
     */
    public IndexSegment(IndexSegmentStore fileStore) {

        super(fileStore,
                ImmutableNodeFactory.INSTANCE,
                true, // always read-only
                fileStore.getIndexMetadata()
                );

        // Type-safe reference to the backing store.
        this.fileStore = (IndexSegmentStore) fileStore;

        _open();

    }

    /**
     * Extended to also close the backing file.
     */
    public void close() {

        if (root == null) {

            throw new IllegalStateException("Already closed.");

        }

        // close the backing file (can be re-opened).
        fileStore.close();

        // release the optional bloom filter.
        bloomFilter = null;
        
        // release the leaf cache.
        leafCache.clear();
        leafCache = null;

        // release buffers and hard reference to the root node.
        super.close();

        // report event.
        ResourceManager.closeIndexSegment(fileStore.getFile().toString());

    }

    /**
     * Re-opens the backing file.
     */
    protected void reopen() {

        if (root == null) {

            /*
             * reload the root node.
             * 
             * Note: This is synchronized to avoid race conditions when
             * re-opening the index from the backing store.
             * 
             * Note: [root] MUST be marked as [volatile] to guarentee correct
             * semantics.
             * 
             * See http://en.wikipedia.org/wiki/Double-checked_locking
             */
            
            synchronized (this) {

                if (root == null) {

                    if (!fileStore.isOpen()) {

                        // reopen the store.
                        fileStore.reopen();
                        
                    }

                    _open();

                }

            }

        }

    }

    /**
     * Overriden to a more constrained type.
     */
    final public IndexSegmentStore getStore() {
        
        return fileStore;
        
    }
    
    /**
     * Note: This caller MUST be synchronized to ensure that at most one thread
     * gets to re-open the index from its backing store.
     */
    private void _open() {

        // @todo config cache size on {@link IndexSegmentStore.Options}
        this.leafCache = new WeakValueCache<Long, ImmutableLeaf>(
                new LRUCache<Long, ImmutableLeaf>(10));

        // Read the root node.
        this.root = readNodeOrLeaf(fileStore.getCheckpoint().addrRoot);

        // Save reference to the optional bloom filter.
        this.bloomFilter = fileStore.getBloomFilter();
        
        // report on the event.
        ResourceManager.openIndexSegment(null/* name */, fileStore.getFile()
                .toString(), fileStore.size());

    }

    final public boolean isReadOnly() {
        
        return true;
        
    }
    
    /**
     * The value of the {@link IndexSegmentCheckpoint#commitTime} field.
     */
    final public long getLastCommitTime() {
        
        return fileStore.getCheckpoint().commitTime;
        
    }
    
    /*
     * bloom filter support.
     */

    /**
     * Returns true if the optional bloom filter reports that the key exists.
     * 
     * @param key
     *            The key.
     * 
     * @return <code>true</code> if the bloom filter believes that the key is
     *         present in the index. When true, you MUST still test the key to
     *         verify that it is, in fact, present in the index. When false, you
     *         SHOULD NOT test the index.
     * 
     * @todo examine the #of weights in use by the bloom filter and its impact
     *       on false positives for character data.
     */
    final protected boolean containsKey(byte[] key) {

        reopen();

        assert bloomFilter != null;

        return bloomFilter.contains(key);

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
        
        throw new UnsupportedOperationException(MSG_READ_ONLY);
        
    }
    
    /**
     * Applies the optional bloom filter if it exists. If the bloom filter
     * reports true, then verifies that the key does in fact exist in the index.
     */
    public boolean contains(byte[] key) {

        reopen();

        if (bloomFilter != null) {

            if (!containsKey(key)) {

                // rejected by the bloom filter.
                return false;

            }

            // fall through.
            
        }

        // test the index.
        return super.contains(key);

    }

    /**
     * Applies the optional bloom filter if it exists. If the bloom filter
     * exists and reports true, then looks up the value for the key in the index
     * (note that the key might not exist in the index since a bloom filter
     * allows false positives, further the key might exist for a deleted entry).
     */
    public Tuple lookup(byte[] key, Tuple tuple) {

        reopen();

        if (bloomFilter != null) {

            if (!containsKey(key)) {

                // rejected by the bloom filter.

                return null;

            }

            // fall through.

        }
        
        /*
         * Lookup against the index (may be a false positive, may be paired to a
         * deleted entry, and we need the tuple paired to the key in any case).
         */
        
        return super.lookup(key, tuple);

    }

    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    final public void removeAll() {
        
        throw new UnsupportedOperationException(MSG_READ_ONLY);
        
    }
    
    /**
     * Typesafe method reads the leaf from the backing file given the address of
     * that leaf. This method is suitable for ad-hoc examination of the leaf or
     * for a low-level iterator based on the prior/next leaf addresses.
     * <p>
     * Note: The parent is NOT set on the leaf but it MAY be defined if the leaf
     * was read from the {@link #leafCache}.
     * 
     * @param addr
     *            The address of a leaf.
     * 
     * @return That leaf.
     */
    public ImmutableLeaf readLeaf(long addr) {

        return (ImmutableLeaf) readNodeOrLeaf(addr);
        
    }

    /**
     * Extended to use a private cache for {@link ImmutableLeaf}s. By adding a
     * leaf cache here we are able to catch all reads of leaves from the backing
     * store and impose an LRU cache semantics for those leaves. This includes
     * reads by the {@link LeafIterator} which are made without using the node
     * hierarchy.
     */
    protected AbstractNode readNodeOrLeaf(long addr) {

        final Long tmp = Long.valueOf(addr);

        {

            // test the leaf cache.
            final ImmutableLeaf leaf = leafCache.get(tmp);

            if (leaf != null) {

                // cache hit.
                return leaf;
                
            }
        
        }

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
        final AbstractNode node = super.readNodeOrLeaf(addr);

        if (node.isLeaf()) {

            /*
             * Put the leaf in the LRU cache.
             */
            
            final ImmutableLeaf leaf = (ImmutableLeaf) node;

            /*
             * Synchronize on the cache first to make sure that the leaf has not
             * been concurrently entered into the cache.
             */

            synchronized (leafCache) {

                if (leafCache.get(tmp) == null) {

                    leafCache.put(tmp, leaf, false/* dirty */);

                }

            }

        }

        return node;
        
    }
    
    /**
     * Variant for scanning all leaves in key order.
     */
    public LeafIterator leafIterator() {
        
        return new LeafIterator(true/* forwardScan */);
        
    }
    
    /**
     * Variant for scanning all leaves.
     * 
     * @param forwardScan
     *            When <code>true</code> {@link ILeafIterator#next()} will
     *            scan in key order and the first leaf to be visited will be the
     *            first leaf in the B+Tree. Otherwise
     *            {@link ILeafIterator#next()} will scan in reverse key order
     *            and the first leaf to be visited will be the last leaf in the
     *            B+Tree.
     */    
    public LeafIterator leafIterator(boolean forwardScan) {
        
        return new LeafIterator(forwardScan);
        
    }
    
    /**
     * Variant for scanning leaves spanning a key range.
     * 
     * @param forwardScan
     *            When <code>true</code> {@link ILeafIterator#next()} will
     *            scan in key order and the first leaf to be visited will be the
     *            leaf which spans the <i>fromKey</i> (and the first leaf in
     *            the B+Tree if the <i>fromKey</i> is <code>null</code>).
     *            Otherwise {@link ILeafIterator#next()} will scan in reverse
     *            key order and the first leaf to be visited will be the leaf
     *            that spans the <i>toKey</i> (and the last leaf in the B+Tree
     *            if the <i>toKey</i> is <code>null</code>).
     * @param fromKey
     *            When non-<code>null</code>, this identifies the first leaf
     *            in <em>key order</em> to be visited. If a tuple for
     *            <i>fromKey</i> exists in the index, this is the leaf which
     *            contains that tuple. If no tuple exists for <i>fromKey</i>
     *            then this is the leaf in which the insertion point for
     *            <i>fromKey</i> would be located. When <code>null</code>
     *            there is no lower bound.
     * @param toKey
     *            When non-<code>null</code>, this identifies the last leaf
     *            in <em>key order</em> to be visited. If a tuple for <i>toKey</i>
     *            exists in the index, this is the leaf which contains that
     *            tuple. If no tuple exists for <i>toKey</i> then this is the
     *            leaf in which the insertion point for <i>toKey</i> would be
     *            located. When <code>null</code> there is no upper bound.
     */
    public LeafIterator leafIterator(boolean forwardScan, byte[] fromKey,
            byte[] toKey) {
        
        return new LeafIterator(forwardScan, fromKey, toKey);
        
    }

    /**
     * Find the leaf that would span the key.
     * 
     * @param key
     *            The key
     * 
     * @return The leaf which would span that key.
     * 
     * @throws IllegalArgumentException
     *             if the <i>key</i> is <code>null</code>.
     */
    public ImmutableLeaf findLeaf(final byte[] key) {
        
        if (key == null)
            throw new IllegalArgumentException();
        
        rangeCheck(key, true/*allowUpperBound*/);
        
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
     */
    public long findLeafAddr(final byte[] key) {

        if (key == null)
            throw new IllegalArgumentException();

        rangeCheck(key, true/*allowUpperBound*/);

        final int height = getStore().getCheckpoint().height; 
        
        if (height == 0) {

            /*
             * When only the root leaf exists, any key is spanned by that node.
             */

            return getStore().getCheckpoint().addrRoot;
            
        }

        final IndexSegmentAddressManager am = getStore().getAddressManager();
        
        AbstractNode node = getRootOrFinger(key);
        
        int i = 0;
        
        while(true) {
            
            final int childIndex = ((Node)node).findChild(key);

            final long childAddr = ((Node)node).childAddr[childIndex];
            
            if(am.isLeafAddr(childAddr)) {
        
                // This is a leaf address so we are done.
                return childAddr;
                
            }

            // desend the node hierarchy.
            node = (Node)((Node)node).getChild(childIndex);
            
            i++;
            
            assert i <= height : "Exceeded tree height";
            
        }
        
    }
    
    /**
     * Implementation uses the {@link ImmutableLeaf#priorAddr} and
     * {@link ImmutableLeaf#nextAddr} fields to provide fast forward and reverse
     * leaf scans without touching the node hierarchy.
     * <p>
     * Note: The <code>parent</code> reference will NOT be set of the leaves
     * visited by this method.
     * 
     * @todo consider refactor so that {@link #next()}, {@link #prior()},
     *       {@link #hasNext()} and {@link #hasPrior()} are wrappers around a
     *       core impl class.
     * 
     * @todo consider adding a method to go to a specific leaf. we can verify
     *       that the leaf is legal by comparing the address to the address
     *       range for the leaves and to the first and last address that the
     *       {@link LeafIterator} is allowed to visit.
     * 
     * @todo consider allowing a mark()/rewind() facility so that you can jump
     *       back to a specific leaf.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class LeafIterator implements ILeafIterator<ImmutableLeaf> {

        /** from the ctor. */
        final protected boolean forwardScan;
        
        /** from the ctor. */
        final protected byte[] fromKey;

        /** from the ctor. */
        final protected byte[] toKey;

        /**
         * The address of the first leaf in key order. This is the first leaf in
         * the B+Tree if {@link #fromKey} is <code>null</code> and otherwise
         * is the leaf in which {@link #fromKey} would be found if it exists in
         * the index and the leaf which is the insertion point for the
         * {@link #fromKey} if it is not in the index.
         */
        private final long addrFirstLeaf;

        /**
         * The address of the last leaf in key order. This is the last leaf in
         * the B+Tree if {@link #toKey} is <code>null</code> and otherwise is
         * the leaf in which {@link #toKey} would be found if it exists in the
         * index and the leaf which is the insertion point for the
         * {@link #toKey} if it is not in the index.
         */
        private final long addrLastLeaf;
        
        /**
         * The current leaf (initially set by the ctor and always defined).
         */
        private ImmutableLeaf current;
        
        final public boolean isForwardScan() {
            
            return forwardScan;
            
        }

        final public boolean isReverseScan() {
            
            return ! forwardScan;
            
        }

        /**
         * Ctor variant for scanning all leaves.
         * 
         * @param forwardScan
         *            When <code>true</code> {@link #next()} will scan in key
         *            order and the first leaf to be visited will be the first
         *            leaf in the B+Tree. Otherwise {@link #next()} will scan in
         *            reverse key order and the first leaf to be visited will be
         *            the last leaf in the B+Tree.
         */
        protected LeafIterator(final boolean forwardScan) {

            this(forwardScan, null, null);
            
        }
        
        /**
         * Ctor variant for scanning leaves spanning a key range.
         * 
         * @param forwardScan
         *            When <code>true</code> {@link #next()} will scan in key
         *            order and the first leaf to be visited will be the leaf
         *            which spans the <i>fromKey</i> (and the first leaf in the
         *            B+Tree if the <i>fromKey</i> is <code>null</code>).
         *            Otherwise {@link #next()} will scan in reverse key order
         *            and the first leaf to be visited will be the leaf that
         *            spans the <i>toKey</i> (and the last leaf in the B+Tree
         *            if the <i>toKey</i> is <code>null</code>).
         * @param fromKey
         *            When non-<code>null</code>, this identifies the first
         *            leaf in <em>key order</em> to be visited. If a tuple for
         *            <i>fromKey</i> exists in the index, this is the leaf
         *            which contains that tuple. If no tuple exists for
         *            <i>fromKey</i> then this is the leaf in which the
         *            insertion point for <i>fromKey</i> would be located. When
         *            <code>null</code> there is no lower bound.
         * @param toKey
         *            When non-<code>null</code>, this identifies the last
         *            leaf in <em>key order</em> to be visited. If a tuple for
         *            <i>toKey</i> exists in the index, this is the leaf which
         *            contains that tuple. If no tuple exists for <i>toKey</i>
         *            then this is the leaf in which the insertion point for
         *            <i>toKey</i> would be located. When <code>null</code>
         *            there is no upper bound.
         */
        protected LeafIterator(final boolean forwardScan, final byte[] fromKey,
                final byte[] toKey) {
            
            this.forwardScan = forwardScan;

            this.fromKey = fromKey;
            
            this.toKey = toKey;
            
            if (fromKey == null) {

                addrFirstLeaf = getStore().getCheckpoint().addrFirstLeaf;

            } else {

                // find leaf for fromKey
                addrFirstLeaf = findLeafAddr(fromKey);

            }

            if (toKey == null) {

                addrLastLeaf = getStore().getCheckpoint().addrLastLeaf;

            } else {

                // find leaf for toKey
                addrLastLeaf = findLeafAddr(toKey);

            }
            
            // addr of the first leaf to be visited.
            final long addr;
            
            if (forwardScan) {
                
                // Start a forward scan with the first leaf in the key order.
                addr = addrFirstLeaf;
                
            } else {
                
                // Start a reverse scan with the last leaf in the key order.
                addr = addrLastLeaf;
                
            }
            
            assert addr != 0L;
            
            // read the first least in the scan order.
            current = readLeaf(addr);
            
        }

        public boolean hasNextInKeyOrder() {
            
            if (current.identity == addrLastLeaf) {

                // on the last leaf allowed by the optional key range.
                return false;

            }

            // next leaf must exist.
            assert current.nextAddr != 0L;

            return true;

        }

        public boolean hasPriorInKeyOrder() {

            if (current.identity == addrFirstLeaf) {

                // on the first leaf allowed by the optional key range.
                return false;

            }

            // prior leaf must exist.
            assert current.priorAddr != 0L;

            return true;

        }
        
        public boolean hasNext() {

            if(forwardScan) {

                return hasNextInKeyOrder();
                
            } else {
                
                return hasPriorInKeyOrder();
                
            }
            
        }

        public boolean hasPrior() {

            if(!forwardScan) {

                return hasNextInKeyOrder();
                
            } else {
                
                return hasPriorInKeyOrder();
                
            }
            
        }

        public ImmutableLeaf nextInKeyOrder() {

            if (!hasNextInKeyOrder()) {

                // No more leaves in key order.
                return null;

            }

            return current = readLeaf(current.nextAddr);

        }

        public ImmutableLeaf priorInKeyOrder() {

            if (!hasPriorInKeyOrder()) {

                // No more leaves in reverse key order.
                return null;
                
            }
            
            return current = readLeaf(current.priorAddr);
            
        }
        
        public ImmutableLeaf current() {

            // should always be defined.
            assert current != null;
            
            return current;
            
        }

        public ImmutableLeaf next() {

            if (!hasNext())
                throw new NoSuchElementException();

            if (forwardScan) {

                return nextInKeyOrder();

            } else {

                return priorInKeyOrder();

            }
            
        }

        public ImmutableLeaf prior() {

            if (!hasNext())
                throw new NoSuchElementException();

            if (forwardScan) {

                return priorInKeyOrder();

            } else {

                return nextInKeyOrder();

            }
            
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

        public ILeafData allocLeaf(IIndex btree, long addr,
                int branchingFactor, IKeyBuffer keys, byte[][] values,
                long[] versionTimestamps, boolean[] deleteMarkers,
                long priorAddr, long nextAddr) {

            return new ImmutableLeaf((AbstractBTree) btree, addr,
                    branchingFactor, keys, values, versionTimestamps,
                    deleteMarkers, priorAddr, nextAddr);

        }

        public INodeData allocNode(IIndex btree, long addr,
                int branchingFactor, int nentries, IKeyBuffer keys,
                long[] childAddr, int[] childEntryCount) {

            return new ImmutableNode((AbstractBTree) btree, addr,
                    branchingFactor, nentries, keys, childAddr, childEntryCount);

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
             * @param branchingFactor
             * @param nentries
             * @param keys
             * @param childKeys
             */
            protected ImmutableNode(AbstractBTree btree, long addr,
                    int branchingFactor, int nentries, IKeyBuffer keys,
                    long[] childKeys, int[] childEntryCount) {

                super(btree, addr, branchingFactor, nentries, keys, childKeys,
                        childEntryCount);

            }

            public void delete() {

                throw new UnsupportedOperationException(MSG_READ_ONLY);

            }

            public Tuple insert(byte[] key, byte[] val, boolean deleted, long timestamp, Tuple tuple) {

                throw new UnsupportedOperationException(MSG_READ_ONLY);

            }

            public Tuple remove(byte[] key, Tuple tuple) {

                throw new UnsupportedOperationException(MSG_READ_ONLY);

            }

        }

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
             * The address of the previous leaf in key order if known, 0L if it
             * known that this is the first leaf, and -1L otherwise.
             */
            public final long priorAddr;

            /**
             * The address of the next leaf in key order if known, 0L if it
             * known that this is the first leaf, and -1L otherwise.
             */
            public final long nextAddr;
            
            /**
             * @param btree
             * @param addr
             * @param branchingFactor
             * @param keys
             * @param values
             */
            protected ImmutableLeaf(AbstractBTree btree, long addr,
                    int branchingFactor, IKeyBuffer keys, byte[][] values,
                    long[] versionTimestamps, boolean[] deleteMarkers,
                    long priorAddr, long nextAddr) {

                super(btree, addr, branchingFactor, keys, values,
                        versionTimestamps, deleteMarkers);

                this.priorAddr = priorAddr;
                
                this.nextAddr = nextAddr;
                
            }

            public void delete() {

                throw new UnsupportedOperationException(MSG_READ_ONLY);

            }

            public Tuple insert(byte[] key, byte[] val, boolean deleted, long timestamp, Tuple tuple) {

                throw new UnsupportedOperationException(MSG_READ_ONLY);

            }

            public Tuple remove(byte[] key, Tuple tuple) {

                throw new UnsupportedOperationException(MSG_READ_ONLY);

            }

        }

    }

}
