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

import com.bigdata.btree.AbstractBTreeTupleCursor.AbstractCursorPosition;
import com.bigdata.btree.IndexSegment.ImmutableNodeFactory.ImmutableLeaf;
import com.bigdata.btree.IndexSegmentStore.Options;
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

        _reopen();

    }

    /**
     * Extended to also close the backing file.
     */
    public void close() {

        if (root == null) {

            throw new IllegalStateException(ERROR_CLOSED);

        }

        // close the backing file (can be re-opened).
        fileStore.close();

//        // release the optional bloom filter.
//        bloomFilter = null;
        
        // release the leaf cache.
        leafCache.clear();
        leafCache = null;

        // release buffers and hard reference to the root node.
        super.close();

        // report event.
        ResourceManager.closeIndexSegment(fileStore.getFile().toString());

    }

//    /**
//     * Re-opens the backing file.
//     */
//    protected void reopen() {
//
//        if (root == null) {
//
//            /*
//             * reload the root node.
//             * 
//             * Note: This is synchronized to avoid race conditions when
//             * re-opening the index from the backing store.
//             * 
//             * Note: [root] MUST be marked as [volatile] to guarentee correct
//             * semantics.
//             * 
//             * See http://en.wikipedia.org/wiki/Double-checked_locking
//             */
//            
//            synchronized (this) {
//
//                if (root == null) {
//
//                    if (!fileStore.isOpen()) {
//
//                        // reopen the store.
//                        fileStore.reopen();
//                        
//                    }
//
//                    _open();
//
//                }
//
//            }
//
//        }
//
//    }

    /**
     * Overriden to a more constrained type.
     */
    final public IndexSegmentStore getStore() {
        
        return fileStore;
        
    }
    
    @Override
    protected void _reopen() {

        if (!fileStore.isOpen()) {

            // reopen the store.
            fileStore.reopen();
            
        }

        final int leafCacheSize = Integer.parseInt(fileStore.getProperties()
                .getProperty(Options.LEAF_CACHE_SIZE,
                        Options.DEFAULT_LEAF_CACHE_SIZE));
        
        this.leafCache = new WeakValueCache<Long, ImmutableLeaf>(
                new LRUCache<Long, ImmutableLeaf>(leafCacheSize));

        // Read the root node.
        this.root = readNodeOrLeaf(fileStore.getCheckpoint().addrRoot);

        /*
         * Save reference to the optional bloom filter (it is read when the file
         * is opened).
         */
        bloomFilter = fileStore.getBloomFilter();
        
        // report on the event.
        ResourceManager.openIndexSegment(null/* name */, fileStore.getFile()
                .toString(), fileStore.size());

    }
    
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
    final public void removeAll() {
        
        throw new UnsupportedOperationException(ERROR_READ_ONLY);
        
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
     * @throws RUntimeException
     *             if the key does not lie within the optional key-range
     *             constraints for an index partition.
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

                throw new UnsupportedOperationException(ERROR_READ_ONLY);

            }

            public Tuple insert(byte[] key, byte[] val, boolean deleted, long timestamp, Tuple tuple) {

                throw new UnsupportedOperationException(ERROR_READ_ONLY);

            }

            public Tuple remove(byte[] key, Tuple tuple) {

                throw new UnsupportedOperationException(ERROR_READ_ONLY);

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

                // prior/next addrs must be known.
                assert priorAddr != -1L;
                assert nextAddr != -1L;

                this.priorAddr = priorAddr;
                
                this.nextAddr = nextAddr;
                
            }

            public void delete() {

                throw new UnsupportedOperationException(ERROR_READ_ONLY);

            }

            public Tuple insert(byte[] key, byte[] val, boolean deleted, long timestamp, Tuple tuple) {

                throw new UnsupportedOperationException(ERROR_READ_ONLY);

            }

            public Tuple remove(byte[] key, Tuple tuple) {

                throw new UnsupportedOperationException(ERROR_READ_ONLY);

            }
    
            public ImmutableLeaf nextLeaf() {
                
                if(nextAddr == 0L) {

                    // no more leaves.

                    if(INFO)
                        log.info("No more leaves");
                    
                    return null;
                    
                }
                
                // return the next leaf in the key order.
                return ((IndexSegment)btree).readLeaf(nextAddr);
                
            }
            
            public ImmutableLeaf priorLeaf() {
                
                if(priorAddr == 0L) {

                    // no more leaves.

                    if(INFO)
                        log.info("No more leaves");
                    
                    return null;
                    
                }
                
                // return the previous leaf in the key order.
                return ((IndexSegment)btree).readLeaf(priorAddr);
                
            }

        }

    }

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

        public IndexSegmentTupleCursor(IndexSegment btree, Tuple<E> tuple,
                byte[] fromKey, byte[] toKey) {

            super(btree, tuple, fromKey, toKey);

        }

        @Override
        final protected CursorPosition<E> newPosition(
                ILeafCursor<ImmutableLeaf> leafCursor, int index, byte[] key) {

            CursorPosition<E> pos = new CursorPosition<E>(this, leafCursor, index, key);

            return pos;

        }

        @Override
        protected CursorPosition<E> newTemporaryPosition(ICursorPosition<ImmutableLeaf, E> p) {

            return new CursorPosition<E>((CursorPosition<E>)p );
            
        }

        /**
         * The {@link IndexSegmentStore} backing the {@link IndexSegment} for
         * that is being traversed by the {@link ITupleCursor}.
         */
        protected IndexSegmentStore getStore() {
            
            return btree.getStore();
            
        }

        /**
         * Return the leaf that spans the optional {@link #getFromKey()}
         * constraint and the first leaf if there is no {@link #getFromKey()}
         * constraint.
         * 
         * @return The leaf that spans the first tuple that can be visited by
         *         this cursor.
         * 
         * @see Leaf#getKeys()
         * @see IKeyBuffer#search(byte[])
         */
        protected ImmutableLeaf getLeafSpanningFromKey() {
            
            final long addr;
            
            if (fromKey == null) {

                addr = getStore().getCheckpoint().addrFirstLeaf;

            } else {

                // find leaf for fromKey
                addr = getIndex().findLeafAddr(fromKey);

            }

            assert addr != 0L;
            
            final ImmutableLeaf leaf = getIndex().readLeaf(addr);

            assert leaf != null;
            
            return leaf;

        }

//        /**
//         * Return the leaf that spans the optional {@link #getToKey()}
//         * constraint and the last leaf if there is no {@link #getFromKey()}
//         * constraint.
//         * 
//         * @return The leaf that spans the first tuple that can NOT be visited
//         *         by this cursor (exclusive upper bound).
//         * 
//         * @see Leaf#getKeys()
//         * @see IKeyBuffer#search(byte[])
//         */
//        protected ImmutableLeaf getLeafSpanningToKey() {
//            
//            final long addr;
//            
//            if (toKey == null) {
//
//                addr = getStore().getCheckpoint().addrLastLeaf;
//
//            } else {
//
//                // find leaf for toKey
//                addr = getIndex().findLeafAddr(toKey);
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

        /**
         * Return the leaf that spans the key.  The caller must check to see
         * whether the key actually exists in the leaf.
         * 
         * @param key
         *            The key.
         *            
         * @return The leaf spanning that key.
         */
        protected ImmutableLeaf getLeafSpanningKey(byte[] key) {
            
            final long addr = getIndex().findLeafAddr(key);

            assert addr != 0L;

            final ImmutableLeaf leaf = getIndex().readLeaf(addr);

            assert leaf != null;

            return leaf;

        }

    }

    @Override
    public ImmutableLeafCursor newLeafCursor(SeekEnum where) {
        
        return new ImmutableLeafCursor(where);
        
    }
    
    @Override
    public ImmutableLeafCursor newLeafCursor(byte[] key) {
        
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
        private ImmutableLeafCursor(ImmutableLeafCursor src) {
            
            if (src == null)
                throw new IllegalArgumentException();
            
            this.leaf = src.leaf;
            
        }
        
        public ImmutableLeafCursor(SeekEnum where) {

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
        
        public ImmutableLeafCursor(byte[] key) {
            
            seek(key);
            
        }

        public ImmutableLeaf seek(byte[] key) {
            
            leaf = findLeaf(key);
            
            return leaf;
            
        }
        
        public ImmutableLeaf seek(ILeafCursor<ImmutableLeaf> src) {
            
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
    
}
