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

import java.io.IOException;

import com.bigdata.journal.ResourceManager;

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
 * FIXME Support efficient leaf scans in forward order, which requires writing
 * the size of the next leaf so that it can be read out when the current leaf is
 * read out, i.e., as a int field outside of the serialized leaf record. We
 * could also do reverse order by serializing the addr of the prior leaf into
 * the leaf since it is always on hand.
 * <p>
 * If this is done, also check {@link Thread#isInterrupted()} and throw an
 * exception when true to support fast abort of scans. See
 * {@link Node#getChild(int)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegment extends AbstractBTree {

    /**
     * Type safe reference to the backing store.
     */
    protected final IndexSegmentFileStore fileStore;

    /**
     * An optional bloom filter that will be used to filter point tests. Since
     * bloom filters do not support removal of keys the option to use a filter
     * is restricted to {@link IndexSegment}s since they are read-only data
     * structures.
     */
    protected it.unimi.dsi.mg4j.util.BloomFilter bloomFilter;

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
     * @see IndexSegmentFileStore#load()
     */
    public IndexSegment(IndexSegmentFileStore fileStore) {

        super(fileStore,
                ImmutableNodeFactory.INSTANCE,
                // FIXME use packed address serializer.
                AddressSerializer.INSTANCE,
//                new CustomAddressSerializer(fileStore.getCheckpoint()),
                fileStore.getMetadata()
                );

        // Type-safe reference to the backing store.
        this.fileStore = (IndexSegmentFileStore) fileStore;

        _open();

    }

    /**
     * Extended to also close the backing file.
     */
    public void close() {

        if (root == null) {

            throw new IllegalStateException("Already closed.");

        }

        // close the backing file.
        fileStore.close();

        // release the optional bloom filter.
        bloomFilter = null;

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

            // reopen the file.
            fileStore.reopen();

            _open();

        }

    }

    /**
     * Note: This is synchronized to ensure that at most one thread gets to
     * re-open the index from its backing store.
     */
    synchronized private void _open() {

        // Read the root node.
        this.root = readNodeOrLeaf(fileStore.getCheckpoint().addrRoot);

        if (fileStore.getCheckpoint().addrBloom == 0L) {

            /*
             * No bloom filter.
             */

            this.bloomFilter = null;

        } else {

            /*
             * Read in the optional bloom filter from its addr.
             */

            try {

                this.bloomFilter = fileStore.readBloomFilter();

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

        }

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
     * Operation is disallowed - the counter is always stored in the mutable
     * btree.
     */
    final public ICounter getCounter() {
        
        throw new UnsupportedOperationException(MSG_READ_ONLY);
        
    }
    
//    /**
//     * Operation is disallowed.
//     */
//    public Tuple insert(byte[] key, byte[] value, boolean delete, long timestamp, Tuple tuple) {
//
//        throw new UnsupportedOperationException(MSG_READ_ONLY);
//
//    }

//    /**
//     * Operation is disallowed.
//     */
//    public Tuple remove(byte[] key, Tuple tuple) {
//
//        throw new UnsupportedOperationException(MSG_READ_ONLY);
//
//    }

    /**
     * Applies the optional bloom filter if it exists. If the bloom filter
     * reports true, then verifies that the key does in fact exist in the index.
     */
    public boolean contains(byte[] key) {

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
                long[] versionTimestamps, boolean[] deleteMarkers) {

            return new ImmutableLeaf((AbstractBTree) btree, addr,
                    branchingFactor, keys, values, versionTimestamps,
                    deleteMarkers);

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
             * @param btree
             * @param addr
             * @param branchingFactor
             * @param keys
             * @param values
             */
            protected ImmutableLeaf(AbstractBTree btree, long addr,
                    int branchingFactor, IKeyBuffer keys, byte[][] values,
                    long[] versionTimestamps, boolean[] deleteMarkers) {

                super(btree, addr, branchingFactor, keys, values,
                        versionTimestamps, deleteMarkers);

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
