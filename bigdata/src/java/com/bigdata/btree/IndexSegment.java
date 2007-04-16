/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
package com.bigdata.btree;

import java.io.DataInput;
import java.io.IOException;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.ResourceManager;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.Bytes;

/**
 * An index segment is read-only btree corresponding to some key range of a
 * segmented and potentially distributed index. The file format of the index
 * segment includes a metadata record, the leaves of the segment in key order,
 * and the nodes of the segment in an arbitrary order. It is possible to map or
 * buffer the part of the file containing the index nodes or the entire file
 * depending on application requirements.
 * <p>
 * Note: iterators returned by this class do not support removal (the nodes and
 * leaves will all refuse mutation operations).
 * 
 * FIXME Support efficient leaf scans in forward order, which requires writing
 * the size of the next leaf so that it can be read out when the current leaf is
 * read out, i.e., as a int field outside of the serialized leaf record. We
 * could also do reverse order by serializing the addr of the prior leaf into
 * the leaf since it is always onhand.
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
    it.unimi.dsi.mg4j.util.BloomFilter bloomFilter;

    /**
     * Text of a message used in exceptions for mutation operations on the index
     * segment.
     */
    final protected transient static String MSG_READ_ONLY = "Read-only index";

    public int getBranchingFactor() {

        reopen();

        return fileStore.metadata.branchingFactor;

    }

    public int getHeight() {

        reopen();

        return fileStore.metadata.height;

    }

    public int getLeafCount() {

        reopen();

        return fileStore.metadata.nleaves;

    }

    public int getNodeCount() {

        reopen();

        return fileStore.metadata.nnodes;

    }

    public int getEntryCount() {

        reopen();

        return fileStore.metadata.nentries;

    }

    public IndexSegment(IndexSegmentFileStore fileStore) {

        this(fileStore, new HardReferenceQueue<PO>(
                new DefaultEvictionListener(),
                BTree.DEFAULT_HARD_REF_QUEUE_CAPACITY,
                BTree.DEFAULT_HARD_REF_QUEUE_SCAN));

    }

    /**
     * Open a read-only index segment.
     * 
     * @param fileStore
     *            The store containing the {@link IndexSegment}.
     * @param hardReferenceQueue
     *            The index segment is read only so we do not need to do IO on
     *            eviction. All the listener needs to do is count queue
     *            evictions to collect statistics on the index performance. The
     *            capacity should be relatively low and the #of entries to scan
     *            should be relatively high since each entry is relatively
     *            large, e.g., try with 100 and 20 respectively.
     * @param valSer
     * 
     * @throws IOException
     * 
     * @todo explore good defaults for the hard reference queue, which should
     *       probably be much smaller as the branching factor grows larger.
     */
    protected IndexSegment(IndexSegmentFileStore fileStore,
            HardReferenceQueue<PO> hardReferenceQueue) {

        super(fileStore, fileStore.metadata.branchingFactor,
                fileStore.metadata.maxNodeOrLeafLength, hardReferenceQueue,
                new CustomAddressSerializer(Addr
                        .getOffset(fileStore.metadata.addrNodes)),
                fileStore.extensionMetadata.getValueSerializer(),
                ImmutableNodeFactory.INSTANCE,
                fileStore.extensionMetadata.getRecordCompressor(),
                fileStore.metadata.useChecksum, fileStore.metadata.indexUUID);

        // Type-safe reference to the backing store.
        this.fileStore = (IndexSegmentFileStore) fileStore;

        _open();

        // report on the event.
        ResourceManager.openIndexSegment(null/* name */, fileStore.getFile()
                .toString(), fileStore.size());

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

    private void _open() {

        // Read the root node.
        this.root = readNodeOrLeaf(fileStore.metadata.addrRoot);

        if (fileStore.metadata.addrBloom == 0L) {

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
     * @return True if the bloom filter believes that the key is present in the
     *         index. When true, you MUST still test the key to verify that it
     *         is, in fact, present in the index. When false, you SHOULD NOT
     *         test the index.
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
     * Operation is disallowed.
     */
    public Object insert(Object key, Object entry) {

        throw new UnsupportedOperationException(MSG_READ_ONLY);

    }

    /**
     * Operation is disallowed.
     */
    public Object remove(Object key) {

        throw new UnsupportedOperationException(MSG_READ_ONLY);

    }

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

            // test the index.
            return super.contains(key);

        }

        // test the index.
        return super.contains(key);

    }

    /**
     * Applies the optional bloom filter if it exists. If the bloom filter
     * exists and reports true, then looks up the value for the key in the index
     * (note that the key might not exist in the index since a bloom filter
     * allows false positives).
     */
    public Object lookup(Object key) {

        if (bloomFilter != null) {

            byte[] _key;

            if (key instanceof byte[]) {

                _key = (byte[]) key;

            } else {
                
                _key = unbox(key);

            }

            if (!containsKey(_key)) {

                // rejected by the bloom filter.
                return null;

            }

            /*
             * Test the index (may be a false positive and we need the value
             * paired to the key in any case).
             */
            return super.lookup(_key);

        }

        // test the index.
        return super.lookup(key);

    }

    /*
     * IBatchBTree (disallows mutation operations, applies optional bloom filter
     * for batch operations).
     */

    /**
     * Disallowed.
     */
    public void insert(BatchInsert op) {

        throw new UnsupportedOperationException(MSG_READ_ONLY);

    }

    /**
     * Disallowed.
     */
    public void remove(BatchRemove op) {

        throw new UnsupportedOperationException(MSG_READ_ONLY);

    }

    /**
     * Apply a batch lookup operation. The bloom filter is used iff it is
     * defined.
     */
    public void lookup(BatchLookup op) {

        if( bloomFilter != null ) {
            
            op.apply(this);
            
        } else {
            
            super.lookup(op);
            
        }
        
    }

    /**
     * Apply a batch existence test operation. The bloom filter is used iff it
     * is defined.
     */
    public void contains(BatchContains op) {

        if( bloomFilter != null ) {
            
            op.apply(this);
            
        } else {
            
            super.contains(op);
            
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
                int branchingFactor, IKeyBuffer keys, Object[] values) {

            return new ImmutableLeaf((AbstractBTree) btree, addr,
                    branchingFactor, keys, values);

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

            public Object insert(Object key, Object val) {

                throw new UnsupportedOperationException(MSG_READ_ONLY);

            }

            public Object remove(Object key) {

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
                    int branchingFactor, IKeyBuffer keys, Object[] values) {

                super(btree, addr, branchingFactor, keys, values);

            }

            public void delete() {

                throw new UnsupportedOperationException(MSG_READ_ONLY);

            }

            public Object insert(Object key, Object val) {

                throw new UnsupportedOperationException(MSG_READ_ONLY);

            }

            public Object remove(Object key) {

                throw new UnsupportedOperationException(MSG_READ_ONLY);

            }

        }

    }

    /**
     * <p>
     * A custom serializer class provides a workaround for node offsets (which
     * are relative to the start of the nodes block) in contrast to leaf offsets
     * (which are relative to a known offset from the start of the index segment
     * file). This condition arises as a side effect of serializing nodes at the
     * same time that the {@link IndexSegmentBuilder} is serializing leaves such
     * that we can not both group the nodes and leaves into distinct regions and
     * know the absolute offset to each node or leave as it is serialized.
     * </p>
     * <p>
     * Addresses are required to be left-shifted by one bit on the
     * {@link INodeData} interface during serialization and the low bit must be
     * a one (1) iff the address is of a child node and a zero (0) iff the
     * address is of a child leaf. During de-serialization, the low bit is
     * examined so that the address may be appropriately decoded and the addr is
     * then right shifted one bit. A leaf address does not require further
     * decodiing. Decoding for a node address requires that we add in the offset
     * of the start of the nodes in the file, which is recorded in
     * {@link IndexSegmentMetadata#offsetNodes} and is specified as a parameter
     * to the {@link CustomAddressSerializer} constructor.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class CustomAddressSerializer implements IAddressSerializer {

        /**
         * The offset within the file of the first node. All nodes are written
         * densely on the file beginning at this offset. The child addresses for
         * a node are relative to this offset and must be corrected during
         * decoding (this is handled automatically by this class).
         */
        protected final int offsetNodes;

        /**
         * Constructor variant used when the offset of the nodes is not known.
         * This is used by the {@link IndexSegmentBuilder}. When using this
         * constructor de-serialization of addresses is disabled.
         */
        public CustomAddressSerializer() {

            this.offsetNodes = 0;

        }

        /**
         * 
         * @param nodesOffset
         *            The offset within the file of the first node. All nodes
         *            are written densely on the file beginning at this offset.
         *            The child addresses for a node are relative to this offset
         *            and must be corrected during decoding (this is handled
         *            automatically by this class). When zero(0) node
         *            deserialization is not permitted (the nodesOffset will be
         *            zero in the metadata record iff no nodes were generated by
         *            the index segment builder).
         * 
         * @see IndexSegmentMetadata#offsetNodes
         */
        public CustomAddressSerializer(long offsetNodes) {

            /*
             * Note: trim to int (we restrict the maximum size of the segment).
             */
            this.offsetNodes = (int) offsetNodes;

            // System.err.println("offsetNodes="+offsetNodes);

        }

        /**
         * This over-estimates the space requirements.
         */
        public int getSize(int n) {

            return Bytes.SIZEOF_LONG * n;

        }

        /**
         * Packs the addresses, which MUST already have been encoded according
         * to the conventions of this class.
         */
        public void putChildAddresses(DataOutputBuffer os, long[] childAddr,
                int nchildren) throws IOException {

            for (int i = 0; i < nchildren; i++) {

                long addr = childAddr[i];

                /*
                 * Children MUST have assigned persistent identity.
                 */
                if (addr == 0L) {

                    throw new RuntimeException(
                            "Child is not persistent: index=" + i);

                }

                // test the low bit. when set this is a node; otherwise a leaf.
                final boolean isLeaf = (addr & 1) == 0;

                // strip off the low bit.
                addr >>= 1;

                final int offset = Addr.getOffset(addr);

                final int nbytes = Addr.getByteCount(addr);

                final int adjustedOffset = (isLeaf ? (offset << 1)
                        : ((offset << 1) | 1));

                // write the adjusted offset (requires decoding).
//                LongPacker.packLong(os, adjustedOffset);
                os.packLong(adjustedOffset);

                // write the #of bytes (does not require decoding).
//                LongPacker.packLong(os, nbytes);
                os.packLong(nbytes);

            }

        }

        /**
         * Unpacks and decodes the addresses.
         */
        public void getChildAddresses(DataInput is, long[] childAddr,
                int nchildren) throws IOException {

            // check that we know the offset for deserialization.
            assert offsetNodes > 0;

            for (int i = 0; i < nchildren; i++) {

                /*
                 * Note: the Address is packed as two long integers. The first
                 * is the offset. The way the packed values are written, the
                 * offset is left-shifted by one and its low bit indicates
                 * whether the referent is a node (1) or a leaf (0).
                 */

                /*
                 * offset (this field must be decoded).
                 */
                long v = LongPacker.unpackLong(is);

                assert v <= Integer.MAX_VALUE;

                // test the low bit. when set this is a node; otherwise a leaf.
                final boolean isLeaf = (v & 1) == 0;

                // right shift by one to remove the low bit.
                v >>= 1;

                // compute the real offset into the file.
                final int offset = isLeaf ? (int) v : (int) v + offsetNodes;

                /*
                 * nbytes (this field does not need any further interpretation).
                 */

                v = LongPacker.unpackLong(is);

                assert v <= Integer.MAX_VALUE;

                final int nbytes = (int) v;

                /*
                 * combine into the correct address.
                 */
                final long addr = Addr.toLong(nbytes, offset);

                if (addr == 0L) {

                    throw new RuntimeException(
                            "Child does not have persistent address: index="
                                    + i);

                }

                childAddr[i] = addr;

            }

        }

        /**
         * Encode an address. The address is left shifted by one bit. If the
         * address is of a node then the low bit is set to one (1) otherwise it
         * will be zero(0).
         * 
         * @param nbytes
         *            The #of bytes in the allocation.
         * @param offset
         *            The offset of the allocation.
         * @param isLeaf
         *            true iff this is the address of a leaf and false iff this
         *            is the address of a node.
         * 
         * @return The encoded address.
         */
        static public long encode(int nbytes, int offset, boolean isLeaf) {

            long addr = Addr.toLong(nbytes, (int) offset);

            addr <<= 1; // (addr << 1)

            if (!isLeaf) {

                addr |= 1; // addr++;

            }

            return addr;

        }

    }

}
