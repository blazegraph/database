package com.bigdata.objndx;

import it.unimi.dsi.mg4j.util.BloomFilter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.io.ByteBufferInputStream;
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
public class IndexSegment extends AbstractBTree implements IIndex {

    /**
     * Type safe reference to the backing store.
     */
    final protected IndexSegmentFileStore fileStore;
    
    /**
     * The root of the btree. Since this is a read-only index the root can never
     * be replaced.
     */
    final protected AbstractNode root;

    /**
     * An optional bloom filter that will be used to filter point tests. Since
     * bloom filters do not support removal of keys the option to use a filter
     * is restricted to {@link IndexSegment}s since they are read-only data
     * structures.
     */
    final it.unimi.dsi.mg4j.util.BloomFilter bloomFilter;
    
    /**
     * Text of a message used in exceptions for mutation operations on the
     * index segment.
     */
    final protected static String MSG_READ_ONLY = "Read-only index";

    public int getBranchingFactor() {
        
        return fileStore.metadata.branchingFactor;
        
    }

    public int getHeight() {
        
        return fileStore.metadata.height;
        
    }

    public int getLeafCount() {
        
        return fileStore.metadata.nleaves;
        
    }

    public int getNodeCount() {
        
        return fileStore.metadata.nnodes;
        
    }

    public AbstractNode getRoot() {
        
        return root;
        
    }
    
    public int getEntryCount() {

        return fileStore.metadata.nentries;
        
    }

    public IndexSegment(IndexSegmentFileStore fileStore, IValueSerializer valSer) {

        this(fileStore, new HardReferenceQueue<PO>(
                new DefaultEvictionListener(),
                BTree.DEFAULT_HARD_REF_QUEUE_CAPACITY,
                BTree.DEFAULT_HARD_REF_QUEUE_SCAN), valSer);
        
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
     * @throws IOException
     * 
     * @todo explore good defaults for the hard reference queue, which should
     *       probably be much smaller as the branching factor grows larger.
     * 
     * FIXME move the value serializer into the metadata record.
     * 
     * FIXME add a boolean flag to mark index segments that are the final result
     * of a compacting merge.  This will make it possible to reconstruct from the
     * file system which index segments are part of the consistent state for a
     * given restart time.
     */
    public IndexSegment(IndexSegmentFileStore fileStore,
            HardReferenceQueue<PO> hardReferenceQueue,
            IValueSerializer valSer) {

        super(fileStore, fileStore.metadata.branchingFactor,
                fileStore.metadata.maxNodeOrLeafLength, hardReferenceQueue,
                new CustomAddressSerializer(Addr
                        .getOffset(fileStore.metadata.addrNodes)), valSer,
                ImmutableNodeFactory.INSTANCE,
                fileStore.metadata.useRecordCompressor ? new RecordCompressor()
                        : null, fileStore.metadata.useChecksum);

        // Type-safe reference to the backing store.
        this.fileStore = (IndexSegmentFileStore) fileStore;
        
        // Read the root node.
        this.root = readNodeOrLeaf(fileStore.metadata.addrRoot);

        if( fileStore.metadata.addrBloom == 0L ) {
        
            /*
             * No bloom filter.
             */
            
            this.bloomFilter = null;
            
        } else {

            /*
             * Read in the optional bloom filter from its addr.
             */

            try {

                this.bloomFilter = readBloomFilter(fileStore.metadata.addrBloom);

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }
                        
        }
        
    }

    /**
     * Reads the bloom filter from the file.
     * 
     * Note: this goes around the {@link IndexSegmentFileStore} API since the bloom filter
     * is not (currently) written as a compressed record and since the size of
     * the largest compressed record does not pay attention to the serialized
     * size of the optional bloom filter.
     */
    protected BloomFilter readBloomFilter(long addr) throws IOException {

        assert addr != 0L;
        
        System.err.println("reading bloom filter: "+Addr.toString(addr));
        
        final int off = Addr.getOffset(addr);
        
        final int len = Addr.getByteCount(addr);
        
        ByteBuffer buf = ByteBuffer.allocate(len);

        buf.limit(len);

        buf.position(0);

        try {

            // read into [dst] - does not modify the channel's position().
            final int nread = fileStore.raf.getChannel().read(buf, off);
            
            assert nread == len;
            
            buf.flip(); // Flip buffer for reading.
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        assert buf.position() == 0;
        assert buf.limit() == len;

        ByteBufferInputStream bais = new ByteBufferInputStream(buf);
        
//        ByteArrayInputStream bais = new ByteArrayInputStream(buf.array());
        
        ObjectInputStream ois = new ObjectInputStream(bais);
        
        try {

            BloomFilter bloomFilter = (BloomFilter) ois.readObject();
            
            log.info("Read bloom filter: minKeys=" + bloomFilter.size()
                    + ", entryCount=" + getEntryCount() + ", bytesOnDisk="
                    + len + ", errorRate=" + fileStore.metadata.errorRate);
            
            return bloomFilter;
            
        }
        
        catch(Exception ex) {
            
            IOException ex2 = new IOException("Could not read bloom filter: "+ex);
            
            ex2.initCause(ex);
            
            throw ex2;
            
        }

    }
    
    /**
     * @todo move to parent class and have various methods test to validate that
     *       the index is open (lookup, insert, remove, scan).
     */
    public void close() {
        
        fileStore.close();
        
    }
    
//    /**
//     * Overrides the base class to use the optional bloom filter when present.
//     * 
//     * @todo Verify that the bloom filter is safe for concurrent readers
//     * 
//     * FIXME restore use of the bloom filter once I update the api to byte[]s.
//     * 
//     * FIXME use the bloom filter for the batch lookup api as well.
//     */
//    public Object lookup(Object key) {
//
//        if (key == null) {
//
//            throw new IllegalArgumentException();
//            
//        }
//
//        Object key2;
//        if(stride > 1) {
//            /*
//             * When the stride is greater than one the application needs to
//             * provide an array parameter anyway so you do not need to copy
//             * anything.
//             */
//            key2 = key;
//        } else {
//            /*
//             * unautobox the key. When unboxing a key, we need to allocate a new
//             * buffer each time in order to support concurrent readers.
//             */
//            key2 = ArrayType.alloc(keyType, 1, stride);
//            unbox(key,key2);
//        }
//
//        if( bloomFilter != null && ! containsKey(key2)) {
//
//            /*
//             * If the bloom filter reports that the key does not exist then we
//             * always believe it.
//             */
//
//            counters.nbloomRejects++;
//            
//            return null;
//        
//        }
//        
//        /*
//         * Either there is no bloom filter or the bloom filter believes that the
//         * key exists. Either way we now lookup the entry in the btree.  Again,
//         * we allocate temporary arrays in order to support concurrent readers.
//         */
//
//        final Object[] values = new Object[1];
//
//        /*
//         * use the super class implementation since we already tested the bloom
//         * filter.
//         */
//        super.lookup(1,key2,values);
//        
//        return values[0];
//
//    }

    /**
     * Operation is not supported.
     */
    public void insert(int ntuples, Object keys, Object[] values) {

        throw new UnsupportedOperationException();
        
    }

    /**
     * Operation is not supported.
     */
    public void remove(int ntuples, Object keys, Object[] values) {

        throw new UnsupportedOperationException();
        
    }

//    /**
//     * Used to unbox an application key into a supplied buffer.
//     * 
//     * @param src
//     *            The application key (Integer, Long, etc).
//     * @param dst
//     *            A polymorphic array with room for a single key.
//     */
//    private void unbox(Object src,Object dst) {
//        assert stride == 1;
//        switch(keyType) {
//        case BYTE: ((byte[])dst)[0] = ((Byte)src).byteValue(); break;
//        case SHORT: ((short[])dst)[0] = ((Short)src).shortValue(); break;
//        case CHAR: ((char[])dst)[0] = ((Character)src).charValue(); break;
//        case INT: ((int[])dst)[0] = ((Integer)src).intValue(); break;
//        case LONG: ((long[])dst)[0] = ((Long)src).longValue(); break;
//        case FLOAT: ((float[])dst)[0] = ((Float)src).floatValue(); break;
//        case DOUBLE: ((double[])dst)[0] = ((Double)src).doubleValue(); break;
//        case OBJECT: ((Object[])dst)[0] = src; break;
//        default: throw new UnsupportedOperationException();
//        }
//    }

    /**
     * Returns true if the optional bloom filter reports that the key exists.
     * 
     * @param key
     *            The key.
     * 
     * @return True if the bloom filter believes that the key is present in the
     *         index. When true, you must still test the key to verify that it
     *         is, in fact, present in the index. When false, you do NOT need to
     *         test the index.
     * 
     * @todo examine the #of weights in use by the bloom filter and its impact
     *       on false positives for character data.
     */
    final protected boolean containsKey(byte[] key) {

        assert bloomFilter != null;
        
        return bloomFilter.contains(key);
        
    }

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
     * Factory for immutable nodes and leaves used by the {@link NodeSerializer}.
     */
    protected static class ImmutableNodeFactory implements INodeFactory {

        public static final INodeFactory INSTANCE = new ImmutableNodeFactory();
        
        private ImmutableNodeFactory() {}
        
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

                super(btree, addr, branchingFactor, nentries, keys,
                        childKeys, childEntryCount);

            }

            public void delete() {

                throw new UnsupportedOperationException(MSG_READ_ONLY);
                
            }

            public Object insert(Object key,Object val) {

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

            public Object insert(Object key,Object val) {

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
            
            System.err.println("offsetNodes="+offsetNodes);
            
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
        public void putChildAddresses(DataOutputStream os, long[] childAddr, int nchildren) throws IOException {
            
            for (int i = 0; i < nchildren; i++) {

                long addr = childAddr[i];

                /*
                 * Children MUST have assigned persistent identity.
                 */
                if (addr == 0L) {

                    throw new RuntimeException("Child is not persistent: index="
                            + i);

                }

                // test the low bit.  when set this is a node; otherwise a leaf.
                final boolean isLeaf = (addr & 1) == 0;
                
                // strip off the low bit.
                addr >>= 1;
                
                final int offset = Addr.getOffset(addr);
                
                final int nbytes = Addr.getByteCount(addr);
                
                final int adjustedOffset = (isLeaf ? (offset << 1)
                        : ((offset << 1) | 1));
                
                // write the adjusted offset (requires decoding).
                LongPacker.packLong(os, adjustedOffset);
                
                // write the #of bytes (does not require decoding).
                LongPacker.packLong(os, nbytes);

            }

        }

        /**
         * Unpacks and decodes the addresses.
         */
        public void getChildAddresses(DataInputStream is, long[] childAddr,
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
                
                // test the low bit.  when set this is a node; otherwise a leaf.
                final boolean isLeaf = (v & 1) == 0;

                // right shift by one to remove the low bit.
                v >>= 1;

                // compute the real offset into the file.
                final int offset = isLeaf? (int)v : (int)v + offsetNodes;
                
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
                            "Child does not have persistent address: index=" + i);

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
        static public long encode(int nbytes,int offset,boolean isLeaf) {
            
            long addr = Addr.toLong(nbytes, (int) offset);
            
            addr <<= 1; // (addr << 1)
            
            if (!isLeaf) {
                
                addr |= 1; // addr++;
            
            }
            
            return addr;
            
        }

    }
    
}
