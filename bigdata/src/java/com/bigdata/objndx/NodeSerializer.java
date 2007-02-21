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
/*
 * Created on Nov 5, 2006
 */
package com.bigdata.objndx;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.io.ByteBufferOutputStream;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.Bytes;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.ChecksumUtility;

/**
 * <p>
 * An instance of this class is used to serialize and de-serialize the
 * {@link INodeData}s and {@link ILeafData}s of an {@link IIndex}. Leaf and
 * non-leaf records have different serialization formats, but their leading
 * bytes use the same format so that you can tell by inspection whether a buffer
 * contains a leaf or a non-leaf node. The header of the record uses a fixed
 * length format so that some fields can be tested without full
 * de-serialization, e.g., the checksum, whether the record contains a leaf vs a
 * node, etc. This fixed record also makes it possible to update some fields in
 * the header once the entire record has been serialized, including the
 * checksum, the #of bytes in the serialized record, and the prior/next
 * addresses for leaves.
 * </p>
 * <p>
 * The methods defined by this class all work with {@link ByteBuffer}s. On
 * read, the buffer must be positioned to the start of the data to be read.
 * After a read, the buffer will be positioned to the first byte after the data
 * read. If there is insufficient data available in the buffer then an
 * {@link BufferUnderflowException} will be thrown. On write, the data will be
 * written starting at the current buffer position. After a write the position
 * will be updated to the first byte after the data written. If there is not
 * enough space remaining in the buffer then a {@link BufferOverflowException}
 * will be thrown.
 * </p>
 * <p>
 * The (de-)serialization interfaces for the values {@link IValueSerializer}
 * hides the use of {@link ByteBuffer}s from the application. The use of
 * compression or packing techniques within the implementations of this
 * interfaces is encouraged.
 * </p>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo automatically resize the decompression buffers as required and start
 *       with a smaller buffer.
 * 
 * @todo consider the use of thread-local variables for the read buffers so that
 *       the buffers may be used by multiple concurrent readers (the bloom
 *       filter is another place where concurrency is limited for readers)
 * 
 * @todo try using an allocation pools for node and leaf objects together with
 *       keys[] and vals[]. We can know when the references are cleared, but at
 *       that point we no longer have the reference so we can not reuse the
 *       large arrays (keys[] and vals[]). It may not make much sense to reuse
 *       vals[] since it always holds Objects. Unless those objects are mutable
 *       we have to clear the array contents so that the values can be GCd. The
 *       same is true when the keys[] contains Objects. The case where reuse
 *       makes the most sense is when it is a primitive data type [] with a
 *       modestly large capacity.<br>
 *       The only way that I can see to manage this is to explicitly deallocate
 *       the leaf/node. This can either be done immediately when it falls off of
 *       the hard reference queue and is written onto the store or we could move
 *       the reference to a secondary hard reference queue for retention of
 *       immutable nodes and leaves. This shoulds be examined in conjunction
 *       with the examination of breaking the hard reference queue into one
 *       queue for nodes and one for leaves.
 * 
 * FIXME add support for serializing the prior/next references when known. we do
 * checksums which makes it trickier to touch up those references after the fact
 * and the next reference is not knowable for the index segments until we have
 * determined the serialized size of the current leaf, which makes that all a
 * bit tricky.
 * 
 * @todo make the checksum optional or do not use when using the record
 *       compressor?
 * 
 * @see IIndex
 * @see INodeData
 * @see ILeafData
 * @see IKeySerializer
 * @see IValueSerializer
 * @see LongPacker
 */
public class NodeSerializer {

    /**
     * An object that knows how to constructor nodes and leaves.
     */
    protected final INodeFactory nodeFactory;
    
    /**
     * The declared maximum branching factor.
     */
    protected final int branchingFactor;
    
    /**
     * An object that knows how to (de-)serialize child addresses.
     */
    protected final IAddressSerializer addrSerializer;
    
    /**
     * An object that knows how to (de-)serialize keys.
     */
    protected final IKeySerializer keySerializer;

    /**
     * An object that knows how to (de-)serialize the values on leaves.
     */
    protected final IValueSerializer valueSerializer;
    
    /**
     * Used to serialize and de-serialize the nodes and leaves of the tree. This
     * is pre-allocated based on the estimated maximum size of a node or leaf.
     * If the buffer overflows it is re-allocated and the operation will be
     * retried.
     */
    protected ByteBuffer _buf;
    
    /**
     * Used to (de-)compress serialized records (optional).
     */
    protected final RecordCompressor recordCompressor;
    
    /**
     * When true, checksums are computed, stored, and verified on read. This
     * option is not recommended in conjunction with a fully buffered store.
     */
    protected final boolean useChecksum;

    /**
     * The default initial capacity multipler for the (de-)serialization buffer.
     * The total initial buffer capacity is this value times the
     * {@link #branchingFactor}.
     */
    public static final transient int DEFAULT_BUFFER_CAPACITY_PER_ENTRY = Bytes.kilobyte32 * 1;
    
    /**
     * The {@link Adler32} checksum. This is an int32 value, even through the
     * {@link Checksum} API returns an int64 (aka long integer) value. The
     * actual checksum is in the lower 32 bit.
     */
    static final int SIZEOF_CHECKSUM = Bytes.SIZEOF_INT;

    /**
     * The size of the field whose value is the length of the serialized record
     * in bytes.
     */
    static final int SIZEOF_NBYTES = Bytes.SIZEOF_INT;
    
    /**
     * The size of the boolean field indicating whether a serialized record
     * contains is a node, leaf, or a linked leaf.
     */
    static final int SIZEOF_NODE_TYPE = Bytes.SIZEOF_BYTE;

    /**
     * The size of the short integer field containing the serialization version
     * used for this node or leaf.
     */
    static final int SIZEOF_VERSION = Bytes.SIZEOF_SHORT;

    /**
     * Size of an <em>unpacked</em> persistent node or leaf address. This is
     * only used for leaves since they need to serialize the prior and next
     * references in fixed length fields.
     */
    static final int SIZEOF_REF = Bytes.SIZEOF_LONG;

    /**
     * Offset of the int32 value that is the {@link Adler32} checksum of the
     * serialized node or leaf. The checksum is computed for all bytes exclusing
     * the first 4 bytes, on which the value of the computed checksum is
     * written.
     */
    static final int OFFSET_CHECKSUM = 0;

    /**
     * Offset of the int32 signed integer whose value is the #of bytes in the
     * serialized record. This is written on the record so that we can validate
     * the checksum immediately when attempting to read a record and thereby
     * prevent inadvertent allocations of arrays for keys and values based on
     * bad data.
     */
    static final int OFFSET_NBYTES = OFFSET_CHECKSUM + SIZEOF_CHECKSUM;
 
    /**
     * Offset of the byte whose value indicates whether this node is a leaf, a
     * linked leaf (having prior and next leaf references), or a node.
     * 
     * @see #TYPE_NODE
     * @see #TYPE_LEAF
     * @see #TYPE_LINKED_LEAF
     */
    static final int OFFSET_NODE_TYPE = OFFSET_NBYTES + SIZEOF_NBYTES;
    
    /**
     * Offset of the short integer whose value is the version identifier for the
     * serialization format.
     * 
     * @see #VERSION0
     */
    static final int OFFSET_VERSION = OFFSET_NODE_TYPE + SIZEOF_NODE_TYPE;

    /**
     * When used, the offset in the leaf header at which the reference of the
     * prior leaf in key order is found.
     */
    static final int OFFSET_PRIOR = OFFSET_VERSION + SIZEOF_VERSION;
 
    /**
     * When used, the offset in the leaf header at which the reference of the
     * next leaf in key order is found.
     */
     static final int OFFSET_NEXT = OFFSET_PRIOR + SIZEOF_REF;

    /**
     * Size of the fixed length header for a serialized node.
     */
    static final int SIZEOF_NODE_HEADER = OFFSET_VERSION + SIZEOF_VERSION;
    
    /**
     * Size of the fixed length header for a serialized leaf when the prior
     * and next references are NOT used.
     */
    static final int SIZEOF_LEAF_HEADER = SIZEOF_NODE_HEADER;

    /**
     * Size of the fixed length header for a serialized leaf when the prior
     * and next references are used.
     */
    static final int SIZEOF_LINKED_LEAF_HEADER = OFFSET_NEXT + SIZEOF_REF;
    
    /**
     * The only defined serialization format.
     */
    private static short VERSION0 = (short)0;

    /**
     * The value (0) indicates a non-leaf node.
     */
    public static final byte TYPE_NODE = (byte)0;
    /**
     * The value (1) indicates a leaf without prior and next references.
     */
    public static final byte TYPE_LEAF = (byte)1;
    /**
     * The value (2) indicates a leaf with prior and next references. This
     * allows us to elide those fields from the leaf header when the data will
     * not be made available by application.
     */
    public static final byte TYPE_LINKED_LEAF= (byte)2;
    
    /**
     * A private instance is used to compute checksums for each
     * {@link AbstractBTree}. This makes is possible to have concurrent reads
     * or writes on multiple btrees that are backed by different stores.
     */
    private final ChecksumUtility chk;
    
    public IValueSerializer getValueSerializer() {
        
        return valueSerializer;
        
    }
    
    /**
     * Constructor is disallowed.
     */
    private NodeSerializer() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Designated constructor.
     * 
     * @param nodeFactory
     *            An object that knows how to construct {@link INodeData}s and
     *            {@link ILeafData leaves}.
     * 
     * @param branchingFactor
     *            The maximum branching factor for nodes or leaves serialized
     *            using this object. This is informative and is used only to
     *            estimate the initialBufferCapacity when that value is not
     *            given.
     * 
     * @param initialBufferCapacity
     *            The initial capacity for {@link #buf} (optional). This will be
     *            resized as necessary if the buffer overflows during a write.
     *            When zero (0), the initial capacity is defaulted to
     *            {@link #DEFAULT_BUFFER_CAPACITY}.
     * 
     * @param addrSerializer
     *            An object that knows how to (de-)serialize the child addresses
     *            on an {@link INodeData}.
     * 
     * @param valueSerializer
     *            An object that knows how to (de-)serialize the values on
     *            {@link ILeafData leaves}.
     * 
     * @param recordCompressor
     *            An object that knows how to (de-)compress a serialized record
     *            (optional).
     * 
     * @param useChecksum
     *            When true the checksum of the serialized record will be
     *            computed and stored in the record. When the record is
     *            de-serialized the checksum will be validated. Note that
     *            computing the checksum is ~ 40% of the cost of
     *            (de-)serialization. Further, when the backing store is a fully
     *            buffered journal the checksum is validating memory (already
     *            parity checked) not disk so the use of the checksum in this
     *            situation is not recommended. The checksum makes the most
     *            sense for an {@link IndexSegment} since the data are being
     *            read from disk.
     * 
     * @todo change recordCompressor to an interface.
     */
    public NodeSerializer(INodeFactory nodeFactory, int branchingFactor,
            int initialBufferCapacity, IAddressSerializer addrSerializer,
            IKeySerializer keySerializer, IValueSerializer valueSerializer,
            RecordCompressor recordCompressor, boolean useChecksum) {

        assert nodeFactory != null;

        assert branchingFactor >= AbstractBTree.MIN_BRANCHING_FACTOR;
        
        assert initialBufferCapacity >= 0;
        
        assert addrSerializer != null;
        
        assert keySerializer != null;

        assert valueSerializer != null;

        this.nodeFactory = nodeFactory;
        
        this.branchingFactor = branchingFactor;
        
        this.addrSerializer = addrSerializer;
        
        this.keySerializer = keySerializer;
        
        this.valueSerializer = valueSerializer;

        this.recordCompressor = recordCompressor;

        this.useChecksum = useChecksum;

        this.chk = useChecksum ? new ChecksumUtility() : null;
        
        if (initialBufferCapacity == 0) {

            initialBufferCapacity = DEFAULT_BUFFER_CAPACITY_PER_ENTRY
                    * branchingFactor;

        }
        
        this._buf = alloc(initialBufferCapacity);

        /*
         * Allocate compression buffer iff a compression algorithm is used.
         * 
         * FIXME The capacity of this buffer is a SWAG. If it is too small then
         * an EOFException will be thrown. This needs to be modified start with
         * a smaller buffer and grow as required. An alternative would be to
         * re-allocate this whenever _buf is resize since the compressed data
         * should never be larger than the original data.
         * 
         * @todo consider discarding [buf] and [cbuf] if the node serializer
         * becomes inactive in order to minimize memory use. they can be
         * reallocated as necesssary.
         */
    
        cbuf = recordCompressor != null //
                ? ByteBuffer.allocate(Bytes.megabyte32) //
//                ? ByteBuffer.allocateDirect(Bytes.megabyte32*2) //
                : null;
        
    }

    /**
     * Allocate a buffer of the stated capacity.
     * 
     * @param capacity
     *            The buffer capacity.
     * 
     * @return The buffer.
     */
    static protected ByteBuffer alloc(int capacity) {
        
//        return (true || capacity < Bytes.kilobyte32 * 8 )? ByteBuffer
//                .allocate(capacity) : ByteBuffer
//                .allocateDirect(capacity);

        /*
         * Note: this always allocates a buffer wrapping a Java <code>byte[]</code>
         * NOT a direct {@link ByteBuffer}. There is a substantial performance
         * gain when you are doing a lot of get/put byte operations to use a
         * wrapped, rather than direct, {@link ByteBuffer} (this was observed
         * using the Sun JDK 1.5.07 with the -server mode).
         * 
         * @todo verify that this is true when using the Transient vs Direct
         * buffer modes and how it interacts with whether the Journal's buffer
         * is transient or direct.
         */

        return ByteBuffer.allocate(capacity);
        
    }

    /**
     * Extends the internal buffer used to serialize nodes and leaves.
     * <p>
     * Note: large buffer requirements are not at all uncommon so we grow the
     * buffer rapidly to avoid multiple resizing and the expense of a too large
     * buffer.
     * 
     * FIXME We can encapsulate the extension of the buffer within a class
     * derived from or using the {@link ByteBufferOutputStream} and simply copy
     * the data when we need to extend the buffer rather than restarting
     * serialization. This will make underestimates of the required buffer
     * capacity much less costly.
     */
    protected void extendBuffer() {

        int capacity = _buf.capacity();
        
        capacity *= 2;

        System.err.println("Extending buffer to capacity=" + capacity
                + " bytes.");

        _buf = alloc(capacity);

    }

    /**
     * De-serialize a node or leaf. This method is used when the caller does not
     * know a-priori whether the reference is to a node or leaf. The decision is
     * made based on inspection of the {@link #OFFSET_NODE_TYPE} byte in the
     * supplied buffer.
     * 
     * @param btree
     *            The btree.
     * @param addr
     *            The address of the node or leaf being de-serialized - see
     *            {@link Addr}.
     * @param buf
     *            The buffer containing the serialized data.
     * 
     * @todo document and verify effects on buf position and limit w/ and w/o
     *       decompression.
     * 
     * @return The de-serialized node.
     */
    public IAbstractNodeData getNodeOrLeaf( IIndex btree, long addr, ByteBuffer buf) {

//        assert btree != null;
//        assert addr != 0L;
        assert buf != null;
        
        /*
         * optionally decompresses the record. note that we must decompress the
         * buffer before we can test the byte that will determine if it is a
         * node or a leaf.
         */
        
        ByteBuffer tmp = buf;
        
        assert tmp.position() == 0;
        
        buf = decompress( buf );

        assert tmp.position() == 0;

        final IAbstractNodeData ret;
        
        final byte nodeType = buf.get(OFFSET_NODE_TYPE);
        
        switch(nodeType) {
        
        case TYPE_NODE: {

            assert tmp.position() == 0;

            // deserialize (already decompressed)
            ret = getNode(btree, addr, buf, true);

            /*
             * @todo it is extremely weird, but this assertion (and the parallel
             * one below) trips during the AbstractBTreeWithJournalTestCase stress tests.
             * this is odd because the code explicitly resets the position of
             * the buffer that it is manipulating as its last step in getNode()
             * and getLeaf().  there must be some odd interaction with _buf but
             * I can not figure it out.
             */
//            assert tmp.position() == 0;
            
            break;
            
        }
        
        case TYPE_LEAF:
        case TYPE_LINKED_LEAF: {

            assert tmp.position() == 0;

            // deserialize (already decompressed)
            ret = getLeaf(btree, addr, buf, true);

            // @todo see the note above.
//            assert tmp.position() == 0;
            
            break;

        }

        default: {
        
            throw new RuntimeException("unknown node type="+nodeType);
            
        }
        
        }
     
        return ret;

    }
    
    /**
     * Serialize a node or leaf.
     * 
     * @param node
     *            The node or leaf.
     * 
     * @return The serialized record.
     * 
     * @exception BufferOverflowException
     *                if there is not enough space remaining in the buffer.
     */
    public ByteBuffer putNodeOrLeaf(IAbstractNodeData node) {
        
        if(node instanceof INodeData) {
            
            return putNode((INodeData)node);
            
        } else {
        
            return putLeaf((ILeafData)node);
            
        }
        
    }
    
    /**
     * Serialize a node onto an internal buffer and returns that buffer. If the
     * operation would overflow then the buffer is extended and the operation is
     * retried.
     * 
     * @param node
     *            The node.
     * 
     * @return the buffer containing the serialized node. the position will be
     *         zero and the limit will be the #of bytes in the serialized node.
     */
    public ByteBuffer putNode(INodeData node) {

        while (true) {

            try {

                return putNode(_buf, node);

            } catch (BufferOverflowException ex) {

                extendBuffer();

            }

        }

    }
    
    private ByteBuffer putNode(ByteBuffer buf, INodeData node) {
        
        assert buf != null;
        assert node != null;

        final int branchingFactor = node.getBranchingFactor();
        final int nentries = node.getEntryCount();
        final int[] childEntryCounts = node.getChildEntryCounts();
        final int nkeys = node.getKeyCount();
        final IKeyBuffer keys = node.getKeys();
        final long[] childAddr = node.getChildAddr();

        /*
         * fixed length node header.
         */

        buf.clear();
        
        final int pos0 = buf.position();

        // checksum
        buf.putInt(0); // will overwrite below with the checksum.

        // #bytes
        buf.putInt(0); // will overwrite below with the actual value.
        
        // nodeType
        buf.put(TYPE_NODE); // this is a non-leaf node.

        // version
        buf.putShort(VERSION0);
        
        /*
         * Setup output stream over the buffer.
         * 
         * Note: I have tested the use of a {@link BufferedOutputStream} here
         * and in putLeaf() and it actually slows things down a smidge.
         */
        DataOutputStream os = new DataOutputStream(//
                new ByteBufferOutputStream(buf)
//              new BufferedOutputStream(new ByteBufferOutputStream(buf))
                );
        
        try {

            // branching factor.
            LongPacker.packLong(os, branchingFactor);

            // #of spanned entries.
            LongPacker.packLong(os, nentries);
            
//            // #of keys
//            LongPacker.packLong(os, nkeys);

            // keys.
            keySerializer.putKeys(os, keys);

            // addresses.
            addrSerializer.putChildAddresses(os, childAddr, nkeys+1);

            // #of entries spanned per child.
            putChildEntryCounts(os,childEntryCounts,nkeys+1);
            
            // Done using the DataOutputStream so flush to the ByteBuffer.
            os.flush();

        }

        catch (EOFException ex) {

            /*
             * Masquerade the EOFException as a buffer overflow since that is
             * what it really represents.
             */
            RuntimeException ex2 = new BufferOverflowException();

            ex2.initCause(ex);

            throw ex2;

        }

        catch (IOException ex) {

            /*
             * This should not occur since we are writing onto a ByteBuffer but
             * it is declared by the methods on DataOutputStream.
             */

            throw new RuntimeException(ex);

        }

        // #of bytes actually written.
        final int nbytes = buf.position() - pos0;
        assert nbytes > SIZEOF_NODE_HEADER;

        // patch #of bytes written on the record format.
        buf.putInt(pos0 + OFFSET_NBYTES, nbytes);

        // compute checksum for data written.
        final int checksum = useChecksum ? chk.checksum(buf, pos0
                + SIZEOF_CHECKSUM, pos0 + nbytes) : 0;

        // System.err.println("computed node checksum: "+checksum);

        // write the checksum into the buffer.
        buf.putInt(pos0, checksum);

        // flip the buffer to prepare for reading.
        buf.flip();

        // optionally compresses the record.
        return compress( buf );
                
    }

    public INodeData getNode(IIndex btree,long addr,ByteBuffer buf) {
        
        return getNode(btree,addr,buf,false);
        
    }

    /**
     * De-serialize the node.
     * 
     * @param btree
     *            The btree to which the node belongs.
     * @param addr
     *            The address of the node - see {@link Addr}.
     * @param buf
     *            The buffer containing the serialized node.
     * @param decompressed
     *            true iff the buffer has already been decompressed using
     *            {@link #decompress(ByteBuffer)}.
     * 
     * @return The deserialized node.
     */
    protected INodeData getNode(IIndex btree,long addr,ByteBuffer buf, boolean decompressed ) {

//        assert btree != null;
//        assert addr != 0L;
        assert buf != null;

        // optionally decompresses the record.
        assert buf.position() == 0;
        if( ! decompressed) {
            buf = decompress( buf );
            assert buf.position() == 0;
        }

        /*
         * Read fixed length node header.
         */

        final int pos0 = buf.position();

        assert pos0 == 0;

        // checksum
        final int readChecksum = buf.getInt(); // read checksum.
        // System.err.println("read checksum="+readChecksum);

        // #of bytes in record.
        final int nbytes = buf.getInt();
        assert nbytes > SIZEOF_NODE_HEADER;

        /*
         * verify checksum now that we know how many bytes of data we expect to
         * read.
         */
        if( useChecksum ) {
        
            final int computedChecksum = chk.checksum(buf, pos0
                    + SIZEOF_CHECKSUM, pos0 + nbytes);

            if (computedChecksum != readChecksum) {

                throw new ChecksumError("Invalid checksum: read "
                        + readChecksum + ", but computed " + computedChecksum);

            }

        }
        
        // nodeType
        if (buf.get() != TYPE_NODE) {

            // expecting a non-leaf node.
            throw new RuntimeException("Not a Node: id=" + addr);

        }

        // version
        final short versionId = buf.getShort();
        
        if (versionId != VERSION0)
            throw new RuntimeException("Unknown serialization version: "
                    + versionId);
        
        /*
         * Setup input stream reading from the buffer.
         */
        final DataInputStream is = new DataInputStream(
                new ByteBufferInputStream(buf));

        try {

            // branching factor.
            final int branchingFactor = (int) LongPacker.unpackLong(is);

            assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;

            // nentries
            final int nentries = (int)LongPacker.unpackLong(is);
            
//            // nkeys
//            final int nkeys = (int) LongPacker.unpackLong(is);

//            // Note: minimum is (m+1/2) unless this is the root node.
//            assert nkeys >= 0 && nkeys < branchingFactor;

            final long[] childAddr = new long[branchingFactor + 1];

            final int[] childEntryCounts = new int[branchingFactor + 1];
            
            // Keys.

//            final Object keys = ArrayType.alloc(keyType, branchingFactor, stride);

            final IKeyBuffer keys = keySerializer.getKeys(is);

            final int nkeys = keys.getKeyCount();
            
            // Child addresses (nchildren == nkeys+1).

            addrSerializer.getChildAddresses(is, childAddr, nkeys+1);

            // #of entries spanned by each child.
            
            getChildEntryCounts(is,childEntryCounts,nkeys+1);

            // verify #of bytes actually read.
            assert buf.position() - pos0 == nbytes;

            // reset the buffer position.
            buf.position(pos0);
            assert buf.position() == 0;
            
            // Done.
            return nodeFactory.allocNode(btree, addr, branchingFactor,
                    nentries, keys, childAddr, childEntryCounts);

        }

        catch (EOFException ex) {

            /*
             * Masquerade an EOF reading on the input stream as a buffer
             * underflow, which is what it really represents.
             */
            RuntimeException ex2 = new BufferUnderflowException();

            ex2.initCause(ex);

            throw ex2;

        }

        catch (IOException ex) {

            /*
             * This should not occur since we are reading from a ByteBuffer, but
             * this is thrown by methods reading on the DataInputStream.
             */
            throw new RuntimeException(ex);

        }

    }
    
    /**
     * Serialize a leaf node onto a buffer. If the operation would overflow then
     * the buffer is extended and the operation is retried.
     * 
     * @param leaf
     *            The leaf node.
     * 
     * @return the buffer containing the serialized leaf. the position will be
     *         zero and the limit will be the #of bytes in the serialized leaf.
     */
    public ByteBuffer putLeaf(ILeafData leaf) {

        while (true) {

            try {

                return putLeaf(_buf,leaf);

            } catch (BufferOverflowException ex) {

                extendBuffer();

            }

        }
        
    }
     
    private ByteBuffer putLeaf(ByteBuffer buf, ILeafData leaf) {

        assert buf != null;
        assert leaf != null;
        
        final int nkeys = leaf.getKeyCount();
        final int branchingFactor = leaf.getBranchingFactor();
        final IKeyBuffer keys = leaf.getKeys();
        final Object[] vals = leaf.getValues();
        
        buf.clear();
        
        /*
         * common data.
         */
        final int pos0 = buf.position();

        // checksum
        buf.putInt(0); // will overwrite below with the checksum.
        
        // nbytes
        buf.putInt(0); // will overwrite below with the actual value.
        
        // nodeType
        buf.put(TYPE_LEAF); // this is a leaf node.

        // version
        buf.putShort(VERSION0);
        
        /*
         * Setup output stream over the buffer.
         * 
         * Note: wrapping this with a BufferedOutputStream is slightly slower.
         */
        DataOutputStream os = new DataOutputStream(//
                new ByteBufferOutputStream(buf)
//                new BufferedOutputStream(new ByteBufferOutputStream(buf))
                );

        try {
            
            // branching factor.
            LongPacker.packLong(os, branchingFactor);

//            // #of keys
//            LongPacker.packLong(os, nkeys);

            // keys.
            keySerializer.putKeys(os, keys);

            // values.
            valueSerializer.putValues(os, vals, nkeys);

            // Done using the DataOutputStream so flush to the ByteBuffer.
            os.flush();
            
        }

        catch (EOFException ex) {

            /*
             * Masquerade the EOFException as a buffer overflow since that is
             * what it really represents.
             */
            RuntimeException ex2 = new BufferOverflowException();

            ex2.initCause(ex);

            throw ex2;

        }

        catch (IOException ex) {

            /*
             * This should not occur since we are writing onto a ByteBuffer but
             * it is declared by the methods on DataOutputStream.
             */
            
            throw new RuntimeException(ex);

        }

        // #of bytes actually written.
        final int nbytes = buf.position() - pos0;
        assert nbytes > SIZEOF_LEAF_HEADER;

        // patch #of bytes written on the record format.
        buf.putInt(pos0 + OFFSET_NBYTES, nbytes);

        // compute checksum
        final int checksum = (useChecksum ? chk.checksum(buf, pos0
                + SIZEOF_CHECKSUM, pos0 + nbytes) : 0);
        // System.err.println("computed leaf checksum: "+checksum);

        // write checksum on buffer.
        buf.putInt(pos0, checksum);

        /*
         * Flip the buffer to prepare it for reading. The position will be zero
         * and the limit will be the #of bytes in the serialized record.
         */
        buf.flip();
        
        // optionally compresses the record.
        return compress( buf );
                
    }

    protected ILeafData getLeaf(IIndex btree,long addr,ByteBuffer buf) {
        
        return getLeaf(btree,addr,buf,false);
        
    }
    
    /**
     * De-serialize a leaf.
     * 
     * @param btree
     *            The owning btree.
     * @param addr
     *            The address of the leaf - see {@link Addr}.
     * @param buf
     *            The buffer containing the serialized leaf.
     * @param decompressed
     *            true iff the buffer contains a record that has already been
     *            decompressed using {@link #decompress(ByteBuffer)}.
     * 
     * @return The deserialized leaf.
     */
    protected ILeafData getLeaf(IIndex btree,long addr,ByteBuffer buf,boolean decompressed) {
        
//        assert btree != null;
//        assert addr != 0L;
        assert buf != null;

        // optionally decompresses the record.
        if( ! decompressed) buf = decompress( buf );

        /*
         * common data.
         */

        final int pos0 = buf.position();

        assert pos0 == 0;
        
        // checksum
        final int readChecksum = buf.getInt(); // read checksum.
        // System.err.println("read checksum="+readChecksum);

        // #bytes.
        final int nbytes = buf.getInt();

        /*
         * verify checksum.
         */
        if (useChecksum) {
        
            final int computedChecksum = chk.checksum(buf, pos0
                    + SIZEOF_CHECKSUM, pos0 + nbytes);

            if (computedChecksum != readChecksum) {

                throw new ChecksumError("Invalid checksum: read "
                        + readChecksum + ", but computed " + computedChecksum);

            }

        }

        // nodeType
        final byte nodeType = buf.get();
        
        if (nodeType != TYPE_LEAF && nodeType != TYPE_LINKED_LEAF) {

            // expecting a leaf.
            throw new RuntimeException("Not a leaf: id=" + addr + ", nodeType="
                    + nodeType);

        }

        // version
        final short versionId = buf.getShort();
        
        if (versionId != VERSION0)
            throw new RuntimeException("Unknown serialization version: "
                    + versionId);

        /*
         * Setup input stream reading from the buffer.
         */
        final DataInputStream is = new DataInputStream(
                new ByteBufferInputStream(buf));

        try {

            // branching factor
            final int branchingFactor = (int) LongPacker.unpackLong(is);

            assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;

//            // nkeys
//            final int nkeys = (int) LongPacker.unpackLong(is);
//
//            // Note: minimum is (m+1)/2 unless root leaf.
//            assert nkeys >= 0 && nkeys <= branchingFactor;

            // keys.

            final IKeyBuffer keys = keySerializer.getKeys(is);

            final int nkeys = keys.getKeyCount();
            
            // values.

            final Object[] values = new Object[branchingFactor + 1];

            valueSerializer.getValues(is, values, nkeys);

            // verify #of bytes actually read.
            assert buf.position() - pos0 == nbytes;

            // reset the buffer position.
            buf.position(0);
            assert buf.position() == 0;
            
            // Done.
            return nodeFactory.allocLeaf(btree, addr, branchingFactor, keys,
                    values);

        }

        catch (EOFException ex) {

            /*
             * Masquerade an EOF reading on the input stream as a buffer
             * underflow, which is what it really represents.
             */
            RuntimeException ex2 = new BufferUnderflowException();

            ex2.initCause(ex);

            throw ex2;

        }

        catch (IOException ex) {

            /*
             * This should not occur since we are reading from a ByteBuffer, but
             * this is thrown by methods reading on the DataInputStream.
             */
            throw new RuntimeException(ex);

        }

    }
    
    /**
     * Write out a packed array of the #of entries spanned by each child of some
     * node.
     * 
     * @param os
     * @param childEntryCounts
     *            The #of entries spanned by each direct child.
     * @param nchildren
     *            The #of elements of that array that are defined.
     * @throws IOException
     */
    protected void putChildEntryCounts(DataOutputStream os,
            int[] childEntryCounts, int nchildren) throws IOException {

        for (int i = 0; i < nchildren; i++) {

            final long nentries = childEntryCounts[i];

            /*
             * Children MUST span some entries.
             */
            if (nentries == 0L) {

                throw new RuntimeException(
                        "Child does not span entries: index=" + i);

            }

            LongPacker.packLong(os, nentries);

        }

    }

    /**
     * Read in a packed array of the #of entries spanned by each child of some
     * node.
     * 
     * @param is
     * @param childEntryCounts
     *            The #of entries spanned by each direct child.
     * @param nchildren
     *            The #of elements of that array that are defined.
     * @throws IOException
     */
    protected void getChildEntryCounts(DataInputStream is,
            int[] childEntryCounts, int nchildren) throws IOException {

        for (int i = 0; i < nchildren; i++) {

            final int nentries = (int) LongPacker.unpackLong(is);

            if (nentries == 0L) {

                throw new RuntimeException(
                        "Child does not span entries: index=" + i);

            }

            childEntryCounts[i] = nentries;

        }

    }

    protected void putTimestamps(DataOutputStream os, long[] timestamps,
            int nentries) throws IOException {

        for (int i = 0; i < nentries; i++) {

            final long timestamp = timestamps[i];

            LongPacker.packLong(os, timestamp);

        }

    }

    protected void getTimestamps(DataInputStream is, long[] timestamps,
            int nentries) throws IOException {

        for (int i = 0; i < nentries; i++) {

            final long timestamp = LongPacker.unpackLong(is);

            timestamps[i] = timestamp;

        }

    }

    /**
     * Compress a record in the buffer.
     * 
     * @param buf
     *            The record. The data from the position to the limit will be
     *            compressed.
     * 
     * @return The record unless compression is enabled in which case the
     *         compressed record is returned.  the position will be zero and
     *         the limit will be the #of bytes in the compressed record.
     */
    protected ByteBuffer compress(ByteBuffer buf) {

        assert buf.position() == 0;
        
        if( recordCompressor == null ) return buf;
        
        // clear compression buffer.
        cbuf.clear();
        
        // setup writer onto compression buffer.
        ByteBufferOutputStream bbos = new ByteBufferOutputStream(cbuf);
        
        // compress the serialized leaf.
        recordCompressor.compress(buf, bbos);
 
        /*
         * reset the position to zero(0) to avoid the side effect on [buf] of
         * compressing the buffer (which advances to position to the limit).
         */
        buf.position(0);
        
        // Note: flush/close are not required.
//        try {
//            
//            // flush the compression buffer.
//            bbos.flush();
//
//            // and close it.
//            bbos.close();
//
//        } catch (IOException ex) {
//            
//            throw new RuntimeException(ex);
//            
//        }
        
        // flip the compressed buffer to prepare for writing or reading.
        cbuf.flip();

        assert cbuf.position() == 0;

//        cbuf.position(0);
        
        return cbuf;
        
    }

    /**
     * Buffer for compressed records.
     */
    final private ByteBuffer cbuf;
        
    /**
     * If compression is enabled, then decompress the data.
     * 
     * Note: the checksum mechanism works well when we are not using a
     * compression technique. depending on the compression technique, a change
     * in the compressed data may trigger a failure of the decompression
     * algorithm. in such cases we never get the decompressed record and there
     * for the checksum is not even computed.  For this reason, you may see a
     * decompression error thrown out of this method when the root cause is a
     * problem with the compressed data record, perhaps as read from the disk
     * or the network.
     * 
     * @param buf
     *            The record. The data from the position to the limit will be
     *            decompressed.
     * 
     * @return When compression is enabled the returned buffer will be a
     *         read-only view onto a shared instance buffer held internally by
     *         the {@link RecordCompressor}. The position will be zero. The
     *         limit will be the #of decompressed bytes. When compression is not
     *         enabled <i>buf</i> is returned unchanged.
     */
    protected ByteBuffer decompress(ByteBuffer buf) {
        
        assert buf.position() == 0;
        
        if( recordCompressor == null ) return buf;
        
        ByteBuffer tmp = buf;
        
        assert tmp.position() == 0;

        ByteBuffer ret = recordCompressor.decompress(buf); // Decompress.

        /*
         * note: this restores the position for [buf] to remove the side
         * effect of decompressing the buffer which causes the position to
         * be advanced to the limit.
         */
        buf.position(0);

        assert buf.position() == 0;
        
        return ret;

    }

}
