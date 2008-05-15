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
/*
 * Created on Nov 5, 2006
 */
package com.bigdata.btree;

import it.unimi.dsi.mg4j.io.InputBitStream;
import it.unimi.dsi.mg4j.io.OutputBitStream;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.io.ByteBufferOutputStream;
import com.bigdata.io.DataOutputBuffer;
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
 * de-serialization, especially whether the record contains a leaf vs a node.
 * This fixed record also makes it possible to update some fields in the header
 * once the entire record has been serialized, including the checksum, the #of
 * bytes in the serialized record, and the prior/next addresses for leaves.
 * </p>
 * <p>
 * The methods defined by this class all work with {@link ByteBuffer}s. On
 * read, the buffer must be positioned to the start of the data to be read.
 * After a read, the buffer will be positioned to the first byte after the data
 * read. If there is insufficient data available in the buffer then an
 * {@link BufferUnderflowException} will be thrown. On write, the data will be
 * written on an internal buffer whose size is automatically extended. The write
 * buffer is reused for each write and quickly achieves a maximum size for any
 * given {@link BTree}.
 * </p>
 * <p>
 * The {@link IDataSerializer} for keys and values hides the use of
 * {@link ByteBuffer}s from the application. The use of compression or packing
 * techniques within the implementations of this interface is encouraged.
 * </p>
 * <p>
 * Note: while the {@link NodeSerializer} is NOT thread-safe for writers, it is
 * thread-safe for readers. This design mirrors the concurrency capabilities of
 * the {@link AbstractBTree}.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo it would be nice to add an object to the {@link IndexMetadata} and this
 *       class that know how to convert an Object into a byte[] value and a
 *       byte[] value into an Object. This would let us hide much of the
 *       complexity of managing byte[]s from applications that are not
 *       interested in that sort of thing.
 * 
 * @todo it would be nice to add an object to the {@link IndexMetadata} and this
 *       class that know how to convert an Object into a key (keys can not be
 *       easily reversed for most indices).
 * 
 * @todo it would be nice to have {@link Set} and {@link Map} implementations
 *       wrapping an {@link AbstractBTree} (or an {@link IIndex}). These might
 *       be written using objects such as described above for converting
 *       application objects to unsigned byte[] keys and application objects
 *       to/from byte[] values.
 * 
 * @todo modify deserialization to use a fast DataInput wrapping a byte[]?
 * 
 * @todo automatically resize the decompression buffers as required and start
 *       with a smaller buffer.
 * 
 * @todo consider the use of thread-local variables for the read buffers so that
 *       the buffers may be used by multiple concurrent readers (the bloom
 *       filter is another place where concurrency is limited for readers)
 * 
 * FIXME add support for serializing the prior/next references when known. we do
 * checksums which makes it trickier to touch up those references after the fact
 * and the next reference is not knowable for the index segments until we have
 * determined the serialized size of the current leaf, which makes that all a
 * bit tricky.
 * 
 * @todo both the checksum and the record compressor should be store level
 *       options. the {@link IDataSerializer} is used to provide index specific
 *       compression. The store level record compressor should provide fast
 *       generic compression.
 * 
 * @see IIndex
 * @see INodeData
 * @see ILeafData
 * @see IDataSerializer
 * @see LongPacker
 */
public class NodeSerializer {

    /**
     * An object that knows how to constructor nodes and leaves.
     */
    private final INodeFactory nodeFactory;
    
    /**
     * The declared maximum branching factor.
     */
    private final int branchingFactor;
    
    /**
     * When <code>true</code> the {@link NodeSerializer} instance will refuse
     * to serialize nodes or leaves (this keeps us from allocating the
     * {@link #_writeBuffer}).
     */
    private final boolean readOnly;
    
    /**
     * An object that knows how to (de-)serialize child addresses. an
     * {@link INodeData}.
     */
    private final IAddressSerializer addrSerializer;

    /**
     * An object that knows how to (de-)serialize keys in a {@link Node}.
     */
    private final IDataSerializer nodeKeySerializer;

    /**
     * An object that knows how to (de-)serialize keys in a {@link Leaf}.
     */
    private final IDataSerializer leafKeySerializer;

    /**
     * An object that knows how to (de-)serialize the values on leaves.
     */
    private final IDataSerializer valueSerializer;
    
    /**
     * Used to serialize the nodes and leaves of the tree. This is pre-allocated
     * based on the estimated maximum size of a node or leaf and grows as
     * necessary when it overflows. The same buffer instance is used to
     * serialize all nodes and leaves of the tree.
     * <p>
     * Note: this buffer is discarded by {@link #close()} when the btree is
     * {@link AbstractBTree#close() closed} and then reallocated on demand.
     * <p>
     * Note: It is important that this field NOT be used for a read-only
     * {@link BTree} since only mutable {@link BTree}s are single threaded -
     * concurrent readers are allowed for read-only btrees.
     * 
     * @see #allocWriteBuffer()
     * @see #close()
     */
    private DataOutputBuffer _writeBuffer;
    
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
    public static final transient int DEFAULT_BUFFER_CAPACITY_PER_ENTRY = Bytes.kilobyte32 / 4;
    
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
    private static short VERSION0 = (short) 0;

    /**
     * The value (0) indicates a non-leaf node.
     */
    public static final byte TYPE_NODE = (byte) 0;

    /**
     * The value (1) indicates a leaf without prior and next references.
     */
    public static final byte TYPE_LEAF = (byte) 1;

    /**
     * The value (2) indicates a leaf with prior and next references. This
     * allows us to elide those fields from the leaf header when the data will
     * not be made available by application.
     */
    public static final byte TYPE_LINKED_LEAF = (byte) 2;

    /**
     * A private instance is used to compute checksums for each
     * {@link AbstractBTree}. This makes is possible to have concurrent reads
     * or writes on multiple btrees that are backed by different stores.
     */
    private final ChecksumUtility chk;

    private final int initialBufferCapacity;

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
     * @param indexMetadata
     *            The {@link IndexMetadata} record for the index.
     * 
     * @param readOnly
     *            <code>true</code> IFF the caller is asserting that they WILL
     *            NOT attempt to serialize any nodes or leaves using this
     *            {@link NodeSerializer} instance.
     * 
     * @param isFullyBuffered
     *            Checksums are disabled for stores that are fully buffered
     *            since the data are always read from memory which we presume is
     *            already parity checked. While a checksum on a fully buffered
     *            store could detect an overwrite, the journal architecture
     *            makes that extremely unlikely and one has never been observed.
     * 
     * @todo change recordCompressor to an interface.
     */
    public NodeSerializer(//
            INodeFactory nodeFactory,//
            int branchingFactor,
            int initialBufferCapacity, //
            IndexMetadata indexMetadata,//
            final boolean readOnly,//
            boolean isFullyBuffered
            ) {

        assert nodeFactory != null;

        assert branchingFactor >= AbstractBTree.MIN_BRANCHING_FACTOR;

        assert initialBufferCapacity >= 0;

        assert indexMetadata != null;

        this.nodeFactory = nodeFactory;

        this.branchingFactor = branchingFactor;

        /*
         * FIXME use packed address serializer (or nibble-based one).
         */
        this.addrSerializer = AddressSerializer.INSTANCE;

        this.readOnly = readOnly;
        
        this.nodeKeySerializer = indexMetadata.getNodeKeySerializer();

        this.leafKeySerializer = indexMetadata.getLeafKeySerializer();

        this.valueSerializer = indexMetadata.getValueSerializer();

        this.recordCompressor = indexMetadata.getRecordCompressor();
        
        this.useChecksum = indexMetadata.getUseChecksum() && isFullyBuffered;

        this.chk = useChecksum ? new ChecksumUtility() : null;

        if (readOnly) {

            this.initialBufferCapacity = 0;
            
            this._writeBuffer = null;
            
        } else {
            
            if (initialBufferCapacity == 0) {

                initialBufferCapacity = DEFAULT_BUFFER_CAPACITY_PER_ENTRY
                        * branchingFactor;

            }

            this.initialBufferCapacity = initialBufferCapacity;

            // set _buf and _os.
            allocWriteBuffer();

        }

        /*
         * Allocate compression buffer iff a compression algorithm is used.
         * 
         * FIXME The capacity of this buffer is a SWAG. If it is too small then
         * an EOFException will be thrown. This needs to be modified start with
         * a smaller buffer and grow as required. An alternative would be to
         * re-allocate this whenever _buf is resize since the compressed data
         * should never be larger than the original data.
         */

        if (recordCompressor != null) {
            /*
             * FIXME The record compressor is not thread-safe since it relies on
             * the [cbuf] field, which is shared across concurrent threads with
             * read access to the same BTree (write access is single threaded).
             */
            throw new UnsupportedOperationException(
                    "Record compressor is not thread-safe");
        }

        cbuf = recordCompressor != null //
        ? ByteBuffer.allocate(Bytes.megabyte32) //
        //                ? ByteBuffer.allocateDirect(Bytes.megabyte32*2) //
                : null;

    }

    /**
     * Releases any buffers. They will be automatically reallocated if the
     * {@link NodeSerializer} is used again.
     * 
     * @todo write tests of this feature, including random closes during the
     *       {@link NodeSerializer} stress test and with and without record
     *       compression (the {@link #cbuf} field is not being automatically
     *       (re-)allocated right now so that will break if we clear the
     *       buffer).
     */
    public void close() {

        _writeBuffer = null;

        cbuf = null;

    }

    /**
     * Allocates {@link #_writeBuffer} with {@link #initialBufferCapacity}.
     * 
     * @throws UnsupportedOperationException
     *             if the {@link NodeSerializer} does not permit writes.
     */
    private void allocWriteBuffer() {

        if (readOnly) {

            throw new UnsupportedOperationException("read-only");
            
        }

        assert _writeBuffer == null;

        _writeBuffer = new DataOutputBuffer(initialBufferCapacity);

    }

    /**
     * De-serialize a node or leaf (thread-safe). This method is used when the
     * caller does not know a-priori whether the reference is to a node or leaf.
     * The decision is made based on inspection of the {@link #OFFSET_NODE_TYPE}
     * byte in the supplied buffer.
     * 
     * @param btree
     *            The btree.
     * @param addr
     *            The address of the node or leaf being de-serialized.
     * @param buf
     *            The buffer containing the serialized data.
     * 
     * @todo document and verify effects on buf position and limit w/ and w/o
     *       decompression.
     * 
     * @return The de-serialized node.
     */
    public IAbstractNodeData getNodeOrLeaf(final IIndex btree, final long addr,
            ByteBuffer buf) {

        //        assert btree != null;
        //        assert addr != 0L;
        assert buf != null;

        /*
         * optionally decompresses the record. note that we must decompress the
         * buffer before we can test the byte that will determine if it is a
         * node or a leaf.
         */

        assert buf.position() == 0;

        buf = decompress(buf);

        assert buf.position() == 0;

        final IAbstractNodeData ret;

        final byte nodeType = buf.get(OFFSET_NODE_TYPE);

        switch (nodeType) {

        case TYPE_NODE: {

            assert buf.position() == 0;

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

            assert buf.position() == 0;

            // deserialize (already decompressed)
            ret = getLeaf(btree, addr, buf, true);

            // @todo see the note above.
            //            assert tmp.position() == 0;

            break;

        }

        default: {

            throw new RuntimeException("unknown node type=" + nodeType);

        }

        }

        return ret;

    }

    /**
     * Serialize a node or leaf onto an internal buffer and return that buffer
     * (NOT thread-safe). The operation writes on an internal buffer which is
     * automatically extended as required.
     * 
     * @param node
     *            The node or leaf.
     * 
     * @return The buffer containing the serialized representation of the node
     *         or leaf in a <em>shared buffer</em>. The contents of this
     *         buffer may be overwritten by the next node or leaf serialized the
     *         same instance of this class. The position will be zero and the
     *         limit will be the #of bytes in the serialized representation.
     */
    public ByteBuffer putNodeOrLeaf(IAbstractNodeData node) {

        if (node instanceof INodeData) {

            return putNode((INodeData) node);

        } else {

            return putLeaf((ILeafData) node);

        }

    }

    /**
     * Serialize a node onto an internal buffer and returns that buffer (NOT
     * thread-safe). The operation writes on an internal buffer which is
     * automatically extended as required.
     * 
     * @param node
     *            The node.
     * 
     * @return The buffer containing the serialized representation of the node
     *         in a <em>shared buffer</em>. The contents of this buffer may
     *         be overwritten by the next node or leaf serialized the same
     *         instance of this class. The position will be zero and the limit
     *         will be the #of bytes in the serialized representation.
     */
    public ByteBuffer putNode(INodeData node) {

        if (_writeBuffer == null) {

            // the buffer was released so we reallocate it.
            allocWriteBuffer();

        }

        try {

            // prepare buffer for reuse.
            _writeBuffer.reset();
            //            _os.position(0L); 

            return putNode2(node);

        } catch (IOException ex) {

            throw new RuntimeException(ex); // exception is not expected.

        }

    }

    private ByteBuffer putNode2(INodeData node) throws IOException {

        assert _writeBuffer != null;
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

        final int pos0 = _writeBuffer.pos();

        // checksum
        _writeBuffer.writeInt(0); // will overwrite below with the checksum.

        // #bytes
        _writeBuffer.writeInt(0); // will overwrite below with the actual value.

        // nodeType
        _writeBuffer.writeByte(TYPE_NODE); // this is a non-leaf node.

        // version
        _writeBuffer.writeShort(VERSION0);

        //        /*
        //         * Setup output stream over the buffer.
        //         * 
        //         * Note: I have tested the use of a {@link BufferedOutputStream} here
        //         * and in putLeaf() and it actually slows things down a smidge.
        //         */
        //        DataOutputStream os = new DataOutputStream(//
        //                new ByteBufferOutputStream(buf)
        ////              new BufferedOutputStream(new ByteBufferOutputStream(buf))
        //                );

        try {

            // branching factor.
            _writeBuffer.packLong(branchingFactor);

            // #of spanned entries.
            _writeBuffer.packLong(nentries);

            //            // #of keys
            //            LongPacker.packLong(os, nkeys);

            // keys
            nodeKeySerializer.write(_writeBuffer, keys);
//            KeyBufferSerializer.INSTANCE.putKeys(_buf, keys);

            // addresses.
            addrSerializer.putChildAddresses(_writeBuffer, childAddr, nkeys + 1);

            // #of entries spanned per child.
            putChildEntryCounts(_writeBuffer, childEntryCounts, nkeys + 1);

            //            // Done using the DataOutputStream so flush to the ByteBuffer.
            //            os.flush();

        }

        catch (EOFException ex) {

            /*
             * Masquerade the EOFException as a buffer overflow since that is
             * what it really represents (@todo since ByteBuffer is not used
             * anymore we do not need to masquerade this and the javadoc should
             * be updated).
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
        final int nbytes = _writeBuffer.pos() - pos0;
        assert nbytes > SIZEOF_NODE_HEADER;

        ByteBuffer buf2 = ByteBuffer.wrap(_writeBuffer.array(), 0, nbytes);

        // patch #of bytes written on the record format.
        buf2.putInt(pos0 + OFFSET_NBYTES, nbytes);

        // compute checksum for data written.
        final int checksum = useChecksum ? chk.checksum(buf2, pos0
                + SIZEOF_CHECKSUM, pos0 + nbytes) : 0;

        // System.err.println("computed node checksum: "+checksum);

        // write the checksum into the buffer.
        buf2.putInt(pos0, checksum);

        /*
         * Note: The position will be zero(0). The limit will be the #of bytes
         * in the buffer.
         */

        //        // flip the buffer to prepare for reading.
        //        buf2.flip();
        // optionally compresses the record.
        return compress(buf2);

    }

    public INodeData getNode(IIndex btree, long addr, ByteBuffer buf) {

        return getNode(btree, addr, buf, false);

    }

    /**
     * De-serialize the node.
     * 
     * @param btree
     *            The btree to which the node belongs.
     * @param addr
     *            The address of the node.
     * @param buf
     *            The buffer containing the serialized node.
     * @param decompressed
     *            true iff the buffer has already been decompressed using
     *            {@link #decompress(ByteBuffer)}.
     * 
     * @return The deserialized node.
     */
    protected INodeData getNode(IIndex btree, long addr, ByteBuffer buf,
            boolean decompressed) {

        //        assert btree != null;
        //        assert addr != 0L;
        assert buf != null;

        // optionally decompresses the record.
        assert buf.position() == 0;
        if (!decompressed) {
            buf = decompress(buf);
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
        assert nbytes > SIZEOF_NODE_HEADER : "nbytes="+nbytes+", but headerSize="+SIZEOF_NODE_HEADER;

        /*
         * verify checksum now that we know how many bytes of data we expect to
         * read.
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
         * 
         * Note: The buffer is never backed by an array since it is read-only
         * (per the IRawStore contract for read) and read-only buffers do not
         * expose the backing array. This means that we would have to duplicate
         * the data in order to use the DataInputBuffer rather than read from
         * the Buffer.
         */
        final DataInput is;
        //        if(false) {
        //            byte[] data = new byte[buf.remaining()];
        //            buf.get(data);
        //            is = new DataInputBuffer(data);
        //        } else {
        is = new DataInputStream(new ByteBufferInputStream(buf));
        //        }

        try {

            // branching factor
            final int branchingFactor = (int) LongPacker.unpackLong(is);

            assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR : "branchingFactor="+branchingFactor+", but must be GTE "+BTree.MIN_BRANCHING_FACTOR;

            // nentries
            final int nentries = (int) LongPacker.unpackLong(is);

            //            // nkeys
            //            final int nkeys = (int) LongPacker.unpackLong(is);

            //            // Note: minimum is (m+1/2) unless this is the root node.
            //            assert nkeys >= 0 && nkeys < branchingFactor;

            final long[] childAddr = new long[branchingFactor + 1];

            final int[] childEntryCounts = new int[branchingFactor + 1];

            // Keys.
//            final IKeyBuffer keys = KeyBufferSerializer.INSTANCE.getKeys(is);
            final IKeyBuffer keys;
            {
                /*
                 * FIXME This de-serializes and then converts to an
                 * ImmutableKeyBuffer for compatibility with current assumptions
                 * in the BTree code.
                 */
                MutableKeyBuffer tmp = new MutableKeyBuffer(branchingFactor);
                nodeKeySerializer.read(is, tmp);
                keys = new ImmutableKeyBuffer(tmp);
            }

            final int nkeys = keys.getKeyCount();
            
            // Child addresses (nchildren == nkeys+1). FIXME custom serialization.
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
     * Serialize a leaf onto an internal buffer and returns that buffer (NOT
     * thread-safe). The operation writes on an internal buffer which is
     * automatically extended as required.
     * 
     * @param leaf
     *            The leaf node.
     * 
     * @return The buffer containing the serialized representation of the leaf
     *         in a <em>shared buffer</em>. The contents of this buffer may
     *         be overwritten by the next node or leaf serialized the same
     *         instance of this class. The position will be zero and the limit
     *         will be the #of bytes in the serialized representation.
     */
    public ByteBuffer putLeaf(ILeafData leaf) {

        if( _writeBuffer == null ) {
            
            // the buffer was released so we reallocate it.
            allocWriteBuffer();
            
        }

        try {

            // prepare buffer for reuse.
            _writeBuffer.reset();
//            _os.position(0L);

            return putLeaf2(leaf);

        } catch (IOException ex) {

            throw new RuntimeException(ex); // exception is not expected.

        }
        
    }
     
    private ByteBuffer putLeaf2(ILeafData leaf) throws IOException {

        assert _writeBuffer != null;
        assert leaf != null;
        
        final int nkeys = leaf.getKeyCount();
        final int branchingFactor = leaf.getBranchingFactor();
        final IKeyBuffer keys = leaf.getKeys();
        final byte[][] vals = leaf.getValues();
        
        /*
         * common data.
         */
        final int pos0 = _writeBuffer.pos();

        // checksum
        _writeBuffer.writeInt(0); // will overwrite below with the checksum.
        
        // nbytes
        _writeBuffer.writeInt(0); // will overwrite below with the actual value.
        
        // nodeType
        _writeBuffer.writeByte(TYPE_LEAF); // this is a leaf node.

        // version
        _writeBuffer.writeShort(VERSION0);
        
        /*
         * Setup output stream over the buffer.
         * 
         * Note: wrapping this with a BufferedOutputStream is slightly slower.
         */
//        DataOutputStream os = new DataOutputStream(//
//                new ByteBufferOutputStream(buf)
////                new BufferedOutputStream(new ByteBufferOutputStream(buf))
//                );

        try {
            
            // branching factor.
            _writeBuffer.packLong( branchingFactor);

//            // #of keys
//            _buf.packLong(nkeys);

            // keys.
            leafKeySerializer.write(_writeBuffer, keys);
//            KeyBufferSerializer.INSTANCE.putKeys(_buf, keys);
            
            // values.
//            // values [branchingFactor + 1]
//            valueSerializer.write(_buf,0/* fromIndex */,nkeys/* toIndex */, vals,vals.length/* deserializedSize */);
//            ByteArrayValueSerializer.INSTANCE.putValues(_buf, vals, nkeys);
            valueSerializer.write(_writeBuffer, new RandomAccessByteArray(0, nkeys, vals));

            if(leaf.hasDeleteMarkers()) {
                
                putDeleteMarkers(_writeBuffer, leaf);
                
            }
            
            if(leaf.hasVersionTimestamps()) {
                
                putVersionTimestamps(_writeBuffer, leaf);
                
            }
            
//            // Done using the DataOutputStream so flush to the ByteBuffer.
//            os.flush();
            
        }

        catch (EOFException ex) {

            /*
             * Masquerade the EOFException as a buffer overflow since that is
             * what it really represents (@todo we do not need to masquerade
             * this exception since we are not using ByteBuffer anymore).
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
        final int nbytes = _writeBuffer.pos() - pos0;
        assert nbytes > SIZEOF_LEAF_HEADER;

        ByteBuffer buf2 = ByteBuffer.wrap(_writeBuffer.array(),0,nbytes);
        
        // patch #of bytes written on the record format.
        buf2.putInt(pos0 + OFFSET_NBYTES, nbytes);

        // compute checksum
        final int checksum = (useChecksum ? chk.checksum(buf2, pos0
                + SIZEOF_CHECKSUM, pos0 + nbytes) : 0);
        // System.err.println("computed leaf checksum: "+checksum);

        // write checksum on buffer.
        buf2.putInt(pos0, checksum);
        
        /*
         * Note: The position will be zero(0).  The limit will be the #of bytes
         * in the buffer.
         */

//        /*
//         * Flip the buffer to prepare it for reading. The position will be zero
//         * and the limit will be the #of bytes in the serialized record.
//         */
//        buf2.flip();
        
        // optionally compresses the record.
        return compress( buf2 );
                
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
     *            The address of the leaf.
     * @param buf
     *            The buffer containing the serialized leaf.
     * @param decompressed
     *            true iff the buffer contains a record that has already been
     *            decompressed using {@link #decompress(ByteBuffer)}.
     * 
     * @return The deserialized leaf.
     */
    protected ILeafData getLeaf(final IIndex btree,final long addr,ByteBuffer buf, final boolean decompressed) {
        
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
         * 
         * @todo It would be faster to bulk copy the ByteBuffer into a shared
         * DataInputBuffer and then read on that -- perhaps using a ThreadLocal
         * since concurrent readers are allowed.
         */
        final DataInputStream is = new DataInputStream(
                new ByteBufferInputStream(buf));

        try {

            // branching factor
            final int branchingFactor = (int) LongPacker.unpackLong(is);

            assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;

//            // nkeys
//            final int nkeys = (int) LongPacker.unpackLong(is);
//               Note: minimum is (m+1)/2 unless root leaf.
//            assert nkeys >= 0 && nkeys <= branchingFactor;

            // keys.
            final IKeyBuffer keys;
            { 
                /*
                 * FIXME currently de-serializes to an ImmutableKeyBuffer for
                 * compatibility with assumptions in the BTree code.
                 */
                MutableKeyBuffer tmp = new MutableKeyBuffer(branchingFactor+1);
                leafKeySerializer.read(is, tmp);
                keys = new ImmutableKeyBuffer(tmp);
            }
//            final IKeyBuffer keys = KeyBufferSerializer.INSTANCE.getKeys(is);

            final int nkeys = keys.getKeyCount();
//            assert nkeys == keys.getKeyCount();
            
            // values.
            final byte[][] values = new byte[branchingFactor + 1][];
            {
                RandomAccessByteArray raba = new RandomAccessByteArray(0, 0,
                        values);
                valueSerializer.read(is, raba);
            }
            
            // delete markers.
            final boolean[] deleteMarkers;
            if(btree.getIndexMetadata().getDeleteMarkers()) {
                deleteMarkers = new boolean[branchingFactor+1];
                getDeleteMarkers(is, nkeys, deleteMarkers );
            } else {
                deleteMarkers = null;
            }
            
            // version timestamps.
            final long[] versionTimestamps;
            if(btree.getIndexMetadata().getVersionTimestamps()) {
                versionTimestamps = new long[branchingFactor+1];
                getVersionTimestamps(is, nkeys, versionTimestamps );
            } else {
                versionTimestamps = null;
            }
            
            // verify #of bytes actually read.
            {
                final int bpos = buf.position();
                assert bpos - pos0 == nbytes : " buf.position()=" + bpos
                        + " + pos0="+pos0+" != "+nbytes;
            }

            // reset the buffer position.
            buf.position(0);
            assert buf.position() == 0;
            
            // Done.
            return nodeFactory.allocLeaf(btree, addr, branchingFactor, keys,
                    values, versionTimestamps, deleteMarkers);

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
     *            The output stream.
     * @param childEntryCounts
     *            The #of entries spanned by each direct child.
     * @param nchildren
     *            The #of elements of that array that are defined.
     * @throws IOException
     * 
     * @todo declare and implement interface and configure in
     *       {@link IndexMetadata}.
     */
    protected void putChildEntryCounts(DataOutput os,
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
    protected void getChildEntryCounts(DataInput is,
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

    /**
     * Write out the delete markers.
     * 
     * @param os
     * @param leaf
     * 
     * @throws IOException
     * 
     * @todo declare and implement interface and configure in {@link IndexMetadata}.
     */
    protected void putDeleteMarkers(DataOutputBuffer os, ILeafData leaf)
            throws IOException {

        final int n = leaf.getKeyCount();

        if (n == 0)
            return;

        OutputBitStream obs = new OutputBitStream(os, 0/* unbuffered! */);

        for (int i = 0; i < n; i++) {

            obs.writeBit(leaf.getDeleteMarker(i));

        }

        obs.flush();

        obs.close();

    }

    /**
     * Read in the delete markers.
     * 
     * @param is
     * @param n
     * @param deleteMarkers
     * 
     * @throws IOException
     */
    protected void getDeleteMarkers(DataInput is, int n, boolean[] deleteMarkers)
            throws IOException {

        if(n==0) return;
        
        InputBitStream ibs = new InputBitStream((InputStream) is, 0/* unbuffered! */);

        for (int i = 0; i < n; i++) {

            deleteMarkers[i] = ibs.readBit() == 0 ? false : true;
            
        }
        
//        ibs.close();
        
    }

    /**
     * Write out the version timestamps.
     * 
     * @todo declare and implement interface and configure in {@link IndexMetadata}.
     * 
     * @todo Experiment with other serialization schemes. One of the more
     *       obvious would be a huffman encoding of the timestamps since I
     *       presume that they will tend to bunch together a lot. If timestamps
     *       are non-negative then we can also pack them (or use the nibble
     *       coding). this should be configured in the IndexMetadata. it will
     *       need to have its own interface since the data are not byte[]s.
     * 
     * @param os
     * @param nentries
     * @param versionTimestamps
     * 
     * @throws IOException
     */
    protected void putVersionTimestamps(DataOutputBuffer os, ILeafData leaf)
            throws IOException {

        final int n = leaf.getKeyCount();

        if (n == 0)
            return;

        for (int i = 0; i < n; i++) {

            final long timestamp = leaf.getVersionTimestamp(i);

            os.writeLong(timestamp);

        }

    }

    protected void getVersionTimestamps(DataInput is, int n,
            long[] versionTimestamps) throws IOException {

        if (n == 0)
            return;

        for (int i = 0; i < n; i++) {

            versionTimestamps[i] = is.readLong();

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
     *         compressed record is returned. the position will be zero and the
     *         limit will be the #of bytes in the compressed record.
     * 
     * @todo compression should be on the byte[] not the slower
     *       {@link ByteBuffer}.
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
    private ByteBuffer cbuf;
        
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
