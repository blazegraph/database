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

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.IndexMetadata.Options;
import com.bigdata.btree.compression.IDataSerializer;
import com.bigdata.btree.compression.RandomAccessByteArray;
import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.compression.IRecordCompressor;
import com.bigdata.io.compression.IRecordCompressorFactory;
import com.bigdata.io.compression.NOPRecordCompressor;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IAddressManager;

/**
 * <p>
 * An instance of this class is used to serialize and de-serialize the
 * {@link INodeData}s and {@link ILeafData}s of an {@link AbstractBTree}. Leaf
 * and non-leaf records have different serialization formats, but their leading
 * bytes use the same format so that you can tell by inspection whether a buffer
 * contains a leaf or a non-leaf node. The header of the record uses a fixed
 * length format so that some fields can be tested without full
 * de-serialization, especially whether the record contains a leaf vs a node.
 * This fixed record also makes it possible to update some fields in the header
 * once the entire record has been serialized, including the checksum, the #of
 * bytes in the serialized record, and the prior/next addresses for leaves.
 * </p>
 * <p>
 * The methods defined by this class all work with {@link ByteBuffer}s. On read,
 * the buffer must be positioned to the start of the data to be read. After a
 * read, the buffer will be positioned to the first byte after the data read. If
 * there is insufficient data available in the buffer then an
 * {@link BufferUnderflowException} will be thrown. On write, the data will be
 * written on an internal buffer whose size is automatically extended. The write
 * buffer is reused for each write and quickly achieves a maximum size for any
 * given {@link BTree}.
 * </p>
 * <p>
 * The {@link IDataSerializer}s for the keys and the values hide the use of
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
 * @see AbstractBTree
 * @see IndexMetadata
 * @see INodeData
 * @see ILeafData
 * @see IDataSerializer
 * @see LongPacker
 */
public class NodeSerializer {

    private final IAddressManager addressManager;
    
    /**
     * An object that knows how to constructor nodes and leaves.
     */
    private final INodeFactory nodeFactory;
    
    /**
     * The declared branching factor for the {@link AbstractBTree} using
     * this {@link NodeSerializer}.
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
     * Factory for record-level (de-)compression of nodes and leaves (optional).
     */
    private final IRecordCompressorFactory recordCompressorFactory;
    
    /**
     * An object that knows how to (de-)compress a node or leaf (optional).
     */
    private IRecordCompressor getRecordCompressor() {
        
        if (recordCompressorFactory == null) {
            
            return NOPRecordCompressor.INSTANCE;
            
        }

        if (!readOnly) {
            
            assert _writeCompressor != null;
            
            // Instance used for writes, which are single threaded.
            return _writeCompressor;
            
        }
        
        return recordCompressorFactory.getInstance();
        
    }

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
     * Instance used for writes - this keeps a hard reference since writes are
     * single threaded.
     */
    private IRecordCompressor _writeCompressor;
    
    private final int initialBufferCapacity;

    /**
     * The default initial capacity multiplier for the (de-)serialization buffer.
     * The total initial buffer capacity is this value times the
     * {@link #branchingFactor}.
     */
    public static final transient int DEFAULT_BUFFER_CAPACITY_PER_ENTRY = Bytes.kilobyte32 / 4;
    
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
     * Offset of the byte whose low bit indicates whether the record is a
     * serialized node (the low bit is a zero) or a serialized leaf (the low bit
     * is a one).
     */
    static final int OFFSET_NODE_TYPE = 0;
    
    /**
     * Offset of the short integer whose value is the version identifier for the
     * serialization format.
     * 
     * @see #VERSION0
     * 
     * FIXME could use the high seven bits of the node type for this.  Just be
     * careful about that sign bit.
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
     * Size of the fixed length header for a serialized node (does not include
     * the prior/next references).
     */
    static final int SIZEOF_NODE_HEADER = SIZEOF_NODE_TYPE + SIZEOF_VERSION;

    /**
     * Size of the fixed length header for a serialized leaf (the prior and next
     * references are -1L unless their values are known).
     */
    static final int SIZEOF_LEAF_HEADER = SIZEOF_NODE_HEADER + (SIZEOF_REF * 2);

    /**
     * The only defined serialization format.
     */
    private static short VERSION0 = (short) 0;

    final private boolean isNode(final byte b) {

        return (b & 0x1) == 0;

    }

    final private boolean isLeaf(final byte b) {

        return (b & 0x1) == 1;

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
     *            The branching factor for nodes or leaves serialized
     *            using this object.
     * 
     * @param initialBufferCapacity
     *            The initial capacity for internal buffer used to serialize
     *            nodes and leaves. The buffer will be resized as necessary
     *            until it is sufficient for the records being serialized for
     *            the {@link BTree}. When zero (0), a default is used. A
     *            non-zero value is worth specifying only when the actual buffer
     *            size is consistently less than the default for some
     *            {@link BTree}. See {@link #DEFAULT_BUFFER_CAPACITY_PER_ENTRY}
     * 
     * @param indexMetadata
     *            The {@link IndexMetadata} record for the index.
     * 
     * @param readOnly
     *            <code>true</code> IFF the caller is asserting that they WILL
     *            NOT attempt to serialize any nodes or leaves using this
     *            {@link NodeSerializer} instance.
     */
    public NodeSerializer(//
            final IAddressManager addressManager,
            final INodeFactory nodeFactory,//
            final int branchingFactor,
            final int initialBufferCapacity, //
            final IndexMetadata indexMetadata,//
            final boolean readOnly,//
            final IRecordCompressorFactory recordCompressorFactory
            ) {

        assert addressManager != null;
        
        assert nodeFactory != null;

        /*
         * @todo already available from IndexMetadata#getBranchingFactor() so
         * drop as argument here.
         */
        assert branchingFactor >= Options.MIN_BRANCHING_FACTOR : "branchingFactor="+branchingFactor;

        assert initialBufferCapacity >= 0;

        assert indexMetadata != null;

        this.addressManager = addressManager;
        
        this.nodeFactory = nodeFactory;

        this.branchingFactor = branchingFactor;

        this.readOnly = readOnly;
        
        this.addrSerializer = indexMetadata.getAddressSerializer();

        this.nodeKeySerializer = indexMetadata.getNodeKeySerializer();

        this.leafKeySerializer = indexMetadata.getTupleSerializer().getLeafKeySerializer();

        this.valueSerializer = indexMetadata.getTupleSerializer().getLeafValueSerializer();

        // MAY be null
        this.recordCompressorFactory = recordCompressorFactory;
        
        if (readOnly) {

            this.initialBufferCapacity = 0;
            
            this._writeBuffer = null;
            
        } else {
            
            if (initialBufferCapacity == 0) {

                this.initialBufferCapacity = DEFAULT_BUFFER_CAPACITY_PER_ENTRY
                        * branchingFactor;

            } else {

                this.initialBufferCapacity = initialBufferCapacity;
                
            }

            // allocate initial write buffer.
            allocWriteBuffer();

        }

    }

    /**
     * Releases any buffers. They will be automatically reallocated if the
     * {@link NodeSerializer} is used again.
     */
    public void close() {

        _writeBuffer = null;
        
        _writeCompressor = null;

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

        assert _writeCompressor == null;

        _writeBuffer = new DataOutputBuffer(initialBufferCapacity);
        
        _writeCompressor = recordCompressorFactory == null ? NOPRecordCompressor.INSTANCE
                : recordCompressorFactory.getInstance();

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
     * @todo document and verify effects on buf position and limit.
     * 
     * @return The de-serialized node.
     */
    public IAbstractNodeData getNodeOrLeaf(final AbstractBTree btree, final long addr,
            final ByteBuffer buf) {

        //        assert btree != null;
        //        assert addr != 0L;
        assert buf != null;

        assert buf.position() == 0;

        final IAbstractNodeData ret;

        final boolean isNode = isNode(buf.get(OFFSET_NODE_TYPE));

        if(isNode) {

            // deserialize
            ret = getNode(btree, addr, buf);

        } else {
            
            // deserialize
            ret = getLeaf(btree, addr, buf);

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

            return putNode2(node);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    private ByteBuffer putNode2(INodeData node) throws IOException {

        assert _writeBuffer != null;
        assert _writeBuffer.pos() == 0;
        assert node != null;

//        assert branchingFactor == node.getBranchingFactor();
        final int nentries = node.getEntryCount();
        final int[] childEntryCounts = node.getChildEntryCounts();
        final int nkeys = node.getKeyCount();
        final IKeyBuffer keys = node.getKeys();
        final long[] childAddr = node.getChildAddr();

        /*
         * fixed length node header.
         */

        // nodeType (Node)
        _writeBuffer.writeByte(0);

        // version
        _writeBuffer.writeShort(VERSION0);

        try {

            // #of spanned entries.
            _writeBuffer.packLong(nentries);

            // keys
            nodeKeySerializer.write(_writeBuffer, keys);

            // addresses.
            addrSerializer.putChildAddresses(addressManager,_writeBuffer, childAddr, nkeys + 1);

            // #of entries spanned per child.
            putChildEntryCounts(_writeBuffer, childEntryCounts, nkeys + 1);

        } catch (EOFException ex) {

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

        // #of bytes actually written.
        final int nbytes = _writeBuffer.pos();

        assert nbytes > SIZEOF_NODE_HEADER;

        final ByteBuffer buf2 = ByteBuffer
                .wrap(_writeBuffer.array(), 0, nbytes);

        /*
         * Note: The position will be zero(0). The limit will be the #of bytes
         * in the buffer.
         */

        return buf2;

    }

    /**
     * De-serialize a node.
     * 
     * @param btree
     *            The btree to which the node belongs.
     * @param addr
     *            The address of the node.
     * @param buf
     *            The buffer containing the serialized node.
     *            
     * @return The deserialized node.
     */
    public INodeData getNode(final AbstractBTree btree, final long addr, final ByteBuffer buf) {

        //        assert btree != null;
        //        assert addr != 0L;
        assert buf != null;
        assert buf.position() == 0;

        /*
         * Read fixed length node header.
         */

        if (isLeaf(buf.get()))
            throw new RuntimeException("Not a Node: addr=" + addr);

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
        final DataInput is = new DataInputStream(new ByteBufferInputStream(buf));

        try {

            // nentries
            final int nentries = (int) LongPacker.unpackLong(is);

            // Note: [nkeys] minimum is (m+1/2) unless this is the root node.
            // assert nkeys >= 0 && nkeys < branchingFactor;

            final long[] childAddr = new long[branchingFactor + 1];

            final int[] childEntryCounts = new int[branchingFactor + 1];

            // Keys.
            final IKeyBuffer keys;
            {
                /*
                 * FIXME This de-serializes and then converts to an
                 * ImmutableKeyBuffer for compatibility with current assumptions
                 * in the BTree code.
                 */
                MutableKeyBuffer tmp = new MutableKeyBuffer(branchingFactor);
                nodeKeySerializer.read(is, tmp);
//                keys = new ImmutableKeyBuffer(tmp);
                keys = tmp;
            }

            final int nkeys = keys.getKeyCount();
            
            // Child addresses (nchildren == nkeys+1).
            addrSerializer.getChildAddresses(addressManager,is, childAddr, nkeys+1);

            // #of entries spanned by each child.
            getChildEntryCounts(is,childEntryCounts,nkeys+1);

            // reset the buffer position.
            buf.position(0);
            
            // Done.
            return nodeFactory.allocNode(btree, addr, branchingFactor,
                    nentries, keys, childAddr, childEntryCounts);

        } catch (EOFException ex) {

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
    public ByteBuffer putLeaf(final ILeafData leaf) {

        if( _writeBuffer == null ) {
            
            // the buffer was released so we reallocate it.
            allocWriteBuffer();
            
        }

        try {

            // prepare buffer for reuse.
            _writeBuffer.reset();

            return putLeaf2(leaf);

        } catch (IOException ex) {

            throw new RuntimeException(ex); // exception is not expected.

        }
        
    }

    /**
     * Update the serialization of a leaf to set the prior and next leaf
     * references and change its serialization type from {@link #TYPE_LEAF} to
     * {@link #TYPE_LINKED_LEAF}.
     * <p>
     * Note: In order to use this method to write linked leaves on the store you
     * have to either write behind at a pre-determined address on the store or
     * settle for writing only the prior or the next leaf address, but not both.
     * It is up to the caller to perform these tricks. All this method does is
     * to touch up the serialized record.
     * <p>
     * Note: This method has NO side-effects on the <i>position</i> or <i>limit</i>
     * of the caller's {@link ByteBuffer}.
     * 
     * @param b
     *            The serialization leaf.
     * @param priorAddr
     *            The address of the previous leaf in key order, <code>0L</code>
     *            if it is known that there is no previous leaf, and
     *            <code>-1L</code> if either: (a) it is not known whether
     *            there is a previous leaf; or (b) it is known but the address
     *            of that leaf is not known to the caller.
     * @param nextAddr
     *            The address of the next leaf in key order, <code>0L</code>
     *            if it is known that there is no next leaf, and
     *            <code>-1L</code> if either: (a) it is not known whether
     *            there is a next leaf; or (b) it is known but the address of
     *            that leaf is not known to the caller.
     * 
     * @see IndexSegmentBuilder
     */
    public void updateLeaf(final ByteBuffer b, final long priorAddr,
            final long nextAddr) {

        if (isNode(b.get(OFFSET_NODE_TYPE))) {
            
            throw new IllegalArgumentException("Not a leaf.");
            
        }

        b.putLong(OFFSET_PRIOR, priorAddr);

        b.putLong(OFFSET_NEXT, nextAddr);
        
    }

    private ByteBuffer putLeaf2(final ILeafData leaf) throws IOException {

        assert _writeBuffer != null;
        assert _writeBuffer.pos() == 0;
        assert leaf != null;
//        assert branchingFactor == leaf.getBranchingFactor();

        /*
         * Either (a) no compression and we can write the body bytes directly
         * after the header; or (b) we write the body into a DataOutputBuffer
         * and then compress it and write it onto the target output buffer.
         */
        
        putLeafHeader(leaf);

        final int nbytes = putLeafBody(_writeBuffer, leaf);
        
        final ByteBuffer buf2 = ByteBuffer.wrap(_writeBuffer.array(), 0, nbytes
                + SIZEOF_LEAF_HEADER);

        /*
         * Note: The position will be zero(0). The limit will be the #of bytes
         * in the buffer.
         */

        return buf2;

    }

    private void putLeafHeader(final ILeafData leaf) throws IOException {
        
        // nodeType
        _writeBuffer.writeByte(1); // this is a leaf.

        // version
        _writeBuffer.writeShort(VERSION0);

        // previous leaf address. Note: -1L indicates UNKNOWN
        _writeBuffer.writeLong(-1L);

        // next leaf address.  Note: -1L indicates UNKNOWN.
        _writeBuffer.writeLong(-1L);

    }

    /**
     * 
     * @param _writeBuffer
     * @param leaf
     * @return The #of bytes written in the leaf body.
     * @throws IOException
     */
    private int putLeafBody(DataOutputBuffer _writeBuffer, final ILeafData leaf)
            throws IOException {
        
        final int nkeys = leaf.getKeyCount();
        final IKeyBuffer keys = leaf.getKeys();
        final byte[][] vals = leaf.getValues();
        
        final int pos0 = _writeBuffer.pos();
        
        try {
            
            // keys.
            leafKeySerializer.write(_writeBuffer, keys);
            
            // values.
            valueSerializer.write(_writeBuffer, new RandomAccessByteArray(0, nkeys, vals));

            if(leaf.hasDeleteMarkers()) {
                
                putDeleteMarkers(_writeBuffer, leaf);
                
            }
            
            if(leaf.hasVersionTimestamps()) {
                
                putVersionTimestamps(_writeBuffer, leaf);
                
            }
            
        } catch (EOFException ex) {

            /*
             * Masquerade the EOFException as a buffer overflow since that is
             * what it really represents (@todo we do not need to masquerade
             * this exception since we are not using ByteBuffer anymore).
             */
            RuntimeException ex2 = new BufferOverflowException();

            ex2.initCause(ex);

            throw ex2;

        }

        // #of bytes actually written.
        final int nbytes = _writeBuffer.pos() - pos0;
        
//        assert nbytes > SIZEOF_LEAF_HEADER;
        
        return nbytes;

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
     * 
     * @return The deserialized leaf.
     */
    public ILeafData getLeaf(final AbstractBTree btree, final long addr,
            final ByteBuffer buf) {
        
//        assert btree != null;
//        assert addr != 0L;
        assert buf != null;
        assert buf.position() == 0;

        /*
         * common data.
         */

        // nodeType
        final byte nodeType = buf.get();
        
        if (isNode(nodeType))
            throw new RuntimeException("Not a leaf: addr=" + addr);
        
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

            final long priorAddr = is.readLong();

            final long nextAddr = is.readLong();
            
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
//                keys = new ImmutableKeyBuffer(tmp);
                keys = tmp;
            }

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

            // reset the buffer position.
            buf.position(0);
            
            // Done.
            return nodeFactory.allocLeaf(btree, addr, branchingFactor, keys,
                    values, versionTimestamps, deleteMarkers, priorAddr,
                    nextAddr);

        } catch (EOFException ex) {

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
     *            
     * @throws IOException
     * 
     * @todo customizable serializer interface configured in {@link IndexMetadata}.
     */
    static protected void putChildEntryCounts(DataOutput os,
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
     * 
     * @todo customizable serializer interface configured in {@link IndexMetadata}.
     */
    static protected void getChildEntryCounts(DataInput is,
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
     * @todo customizable serializer interface configured in {@link ITupleSerializer}.
     */
    static protected void putDeleteMarkers(DataOutputBuffer os, ILeafData leaf)
            throws IOException {

        final int n = leaf.getKeyCount();

        if (n == 0)
            return;

        final OutputBitStream obs = new OutputBitStream(os,
                0/* unbuffered! */, false/* reflectionTest */);

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
    static protected void getDeleteMarkers(DataInput is, int n, boolean[] deleteMarkers)
            throws IOException {

        if (n == 0)
            return;

        final InputBitStream ibs = new InputBitStream((InputStream) is,
                0/* unbuffered! */, false/* reflectionTest */);

        for (int i = 0; i < n; i++) {

            deleteMarkers[i] = ibs.readBit() == 0 ? false : true;
            
        }
        
//        ibs.close();
        
    }

    /**
     * Write out the version timestamps.
     * 
     * @param os
     * @param nentries
     * @param versionTimestamps
     * 
     * @throws IOException
     * 
     * @todo customizable serializer interface configured in {@link ITupleSerializer}.
     * 
     * @todo Experiment with other serialization schemes. One of the more
     *       obvious would be a huffman encoding of the timestamps since I
     *       presume that they will tend to bunch together a lot. If timestamps
     *       are non-negative then we can also pack them (or use the nibble
     *       coding). this should be configured in the IndexMetadata. it will
     *       need to have its own interface since the data are not byte[]s.
     */
    static protected void putVersionTimestamps(DataOutputBuffer os, ILeafData leaf)
            throws IOException {

        final int n = leaf.getKeyCount();

        if (n == 0)
            return;

        for (int i = 0; i < n; i++) {

            final long timestamp = leaf.getVersionTimestamp(i);

            os.writeLong(timestamp);

        }

    }

    static protected void getVersionTimestamps(DataInput is, int n,
            long[] versionTimestamps) throws IOException {

        if (n == 0)
            return;

        for (int i = 0; i < n; i++) {

            versionTimestamps[i] = is.readLong();

        }

    }

}
