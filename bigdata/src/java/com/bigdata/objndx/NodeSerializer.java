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

import com.bigdata.journal.Bytes;

/**
 * <p>
 * An instance of this class is used to serialize and de-serialize the
 * {@link INodeData}s and {@link ILeafData}s of an {@link IBTree}. Leaf and
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
 * The (de-)serialization interfaces for the keys {@link IKeySerializer} and the
 * values {@link IValueSerializer} both hide the use of {@link ByteBuffer}s
 * from the application. The use of compression or packing techniques within the
 * implementations of those interfaces is encouraged.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Compute the #of shared bytes (common prefix) for the low and high key,
 *       write that prefix once, and then mask off that prefix for each key
 *       written. Modify search to only search that part of the key that is
 *       recorded in each Node and only the new part of the key in the Leaf.
 *       This is both less storage and less time searching. The full key can
 *       then be reconstructed as required.
 * 
 * @todo use allocation pools for node, leaf, key[], and value[] objects?
 * 
 * FIXME add support for serializing the prior/next references when known. we do
 * checksums which makes it trickier to touch up those references after the fact
 * and the next reference is not knowable for the index segments until we have
 * determined the serialized size of the current leaf, which makes that all a
 * bit tricky.
 * 
 * @see IBTree
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
     * An object that knows how to (de-)serialize keys.
     */
    protected final IKeySerializer keySerializer;

    /**
     * An object that knows how to (de-)serialize the values on leaves.
     */
    protected final IValueSerializer valueSerializer;
    
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
     * The object index is used in a single threaded context. Therefore a
     * single private instance is used to compute checksums.
     */
    private static final ChecksumUtility chk = new ChecksumUtility();
    
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
     * @param keySerializer
     *            An object that knows how to (de-)serialize the keys on
     *            {@link INodeData}s and {@link ILeafData leaves} of a btree.
     * 
     * @param valueSerializer
     *            An object that knows how to (de-)serialize the values on
     *            {@link ILeafData leaves}.
     */
    public NodeSerializer(INodeFactory nodeFactory, IKeySerializer keySerializer, IValueSerializer valueSerializer) {

        assert nodeFactory != null;

        assert keySerializer != null;

        assert valueSerializer != null;

        this.nodeFactory = nodeFactory;
        
        this.keySerializer = keySerializer;
        
        this.valueSerializer = valueSerializer;
        
    }

    /**
     * Return the maximum serialized size (in bytes) of a node or leaf.
     * 
     * @param isLeaf
     *            True iff the maximum size will be reported for a leaf.
     * @param nkeys
     *            The #of keys for the node or leaf. Note that the maximum #of
     *            keys for a node is one less than the maximum #of keys for a
     *            leaf.
     *            
     * @return The maximum size of the serialized record in bytes.
     * 
     * FIXME There is no fixed upper limit for URIs (or strings in general),
     * therefore the btree may have to occasionally resize its buffer to
     * accomodate very long variable length keys.
     */
    protected int getSize( boolean isLeaf, int nkeys ) {
        
        /*
         * The branching factor and #of keys are packed values, but this is
         * their maximum size.
         */
        final int flexHeader = (Bytes.SIZEOF_INT*2) ;
        
        int keysSize = keySerializer.getSize(nkeys);

        if (isLeaf) {

            int valuesSize = valueSerializer.getSize(nkeys);

            return SIZEOF_LINKED_LEAF_HEADER + flexHeader + keysSize + valuesSize;

        } else {

            int valuesSize = (SIZEOF_REF * (nkeys + 1));

            return SIZEOF_NODE_HEADER + flexHeader + keysSize + valuesSize;

        }
        
    }

    /**
     * De-serialize a node or leaf using absolute offsets into the buffer. This
     * method is used when the caller does not know a-priori whether the
     * reference is to a node or leaf. The decision is made based on inspection
     * of the {@link #OFFSET_NODE_TYPE} byte in the buffer.
     * 
     * @param btree
     *            The btree.
     * @param id
     *            The persistent identitifer of the node or leaf being
     *            de-serialized.
     * @param buf
     *            The buffer.
     * 
     * @return The de-serialized node.
     */
    public IAbstractNodeData getNodeOrLeaf( IBTree btree, long id, ByteBuffer buf) {

        assert btree != null;
        assert id != 0L;
        assert buf != null;
        
        if( buf.get(OFFSET_NODE_TYPE) == TYPE_NODE ) {

            return getNode(btree,id,buf);
            
        } else {
            
            return getLeaf(btree,id,buf);

        }

    }
    
    /**
     * Serialize a node or leaf onto a buffer.
     * 
     * @param buf
     *            The buffer. The node or leaf will be serialized starting at
     *            the current position. The position will be advanced as a side
     *            effect.
     * @param node
     *            The node or leaf.
     * 
     * @exception BufferOverflowException
     *                if there is not enough space remaining in the buffer.
     */
    public void putNodeOrLeaf(ByteBuffer buf, IAbstractNodeData node) {
        
        if(node instanceof INodeData) {
            
            putNode(buf,(INodeData)node);
            
        } else {
        
            putLeaf(buf,(ILeafData)node);
            
        }
        
    }
    
    /**
     * Serialize a non-leaf node onto a buffer.
     * 
     * @param buf
     *            The buffer. The node will be serialized starting at the
     *            current position. The position will be advanced as a side
     *            effect.
     * @param node
     *            The node.
     * 
     * @exception BufferOverflowException
     *                if there is not enough space remaining in the buffer.
     */
    public void putNode(ByteBuffer buf, INodeData node) {

        assert buf != null;
        assert node != null;

        final int nkeys = node.getKeyCount();
        final int branchingFactor = node.getBranchingFactor();
        final Object keys = node.getKeys();
        final long[] childAddr = node.getChildAddr();

        /*
         * fixed length node header.
         */

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
         * setup output stream over the buffer.
         */
        DataOutputStream os = new DataOutputStream(new ByteBufferOutputStream(
                buf));
        
        int i = 0;
        try {

            // branching factor.
            LongPacker.packLong(os, branchingFactor);

            // #of keys
            LongPacker.packLong(os, nkeys);

            // keys.
            keySerializer.putKeys(os, keys, nkeys);

            // values.
            for (i = 0; i <= nkeys; i++) {

                final long addr = childAddr[i];

                /*
                 * Children MUST have assigned persistent identity.
                 */
                if (addr == 0L) {

                    throw new RuntimeException("Child is not persistent: node="
                            + node + ", child index=" + i);

                }

                LongPacker.packLong(os, addr);
//                os.writeLong(addr);

            }

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
        final int checksum = chk.checksum(buf, pos0 + SIZEOF_CHECKSUM, pos0
                + nbytes);

        // System.err.println("computed node checksum: "+checksum);

        // write the checksum into the buffer.
        buf.putInt(pos0, checksum);

    }

    public INodeData getNode(IBTree btree,long id,ByteBuffer buf) {

        assert btree != null;
        assert id != 0L;
        assert buf != null;

        final ArrayType keyType = keySerializer.getKeyType();

        /*
         * Read fixed length node header.
         */

        final int pos0 = buf.position();

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
        final int computedChecksum = chk.checksum(buf, pos0 + SIZEOF_CHECKSUM,
                pos0 + nbytes);

        if (computedChecksum != readChecksum) {

            throw new ChecksumError("Invalid checksum: read " + readChecksum
                    + ", but computed " + computedChecksum);

        }

        // nodeType
        if (buf.get() != TYPE_NODE) {

            // expecting a non-leaf node.
            throw new RuntimeException("Not a Node: id=" + id);

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

            // nkeys
            final int nkeys = (int) LongPacker.unpackLong(is);

            // @todo minimum is (m+1/2) unless this is the root node.
            assert nkeys >= 0 && nkeys < branchingFactor;

            final long[] childAddr = new long[branchingFactor + 1];

            /*
             * Keys.
             */

            final Object keys = ArrayType.alloc(keyType, branchingFactor);

            keySerializer.getKeys(is, keys, nkeys);

            /*
             * Child references (nchildren == nkeys+1).
             */

            for (int i = 0; i <= nkeys; i++) {

//                final long addr = is.readLong();
                final long addr = LongPacker.unpackLong(is);

                if (addr == 0L) {

                    throw new RuntimeException(
                            "Child does not have persistent address: index="
                                    + i);

                }

                childAddr[i] = addr;

            }

            // verify #of bytes actually read.
            assert buf.position() - pos0 == nbytes;

            // Done.
            return nodeFactory.allocNode(btree, id, branchingFactor, keyType,
                    nkeys, keys, childAddr);

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
     * Serialize a leaf node onto a buffer.
     * 
     * @param buf
     *            The buffer. The node will be serialized starting at the
     *            current position. The position will be advanced as a side
     *            effect.
     * @param leaf
     *            The leaf node.
     */
    public void putLeaf(ByteBuffer buf, ILeafData leaf) {

        assert buf != null;
        assert leaf != null;
        
        final int nkeys = leaf.getKeyCount();
        final int branchingFactor = leaf.getBranchingFactor();
        final Object keys = leaf.getKeys();
        final Object[] vals = leaf.getValues();
        
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
         */
        DataOutputStream os = new DataOutputStream(new ByteBufferOutputStream(
                buf));

        try {
            
            // branching factor.
            LongPacker.packLong(os, branchingFactor);

            // #of keys
            LongPacker.packLong(os, nkeys);

            // keys.
            keySerializer.putKeys(os, keys, nkeys);

            /*
             * values.
             */
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
        final int checksum = chk.checksum(buf, pos0 + SIZEOF_CHECKSUM, pos0
                + nbytes);
        // System.err.println("computed leaf checksum: "+checksum);

        // write checksum on buffer.
        buf.putInt(pos0, checksum);

    }

    public ILeafData getLeaf(IBTree btree,long id,ByteBuffer buf) {
        
        assert btree != null;
        assert id != 0L;
        assert buf != null;

        final ArrayType keyType = keySerializer.getKeyType();

        /*
         * common data.
         */

        final int pos0 = buf.position();

        // checksum
        final int readChecksum = buf.getInt(); // read checksum.
        // System.err.println("read checksum="+readChecksum);

        // #bytes.
        final int nbytes = buf.getInt();

        /*
         * verify checksum.
         */
        final int computedChecksum = chk.checksum(buf, pos0 + SIZEOF_CHECKSUM,
                pos0 + nbytes);

        if (computedChecksum != readChecksum) {

            throw new ChecksumError("Invalid checksum: read " + readChecksum
                    + ", but computed " + computedChecksum);

        }

        // nodeType
        final byte nodeType = buf.get();
        
        if (nodeType != TYPE_LEAF && nodeType != TYPE_LINKED_LEAF) {

            // expecting a leaf.
            throw new RuntimeException("Not a leaf: id=" + id + ", nodeType="
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

            // nkeys
            final int nkeys = (int) LongPacker.unpackLong(is);

            // @todo limit is (m+1)/2 unless root leaf.
            assert nkeys >= 0 && nkeys <= branchingFactor;

            /*
             * Keys.
             */

            final Object keys = ArrayType.alloc(keyType, branchingFactor + 1);

            keySerializer.getKeys(is, keys, nkeys);

            /*
             * Values.
             */

            final Object[] values = new Object[branchingFactor + 1];

            valueSerializer.getValues(is, values, nkeys);

            // verify #of bytes actually read.
            assert buf.position() - pos0 == nbytes;

            // Done.
            return nodeFactory.allocLeaf(btree, id, branchingFactor, keyType,
                    nkeys, keys, values);

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

}
