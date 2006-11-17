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
package com.bigdata.objectIndex;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import com.bigdata.journal.Bytes;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SlotMath;
import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

/**
 * <p>
 * An instance of this class is used to serialize and de-serialize the
 * {@link Node}s and {@link Leaf}s of a {@link BTree}. Leaf and non-leaf
 * records have different serialization formats, but their leading bytes use the
 * same format so that you can tell by inspection whether a buffer contains a
 * leaf or a non-leaf node.
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
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Modify the serialized "reference" form to be smaller by writing the #of
 * slots and not the #of bytes in the serialized record and by also accepting
 * pragamatic limits on both the #of slots that may be addressed in the journal
 * for a given slot size.
 * 
 * @todo Since the serialization record no longer has a fixed size, can we go a
 *       little further and compute its maximum size for a node and then report
 *       only the actual #of bytes used. This would let us pack some values and
 *       could be a big savings. We could also explore key compression, which
 *       might be very useful for an index that is expected to be dense. Perhaps
 *       we could just use the extser package at this point - probably it is
 *       safer to do this without extser so that it is simpler ("ready to
 *       hand").
 * 
 * @todo Modify the test helpers to generate random nodes by splitting two
 *       random leaves and assigning those leaves persistent identity and
 *       restore the commented out assertion in {@link AbstractNode#getParent()}
 * 
 * @todo Generalize to allow non-int[] keys and other kinds of value[]s.
 */
public class NodeSerializer {

    /**
     * Used to compute the #of slots from the #of bytes.
     */
    final protected SlotMath slotMath;

    /**
     * The {@link Adler32} checksum. This is an int32 value, even through the
     * {@link Checksum} API returns an int64 (aka long integer) value. The
     * actual checksum is in the lower 32 bit.
     */
    static final int SIZEOF_ADLER32 = Bytes.SIZEOF_INT;

    /**
     * The size of the field whose value is the length of the serialized record
     * in bytes.
     */
    static final int SIZEOF_NBYTES = Bytes.SIZEOF_SHORT;
    
    /**
     * The size of the boolean field indicating whether a serialized record
     * contains is a node or a leaf.
     */
    static final int SIZEOF_IS_LEAF = Bytes.SIZEOF_BYTE;

    /**
     * The size of the field containing the branching factor (aka order) for the
     * serialized node.
     */
    static final int SIZEOF_ORDER = Bytes.SIZEOF_SHORT;
    
    /**
     * #of keys in the node.  The #of children for a {@link Node} is nkeys + 1.
     * The #of values for a leave is equal to the #of keys.
     */
    static final int SIZEOF_NKEYS = Bytes.SIZEOF_SHORT;

    /**
     * Size of a node or leaf reference. The value must be interpreted per
     * {@link #putNodeRef(ByteBuffer, long)}.
     */
    static final int SIZEOF_REF = Bytes.SIZEOF_LONG;
//    static final int SIZEOF_REF = Bytes.SIZEOF_INT;

    /**
     * The key is an int32 within segment persistent identifier.
     */
    static final int SIZEOF_KEY = Bytes.SIZEOF_INT;

    /**
     * Size of an {@link ISlotsAllocation} encoded as a long integer. This is
     * used for non-node references (references to data versions in the
     * journal). Since we do not know the size of the referenced objects in
     * advanced we can not serialize these as int32 and we have to serialize the
     * full int64 value.
     */
    static final int SIZEOF_SLOTS = Bytes.SIZEOF_LONG;
    
    /**
     * Size of a value for a non-leaf node. The value must be interpreted per
     * {@link #putNodeRef(ByteBuffer, long)}.
     */
    static final int SIZEOF_NODE_VALUE = SIZEOF_REF;

    /**
     * Size of a version counter.
     */
    static final int SIZEOF_VERSION_COUNTER = Bytes.SIZEOF_SHORT;
    
    /**
     * Size of a value for a leaf node. The value is an encoded
     * {@link IObjectIndexEntry}.
     */
    static final int SIZEOF_LEAF_VALUE
            = SIZEOF_VERSION_COUNTER // versionCounter
            + SIZEOF_SLOTS // currentVersion (slots as long)
            + SIZEOF_SLOTS // preExistingVersion (slots as long)
            ;

    /**
     * Offset of the int32 value that is the {@link Adler32} checksum of the
     * serialized node or leaf. The checksum is computed for all bytes
     * exclusing the first 4 bytes, on which the value of the computed
     * checksum is written.
     */
    static final int OFFSET_CHECKSUM = 0;

    /**
     * Offset of the int16 signed integer whose value is the #of bytes in the
     * serialized record. This is written on the record so that we can validate
     * the checksum immediately when attempting to read a record and thereby
     * prevent inadvertent allocations of arrays for keys and values based on
     * bad data.
     */
    static final int OFFSET_NBYTES = OFFSET_CHECKSUM + SIZEOF_ADLER32;
    
    /**
     * Offset of the byte whose value indicates whether this node is a leaf
     * (1) or a non-leaf node (0).
     */
    static final int OFFSET_IS_LEAF = OFFSET_NBYTES + SIZEOF_NBYTES;

    /**
     * Offset of the short integer whose value is the non-negative index of
     * the #of keys in this node.
     */
    static final int OFFSET_ORDER = OFFSET_IS_LEAF + SIZEOF_IS_LEAF;

    /**
     * Offset of the short integer whose value is the non-negative index of
     * the #of keys in this node.
     */
    static final int OFFSET_NKEYS = OFFSET_ORDER + SIZEOF_ORDER;

    /**
     * Offset of the first key within the buffer. The keys are an array of int32
     * values that represent persistent within segment object identifiers. The
     * keys are maintained in sorted order and are filled from the end of the
     * array. The capacity of the array is fixed by the {@link #pageSize}
     * specified for the index.
     * 
     * @see AbstractNode#NEGINF
     * @see AbstractNode#POSINF
     */
    static final int OFFSET_KEYS = OFFSET_NKEYS + SIZEOF_NKEYS;
    
    /**
     * The object index is used in a single threaded context. Therefore a
     * single private instance is used to compute checksums.
     */
    private static final ChecksumUtility chk = new ChecksumUtility();
    
    private NodeSerializer() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Constructor computes constants that depend only on the page size of
     * the index.
     * 
     * @param slotMath
     *            Used to decode a long integer encoding an
     *            {@link ISlotAllocation}.
     */
    NodeSerializer(SlotMath slotMath) {

        assert slotMath != null;

        this.slotMath = slotMath;
        
    }

    /**
     * The #of bytes requires to serialize this node or leaf.
     * 
     * @param node
     *            The node or leaf.
     * 
     * @return The #of bytes required to serialize that node or leaf.
     */
    public int getSize(AbstractNode node) {

        return getSize( node.isLeaf(), node.nkeys );
        
    }
    
    int getSize( boolean isLeaf, int nkeys ) {
        
        if (isLeaf) {

            int OFFSET_LEAF_VALUES = OFFSET_KEYS + (SIZEOF_KEY * nkeys);

            int LEAF_SIZE = OFFSET_LEAF_VALUES + (SIZEOF_LEAF_VALUE * nkeys);

            return LEAF_SIZE;

        } else {

            int OFFSET_NODE_VALUES = OFFSET_KEYS + (SIZEOF_KEY * nkeys);

            int NODE_SIZE = OFFSET_NODE_VALUES
                    + (SIZEOF_NODE_VALUE * (nkeys + 1));

            return NODE_SIZE;

        }
        
    }

    /**
     * De-serialize a node or leaf. This method is used when the caller does not
     * know a-priori whether the reference is to a node or leaf. The decision is
     * made based on inspection of the {@link #OFFSET_IS_LEAF} byte in the
     * buffer.
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
    AbstractNode getNodeOrLeaf( BTree btree, long id, ByteBuffer buf) {

        assert btree != null;
        assert id != 0L;
        assert buf != null;
        
        if (buf.limit() < OFFSET_KEYS) {

            throw new RuntimeException(
                    "Buffer is too small to contain a node or leaf.");
            
        }
        
        if( buf.get(OFFSET_IS_LEAF) == 1 ) {

            return getLeaf(btree,id,buf);

        } else {
            
            return getNode(btree,id,buf);
            
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
    void putNode(ByteBuffer buf, Node node) {

        assert buf != null;
        assert node != null;
        assert node.branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
        assert node.branchingFactor < Short.MAX_VALUE;
        assert node.nkeys >= 0 && node.nkeys < node.branchingFactor;

        if (node.dirtyChildren.size() > 0) {

            /*
             * Note: You can not serialize a node that has dirty children since
             * the childKeys[] array will not contain the persistent identity
             * for any child that has not already been serialized.
             */
            
            throw new IllegalStateException("Dirty children exist.");
            
        }

        final int nkeys = node.nkeys;

        /*
         * common data.
         */

        final int pos0 = buf.position();

        // checksum
        buf.putInt(0); // will overwrite below with the checksum.

        // #bytes
        buf.putShort((short)0); // will overwrite below with the actual value.
        
        // isLeaf
        buf.put((byte) 0); // this is a non-leaf node.
        
        // branching factor.
        buf.putShort((short)node.branchingFactor);
        
        // #of keys
        buf.putShort((short) node.nkeys);
        
        // keys.
        
        int lastKey = AbstractNode.NEGINF;
        
        for (int i = 0; i < nkeys; i++) {

            final int key = node.keys[i];
            
            assert key > lastKey; // verify increasing and minimum.
            
            assert key < AbstractNode.POSINF; // verify maximum.

            buf.putInt(key);
            
            lastKey = key;
            
        }
        
        // values.
        for (int i = 0; i <= nkeys; i++) {
        
            final long childKey = node.childKeys[i];
            
            // children MUST have assigned persistent identity.
            assert childKey != 0L;
            
            putNodeRef(buf, childKey);
            
        }

        // #of bytes actually written.
        final int nbytes = buf.position() - pos0;
        assert nbytes>=OFFSET_KEYS;
        assert nbytes<=Short.MAX_VALUE;
        
        // patch #of bytes written on the record format.
        buf.putShort(pos0+OFFSET_NBYTES,(short)nbytes);
        
        // compute checksum for data written.
        final int checksum = chk.checksum(buf, pos0 + SIZEOF_ADLER32, pos0
                + nbytes);
        
//        System.err.println("computed node checksum: "+checksum);

        // write the checksum into the buffer.
        buf.putInt(pos0, checksum);
        
    }

    Node getNode(BTree btree,long id,ByteBuffer buf) {

        assert btree != null;
        assert id != 0L;
        assert buf != null;

        final int remaining = buf.remaining();
        
        /*
         * common data.
         */

        final int pos0 = buf.position();

        // checksum
        final int readChecksum = buf.getInt(); // read checksum.
//        System.err.println("read checksum="+readChecksum);

        // #of bytes in record. 
        final int nbytes = buf.getShort();
        assert nbytes>=OFFSET_KEYS;

        /*
         * verify checksum now that we know how many bytes of data we expect
         * to read.
         */
        final int computedChecksum = chk.checksum(buf, pos0 + SIZEOF_ADLER32,
                pos0 + nbytes);
        
        if (computedChecksum != readChecksum) {
        
            throw new ChecksumError("Invalid checksum: read " + readChecksum
                    + ", but computed " + computedChecksum);
            
        }

        // isLeaf
        if( buf.get() != 0 ) {

            // expecting a non-leaf node.
            throw new RuntimeException("Not a Node: id="+id);
            
        }

        // branching factor.
        final int branchingFactor = buf.getShort();

        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
        
        // nkeys
        final int nkeys = buf.getShort();

        assert nkeys >= 0 && nkeys < branchingFactor;

        // check the buffer size know that we known the #of keys.
        assert remaining == getSize(false,nkeys );

        final int[] keys = new int[branchingFactor - 1];

        final long[] children = new long[branchingFactor];

        /*
         * keys.
         */
        
        int lastKey = Node.NEGINF;

        for (int i = 0; i < nkeys; i++) {

            int key = buf.getInt();

            assert key > lastKey; // verify keys are in ascending order.

            assert key < Node.POSINF; // verify keys in legal range.

            keys[i] = lastKey = key;

        }

        /*
         * child references (nchildren == nkeys+1).
         */

        for (int i = 0; i <= nkeys; i++) {

            children[i] = getNodeRef(buf);

        }

        // verify #of bytes actually read.
        assert buf.position() - pos0 == nbytes;
        
        // Done.
        return new Node( btree, id, branchingFactor, nkeys, keys, children);

    }

    /**
     * Serialize a leaf node onto a buffer.
     * 
     * @param buf
     *            The buffer. The node will be serialized starting at the
     *            current position. The position will be advanced as a side
     *            effect. The remaining bytes in the buffer must equal
     *            {@link #LEAF_SIZE} as a pre-condition and will be ZERO(0) as a
     *            post-condition.
     * @param node
     *            The node. Must be a leaf node.
     */
    void putLeaf(ByteBuffer buf, Leaf node) {

        assert buf != null;
        assert node != null;
        assert node.nkeys >= 0 && node.nkeys <= node.branchingFactor;
        
        final int nkeys = node.nkeys;
        
        /*
         * common data.
         */
        // checksum
        final int pos0 = buf.position();

        buf.putInt(0); // will overwrite below with the checksum.
        
        // nbytes
        buf.putShort((short)0); // will overwrite below with the actual value.
        
        // isLeaf
        buf.put((byte) 1); // this is a leaf node.
        
        // branching factor.
        buf.putShort((short)node.branchingFactor);
        
        // #of keys
        buf.putShort((short) node.nkeys);
        
        // keys.
        
        int lastKey = AbstractNode.NEGINF;
        
        for (int i = 0; i < nkeys; i++) {

            final int key = node.keys[i];
            
            assert key > lastKey; // verify increasing and minimum.
            
            assert key < AbstractNode.POSINF; // verify maximum.

            buf.putInt(key);
            
            lastKey = key;
            
        }
        
        /*
         * values.
         */
        for (int i = 0; i < nkeys; i++) {

            final IObjectIndexEntry entry = node.values[i];

            assert entry != null;

            // May be null (indicates the current version is deleted).
            final ISlotAllocation currentVersionSlots = entry
                    .getCurrentVersionSlots();

            // May be null (indicates first version in this isolation/tx).
            final ISlotAllocation preExistingVersionSlots = entry
                    .getPreExistingVersionSlots();

            buf.putShort(entry.getVersionCounter());

            buf.putLong((currentVersionSlots == null ? 0L : currentVersionSlots
                    .toLong()));

            buf.putLong((preExistingVersionSlots == null ? 0L
                    : preExistingVersionSlots.toLong()));

        }

        // #of bytes actually written.
        final int nbytes = buf.position() - pos0;
        assert nbytes>=OFFSET_KEYS;
        assert nbytes<=Short.MAX_VALUE;
        
        // patch #of bytes written on the record format.
        buf.putShort(pos0+OFFSET_NBYTES,(short)nbytes);
       
        // compute checksum
        final int checksum = chk.checksum(buf, pos0 + SIZEOF_ADLER32, pos0
                + nbytes);
//        System.err.println("computed leaf checksum: "+checksum);
        
        // write checksum on buffer.
        buf.putInt(pos0, checksum);

    }

    Leaf getLeaf(BTree btree,long id,ByteBuffer buf) {
        
        assert btree != null;
        assert id != 0L;
        assert buf != null;

        final int remaining = buf.remaining();
        
        /*
         * common data.
         */
        
        final int pos0 = buf.position();

        // checksum
        final int readChecksum = buf.getInt(); // read checksum.
//        System.err.println("read checksum="+readChecksum);
        
        // #bytes.
        final int nbytes = buf.getShort();
        
        /*
         * verify checksum.
         */
        final int computedChecksum = chk.checksum(buf, pos0 + SIZEOF_ADLER32,
                pos0 + nbytes);
        
        if (computedChecksum != readChecksum) {
        
            throw new ChecksumError("Invalid checksum: read " + readChecksum
                    + ", but computed " + computedChecksum);
            
        }
        
        // isLeaf
        if( buf.get() != 1 ) {

            // expecting a non-leaf node.
            throw new RuntimeException("Not a Node: id="+id);
            
        }

        // branching factor
        final int branchingFactor = buf.getShort();

        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
        
        // nkeys
        final int nkeys = buf.getShort();

        assert nkeys >=0 && nkeys <= branchingFactor;
        
        assert remaining >= getSize(true,nkeys);

        /*
         * Keys.
         */

        final int[] keys = new int[branchingFactor];

        int lastKey = Node.NEGINF;

        for (int i = 0; i < nkeys; i++) {

            int key = buf.getInt();

            assert key > lastKey; // verify keys are in ascending order.

            assert key < Node.POSINF; // verify keys in legal range.

            keys[i] = lastKey = key;

        }

        /*
         * Values.
         */

        final IObjectIndexEntry[] values = new IObjectIndexEntry[branchingFactor];

        for (int i = 0; i < nkeys; i++) {

            final short versionCounter = buf.getShort();

            final long currentVersion = buf.getLong();

            final long preExistingVersion = buf.getLong();

            values[i] = new IndexEntry(slotMath, versionCounter,
                    currentVersion, preExistingVersion);

        }

        // verify #of bytes actually read.
        assert buf.position() - pos0 == nbytes;
        
        // Done.
        return new Leaf( btree, id, branchingFactor, nkeys, keys, values);

    }

    /**
     * When writing a reference to a node or leaf we only write the firstSlot
     * value (int32). However, we write -(firstSlot) if the reference is to a
     * leaf node (we can decide this based on the size of the allocation). When
     * the reference is a "null", we just write zero(0). This helps us keep the
     * size of the non-leaf nodes down and improves overall utilization of the
     * store.
     * 
     * @param buf
     *            The buffer on which we write an int32 value.
     * @param longValue
     *            The {@link ISlotAllocation} of the reference, encoded as a
     *            long integer.
     */
    private void putNodeRef(ByteBuffer buf, long longValue) {

        buf.putLong(longValue);
        
//        if( longValue == 0L ) {
//
//            // Special case for null ref.
//            buf.putInt(0);
//            
//            return;
//            
//        }
//        
//        final int nbytes = SlotMath.getByteCount(longValue);
//        
//        final int firstSlot = SlotMath.getFirstSlot(longValue);
//        
//        if( nbytes == NODE_SIZE) {
//        
//            // Store as firstSlot (positive integer).
//            buf.putInt(firstSlot);
//            
//        } else if( nbytes == LEAF_SIZE ) {
//            
//            // Store as -(firstSlot) (negative integer).
//            buf.putInt(-firstSlot);
//            
//        } else {
//            
//            throw new AssertionError(
//                    "Allocation size matches neither node nor leaf: firstSlot="
//                            + firstSlot + ", nbytes=" + nbytes);
//            
//        }
        
    }

    /**
     * Reads an int32 value from the buffer and decodes it.
     * 
     * @param buf
     *            The buffer from which to read the value.
     * 
     * @return The {@link ISlotAllocation} for the reference or zero(0L) iff
     *         this was a null reference.
     * 
     * @see #putNodeRef(ByteBuffer, long)
     */
    private long getNodeRef(ByteBuffer buf) {

        return buf.getLong();
        
//        final int firstSlot = buf.getInt();
//        
//        final long longValue;
//        
//        if (firstSlot == 0) {
//            
//            longValue = 0;
//            
//        } else if (firstSlot > 0) {
//            
//            longValue = SlotMath.toLong(NODE_SIZE, firstSlot);
//            
//        } else {
//            
//            longValue = SlotMath.toLong(LEAF_SIZE, -firstSlot);
//            
//        }
//        
//        return longValue;
        
    }

}
