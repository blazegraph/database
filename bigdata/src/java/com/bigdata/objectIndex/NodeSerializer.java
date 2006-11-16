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

import java.nio.ByteBuffer;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import com.bigdata.journal.Bytes;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SlotMath;
import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

/**
 * An instance of this class is used to serialize and de-serialize nodes and
 * leaves of an {@link ObjectIndex} with a given #of keys per node (aka
 * branching factor). Leaf and non-leaf nodes have different serialization
 * formats and require a different capacity buffer, but their leading bytes use
 * the same format so that you can tell by inspection whether a buffer contains
 * a leaf or a non-leaf node.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME nkeys semantics differ from first semantics.  The serializes need to be
 * updated to reflect that difference.
 * 
 * FIXME Is there really any reason to have fixed size serialization? The key
 * impact seems to be the size of the non-leaf nodes (since they would require
 * the size of the child node). References can still be negative (long) integers
 * to differentiate leaf vs non-leaf nodes, but we can also figure out whether
 * the node is a leaf or not during de-serialization. The big win for accepting
 * variable size allocation is that we can "right fit" each node into free slots
 * on the journal. (Since node are immutable once written, it does not make
 * sense to write a constant size since we will never update the node in its
 * current allocation.)
 * 
 * @todo Nodes will have hard references until they are ready to be serialized,
 *       at which point the hard references must be converted to
 *       {@link ISlotAllocation}s. The code below assumes that this conversion
 *       has already been performed and does not anticipate the runtime data
 *       structures that will be required to operate with hard references or
 *       {@link ISlotAllocation}s as appropriate. (An alternative design might
 *       be to assign negative long integers to transient nodes and convert
 *       those references when a node is serialized. However we do this, the
 *       reference concept needs to encapsulate both kinds of reference, provide
 *       for conversion of the reference type during (de-)serialization, and
 *       support copy-on-write semantics.)
 * 
 * @todo Consider making nodes and leafs the same size and just having more
 *       key/value pairs in nodes. That will give us a higher branching factor
 *       for nodes in combination with the same size allocations for leaves and
 *       nodes with the potential for better allocation behavior by the journal.
 */
class NodeSerializer {

    final SlotMath slotMath;

    /**
     * The #of keys per node (aka branching factor).
     * 
     * @todo consider serializing this with each node and leave so that the
     *       serialized nodes may be more dense and so that nodes and leaves may
     *       vary their capacity.
     */
    final int pageSize;

    /**
     * The {@link Adler32} checksum. This is an int32 value, even through the
     * {@link Checksum} API returns an int64 (aka long integer) value. The
     * actual checksum is in the lower 32 bit.
     */
    static final int SIZEOF_ADLER32 = Bytes.SIZEOF_INT;

    static final int SIZEOF_IS_LEAF = Bytes.SIZEOF_BYTE;

    /**
     * #of keys in the node.  The #of children for a {@link Node} is nkeys + 1.
     * The #of values for a leave is equal to the #of keys.
     */
    static final int SIZEOF_NKEYS = Bytes.SIZEOF_SHORT;

    /**
     * Size of a node or leaf reference. The value must be interpreted per
     * {@link #putNodeRef(ByteBuffer, long)}.
     */
    static final int SIZEOF_REF = Bytes.SIZEOF_INT;

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
     * Offset of the byte whose value indicates whether this node is a leaf
     * (1) or a non-leaf node (0).
     */
    static final int OFFSET_IS_LEAF = OFFSET_CHECKSUM + SIZEOF_ADLER32;

    /**
     * Offset of the short integer whose value is the non-negative index of
     * the #of keys in this node.
     */
    static final int OFFSET_NKEYS = OFFSET_IS_LEAF + SIZEOF_IS_LEAF;

    /**
     * Offset of the short integer whose value is the non-negative index of
     * branching factor for this node.
     */
    static final int OFFSET_ORDER = OFFSET_NKEYS+ SIZEOF_NKEYS;

    /*
     * @todo This is a possible location for a parent node reference. We might
     * need this in order to support copy-on-write. Alternatively, we could pass
     * the parent node into recursive calls and clone nodes on the way back up
     * making adjustments as necessary.
     */

    /**
     * Offset of the first key within the buffer. The keys are an array of int32
     * values that represent persistent within segment object identifiers. The
     * keys are maintained in sorted order and are filled from the end of the
     * array. The capacity of the array is fixed by the {@link #pageSize}
     * specified for the index.
     * 
     * @see Node#NEGINF_KEY
     * @see Node#POSINF_KEY
     */
    static final int OFFSET_KEYS = OFFSET_ORDER + SIZEOF_NKEYS;

    /**
     * Offset to first value within the buffer for a non-leaf node. The
     * values are an array of {@link ISlotAllocation}s encoded as long
     * integers. Each value gives the location of the current version of the
     * corresponding child node for the key having the same array index. The
     * values are maintained in correspondence with the keys. The capacity
     * of the array is fixed by the {@link #pageSize} specified for the
     * index.
     */
    final int OFFSET_NODE_VALUES;

    /**
     * Offset to the first value within the buffer for a leaf node. The
     * values are an array of serialized {@link IObjectIndexEntry} objects.
     * The values are maintained in correspondence with the keys. The
     * capacity of the array is fixed by the {@link #pageSize} specified for
     * the index.
     */
    final int OFFSET_LEAF_VALUES;

//    /** Offset of the reference to prior leaf (iff leaf). */
//    final int OFFSET_LEAF_PRIOR;
//
//    /** Offset of the reference to prior leaf (iff leaf). */
//    final int OFFSET_LEAF_NEXT;

    /** total non-leaf node size. */
    final int NODE_SIZE;

    /** total leaf node size. */
    final int LEAF_SIZE;

    /** #of slots per non-leaf node. */
    final int slotsPerNode;
    
    /** #of slots per leaf node. */
    final int slotsPerLeaf;
    
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
     * 
     * @param pageSize
     *            The page size (aka branching factor).
     */
    NodeSerializer(SlotMath slotMath, int pageSize) {

        assert slotMath != null;
        assert pageSize > 0;

        this.slotMath = slotMath;
        this.pageSize = pageSize;

        OFFSET_NODE_VALUES = OFFSET_KEYS + (SIZEOF_KEY * pageSize);
        NODE_SIZE = OFFSET_NODE_VALUES + (SIZEOF_NODE_VALUE * pageSize);

        OFFSET_LEAF_VALUES = OFFSET_KEYS + (SIZEOF_KEY * pageSize);
//        OFFSET_LEAF_PRIOR = OFFSET_LEAF_VALUES
//                + (SIZEOF_LEAF_VALUE * pageSize);
//        OFFSET_LEAF_NEXT = OFFSET_LEAF_PRIOR + SIZEOF_REF;
//        LEAF_SIZE = OFFSET_LEAF_NEXT + SIZEOF_REF;
        LEAF_SIZE = OFFSET_LEAF_VALUES + (SIZEOF_LEAF_VALUE * pageSize);

        slotsPerNode = slotMath.getSlotCount(NODE_SIZE);
        slotsPerLeaf = slotMath.getSlotCount(LEAF_SIZE);

    }

    /**
     * De-serialize a node or leaf. This method is used when the caller does not
     * know a-priori whether the reference is to a node or leaf. The decision is
     * made based on inspection of the reference and verified by testing the
     * data in the buffer.
     * 
     * @param ndx
     *            The object index.
     * @param recid
     *            The reference.
     * @param buf
     *            The buffer.
     *            
     * @return The de-serialized node.
     */
    AbstractNode getNodeOrLeaf( ObjectIndex ndx, long recid, ByteBuffer buf) {

//        assert ndx != null; // @todo enable this assertion.
        assert recid != 0L;
        assert buf != null;
        
        final int nbytes = SlotMath.getByteCount(recid);
        
        if( nbytes == NODE_SIZE) {
        
            assert buf.get(OFFSET_IS_LEAF) == 0;

            return getNode(ndx,recid,buf);
            
        } else if( nbytes == LEAF_SIZE ) {
            
            assert buf.get(OFFSET_IS_LEAF) == 1;

            return getLeaf(ndx,recid,buf);

        } else {
            
            throw new AssertionError(
                    "Allocation size matches neither node nor leaf: nbytes="
                            + nbytes);
            
        }

    }
    
    /**
     * Serialize a non-leaf node onto a buffer.
     * 
     * @param buf
     *            The buffer. The node will be serialized starting at the
     *            current position. The position will be advanced as a side
     *            effect. The remaining bytes in the buffer must equal
     *            {@link #NODE_SIZE} as a pre-condition and will be ZERO(0)
     *            as a post-condition.
     * @param node
     *            The node. Must be a non-leaf node.
     */
    void putNode(ByteBuffer buf, Node node) {

        assert buf != null;
        assert node != null;
        assert node.nkeys >= 0 && node.nkeys < pageSize;
        assert buf.remaining() == NODE_SIZE;
        
        /*
         * common data.
         */
        // checksum
        // The offset at which to write the checksum.
        final int pos0 = buf.position();
        buf.putInt(0); // will overwrite below with the checksum.
        // isLeaf
        buf.put((byte) 0); // this is a non-leaf node.
        // #of keys
        buf.putShort((short) node.nkeys);
        // keys.
        for (int i = 0; i < node.branchingFactor-1; i++) {
            buf.putInt(node.keys[i]);
        }
        /*
         * non-leaf node specific data.
         */
        // values.
        for (int i = 0; i < node.branchingFactor; i++) {
            long val = node.childKeys[i];
            putNodeRef(buf, val);
        }
        
        assert buf.position() == buf.limit();

        // compute checksum and write it on the buffer.
        final int checksum = chk.checksum(buf, pos0 + SIZEOF_ADLER32, pos0
                + NODE_SIZE);
//        System.err.println("computed node checksum: "+checksum);
        buf.putInt(pos0, checksum);
        assert buf.getInt(pos0) == checksum;
    
        assert buf.position() == buf.limit();

    }

    Node getNode(ObjectIndex ndx,long recid,ByteBuffer buf) {

//        assert ndx != null; // @todo enable this assertion.
        assert recid != 0L;
        assert buf != null;

        if (buf.remaining() != NODE_SIZE) {
            throw new IllegalArgumentException(
                    "Wrong #bytes remaining in buffer for a non-leaf node: expected="
                            + NODE_SIZE + ", actual=" + buf.remaining());
        }

        /*
         * common data.
         */
        
        // checksum
        final int pos0 = buf.position();
        
        final int readChecksum = buf.getInt(); // read checksum.
//        System.err.println("read checksum="+readChecksum);
        
        final int computedChecksum = chk.checksum(buf, pos0 + SIZEOF_ADLER32, pos0
                + NODE_SIZE);
        
        if (computedChecksum != readChecksum) {
        
            throw new ChecksumError("Invalid checksum: read " + readChecksum
                    + ", but computed " + computedChecksum);
            
        }
        
        // isLeaf
        assert buf.get() == 0; // expecting a non-leaf node.
        
        // nkeys
        final int nkeys = buf.getShort();

        // keys & values.
        
        int[] keys = null;
        
        long[] children = null;
        
        if (nkeys == pageSize-1) {
            
            /*
             * If there are no keys or values then there is nothing more to
             * read.
             */
            
            buf.position(buf.limit()); // satisify post-condition.
            
        } else {

            // Skip over undefined keys.
            buf.position(pos0+OFFSET_KEYS+nkeys*SIZEOF_KEY);

            keys = new int[pageSize];
            
            int lastKey = Node.NEGINF;
            
            for (int i = nkeys; i < pageSize; i++) {
                
                int key = buf.getInt();
                
                assert key > lastKey; // verify keys are in ascending order.
                
                assert key < Node.POSINF; // verify keys in legal range.
                
                keys[i] = lastKey = key;
                
            }
            
            /*
             * non-leaf node specific data (children).
             */

            // Skip over undefined values.
            buf.position(pos0+OFFSET_NODE_VALUES+nkeys*SIZEOF_NODE_VALUE);

            children = new long[pageSize];
            
            for (int i = nkeys; i < pageSize; i++) {
                
                children[i] = getNodeRef(buf);
                
            }
            
        }

        assert buf.position() == buf.limit();

        // Done.
        return new Node((BTree)ndx, recid, nkeys, keys, children);

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
        assert node.nkeys >= 0 && node.nkeys < pageSize;
        assert buf.remaining() == LEAF_SIZE;

        /*
         * common data.
         */
        // checksum
        final int pos0 = buf.position(); // offset at which to write
        // checksum.
        buf.putInt(0); // will overwrite below with the checksum.
        // isLeaf
        buf.put((byte) 1); // this is a leaf node.
        // first
        buf.putShort((short) node.nkeys);
        // keys.
        for (int i = 0; i < pageSize-1; i++) {
            buf.putInt(node.keys[i]);
        }
        /*
         * leaf-node specific data.
         */
        for (int i = 0; i < pageSize; i++) { // write values[].
            IObjectIndexEntry entry = node.values[i];
            if (entry == null) {
                buf.putShort((short) 0);
                buf.putLong(0);
                buf.putLong(0);
            } else {
                // May be null (indicates the current version is deleted).
                final ISlotAllocation currentVersionSlots = entry.getCurrentVersionSlots();
                // May be null (indicates first version in this isolation/tx).
                final ISlotAllocation preExistingVersionSlots = entry.getPreExistingVersionSlots();
                buf.putShort(entry.getVersionCounter());
                buf.putLong((currentVersionSlots == null ? 0L
                        : currentVersionSlots.toLong()));
                buf.putLong((preExistingVersionSlots == null ? 0L
                        : preExistingVersionSlots.toLong()));
            }
        }
//        putNodeRef(buf, node._previous);
//        putNodeRef(buf, node._next);

        assert buf.position() == buf.limit();

        // compute checksum and write it on the buffer.
        final int checksum = chk.checksum(buf, pos0 + SIZEOF_ADLER32, pos0
                + LEAF_SIZE);
//        System.err.println("computed leaf checksum: "+checksum);
        buf.putInt(pos0, checksum);
        assert buf.getInt(pos0) == checksum;

        assert buf.position() == buf.limit();

    }

    Leaf getLeaf(ObjectIndex ndx,long recid,ByteBuffer buf) {
        
//        assert ndx != null; // @todo enable this assertion.
        assert recid != 0L;
        assert buf != null;
        if (buf.remaining() != LEAF_SIZE) {
            throw new IllegalArgumentException(
                    "Wrong #bytes remaining in buffer for a leaf node: expected="
                            + LEAF_SIZE + ", actual=" + buf.remaining());
        }

        /*
         * common data.
         */
        // checksum
        final int pos0 = buf.position();
        final int readChecksum = buf.getInt(); // read checksum.
//        System.err.println("read checksum="+readChecksum);
        final int computedChecksum = chk.checksum(buf, pos0 + SIZEOF_ADLER32, pos0
                + LEAF_SIZE);
        if (computedChecksum != readChecksum) {
            throw new ChecksumError("Invalid checksum: read " + readChecksum
                    + ", but computed " + computedChecksum);
        }
        
        // isLeaf
        assert buf.get() == 1; // expecting a leaf node.

        // nkeys
        final int nkeys = buf.getShort();
        
        /*
         * keys and values.
         */
        
        int[] keys = null;
        
        IndexEntry[] values = null;
        
        if (nkeys == pageSize-1) {
        
            /*
             * If there are no keys or values then there is nothing more to
             * read.
             */
            
            buf.position(pos0 + LEAF_SIZE);
            
        } else {
            
            /*
             * Keys.
             */

            // Skip over undefined keys.
            buf.position(pos0+OFFSET_KEYS+nkeys*SIZEOF_KEY);

            keys = new int[pageSize];
            
            int lastKey = Node.NEGINF;
            
            for (int i = nkeys; i < pageSize; i++) {
                
                int key = buf.getInt();
                
                assert key > lastKey; // verify keys are in ascending order.
                
                assert key < Node.POSINF; // verify keys in legal range.
                
                keys[i] = lastKey = key;
                
            }
            
            /*
             * leaf node specific data (values).
             */

            // Skip over undefined values.
            buf.position(pos0+OFFSET_LEAF_VALUES+nkeys*SIZEOF_LEAF_VALUE);

            // values.
            values = new IndexEntry[pageSize];
            
            for (int i = nkeys; i < pageSize; i++) {
            
                final short versionCounter = buf.getShort();
                
                final long currentVersion = buf.getLong();
                
                final long preExistingVersion = buf.getLong();
                
                values[i] = new IndexEntry(slotMath, versionCounter,
                            currentVersion, preExistingVersion);
                
            }
        }

//        final long previous = getNodeRef(buf);
//        
//        final long next = getNodeRef(buf);

        assert buf.position() == buf.limit();
        
        // Done.
        return new Leaf((BTree)ndx, recid, nkeys, keys, values);
//                previous, next);

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

        if( longValue == 0L ) {

            // Special case for null ref.
            buf.putInt(0);
            
            return;
            
        }
        
        final int nbytes = SlotMath.getByteCount(longValue);
        
        final int firstSlot = SlotMath.getFirstSlot(longValue);
        
        if( nbytes == NODE_SIZE) {
        
            // Store as firstSlot (positive integer).
            buf.putInt(firstSlot);
            
        } else if( nbytes == LEAF_SIZE ) {
            
            // Store as -(firstSlot) (negative integer).
            buf.putInt(-firstSlot);
            
        } else {
            
            throw new AssertionError(
                    "Allocation size matches neither node nor leaf: firstSlot="
                            + firstSlot + ", nbytes=" + nbytes);
            
        }
        
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

        final int firstSlot = buf.getInt();
        
        final long longValue;
        
        if (firstSlot == 0) {
            
            longValue = 0;
            
        } else if (firstSlot > 0) {
            
            longValue = SlotMath.toLong(NODE_SIZE, firstSlot);
            
        } else {
            
            longValue = SlotMath.toLong(LEAF_SIZE, -firstSlot);
            
        }
        
        return longValue;
        
    }
}
