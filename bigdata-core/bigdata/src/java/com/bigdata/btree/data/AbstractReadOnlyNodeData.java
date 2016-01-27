/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Aug 5, 2009
 */

package com.bigdata.btree.data;

import java.nio.ByteBuffer;

import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.Leaf;
import com.bigdata.btree.Node;
import com.bigdata.util.Bytes;

/**
 * Abstract base class for a read-only view of the data for B+Tree node or leaf.
 * The data are stored in a {@link ByteBuffer}. Access to the keys, values and
 * other metadata are via operations on that {@link ByteBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractReadOnlyNodeData<U extends IAbstractNodeData> {

    /**
     * A B+Tree node data record.
     */
    public static final byte NODE = 0;

    /**
     * A B+Tree leaf data record.
     */
    public static final byte LEAF = 1;

    /**
     * A B+Tree leaf data record with with priorAddr and nextAddr fields. This
     * is used for the leaves in an {@link IndexSegment}.
     */
    public static final byte LINKED_LEAF = 2;

    /**
     * Return <code>true</code> iff the byte indicates an {@link INodeData}
     * record.
     * 
     * @param b
     *            The byte value.
     * @return
     */
    public static boolean isNode(final byte b) {

        switch (b) {
        case NODE:
            return true;
        case LEAF:
        case LINKED_LEAF:
            return false;
        default:
            throw new AssertionError("Found" + b + " but expected {0,1,2}");
        }

    }

    /**
     * Return <code>true</code> iff the byte indicates an {@link ILeafData}
     * record. Note that this will return true for both {@link #LEAF} and
     * {@link #LINKED_LEAF}.
     * 
     * @param b
     *            The byte value.
     * @return
     */
    public static boolean isLeaf(final byte b) {

        switch (b) {
        case NODE:
            return false;
        case LEAF:
        case LINKED_LEAF:
            return true;
        default:
            throw new AssertionError();
        }

    }

    /**
     * The initial version.
     */
    public static final short VERSION0 = 0;
    
	/**
	 * This version introduces:
	 * <dl>
	 * <dt>spannedTupleCount</dt>
	 * <dd>Both the #of children spanned by the node (<code>nentries</code>) and
	 * the #of children spanned by each child of the node (
	 * <code>childEntryCount</code>) are int64 integers in this version. The
	 * manner in which those integers are coded within the record has also
	 * changed.</dd>
	 * </dl>
	 */
//    FIXME ADD THIS STUFF INT64_BRANCH!
//	 * <dt>Index UUID</dt>
//	 * <dd>The UUID of the owning B+Tree is written into the record. This makes
//	 * possible certain forensic analysis and data recovery which rely on
//	 * scanning a file to identify records for a specific index.</dd>
//	 * <dt>Record version number</dt>
//	 * <dd>A version number has been introduced into each node record. The
//	 * record version number is strictly sequential. The next record version
//	 * number to be assigned is recorded in the {@link Checkpoint} record. The
//	 * record version number makes possible some forensic analysis and data
//	 * recovery which relies on the record version numbers to select among
//	 * multiple versions of a tuple.</dd>
    public final static transient short VERSION1 = 0x01;

    /**
     * The current version.
     */
    public final static transient short currentVersion = VERSION1;

    /**
     * Bit flag for a leaf carrying delete markers for each tuple.
     */
    public static final short FLAG_DELETE_MARKERS = 1 << 0;

    /**
     * Bit flag for a leaf carrying version timestamps for each tuple.
     */
    public static final short FLAG_VERSION_TIMESTAMPS = 1 << 1;

	/**
	 * Bit flag for a node or leaf on the RWStore using native int32 addresses
	 * for persistence.
	 * 
	 * FIXME Integrate this!  Also, make sure that we test with this mode of
	 * data storage in the coder test suite. 
	 */
    public static final short FLAG_RWSTORE_ADDRS = 1 << 2;

    /**
     * Marks a directory page as an overflow directory page (HTree).
     */
    public static final short FLAG_OVERFLOW_DIRECTORY = 1 << 3;

//    /**
//     * Bit flag indicating that the tuple revision timestamps have been written
//     * out using an array n-bit deltas computed as
//     * <code>maxTimestamp - minTimestamp</code>, where n is the number of bits
//     * required to code (maxTimestamp - minTimestamp). This is a relatively
//     * compact coding.
//     */
//    protected static final short DELTA_VERSION_TIMESTAMPS = 1 << 2;

//	/**
//	 * Bit flag indicating that the int32 hash of the key should be stored in
//	 * the leaf data record. The function used to compute hash code will be
//	 * known to the owning data structure. This is primarily intended for use
//	 * with hash trees.
//	 */
//    protected static final short FLAG_HASH_KEYS = 1 << 3;

    /**
     * Bit flag for a leaf carrying raw record bit flags.
     */
    public static final short FLAG_RAW_RECORDS = 1 << 3;
    
    /**
     * The size of the field in the data record which encodes whether the data
     * record represents a B+Tree {@link #NODE}, a {@link #LEAF}, or a
     * {@link #LINKED_LEAF}.
     */
    static public final int SIZEOF_TYPE = Bytes.SIZEOF_BYTE;

    /**
     * The size of the field in the data record which encodes the serialization
     * version of the data record. The offset of this field depends on whether
     * the record is a node or leaf versus a linked leaf. For a linked leaf, the
     * version information starts after the prior/next addr fields.  Otherwise
     * it starts immediately after the type field.
     */
    static public final int SIZEOF_VERSION = Bytes.SIZEOF_SHORT;

    /**
     * The size of the field in the data record which encodes bit flags for the
     * node or leaf.
     */
    static public final int SIZEOF_FLAGS = Bytes.SIZEOF_SHORT; // @todo byte

    /**
     * The size of the field in the data record which encodes the #of keys in
     * the node or leaf. The #of children in a node is always
     * <code>nkeys+1</code>.
     */
    static public final int SIZEOF_NKEYS = Bytes.SIZEOF_INT; // @todo short?

    /**
     * The size of the field in the data record which encodes the #of bytes in
     * the coded keys (or the coded values). This is an int32 because it is
     * possible that an int16 would limit the size of records for RMI.
     */
    static public final int SIZEOF_KEYS_SIZE = Bytes.SIZEOF_INT;

	// Note: This was int32 for VERSION0 and is now int64 (in principle) and a
	// variable length coding is used when it is written out so this field is
    // no longer a constant.
//    /**
//     * The size of the field in the data record which encodes the #of tuples
//     * spanned by a node.
//     */
//    static protected final int SIZEOF_ENTRY_COUNT = Bytes.SIZEOF_INT;

    /**
     * The size of a field in the data record which encodes the address of a
     * child node or leaf. TODO Handle int32 addresses natively with FLAG_RWSTORE_ADDRS
     */
    static public final int SIZEOF_ADDR = Bytes.SIZEOF_LONG;

    /**
     * The size of a field in the data record which encodes the revision
     * timestamp of a tuple in a leaf.
     */
    static public final int SIZEOF_TIMESTAMP = Bytes.SIZEOF_LONG;

    /**
     * The offset of the byte field which codes the type of the node or leaf.
     */
    static public final int O_TYPE = 0;

    /**
     * The offset of the long field which codes the address of the previous
     * leaf. This field IS NOT present unless the record type is
     * {@link AbstractReadOnlyNodeData#LINKED_LEAF}.
     */
    static public final int O_PRIOR = 0 + SIZEOF_TYPE;

    /**
     * The offset of the long field which codes the address of the next leaf.
     * This field IS NOT present unless the record type is
     * {@link AbstractReadOnlyNodeData#LINKED_LEAF}.
     */
    static public final int O_NEXT = 1 + SIZEOF_ADDR;

    /**
     * Core ctor. There are two basic use cases. One when you already have the
     * {@link ByteBuffer} with the encoded node or leaf data and one when you
     * have a mutable {@link Node} or {@link Leaf} and you want to persist it.
     * In the latter case, the derived class allocates a {@link ByteBuffer} and
     * encodes the data onto that buffer.
     */
    protected AbstractReadOnlyNodeData() {
        
    }

}
