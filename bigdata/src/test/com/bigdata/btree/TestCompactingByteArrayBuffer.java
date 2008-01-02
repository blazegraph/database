/*

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
 * Created on Dec 23, 2007
 */

package com.bigdata.btree;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.ibm.icu.impl.ByteBuffer;

import junit.framework.TestCase;

/**
 * Test harness used to develop a compacting buffer for maintaining branch nodes
 * and leaves in a B+Tree that minimizes copying on mutation of the node, helps
 * to minimize heap churn and GC latency related to long lived allocations, and
 * maintains the data in a serializable format.
 * <p>
 * The basic data model is a managed byte[] on which we can write. Random
 * updates in the array are allowed and variable length data are simply appended
 * onto the end of the array. The array will grow if necessary, in which case
 * the data are copied onto a new byte[]. The copy is a compacting operation,
 * similar to GC, in which only the "live" bytes are copied forward. Compacting
 * restores the sort order of the keys. During mutations, the sort order is
 * maintained by an indirection vector having the offset and length of the
 * current location for each key. The order of the indirection vector is
 * maintained by an insertion sort.
 * 
 * @todo The base class should be shared with {@link DataOutputBuffer}, should
 *       NOT throw IOException, should be extended to provide both random
 *       read/write, and should be extended for node/leaf data structures and
 *       their compaction semantics. The same class can support branch nodes and
 *       leaves.
 * 
 * @todo representation is either directly serializable or fast to serialize and
 *       de-serialize. note that serialization onto a {@link DataOutputBuffer}
 *       tends to be cheap while de-serialization tends to be much more
 *       expensive, in part because there are more allocations.
 * 
 * @todo reuse buffers for a btree.
 *       <p>
 *       note that the disk write cache causes copying (to prevent the data from
 *       being changed if the write cache is flushed). however if the btree
 *       copies the data from the {@link ByteBuffer} then the byte[]s backing
 *       the {@link ByteBuffer} will be short lived allocations (in the
 *       nursery). the option is to let the caller pass in a buffer of
 *       sufficient size and to let the caller decode the record length and keep
 *       track of the #of valid bytes in the returned buffer.
 * 
 * @todo minimize cost of mutations (insert/update/remove).
 *       <p>
 *       Note that adaptive packed memory arrays also seek to minimize the cost
 *       of mutations. They leave "holes" in the data such that the cost of
 *       mutations on a large set of ordered items (in the millions) may be
 *       minimized. Periodically new holes are created to retain a balance of
 *       the distribution of holes vs the #of entries in the array. Compact
 *       serialization would naturally copy the data into a dense form.
 *       De-serialization could lazily restore the holes on the first mutation.
 * 
 * @todo minimize cost of mutations when the prefix length changes. it will be
 *       relatively expensive to re-factor the keys to isolate the longest
 *       common prefix. the prefix may be determined by comparing the 1st key
 *       and either the last key or the separator key for the right sibling (if
 *       available). none of these options are very stable as mutations on the
 *       node or its right sibling could change the first key, the last key, and
 *       the separator for the right sibling.
 *       <p>
 *       Explore cases where the prefix length changes and see if we can handle
 *       deferred growth of the prefix. This should be possible if we track both
 *       the prefix offset and length since we can just reduce the prefix length
 *       and then we will just compare more bytes than are abolutely required in
 *       each of the remainder keys. Likewise, examine cases where the prefix
 *       length shrinks and see if we can minimize the cost of the mutation by
 *       only changing some of the keys and their offsets. Finally, are there
 *       cases where the prefix bytes change with or without a change in length
 *       and how does that get handled.
 * 
 * @todo support dictionary-based compression for keys and values.
 *       <p>
 *       ordered preserving compression may be used for keys in their
 *       de-serialized state, reducing the #of bytes in the key. special
 *       comparison logic is required since a probe key containing symbols not
 *       in the code dictionary can not be mapped into a compressed key,
 *       resulting in an insertion point vs a retrieval index.
 *       <p>
 *       Hu-tucker compression is not static, meaning that each node or leaf
 *       will have its own order preserving compression. In this case the key
 *       must be fully materialized when extracting separator keys for the
 *       parent. (If the order preserving compression is static then we do not
 *       need to decode before extracting a key and any key can be mapped in.)
 *       <p>
 *       The compression algorithms should be definable by the application. This
 *       makes sense even for order preserving compression of the keys where we
 *       expect to use hu-tucker or a variant since the choice of the alphabet
 *       can vary by application (byte vs int vs long) and since people may then
 *       experiment with other order-preserving compression techniques.
 *       <p>
 *       application defined non-order preserving compression may be used for
 *       keys and values. for keys, we have to de-compress during
 *       de-serialization since we need to compare the uncompressed keys in
 *       order to have the order semantics. for values, we do not need to
 *       decompress until we deliver the value to the application. an example of
 *       the use of non-order preserving compression for keys would be the
 *       long[3] keys in an RDF statement index. When the buffer is used to
 *       promote serialization the keys can be compressed using a non-order
 *       preserving technique that assigns bit codes to values in inverse
 *       frequency (in fact, this is quite similar to hu-tucker). Note that
 *       order preserving compression is not meaningful for the on the wire
 *       format between a client and a data service since the latter will need
 *       to use the uncompressed keys and the keys will then be recoded using a
 *       local order preserving technique if that is supported by the index
 *       partition.
 * 
 * @todo support micro indexing? this is where a branch structure is represented
 *       over the keys within the node or leaf to minimize the costs of the
 *       binary search. whether this is efficient or not depends on the behavior
 *       of the code with respect to cache lines.
 * 
 * @todo reserve a few bits for a code indicating which of the supported
 *       alternative representations is in effect.
 * 
 * @todo store a field which is the pre-compression size when the keys are
 *       compressed or perhaps the target "mutable" size if the buffer is to
 *       undergo mutations. this will let us choose a suitable buffer size when
 *       "de-serializing".
 * 
 * @todo support a btree specific cache of buffers for nodes (and one for
 *       leaves). the target buffer size is estimated based on the maximum
 *       accepted mutable size before we force compaction and is never less than
 *       the largest compact node/leaf size that we have seen. There is slop in
 *       this estimate and the upper value for the capacity is bounded by how
 *       many mutations we permit before forcing compaction while the lower
 *       value for the capacity is bounded by the actual size of de-serialized
 *       records. In a scenario in which mutation does not occur the capacity
 *       should remain bounded by the actual record sizes.  It's ok for us to 
 *       scan the pool N references looking for a suitable buffer and then we
 *       start to discard buffers that are too small and update the capacity
 *       bound.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCompactingByteArrayBuffer extends TestCase {

    /**
     * 
     */
    public TestCompactingByteArrayBuffer() {
    }

    /**
     * @param arg0
     */
    public TestCompactingByteArrayBuffer(String arg0) {
        super(arg0);
    }

    /**
     * Trial balloon for node/leaf data structure using a mutable buffer and a
     * compacting GC.
     * 
     * @todo also implement {@link INodeData} or have two concrete classes of
     *       the same {@link CompactingByteBuffer} base class that exposes the
     *       node vs leaf data interfaces.
     * 
     * @todo reconcile with the {@link NodeSerializer}. remove the use of the
     *       {@link IValueSerializer} and only support serialization of byte[]
     *       values (and version counters when the index is unisolated).
     *       <p>
     *       Should the btree api automatically (de-)serialize values using a
     *       local extSer data structure or should strong typing of values be
     *       required or should people use a utility object to wrap a btree and
     *       provide key/val encoding and decoding?
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class CompactingByteBuffer extends ByteArrayBuffer implements ILeafData {

        /**
         * The serialization version.
         */
        private static transient int SIZEOF_VERSION = Bytes.SIZEOF_BYTE;
        /**
         * A set of bit flags.
         * 
         * @todo if more than 8 bits are required then look into generalized bit
         *       stream support or just read the data as a short, int or long and
         *       then do the bit stuff on that value.
         */
        private static transient int SIZEOF_FLAGS = Bytes.SIZEOF_BYTE;
        /**
         * The branching factor (m).
         */
        private static transient int SIZEOF_BRANCHING_FACTOR = Bytes.SIZEOF_SHORT;
        /**
         * The #of keys.
         */
        private static transient int SIZEOF_NKEYS = Bytes.SIZEOF_SHORT;

        private static transient int OFFSET_VERSION = 0x0;
        private static transient int OFFSET_FLAGS = OFFSET_VERSION + SIZEOF_VERSION; 
        private static transient int OFFSET_BRANCHING_FACTOR = OFFSET_FLAGS + SIZEOF_BRANCHING_FACTOR;
        private static transient int OFFSET_NKEYS = OFFSET_BRANCHING_FACTOR + SIZEOF_NKEYS;

        /**
         * Mask for flags revealing the bit whose value is ONE (1) iff the record
         * represents a leaf (otherwise it represents a node).
         */
        private static transient int MASK_IS_LEAF = 0x01;
        
        /*
         * @todo fields m (branchingFactor), isLeaf, nkeys(aka nvals), keys, vals.
         */
        
        public boolean isLeaf() {
            
            return (buf[OFFSET_FLAGS] & MASK_IS_LEAF) == 1;
            
        }
        
        public int getBranchingFactor() {

            return 0;
            
        }

        public int getEntryCount() {
            // TODO Auto-generated method stub
            return 0;
        }

        public int getKeyCount() {
            // TODO Auto-generated method stub
            return 0;
        }

        public IKeyBuffer getKeys() {
            // TODO Auto-generated method stub
            return null;
        }

        public int getValueCount() {
            // TODO Auto-generated method stub
            return 0;
        }

        public Object[] getValues() {
            // TODO Auto-generated method stub
            return null;
        }

    }
    
}
