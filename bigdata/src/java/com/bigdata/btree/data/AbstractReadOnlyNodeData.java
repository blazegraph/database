/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 5, 2009
 */

package com.bigdata.btree.data;

import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.bigdata.btree.IAbstractNodeData;
import com.bigdata.btree.IKeyBuffer;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.Leaf;
import com.bigdata.btree.Node;
import com.bigdata.btree.compression.HuffmanSerializer;
import com.bigdata.rawstore.Bytes;

/**
 * Abstract base class for a read-only view of the data for B+Tree node or leaf.
 * The data are stored in a {@link ByteBuffer}. Access to the keys, values and
 * other metadata are via operations on that {@link ByteBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractReadOnlyNodeData<U extends IAbstractNodeData>
        implements IAbstractNodeData {

    /**
     * A B+Tree node data record.
     */
    protected static final byte NODE = 0;
    
    /**
     * A B+Tree leaf data record.
     */
    protected static final byte LEAF = 1;

    /**
     * A B+Tree leaf data record with with priorAddr and nextAddr fields. This
     * is used for the leaves in an {@link IndexSegment}.
     */
    protected static final byte LINKED_LEAF = 2;

    /**
     * Version zero is the only version defined at this time.
     */
    protected static final short VERSION0 = 0;

    /**
     * Bit flag for a leaf carrying delete markers for each tuple.
     */
    protected static final short FLAG_DELETE_MARKERS = 1 << 0;

    /**
     * Bit flag for a leaf carrying version timestamps for each tuple.
     */
    protected static final short FLAG_VERSION_TIMESTAMPS = 1 << 1;

    /**
     * The size of the field in the data record which encodes whether the data
     * record represents a B+Tree {@link #NODE}, a {@link #LEAF}, or a
     * {@link #LINKED_LEAF}.
     */
    static protected final int SIZEOF_TYPE = Bytes.SIZEOF_BYTE;

    /**
     * The size of the field in the data record which encodes the serialization
     * version of the data record.
     */
    static protected final int SIZEOF_VERSION = Bytes.SIZEOF_SHORT;

    /**
     * The size of the field in the data record which encodes bit flags for the
     * node or leaf.
     */
    static protected final int SIZEOF_FLAGS = Bytes.SIZEOF_SHORT; // @todo byte

    /**
     * The size of the field in the data record which encodes the #of keys in
     * the node or leaf. The #of children in a node is always
     * <code>nkeys+1</code>.
     */
    static protected final int SIZEOF_NKEYS = Bytes.SIZEOF_INT; // @todo short?

    /**
     * The size of the field in the data record which encodes the #of tuples
     * spanned by a node.
     */
    static protected final int SIZEOF_ENTRY_COUNT = Bytes.SIZEOF_INT;

    /**
     * The size of a field in the data record which encodes the address of a
     * child node or leaf.
     */
    static protected final int SIZEOF_ADDR = Bytes.SIZEOF_LONG;

    /**
     * The size of a field in the data record which encodes the revision
     * timestamp of a tuple in a leaf.
     */
    static protected final int SIZEOF_TIMESTAMP = Bytes.SIZEOF_LONG;

    /**
     * Return a read-only view of the backing {@link ByteBuffer}.
     */
    abstract public ByteBuffer buf();

    /**
     * Core ctor. There are two basic use cases. One when you already have the
     * {@link ByteBuffer} with the encoded node or leaf data and one when you
     * have a mutable {@link Node} or {@link Leaf} and you want to persist it.
     * In the latter case, the derived class allocates a {@link ByteBuffer} and
     * encodes the data onto that buffer.
     */
    protected AbstractReadOnlyNodeData() {
        
    }

    /**
     * Encode the keys into a byte[].
     * 
     * @return The encoded keys.
     * 
     * @see HuffmanSerializer
     * 
     *      FIXME implement at least hu-tucker encoding for the keys.
     */
    protected byte[] encodeKeys(final IAbstractNodeData data) {

        throw new UnsupportedOperationException();
        
    }
    
    /**
     * @todo consider writing an IKeyBuffer for each supported key encoding
     *       (Hu-Tucker principally). That would let us plug in different key
     *       encoding schemes. However, the binary image of the keys in the
     *       buffer still needs to obey the general constraints for the data
     *       recorded as encoded in the buffer (byte aligned, random access).
     */
    public IKeyBuffer getKeys() {
        // TODO Auto-generated method stub
        return null;
    }

    public void copyKey(final int index, final OutputStream os) {
        // TODO Auto-generated method stub
        
    }

    public byte[] getKey(final int index) {
        // TODO Auto-generated method stub
        return null;
    }
    
}
