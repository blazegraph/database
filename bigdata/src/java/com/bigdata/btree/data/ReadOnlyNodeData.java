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

import java.nio.ByteBuffer;

import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.btree.raba.codec.IRabaDecoder;
import com.bigdata.rawstore.Bytes;

/**
 * A read-only view of the data for a B+Tree node.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyNodeData extends AbstractReadOnlyNodeData<INodeData>
        implements INodeData {
    
    /** A read-only view of the backing {@link ByteBuffer}. */
    private final ByteBuffer b;
    
    // fields which are cached by the ctor.
    private final int nkeys;
    private final int nentries;

    /**
     * Offset of the encoded childAddr[] in the buffer.
     */
    private final int O_childAddr;
    
    /**
     * Offset of the encoded childEntryCount[] in the buffer.
     */
    private final int O_childEntryCount;

    /**
     * Offset of the encoded keys in the buffer.
     */
    private final int O_keys;

    private final IRaba keys;

    public final ByteBuffer buf() {

        return b;
        
    }
    
    /**
     * Constructor wraps a buffer containing an encoded node data record.
     * 
     * @param b
     *            The buffer containing the data for the node.
     */
    public ReadOnlyNodeData(final ByteBuffer b, final IRabaCoder keysCoder) {

        if (b == null)
            throw new IllegalArgumentException();

        if (keysCoder == null)
            throw new IllegalArgumentException();

        final byte type = b.get();

        switch (type) {
        case NODE:
            break;
        case LEAF:
            throw new AssertionError();
        case LINKED_LEAF:
            throw new AssertionError();
        default:
            throw new AssertionError("type=" + type);
        }

        final int version = b.getInt();
        switch (version) {
        case VERSION0:
            break;
        default:
            throw new AssertionError("version=" + version);
        }
        
        // skip over flags (they are unused for a node).
        b.getShort();
        
        this.nkeys = b.getInt();
        
        this.nentries = b.getInt();
        
        final int keysSize = b.getInt();
        
        O_childAddr = b.position();
        
        O_childEntryCount = O_childAddr + (nkeys + 1) * SIZEOF_ADDR;

        O_keys = O_childEntryCount + (nkeys + 1) * SIZEOF_ENTRY_COUNT;
        b.position(O_keys);
        b.limit(b.position() + keysSize);
        this.keys = keysCoder.decode(b.slice());
        assert b.position() == O_keys + keysSize;
        
        // save reference to buffer
        this.b = (b.isReadOnly() ? b : b.asReadOnlyBuffer());

    }

    /**
     * Serialize the node onto a newly allocated buffer.
     * 
     * @param data
     *            The data to be encoded.
     */
    public ReadOnlyNodeData(final INodeData node, final IRabaCoder keysCoder) {

        if (node == null)
            throw new IllegalArgumentException();

        if (keysCoder == null)
            throw new IllegalArgumentException();

        // cache some fields.
        this.nkeys = node.getKeyCount();
        this.nentries = node.getSpannedTupleCount();

        // encode the keys.
        this.keys = keysCoder.encode(node.getKeys());
        final ByteBuffer encodedKeys = ((IRabaDecoder) keys).data();

        // figure out how the size of the buffer (exact fit).
        final int capacity = //
                SIZEOF_TYPE + //
                SIZEOF_VERSION + //
                SIZEOF_FLAGS + //
                SIZEOF_NKEYS + //
                SIZEOF_ENTRY_COUNT + //
                Bytes.SIZEOF_INT + // keysSize
                SIZEOF_ADDR * (nkeys + 1) + // childAddr[]
                SIZEOF_ENTRY_COUNT * (nkeys + 1) + // childEntryCount[]
                encodedKeys.capacity() // keys
        ;
        
        final ByteBuffer b = ByteBuffer.allocate(capacity);

        b.put(NODE);

        b.putShort(VERSION0);
        
        b.putShort((short) 0/* flags */);
        
        b.putInt(nkeys);
        
        b.putInt(nentries);

        b.putInt(encodedKeys.capacity()); // keySize
        
        // childAddr[]
        O_childAddr = b.position();
        for (int i = 0; i <= nkeys; i++) {
            
            b.putLong(node.getChildAddr(i));
            
        }
        
        // childEntryCount[]
        O_childEntryCount = b.position();
        for (int i = 0; i <= nkeys; i++) {
            
            b.putInt(node.getChildEntryCount(i));
            
        }
        
        // write the encoded keys on the buffer.
        O_keys = b.position();
        encodedKeys.limit(encodedKeys.capacity());
        encodedKeys.rewind();
        b.put(encodedKeys);

        assert b.position() == b.limit();
        
        // prepare buffer for writing on the store [limit := pos; pos : =0] 
        b.flip();
        
        // save read-only reference to the buffer.
        this.b = b.asReadOnlyBuffer();
        
    }

    /**
     * Always returns <code>false</code>.
     */
    final public boolean isLeaf() {

        return false;

    }

    /**
     * Yes.
     */
    final public boolean isReadOnly() {
        
        return true;
        
    }
    
    /**
     * {@inheritDoc}. This field is cached.
     */
    final public int getKeyCount() {
        
        return nkeys;
        
    }

    /**
     * {@inheritDoc}. This field is cached.
     */
    final public int getChildCount() {
        
        return nkeys + 1;
        
    }

    /**
     * {@inheritDoc}. This field is cached.
     */
    final public int getSpannedTupleCount() {
        
        return nentries;
        
    }

    /**
     * Bounds check.
     * 
     * @throws IndexOutOfBoundsException
     *             if <i>index</i> is LT ZERO (0)
     * @throws IndexOutOfBoundsException
     *             if <i>index</i> is GE <i>nkeys</i>
     */
    protected boolean assertChildIndex(final int index) {
        
        if (index < 0 || index > nkeys)
            throw new IndexOutOfBoundsException();
        
        return true;
        
    }

    final public long getChildAddr(final int index) {

        assert assertChildIndex(index);

        return b.getLong(O_childAddr + index * SIZEOF_ADDR);

    }

    final public int getChildEntryCount(final int index) {

        assert assertChildIndex(index);

        return b.getInt(O_childEntryCount + index * SIZEOF_ENTRY_COUNT);

    }

    final public IRaba getKeys() {

        return keys;

    }

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getName() + "{");

        ReadOnlyNodeData.toString(this, sb);

        sb.append("}");
        
        return sb.toString();
        
    }

    /**
     * Utility method formats the {@link INodeData}.
     * 
     * @param node
     *            A node data record.
     * @param sb
     *            The representation will be written onto this object.
     * 
     * @return The <i>sb</i> parameter.
     */
    static public StringBuilder toString(final INodeData node,
            final StringBuilder sb) {

        final int nchildren = node.getChildCount();

        sb.append(", nchildren=" + nchildren);

        sb.append(", spannedTupleCount=" + node.getSpannedTupleCount());

        sb.append(", keys=" + node.getKeys());

        {

            sb.append(", childAddr=[");

            for (int i = 0; i < nchildren; i++) {

                if (i > 0)
                    sb.append(", ");

                sb.append(node.getChildAddr(i));

            }

            sb.append("]");

        }

        {

            sb.append(", childEntryCount=[");

            for (int i = 0; i < nchildren; i++) {

                if (i > 0)
                    sb.append(", ");

                sb.append(node.getChildEntryCount(i));

            }

            sb.append("]");

        }

        return sb;

    }

}
