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
import com.bigdata.btree.ILeafData;
import com.bigdata.btree.compression.HuffmanSerializer;
import com.bigdata.rawstore.Bytes;

/**
 * A read-only view of the data for a B+Tree leaf.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyLeafData extends AbstractReadOnlyNodeData<ILeafData>
        implements ILeafData {

    /** A read-only view of the backing {@link ByteBuffer}. */
    private final ByteBuffer b;
    
    // fields which are cached by the ctor.
    private final boolean doubleLinked;
    private final int nkeys;
    private final short flags;

    /**
     * Offset of the encoded timestamp[] in the buffer -or- <code>-1</code> if
     * the leaf does not report those data.
     */
    private final int O_timestamps;

    /**
     * Offset of the encoded delete markers in the buffer -or- <code>-1</code>
     * if the leaf does not report those data.
     */
    private final int O_deleteMarkers;

    /**
     * Offset of the encoded keys in the buffer.
     */
    private final int O_keys;
    
    /**
     * Offset of the encoded values in the buffer.
     */
    private final int O_values;

    public final ByteBuffer buf() {

        return b;
        
    }

    /**
     * Wrap a record containing the encoded data for a leaf.
     * 
     * @param b
     *            A buffer containing the leaf data.
     */
    protected ReadOnlyLeafData(final ByteBuffer b) {

        final byte type = b.get();

        switch (type) {
        case NODE:
            throw new AssertionError();
        case LEAF:
            doubleLinked = true;
            break;
        case LINKED_LEAF:
            doubleLinked = false;
            break;
        default:
            throw new AssertionError("type=" + type);
        }

        if (doubleLinked) {
            
            // skip over the prior/next addr.
            b.position(b.position() + SIZEOF_ADDR * 2);
            
        }
        
        final int version = b.getInt();
        switch (version) {
        case VERSION0:
            break;
        default:
            throw new AssertionError("version=" + version);
        }

        /*
         * @todo Cross check flags against the B+Tree when we wrap the record in
         * a Leaf.
         */
        flags = b.getShort();
        final boolean hasVersionTimestamps = ((flags & FLAG_VERSION_TIMESTAMPS) != 0);
        final boolean hasDeleteMarkers = ((flags & FLAG_DELETE_MARKERS) != 0);

        this.nkeys = b.getInt();

        final int keysSize = b.getInt();
        
        final int valuesSize = b.getInt();

        if (hasVersionTimestamps) {
            O_timestamps = b.position();
            b.position(b.position() + nkeys * SIZEOF_TIMESTAMP);
        } else {
            O_timestamps = -1;
        }

        if (hasDeleteMarkers) {
            O_deleteMarkers = O_timestamps + nkeys;// @todo pack 8 to the byte.
            b.position(b.position() + nkeys);// @todo pack 8 to the byte.
        } else {
            O_deleteMarkers = -1;
        }
        
        O_keys = b.position();
        b.position(b.position() + keysSize);

        O_values = b.position();
        b.position(b.position() + valuesSize);

        assert b.position() == b.limit();
        
        b.flip();
        
        this.b = b.asReadOnlyBuffer();

    }

    /**
     * Encode the leaf data onto a newly allocated buffer.
     * 
     * @param leaf
     *            The leaf data.
     * @param doubleLinked
     *            <code>true</code> to generate a data record with room for the
     *            priorAddr and nextAddr fields.
     */
    public ReadOnlyLeafData(final ILeafData leaf, final boolean doubleLinked) {

        // cache some fields.
        this.doubleLinked = doubleLinked;
        this.nkeys = leaf.getKeyCount();

        // encode the keys.
        final byte[] encodedKeys = encodeKeys(leaf);

        // encode the values.
        final byte[] encodedValues = encodeValues(leaf);

        // figure out how the size of the buffer (exact fit).
        final int capacity = //
                SIZEOF_TYPE + //
                (doubleLinked ? SIZEOF_ADDR * 2 : 0) + // priorAddr, nextAddr.
                SIZEOF_VERSION + // version
                SIZEOF_FLAGS + // flags
                SIZEOF_NKEYS + // nkeys
                Bytes.SIZEOF_INT + // keysSize
                Bytes.SIZEOF_INT + // valuesSize
                (leaf.hasVersionTimestamps() ? SIZEOF_TIMESTAMP * nkeys : 0) + //
                (leaf.hasDeleteMarkers() ? nkeys : 0) + // @todo bit code 8 to
                                                        // the byte
                encodedKeys.length + // keys
                encodedValues.length // values
        ;
        
        final ByteBuffer b = ByteBuffer.allocate(capacity);

        b.put((byte) (doubleLinked ? LINKED_LEAF : LEAF));
        
        if(doubleLinked) {

            /*
             * Skip over priorAddr/nextAddr fields (java will have zeroed the
             * entire buffer when we allocated it). These fields need to be
             * filled in on the record after it has been serialized (and
             * potentially after it has been compressed) so we know its space on
             * disk requirements.
             */

            b.position(b.position() + SIZEOF_ADDR * 2);
            
        }

        b.putShort(VERSION0);
        
        short flags = 0;
        if (leaf.hasDeleteMarkers()) {
            flags |= FLAG_DELETE_MARKERS;
        }
        if (leaf.hasVersionTimestamps()) {
            flags |= FLAG_VERSION_TIMESTAMPS;
        }
        this.flags = flags;

        b.putShort(flags);
        
        b.putInt(nkeys);

        b.putInt(encodedKeys.length); // keysSize
        
        b.putInt(encodedValues.length); // valuesSize
        
        // timestamps
        if (leaf.hasVersionTimestamps()) {

            O_timestamps = b.position();
            
            for (int i = 0; i < nkeys; i++) {

                b.putLong(leaf.getVersionTimestamp(i));

            }

        } else {
        
            O_timestamps = -1;
            
        }

        // delete markers @todo bit code 8 markers to the byte.
        if (leaf.hasDeleteMarkers()) {

            O_deleteMarkers = b.position();
            
            for (int i = 0; i < nkeys; i++) {

                b.put((byte) (leaf.getDeleteMarker(i) ? 1 : 0));

            }

        } else {
        
            O_deleteMarkers = -1;
            
        }
        
        // write the encoded keys on the buffer.
        O_keys = b.position();
        b.put(encodedKeys);

        // write the encoded values on the buffer.
        O_values = b.position();
        b.put(encodedValues);

        // prepare buffer for writing on the store [limit := pos; pos : =0] 
        b.flip();
        
        // save read-only reference to the buffer.
        this.b = b.asReadOnlyBuffer();
        
    }
    
    /**
     * Always returns <code>true</code>.
     */
    final public boolean isLeaf() {

        return true;

    }

    /**
     * Return <code>true</code> if the leaf encodes the address or the prior and
     * next leaves.
     */
    public final boolean isDoubleLinked() {

        return doubleLinked;

    }

    /**
     * {@inheritDoc}. This field is cached.
     */
    final public int getKeyCount() {

        return nkeys;

    }

    /**
     * For a leaf the #of entries is always the #of keys.
     */
    final public int getEntryCount() {
        
        return nkeys;
        
    }

    /**
     * Encode the values into a byte[].
     * 
     * @return The encoded values.
     * 
     * @see HuffmanSerializer
     * 
     *      FIXME implement at least huffman encoding for the values.
     */
    protected byte[] encodeValues(final IAbstractNodeData data) {

        throw new UnsupportedOperationException();
        
    }
    
    final public int getValueCount() {
        
        return nkeys;
        
    }
    
    public boolean hasVersionTimestamps() {
        
        return (flags & FLAG_VERSION_TIMESTAMPS) != 0;
        
    }

    public boolean hasDeleteMarkers() {

        return (flags & FLAG_DELETE_MARKERS) != 0;
        
    }

    public long getVersionTimestamp(final int index) {

        if (!hasVersionTimestamps())
            throw new UnsupportedOperationException();

        return b.getLong(O_timestamps + index * SIZEOF_TIMESTAMP);

    }

    public boolean getDeleteMarker(final int index) {

        if (!hasDeleteMarkers())
            throw new UnsupportedOperationException();

        // @todo pack 8 to the byte.
        return b.get(O_timestamps + index) == 1;
        
    }

    public void copyValue(int index, OutputStream os) {
        // TODO Auto-generated method stub
        
    }

    public byte[][] getValues() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isNull(int index) {
        // TODO Auto-generated method stub
        return false;
    }

}
