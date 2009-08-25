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

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.btree.raba.codec.IRabaDecoder;
import com.bigdata.rawstore.Bytes;

/**
 * A read-only view of the data for a B+Tree leaf based on a compact record
 * format. While some fields are cached, for the most part the various data
 * fields, including the keys and values, are accessed in place in the data
 * record in order to minimize the memory footprint of the leaf. The keys and
 * values are coded using a caller specified {@link IRabaCoder}. The specific
 * coding scheme is specified by the {@link IndexMetadata} for the B+Tree
 * instance and is not stored within the leaf data record.
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
    private final IRaba keys;
    private final IRaba vals; 

    /**
     * Offset of the encoded timestamp[] in the buffer -or- <code>-1</code> if
     * the leaf does not report those data.
     */
    private final int O_versionTimestamps;

    /**
     * Offset of the bit flags in the buffer encoding the presence of deleted
     * tuples -or- <code>-1</code> if the leaf does not report those data.
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
    protected ReadOnlyLeafData(final ByteBuffer b, final IRabaCoder keysCoder,
            final IRabaCoder valuesCoder) {

        if (b == null)
            throw new IllegalArgumentException();

        if (keysCoder == null)
            throw new IllegalArgumentException();
        
        if (valuesCoder == null)
            throw new IllegalArgumentException();

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

        // version timestamps
        if (hasVersionTimestamps) {

            O_versionTimestamps = b.position();
            
            // advance past the timestamps.
            b.position(b.position() + nkeys * SIZEOF_TIMESTAMP);
            
        } else {
            
            O_versionTimestamps = -1;
            
        }

        // delete markers
        if (hasDeleteMarkers) {
            
            O_deleteMarkers = b.position();

            // advance past the bit flags.
            b.position(b.position() + BytesUtil.bitFlagByteLength(nkeys));// bit coded.
            
        } else {
            
            O_deleteMarkers = -1;
            
        }

        // keys
        O_keys = b.position();
        b.limit(b.position() + keysSize);
        this.keys = keysCoder.decode(b.slice());
        assert b.position() == O_keys + keysSize;

        // values
        O_values = b.position();
        b.limit(b.position() + valuesSize);
        this.vals = valuesCoder.decode(b.slice());
        assert b.position() == O_values + valuesSize;

        assert b.position() == b.limit();
        
        // flip [limit=pos; pos=0].
        b.flip();

        // save reference to buffer
        this.b = (b.isReadOnly() ? b : b.asReadOnlyBuffer());

    }

    /**
     * Encode the leaf data onto a newly allocated buffer.
     * 
     * @param leaf
     *            The leaf data.
     * @param keysCoder
     *            The object which will be used to code the keys into the
     *            record.
     * @param valuesCoder
     *            The object which will be used to code the values into the
     *            record.
     * @param doubleLinked
     *            <code>true</code> to generate a data record with room for the
     *            priorAddr and nextAddr fields.
     */
    public ReadOnlyLeafData(final ILeafData leaf, final IRabaCoder keysCoder,
            final IRabaCoder valuesCoder, final boolean doubleLinked) {

        if (leaf == null)
            throw new IllegalArgumentException();

        if (keysCoder == null)
            throw new IllegalArgumentException();
        
        if (valuesCoder == null)
            throw new IllegalArgumentException();

        // cache some fields.
        this.doubleLinked = doubleLinked;
        this.nkeys = leaf.getKeyCount();

        // encode the keys.
        this.keys = keysCoder.encode(leaf.getKeys());
        final ByteBuffer encodedKeys = ((IRabaDecoder) keys).data();

        // encode the values.
        this.vals = valuesCoder.encode(leaf.getValues());
        final ByteBuffer encodedValues = ((IRabaDecoder) vals).data();

        // figure out how the size of the buffer (exact fit).
        final int capacity = //
                SIZEOF_TYPE + //
                (doubleLinked ? SIZEOF_ADDR * 2 : 0) + // priorAddr, nextAddr.
                SIZEOF_VERSION + // version
                SIZEOF_FLAGS + // flags
                SIZEOF_NKEYS + // nkeys
                Bytes.SIZEOF_INT + // keysSize
                Bytes.SIZEOF_INT + // valuesSize
                BytesUtil.bitFlagByteLength(nkeys)+// nulls
                (leaf.hasVersionTimestamps() ? SIZEOF_TIMESTAMP * nkeys : 0) + //
                (leaf.hasDeleteMarkers() ? BytesUtil.bitFlagByteLength(nkeys) : 0) + // deleted
                encodedKeys.capacity() + // keys
                encodedValues.capacity() // values
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

        b.putInt(encodedKeys.capacity()); // keysSize
        
        b.putInt(encodedValues.capacity()); // valuesSize
        
        // version timestamps
        if (leaf.hasVersionTimestamps()) {

            O_versionTimestamps = b.position();
            
            for (int i = 0; i < nkeys; i++) {

                b.putLong(leaf.getVersionTimestamp(i));

            }

        } else {
        
            O_versionTimestamps = -1;
            
        }

        // delete markers (bit coded).
        if (leaf.hasDeleteMarkers()) {

            O_deleteMarkers = b.position();

            for (int i = 0; i < nkeys;) {

                byte bits = 0;
                
                for (int j = 0; j < 8 && i < nkeys; j++, i++) {

                    if(leaf.getDeleteMarker(i)) {

                        bits |= 1 << j;
                            
                    }
                    
                }

                b.put(bits);

            }

        } else {
        
            O_deleteMarkers = -1;
            
        }
        
        // write the encoded keys on the buffer.
        O_keys = b.position();
        encodedKeys.limit(encodedKeys.capacity());
        encodedKeys.rewind();
        b.put(encodedKeys);

        // write the encoded values on the buffer.
        O_values = b.position();
        encodedValues.limit(encodedValues.capacity());
        encodedValues.rewind();
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
     * Yes.
     */
    final public boolean isReadOnly() {
        
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
     * For a leaf the #of tuples is always the #of keys.
     */
    final public int getSpannedTupleCount() {
        
        return nkeys;
        
    }

    /**
     * For a leaf, the #of values is always the #of keys.
     */
    final public int getValueCount() {
        
        return nkeys;
        
    }
    
    final public boolean hasVersionTimestamps() {
        
        return (flags & FLAG_VERSION_TIMESTAMPS) != 0;
        
    }

    final public boolean hasDeleteMarkers() {

        return (flags & FLAG_DELETE_MARKERS) != 0;
        
    }

    final public long getVersionTimestamp(final int index) {

        if (!hasVersionTimestamps())
            throw new UnsupportedOperationException();

        return b.getLong(O_versionTimestamps + index * SIZEOF_TIMESTAMP);

    }

    final public boolean getDeleteMarker(final int index) {

        if (!hasDeleteMarkers())
            throw new UnsupportedOperationException();

        return BytesUtil.getBit(buf(), O_deleteMarkers, index);

    }
    
    final public IRaba getKeys() {
        
        return keys;
        
    }

    final public IRaba getValues() {

        return vals;
        
    }

//    final public void copyValue(final int index, final OutputStream os) {
//
//        vals.copy(index, os);
//        
//    }
//
//    final public boolean isNull(final int index) {
//
//        return vals.isNull(index);
//
//    }

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getName() + "{");

        ReadOnlyLeafData.toString(this, sb);

        sb.append("}");
        
        return sb.toString();
        
    }

    /**
     * Utility method formats the {@link ILeafData}.
     * 
     * @param leaf
     *            A leaf data record.
     * @param sb
     *            The representation will be written onto this object.
     * 
     * @return The <i>sb</i> parameter.
     * 
     * @todo prior/next links (not in the API).
     */
    static public StringBuilder toString(final ILeafData leaf,
            final StringBuilder sb) {

        final int nkeys = leaf.getKeyCount();

        sb.append(", keys=" + leaf.getKeys());
        
        sb.append(", vals=" + leaf.getValues());

        if (leaf.hasDeleteMarkers()) {

            sb.append(", deleteMarkers=[");

            for (int i = 0; i < nkeys; i++) {

                if (i > 0)
                    sb.append(", ");

                sb.append(leaf.getDeleteMarker(i));

            }

            sb.append("]");

        }

        if (leaf.hasVersionTimestamps()) {

            sb.append(", versionTimestamps=[");

            for (int i = 0; i < nkeys; i++) {

                if (i > 0)
                    sb.append(", ");

                // sb.append(new Date(leaf.getVersionTimestamp(i)).toString());
                sb.append(leaf.getVersionTimestamp(i));

            }

            sb.append("]");

        }

        return sb;

    }

}
