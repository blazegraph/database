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
 * Created on Aug 28, 2009
 */

package com.bigdata.btree.data;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;

/**
 * Default implementation for immutable {@link ILeafData} records.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultLeafCoder implements IAbstractNodeCoder<ILeafData> {

    protected static final Logger log = Logger
            .getLogger(DefaultLeafCoder.class);

    protected static final byte VERSION0 = 0x00;
    
    private IRabaCoder keysCoder;
    private IRabaCoder valsCoder;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final byte version = in.readByte();
        switch(version) {
        case VERSION0:
            break;
        default:
            throw new IOException();
        }

        keysCoder = (IRabaCoder) in.readObject();
        
        valsCoder = (IRabaCoder) in.readObject();
        
    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        out.write(VERSION0);
        
        out.writeObject(keysCoder);
        
        out.writeObject(valsCoder);
        
    }

    /** Yes. */
    final public boolean isLeafCoder() {
        
        return true;
        
    }

    /** No. */
    public boolean isNodeCoder() {

        return false;
        
    }

    public String toString() {

        return super.toString() + "{keysCoder=" + keysCoder + ", valsCoder="
                + valsCoder + "}";

    }

    /**
     * 
     * @param keysCoder
     *            The {@link IRabaCoder} for the leaf's keys.
     * @param valsCoder
     *            The {@link IRabaCoder} for the leaf's values.
     */
    public DefaultLeafCoder(final IRabaCoder keysCoder,
            final IRabaCoder valsCoder) {

        if (keysCoder == null)
            throw new IllegalArgumentException();

        if (valsCoder == null)
            throw new IllegalArgumentException();

        this.keysCoder = keysCoder;

        this.valsCoder = valsCoder;

    }

    public ILeafData decode(final AbstractFixedByteArrayBuffer data) {

        return new ReadOnlyLeafData(data, keysCoder, valsCoder);

    }

    public AbstractFixedByteArrayBuffer encode(final ILeafData leaf,
            final DataOutputBuffer buf) {
        
        if (leaf == null)
            throw new IllegalArgumentException();

        if (buf == null)
            throw new IllegalArgumentException();

        // cache some fields.
//        this.doubleLinked = doubleLinked;
        final int nkeys = leaf.getKeyCount();

//        // figure out how the size of the buffer (exact fit).
//        final int capacity = //
//                SIZEOF_TYPE + //
//                (doubleLinked ? SIZEOF_ADDR * 2 : 0) + // priorAddr, nextAddr.
//                SIZEOF_VERSION + // version
//                SIZEOF_FLAGS + // flags
//                SIZEOF_NKEYS + // nkeys
//                Bytes.SIZEOF_INT + // keysSize
//                Bytes.SIZEOF_INT + // valuesSize
//                BytesUtil.bitFlagByteLength(nkeys)+// nulls
//                (leaf.hasVersionTimestamps() ? SIZEOF_TIMESTAMP * nkeys : 0) + //
//                (leaf.hasDeleteMarkers() ? BytesUtil.bitFlagByteLength(nkeys) : 0) + // deleted
//                encodedKeys.len() + // keys
//                encodedValues.len() // values
//        ;

        // The byte offset of the start of the coded record into the buffer.
        final int O_origin = buf.pos();
        
//        final ByteBuffer b = ByteBuffer.allocate(capacity);
//        buf.ensureCapacity(capacity);

        final boolean doubleLinked = leaf.isDoubleLinked();
        buf.putByte((byte) (doubleLinked ? AbstractReadOnlyNodeData.LINKED_LEAF
                : AbstractReadOnlyNodeData.LEAF));

        if(doubleLinked) {

            /*
             * Skip over priorAddr/nextAddr fields (java will have zeroed the
             * entire buffer when we allocated it). These fields need to be
             * filled in on the record after it has been serialized (and
             * potentially after it has been compressed) so we know its space on
             * disk requirements.
             */

            buf.skip(AbstractReadOnlyNodeData.SIZEOF_ADDR * 2);
            
        }

        buf.putShort(AbstractReadOnlyNodeData.VERSION0);
        
        short flags = 0;
        if (leaf.hasDeleteMarkers()) {
            flags |= AbstractReadOnlyNodeData.FLAG_DELETE_MARKERS;
        }
        if (leaf.hasVersionTimestamps()) {
            flags |= AbstractReadOnlyNodeData.FLAG_VERSION_TIMESTAMPS;
        }

        buf.putShort(flags);
        
        buf.putInt(nkeys); // pack?

        final int O_keysSize = buf.pos();
        
        // skip past the keysSize and valuesSize fields.
        buf.skip(AbstractReadOnlyNodeData.SIZEOF_KEYS_SIZE * 2);
        
//        buf.putInt(encodedKeys.length); // keysSize
//        
//        buf.putInt(encodedValues.length); // valuesSize
        
        // encode the keys into the buffer
        final AbstractFixedByteArrayBuffer encodedKeys = keysCoder.encode(leaf
                .getKeys(), buf);

        // encode the values into the buffer.
        final AbstractFixedByteArrayBuffer encodedValues = valsCoder.encode(
                leaf.getValues(), buf);

        /*
         * Patch the buffer to indicate the byte length of the encoded keys and
         * the encoded values.
         */
        buf.putInt(O_keysSize, encodedKeys.len());
        buf.putInt(O_keysSize + AbstractReadOnlyNodeData.SIZEOF_KEYS_SIZE,
                encodedValues.len());
        
        // delete markers (bit coded).
        final int O_deleteMarkers;
        if (leaf.hasDeleteMarkers()) {

            O_deleteMarkers = buf.pos();

            for (int i = 0; i < nkeys;) {

                byte bits = 0;
                
                for (int j = 0; j < 8 && i < nkeys; j++, i++) {

                    if(leaf.getDeleteMarker(i)) {

                        // Note: bit order is per BitInputStream & BytesUtil!
                        bits |= 1 << (7 - j);
                            
                    }
                    
                }

                buf.putByte(bits);

            }

        } else {
        
            O_deleteMarkers = -1;
            
        }

        // The byte offset to minVersionTimestamp.
        final int O_versionTimestamps;
        if (leaf.hasVersionTimestamps()) {

            /*
             * The (min,max) are written out as full length long values. The per
             * tuple revision timestamps are written out using the minimum #of
             * bits required to code the data.
             */

            final long min = leaf.getMinimumVersionTimestamp();

            final long max = leaf.getMaximumVersionTimestamp();

//            final long delta = max - min;
//            assert delta >= 0;

            // will be in [1:64]
            final byte versionTimestampBits = (byte) (Fast
                    .mostSignificantBit(max - min) + 1);

            // one byte.
            buf.putByte((byte) versionTimestampBits);

            // offset of minVersionTimestamp.
            O_versionTimestamps = buf.pos();

            // int64
            buf.putLong(min);

            // int64
            buf.putLong(max);

            /*
             * FIXME pre-extend the buffer and use slice ctor for obs for less
             * allocation and copying.
             * 
             * FIXME Use pluggable coding for version timestamps.
             */
            final int byteLength = BytesUtil.bitFlagByteLength(nkeys
                    * versionTimestampBits/* nbits */);
            final byte[] a = new byte[byteLength];
            final OutputBitStream obs = new OutputBitStream(a);
            try {
                
                // array of [versionTimestampBits] fields.
                for (int i = 0; i < nkeys; i++) {

                    final long deltat = leaf.getVersionTimestamp(i) - min;
                    assert deltat >= 0;
                    
                    obs.writeLong(deltat, versionTimestampBits);

                }

                obs.flush();
                
                // copy onto the buffer.
                buf.put(a);
                
            } catch (IOException e) {
                throw new RuntimeException(e);
                // Note: close is not necessary if flushed and backed by byte[].
//            } finally {
//                try {
//                    obs.close();
//                } catch (IOException e) {
//                    log.error(e);
//                }
            }

        } else {

            O_versionTimestamps = -1;

        }

        return buf.slice(O_origin, buf.pos() - O_origin);
        
    }

    /**
     * A read-only view of the data for a B+Tree leaf based on a compact record
     * format. While some fields are cached, for the most part the various data
     * fields, including the keys and values, are accessed in place in the data
     * record in order to minimize the memory footprint of the leaf. The keys and
     * values are coded using a caller specified {@link IRabaCoder}. The specific
     * coding scheme is specified by the {@link IndexMetadata} for the B+Tree
     * instance and is not stored within the leaf data record.
     * <p>
     * Note: The leading byte of the record format codes for a leaf, a double-linked
     * leaf or a node in a manner which is compatible with {@link ReadOnlyNodeData}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class ReadOnlyLeafData extends AbstractReadOnlyNodeData<ILeafData>
            implements ILeafData {

        /** The backing buffer. */
        private final AbstractFixedByteArrayBuffer b;
        
        // fields which are cached by the ctor.
//        private final boolean doubleLinked;
        private final int nkeys;
        private final short flags;
        private final IRaba keys;
        private final IRaba vals;
        
        /**
         * Offset of the bit flags in the buffer encoding the presence of deleted
         * tuples -or- <code>-1</code> if the leaf does not report those data.
         */
        private final int O_deleteMarkers;

        /**
         * The byte offset of the minimum version timestamp in the buffer -or-
         * <code>-1</code> if the leaf does not report those data. The minimum
         * timestamp is coded as a full length long. The next field in the
         * buffer is the maximum version timestamp. The following fields are an
         * array of {@link #nkeys} coded timestamp values (one per tuple). Those
         * timestamps are coded in {@link #versionTimestampBits} each.
         */
        private final int O_versionTimestamps;

        /**
         * The #of bits used to code the version timestamps -or- ZERO (0) if
         * they are not present.
         */
        private final int versionTimestampBits;

        public final AbstractFixedByteArrayBuffer data() {

            return b;
            
        }

        /**
         * Decode in place (wraps a record containing the encoded data for a leaf).
         * 
         * @param buf
         *            A buffer containing the leaf data.
         */
        protected ReadOnlyLeafData(final AbstractFixedByteArrayBuffer buf,
                final IRabaCoder keysCoder, final IRabaCoder valuesCoder) {

            if (buf == null)
                throw new IllegalArgumentException();

            if (keysCoder == null)
                throw new IllegalArgumentException();
            
            if (valuesCoder == null)
                throw new IllegalArgumentException();

            int pos = O_TYPE;
            final byte type = buf.getByte(pos);
            pos += SIZEOF_TYPE;

            final boolean doubleLinked;
            switch (type) {
            case NODE:
                throw new AssertionError();
            case LEAF:
                doubleLinked = false;
                break;
            case LINKED_LEAF:
                doubleLinked = true;
                break;
            default:
                throw new AssertionError("type=" + type);
            }

            if (doubleLinked) {
                
                // skip over the prior/next addr.
                pos += SIZEOF_ADDR * 2;
                
            }
            
            final int version = buf.getShort(pos);
            pos += SIZEOF_VERSION;
            switch (version) {
            case VERSION0:
                break;
            default:
                throw new AssertionError("version=" + version);
            }

            flags = buf.getShort(pos);
            pos += SIZEOF_FLAGS;
            final boolean hasVersionTimestamps = ((flags & FLAG_VERSION_TIMESTAMPS) != 0);
            final boolean hasDeleteMarkers = ((flags & FLAG_DELETE_MARKERS) != 0);

            this.nkeys = buf.getInt(pos);
            pos += SIZEOF_NKEYS;

            final int keysSize = buf.getInt(pos);
            pos += SIZEOF_KEYS_SIZE;
            
            final int valuesSize = buf.getInt(pos);
            pos += SIZEOF_KEYS_SIZE;

            // keys
//            final int O_keys = pos;
//            buf.limit(buf.pos() + keysSize);
            this.keys = keysCoder.decode(buf.slice(pos, keysSize));
//            assert buf.position() == O_keys + keysSize;
            pos += keysSize;// skip over the keys.

            // values
//            final int O_values = pos;
//            buf.limit(buf.position() + valuesSize);
            this.vals = valuesCoder.decode(buf.slice(pos,valuesSize));
//            assert buf.position() == O_values + valuesSize;
            pos += valuesSize;// skip over the values.

            // delete markers
            if (hasDeleteMarkers) {
                
                O_deleteMarkers = pos;

                // advance past the bit flags.
                pos += BytesUtil.bitFlagByteLength(nkeys);// bit coded.
                
            } else {
                
                O_deleteMarkers = -1;
                
            }

            // version timestamps
            if (hasVersionTimestamps) {

                versionTimestampBits = buf.getByte(pos);
                pos++;

                O_versionTimestamps = pos;
                // advance past the timestamps.
                pos += (2 * SIZEOF_TIMESTAMP)
                        + BytesUtil.bitFlagByteLength(nkeys
                                * versionTimestampBits/* nbits */);

//                // advance past the timestamps.
//                pos += nkeys * SIZEOF_TIMESTAMP;
                
            } else {
                
                O_versionTimestamps = -1;
                versionTimestampBits = 0;
                
            }

//            assert buf.pos() == buf.limit();
            
            // flip [limit=pos; pos=0].
//            buf.flip();

            // save reference to buffer
//            this.b = (b.isReadOnly() ? b : b.asReadOnlyBuffer());
            this.b = buf;

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

        public long getMinimumVersionTimestamp() {

            if (!hasVersionTimestamps())
                throw new UnsupportedOperationException();

            return b.getLong(O_versionTimestamps);

        }

        public long getMaximumVersionTimestamp() {

            if (!hasVersionTimestamps())
                throw new UnsupportedOperationException();

            return b.getLong(O_versionTimestamps + SIZEOF_TIMESTAMP);

        }

        final public long getVersionTimestamp(final int index) {

            if (!hasVersionTimestamps())
                throw new UnsupportedOperationException();

//            return b.getLong(O_versionTimestamps + index * SIZEOF_TIMESTAMP);

            final InputBitStream ibs = b.getInputBitStream();
            try {

                final long bitpos = ((O_versionTimestamps + (2L * SIZEOF_TIMESTAMP)) << 3)
                        + (index * versionTimestampBits);

                ibs.position(bitpos);

                final long deltat = ibs
                        .readLong(versionTimestampBits/* nbits */);

                return getMinimumVersionTimestamp() + deltat;
                
            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
            } finally {
                try {
                    ibs.close();
                } catch (IOException ex) {
                    log.error(ex);
                }
            }

        }

        final public boolean getDeleteMarker(final int index) {

            if (!hasDeleteMarkers())
                throw new UnsupportedOperationException();

            return b.getBit((O_deleteMarkers << 3) + index);

        }
        
        final public IRaba getKeys() {
            
            return keys;
            
        }

        final public IRaba getValues() {

            return vals;
            
        }

        public String toString() {

            final StringBuilder sb = new StringBuilder();

            sb.append(getClass().getName() + "{");

            DefaultLeafCoder.toString(this, sb);

            sb.append("}");
            
            return sb.toString();
            
        }

        /*
         * Double-linked leaf support.
         */
        
        /**
         * Return <code>true</code> if the leaf encodes the address or the prior and
         * next leaves.
         */
        public final boolean isDoubleLinked() {

            return b.getByte(0) == LINKED_LEAF;

        }

//        /**
//         * Update the data record to set the prior and next leaf address.
//         * <p>
//         * Note: In order to use this method to write linked leaves on the store you
//         * have to either write behind at a pre-determined address on the store or
//         * settle for writing only the prior or the next leaf address, but not both.
//         * It is up to the caller to perform these tricks. All this method does is
//         * to touch up the serialized record.
//         * <p>
//         * Note: This method has NO side-effects on the <i>position</i> or
//         * <i>limit</i> of the internal {@link ByteBuffer}.
//         * 
//         * @param priorAddr
//         *            The address of the previous leaf in key order, <code>0L</code>
//         *            if it is known that there is no previous leaf, and
//         *            <code>-1L</code> if either: (a) it is not known whether there
//         *            is a previous leaf; or (b) it is known but the address of that
//         *            leaf is not known to the caller.
//         * @param nextAddr
//         *            The address of the next leaf in key order, <code>0L</code> if
//         *            it is known that there is no next leaf, and <code>-1L</code>
//         *            if either: (a) it is not known whether there is a next leaf;
//         *            or (b) it is known but the address of that leaf is not known
//         *            to the caller.
//         * 
//         * @see IndexSegmentBuilder
//         */
//        public void updateLeaf(final long priorAddr, final long nextAddr) {
    //
//            if (!isDoubleLinked()) {
    //
//                // Not double-linked.
//                throw new UnsupportedOperationException();
//                
//            }
    //
//            /*
//             * Note: these fields are written immediately after the byte indicating
//             * whether this is a leaf, linked-leaf, or node.
//             */
//            
//            b.putLong(O_PRIOR, priorAddr);
    //
//            b.putLong(O_NEXT + SIZEOF_ADDR, nextAddr);
    //
//        }

        public final long getPriorAddr() {
            
            if(!isDoubleLinked())
                throw new UnsupportedOperationException();

            return b.getLong(O_PRIOR);
            
        }
        
        public final long getNextAddr() {
            
            if(!isDoubleLinked())
                throw new UnsupportedOperationException();

            return b.getLong(O_NEXT);
            
        }

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
     */
    static public StringBuilder toString(final ILeafData leaf,
            final StringBuilder sb) {

        final int nkeys = leaf.getKeyCount();

        if(leaf.isDoubleLinked()) {
            
            sb.append(", priorAddr=" + leaf.getPriorAddr());

            sb.append(", nextAddr=" + leaf.getNextAddr());

        }
        
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

            sb.append(", versionTimestamps={min="
                    + leaf.getMinimumVersionTimestamp() + ",max="
                    + leaf.getMaximumVersionTimestamp() + "[");

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
