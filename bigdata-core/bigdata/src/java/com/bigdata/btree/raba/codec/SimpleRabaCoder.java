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
 * Created on Aug 13, 2009
 */

package com.bigdata.btree.raba.codec;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.bigdata.btree.raba.IRaba;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;

/**
 * This class does not offer any compression. It merely stores the byte[][] in a
 * {@link ByteBuffer} together with offset information required to extract the
 * original byte[]s using a random access pattern. It supports both B+Tree keys
 * and B+Tree values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class SimpleRabaCoder implements IRabaCoder, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 3385188183979794781L;

    /**
     * The original version for the coded data record.
     */
    private static final byte VERSION0 = 0x00;

    /**
     * New version for the coded data record also records the capacity of
     * the original {@link IRaba}.
     */
    private static final byte VERSION1 = 0x01;

    private static final byte CURRENT_VERSION = VERSION1;
    
    public static transient final SimpleRabaCoder INSTANCE = new SimpleRabaCoder();
    
    /**
     * Yes.
     */
    @Override
    final public boolean isKeyCoder() {
        
        return true;
        
    }
    
    /**
     * Yes.
     */
    @Override
    final public boolean isValueCoder() {
        
        return true;
        
    }
    
    @Override
    public boolean isDuplicateKeys() {

        return false;
        
    }

   /**
     * De-serialization ctor. Use {@link #INSTANCE} otherwise.
     */
    public SimpleRabaCoder() {
        
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        // NOP

    }

    /** The size of the version field. */
    static private final int SIZEOF_VERSION = 1;
    /** The size of the bit flags. */
    static private final int SIZEOF_FLAGS = 1;
    /** The size of the size field. */
    static private final int SIZEOF_SIZE = Bytes.SIZEOF_INT;
	/**
	 * The size of the capacity field.
	 * @since #VERSION1
	 */
    static private final int SIZEOF_CAPACITY = Bytes.SIZEOF_INT;
    /** The size of the field coding the #of elements in the offset[]. */
    static private final int SIZEOF_OFFSET = Bytes.SIZEOF_INT;

    /** The byte offset to the version identifier. */
    static private final int O_VERSION = 0;
    /** The byte offset of the bit flags. */
    static private final int O_FLAGS = O_VERSION + SIZEOF_VERSION;
    /** The byte offset of the field coding the #of entries in the raba. */
    static private final int O_SIZE = O_FLAGS + SIZEOF_FLAGS;
    /** The byte offset to the bit flags coding the nulls. */
	static private final int O_NULLS(final byte version) {
		if (version == VERSION0)
			return O_SIZE + SIZEOF_SIZE;
		return O_SIZE + SIZEOF_SIZE + SIZEOF_CAPACITY;
    }

    @Override
    public ICodedRaba encodeLive(final IRaba raba, final DataOutputBuffer buf) {

        if (raba == null)
            throw new IllegalArgumentException();
        
        if (buf == null)
            throw new IllegalArgumentException();
        
        // The #of entries.
        final int size = raba.size();

        // The logical capacity of the raba.
        final int capacity = raba.capacity();
        
        // iff the raba represents B+Tree keys.
        final boolean isKeys = raba.isKeys();

        // #of bytes for the offset[].
        final int sizeOfOffsets = (size + 1) * SIZEOF_OFFSET;
        
        // The byte offset of the origin of the coded data in the buffer.
        final int O_origin = buf.pos();
        
        // version
        assert buf.pos() == O_VERSION + O_origin;
        buf.putByte(CURRENT_VERSION);

        // a byte containing a single bit flag.
        assert buf.pos() == O_FLAGS + O_origin;
        buf.putByte((byte) (isKeys ? 1 : 0));
        
        // #of entries.
        assert buf.pos() == O_SIZE + O_origin;
        buf.putInt(size);

        // logical capacity.
		if (CURRENT_VERSION >= VERSION1)
			buf.putInt(capacity);

        // bit flag nulls
        if (!isKeys) {
            
//            assert buf.pos() == O_NULLS + O_origin;
            
            for (int i = 0; i < size;) {

                byte bits = 0;

                for (int j = 0; j < 8 && i < size; j++, i++) {

                    if (raba.isNull(i)) {

                        // Note: bit order is per BitInputStream & BytesUtil!
                        bits |= 1 << (7 - j);

                    }

                }

                buf.putByte(bits);

            }

        }

        // offset[]
//        assert size > 0 && buf.pos() > O_NULLS + O_origin || size == 0 || isKeys;
        final int O_offsets = buf.pos() + sizeOfOffsets - O_origin;
        int lastOffset = O_offsets;
        for (int i = 0; i < size; i++) {

            if (raba.isNull(i)) {
                
                buf.putInt(lastOffset);
                
            } else {
             
                buf.putInt(lastOffset);
                
                lastOffset += raba.length(i);
                
            }

        }
        
        buf.putInt(lastOffset);

        // byte[]s
        for (int i = 0; i < size; i++) {

            if (!raba.isNull(i)) {
                
                buf.put(raba.get(i));
                
            }
            
        }

        assert buf.pos() == buf.limit() : buf.toString() + " : src=" + raba;

        final AbstractFixedByteArrayBuffer slice = buf.slice(//
                O_origin, buf.pos() - O_origin);

        return new CodedRabaImpl(slice, isKeys, size, capacity, CURRENT_VERSION);
//        return new CodedRabaImpl(slice);

    }

    @Override
    public AbstractFixedByteArrayBuffer encode(final IRaba raba,
            final DataOutputBuffer buf) {

        /*
         * There is nearly zero overhead for this code path when compared to
         * encodeLive().
         */
        
        return encodeLive(raba, buf).data();

    }
    
    @Override
    public ICodedRaba decode(final AbstractFixedByteArrayBuffer data) {

        return new CodedRabaImpl(data);

    }

    /**
     * Class provides in place access to the "coded" logical byte[][].
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class CodedRabaImpl extends AbstractCodedRaba {

        /**
         * The #of entries (cached).
         */
        private final int size;

        /**
         * The logical capacity of the {@link IRaba} (cached).
         */
        private final int capacity;
        
        /** Offset to the bit flag nulls. */
        private final int o_nulls;
        
        /**
         * If the record codes B+Tree keys.
         */
        private final boolean isKeys;
        
        private final AbstractFixedByteArrayBuffer data;

        /**
         * 
         * @param data
         */
        public CodedRabaImpl(final AbstractFixedByteArrayBuffer data) {

            if (data == null)
                throw new IllegalArgumentException();
            
            this.data = data;
            
            final byte version = data.getByte(O_VERSION);

            switch (version) {
            case VERSION0:
            case VERSION1:
                break;
            default:
                throw new RuntimeException("Unknown version: " + version);
            }

            // Note: Only one bit flag, so we do not need a mask.
            this.isKeys = data.getByte(O_FLAGS) != 0;
            
            this.size = data.getInt(O_SIZE);

			if (version >= VERSION1)
				this.capacity = data.getInt(O_SIZE + SIZEOF_SIZE);
            else
            	this.capacity = size;

            if (size < 0)
                throw new IllegalArgumentException();

            if (capacity < 0)
                throw new IllegalArgumentException();
            
            // #of bytes required for the nulls bit flags.
            final int bitFlagByteCount = isKeys ? 0 : BytesUtil
                    .bitFlagByteLength(size);
            
            o_nulls = O_NULLS(version);
            
            // offset of the offset[].
            O_offsets = o_nulls + bitFlagByteCount;

        }
        
		public CodedRabaImpl(final AbstractFixedByteArrayBuffer data,
				final boolean isKeys, final int size, final int capacity,
				final byte version) {

            this.data = data;
            this.isKeys = isKeys;
            this.size = size;
            this.capacity = capacity;
            
            // #of bytes required for the nulls bit flags.
            final int bitFlagByteCount = isKeys ? 0 : BytesUtil
                    .bitFlagByteLength(size);
            
            o_nulls = O_NULLS(version);

            // offset of the offset[].
            O_offsets = o_nulls + bitFlagByteCount;
            
        }

        /**
         * The offset into the buffer of the offset[]. This array is also used
         * to compute the length of any given byte[] by subtracting the offset
         * of the next byte[] from the offset of the desired byte[], which is
         * why we write <code>size+1</code> values into this array.
         */
        private final int O_offsets;
        
        @Override
        final public AbstractFixedByteArrayBuffer data() {
            
            return data;
            
        }

        @Override
        public boolean isKeys() {

            return isKeys;
            
        }

        @Override
        final public int capacity() {
            
            return capacity;
            
        }

        @Override
        final public int size() {
            
            return size;
            
        }

        @Override
        final public boolean isEmpty() {
            
            return size == 0;
            
        }

        @Override
        final public boolean isFull() {
            
            return true;
            
        }

        protected void rangeCheck(final int index) {
            
            if (index < 0 || index >= size)
                throw new IndexOutOfBoundsException();
            
        }
        
        @Override
        public boolean isNull(final int index) {

			if (index >= size && index < capacity) {
				// everything beyond the size and up to the capacity is a null.
				return true;
			}
        	
            rangeCheck(index);

            if (isKeys)
                return false;
            
            return data.getBit((o_nulls << 3) + index);

        }

        @Override
        public int length(final int index) {
            
            if (isNull(index))
                throw new NullPointerException();
            
            final int offset = data.getInt(O_offsets + index * SIZEOF_OFFSET);

            final int offset2 = data.getInt(O_offsets + (index + 1)
                    * SIZEOF_OFFSET);

            final int length = offset2 - offset;
            
            assert length >= 0;
            
            return length;
            
        }

        @Override
        public byte[] get(final int index) {

            if (isNull(index))
                return null;
            
            final int offset = data.getInt(O_offsets + index * SIZEOF_OFFSET);

            final int offset2 = data.getInt(O_offsets + (index + 1)
                    * SIZEOF_OFFSET);

            final int length = offset2 - offset;
            
            assert length >= 0;

            final byte[] a = new byte[length];

            /*
             * Copy the byte[] from the buffer.
             */
//             * 
//             * Note: The buffer is duplicated first to avoid concurrent
//             * modification to the buffer's internal state.
//             */
//            final ByteBuffer data = this.data.duplicate();
//            data.limit(offset2);
//            data.position(offset);
//            data.get(a);

            data.get(offset, a, 0/* dstoff */, length);
            
            return a;

        }

        @Override
        public int copy(final int index, final OutputStream os) {

            if (isNull(index))
                throw new NullPointerException();
            
            final int offset = data.getInt(O_offsets + index * SIZEOF_OFFSET);

            final int offset2 = data.getInt(O_offsets + (index + 1)
                    * SIZEOF_OFFSET);

            final int length = offset2 - offset;
            
            assert length >= 0;

            try {
            
                data.writeOn(os, offset, length);

            } catch (IOException ex) {
                
                throw new RuntimeException(ex);
                
            }

            return length;
            
        }

        /*
         * Search
         */
        
        @Override
        public int search(final byte[] key) {

            if (!isKeys())
                throw new UnsupportedOperationException();
            
            /*
             * Note: base, mid, low, and high are offsets into the offset[]. The
             * offset[] has size+1 entries, but only the first size entries
             * correspond to byte[] values stored in the record. The last entry
             * is just so we can figure out the length of the last byte[] stored
             * in the record.
             */
            
            final int base = 0;
            final int nmem = size;
    
            int low = 0;

            int high = nmem - 1;

            while (low <= high) {

                final int mid = (low + high) >> 1;

                final int offset = base + mid;

                // offset into the buffer of the start of that byte[].
                final int aoff = data
                        .getInt(O_offsets + offset * SIZEOF_OFFSET);

                // length of that byte[].
                final int alen = data.getInt(O_offsets + (offset + 1)
                        * SIZEOF_OFFSET)
                        - aoff;

                // compare actual data vs probe key.
                final int tmp = BytesUtil.compareBytesWithLenAndOffset(//
                        data.off() + aoff, alen, data.array(), //
                        0, key.length, key);

                if (tmp < 0) {

                    // Actual LT probe, restrict lower bound and try again.
                    low = mid + 1;

                } else if (tmp > 0) {

                    // Actual GT probe, restrict upper bound and try again.
                    high = mid - 1;

                } else {

                    // Actual EQ probe. Found : return offset.

                    return offset;

                }

            }

            // Not found: return insertion point.

            final int offset = (base + low);

            return -(offset + 1);

        }
        
    }

}
