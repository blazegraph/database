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
 * Created on Aug 13, 2009
 */

package com.bigdata.btree.raba.codec;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.rawstore.Bytes;

/**
 * This class does not offer any compression. It merely stores the byte[][] in a
 * {@link ByteBuffer} together with offset information required to extract the
 * original byte[]s using a random access pattern. It supports both B+Tree keys
 * and B+Tree values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SimpleRabaCoder implements IRabaCoder {

    private static final byte VERSION0 = 0x00;

    public static transient final SimpleRabaCoder INSTANCE = new SimpleRabaCoder();
    
    /**
     * Yes.
     */
    final public boolean isKeyCoder() {
        
        return true;
        
    }
    
    /**
     * Yes.
     */
    final public boolean isValueCoder() {
        
        return true;
        
    }
    
    private SimpleRabaCoder() {
        
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP

    }

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
    /** The size of the elements in the offset[]. */
    static private final int SIZEOF_OFFSET = Bytes.SIZEOF_INT;

    static private final int O_VERSION = 0;
    static private final int O_FLAGS = O_VERSION + SIZEOF_VERSION;
    static private final int O_SIZE = O_FLAGS + SIZEOF_FLAGS;
    static private final int O_NULLS = O_SIZE + SIZEOF_SIZE;
    
    public IRabaDecoder encode(final IRaba raba) {

        // The #of entries.
        final int size = raba.size();

        // The total byte length of the entries.
        int totalByteLength = 0;
        int nullCount = 0;
        for (int i = 0; i < size; i++) {
            
            if (raba.isNull(i)) {
                
                nullCount++;
                
            } else {
                
                totalByteLength += raba.length(i);
                
            }

        }

        // #of bytes required for the nulls bit flags.
        final int bitFlagByteCount = BytesUtil.bitFlagByteLength(size);

        final int sizeOfOffsets = (size + 1) * SIZEOF_OFFSET;
        
        final int capacity = //
                SIZEOF_VERSION + // version
                SIZEOF_FLAGS+ // bit flags
                SIZEOF_SIZE+ // size
                bitFlagByteCount + // nulls
                sizeOfOffsets + // sizeof(offset[])
                totalByteLength // byte[] values
        ;

        final ByteBuffer b = ByteBuffer.allocate(capacity);

        // version
        assert b.position() == O_VERSION;
        b.put(VERSION0);

        // a byte containing a single bit flag.
        assert b.position() == O_FLAGS;
        b.put((byte) (raba.isKeys() ? 1 : 0));
        
        // #of entries.
        assert b.position() == O_SIZE;
        b.putInt(size);

        // bit flag nulls : @todo not required for keys.
        assert b.position() == O_NULLS;
        for (int i = 0; i < size;) {

            byte bits = 0;

            for (int j = 0; j < 8 && i < size; j++, i++) {

                if (raba.isNull(i)) {

                    bits |= 1 << j;

                }

            }

            b.put(bits);

        }

        // offset[]
        assert size > 0 && b.position() > O_NULLS || size == 0;
        final int O_offsets = b.position() + sizeOfOffsets;
        int lastOffset = O_offsets;
        for (int i = 0; i < size; i++) {

            if (raba.isNull(i)) {
                
                b.putInt(lastOffset);
                
            } else {
             
                b.putInt(lastOffset);
                
                lastOffset += raba.length(i);
                
            }

        }
        
        b.putInt(lastOffset);

        // byte[]s
        for (int i = 0; i < size; i++) {

            if (!raba.isNull(i)) {
                
                b.put(raba.get(i));
                
            }
            
        }

        assert b.position() == b.limit() : b.toString() + " : src=" + raba;
        assert b.position() == b.capacity() : b.toString() + " : src=" + raba;

        b.flip();
        
        return new SimpleDataDecoder(b.asReadOnlyBuffer());
        
    }

    public IRabaDecoder decode(final ByteBuffer data) {

        return new SimpleDataDecoder(data);

    }

    /**
     * Class provides in place access to the "coded" logical byte[][].
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class SimpleDataDecoder extends AbstractRabaDecoder {

        /**
         * The #of entries (cached).
         */
        private final int size;

        /**
         * If the record codes B+Tree keys.
         */
        private final boolean isKeys;
        
        private final ByteBuffer data;

        /**
         * 
         * @param data
         */
        public SimpleDataDecoder(final ByteBuffer data) {

            if (data == null)
                throw new IllegalArgumentException();
            
            this.data = data;
            
            final byte version = data.get(O_VERSION);

            switch (version) {
            case VERSION0:
                break;
            default:
                throw new RuntimeException("Unknown version: " + version);
            }

            // Note: Only one bit flag, so we do not need a mask.
            this.isKeys = data.get(O_FLAGS) != 0;
            
            this.size = data.getInt(O_SIZE);
            
            if (size < 0)
                throw new IllegalArgumentException();
            
            // #of bytes required for the nulls bit flags.
            final int bitFlagByteCount = BytesUtil.bitFlagByteLength(size);
            
            // offset of the offset[].
            O_offsets = O_NULLS + bitFlagByteCount;

        }

        /**
         * The offset into the buffer of the offset[]. This array is also used
         * to compute the length of any given byte[] by subtracting the offset
         * of the next byte[] from the offset of the desired byte[], which is
         * why we write <code>size+1</code> values into this array.
         */
        private final int O_offsets;
        
        final public ByteBuffer data() {
            return data;
        }

        public boolean isKeys() {
            return isKeys;
        }

        final public int capacity() {
            return size;
        }

        final public int size() {
            return size;
        }

        final public boolean isEmpty() {
            return size == 0;
        }

        final public boolean isFull() {
            return true;
        }

        protected void rangeCheck(final int index) {
            
            if (index < 0 || index >= size)
                throw new IndexOutOfBoundsException();
            
        }
        
        public boolean isNull(final int index) {

            rangeCheck(index);
            
            return BytesUtil.getBit(data, O_NULLS, index);

        }

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
             * 
             * Note: The buffer is duplicated first to avoid concurrent
             * modification to the buffer's internal state.
             */
            final ByteBuffer data = this.data.duplicate();
            data.limit(offset2);
            data.position(offset);
            data.get(a);

            return a;

        }

        public int copy(final int index, final OutputStream os) {
            
            final byte[] a = get(index);
            
            try {
            
                os.write(a);
                
            } catch (IOException ex) {
                
                throw new RuntimeException(ex);
                
            }

            return a.length;
            
        }

        /*
         * Search
         */
        
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
                final int tmp = BytesUtil.compareBytes(data, aoff, alen, key);

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
