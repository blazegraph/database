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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;

/**
 * This class does not offer any compression. It merely stores byte[][] whose
 * individual elements have a fixed length specified in the constructor in a
 * manner suitable for fast random access. This is useful if the values are a
 * fixed length, application specific is either not obvious or not desired, and
 * you wish to emphasize speed of data access over compression. This class only
 * supports B+Tree values.
 * 
 * <h2>Binary Format</h2>
 * 
 * <pre>
 * version : byte
 * size    : int32
 * nulls   : BytesUtil.bitFlagByteLength(size)
 * values  : size * len
 * </pre>
 * 
 * where
 * <dl>
 * <dt>size</dt>
 * <dd>The #of elements in the {@link IRaba}.</dd>
 * <dt>nulls</dt>
 * <dd>A vector of bit flags identifying <code>null</code> values. The length of
 * the vector is rounded up to the nearest whole byte based on <i>size</i>.</dd>
 * <dt>len</dt>
 * <dd>The fixed length of each non-<code>null</code> value as specified to the
 * constructor.</dd>
 * <dt>values</dt>
 * <dd>The uncompressed representation of each fixed length value. In order to
 * avoid the overhead of an offset map, <code>null</code> values occupy the same
 * space in the array as non-<code>null</code> values.</dd>
 * </dl>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FixedLengthValueRabaCoder implements IRabaCoder, Externalizable {

    private static final byte VERSION0 = 0x00;

    /**
     * The length of the individual byte[] elements.
     */
    private int len; 
    
    /**
     * No.
     */
    final public boolean isKeyCoder() {
        
        return false;
        
    }
    
    /**
     * Yes.
     */
    final public boolean isValueCoder() {
        
        return true;
        
    }

    /**
     * The required length for all non-<code>null</code> values.
     */
    final public int getLength() {
        
        return len;
        
    }
    
    /**
     * De-serialization ctor.
     */
    public FixedLengthValueRabaCoder() {

    }

    /**
     * Designated constructor.
     * 
     * @param len
     *            The length of the byte[] value for each non-<code>null</code>
     *            tuple. {@link IRaba}s having values with a different length
     *            will result in a runtime exception when they are encoded.
     */
    public FixedLengthValueRabaCoder(final int len) {

        this.len = len;
        
    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        out.writeInt(len);

    }

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        len = in.readInt();

    }

    /** The size of the version field. */
    static private final int SIZEOF_VERSION = 1;
    /** The size of the size field. */
    static private final int SIZEOF_SIZE = Bytes.SIZEOF_INT;

    /** The byte offset to the version identifier. */
    static private final int O_VERSION = 0;
    /** The byte offset of the field coding the #of entries in the raba. */
    static private final int O_SIZE = O_VERSION + SIZEOF_VERSION;
    /** The byte offset to the bit flags coding the nulls. */
    static private final int O_NULLS = O_SIZE + SIZEOF_SIZE;

    /**
     * {@inheritDoc}
     * <p>
     * Instances of this class will reject {@link IRaba} instances having non-
     * <code>null</code> values whose length is not the length specified to the
     * constructor.
     * 
     * @throws UnsupportedOperationException
     *             if the {@link IRaba} has a non-<code>null</code> value with a
     *             length other than the length specified to the constructor.
     */
    public ICodedRaba encodeLive(final IRaba raba, final DataOutputBuffer buf) {

        if (raba == null)
            throw new IllegalArgumentException();
        
        if (buf == null)
            throw new IllegalArgumentException();

        if (raba.isKeys())
            throw new UnsupportedOperationException();

        // The #of entries.
        final int size = raba.size();

        // The exact capacity for the coded raba.
        final int capacity = SIZEOF_VERSION + SIZEOF_SIZE
                + BytesUtil.bitFlagByteLength(size) + size * len;

        // Ensure that there is enough free capacity in the buffer.
        buf.ensureFree(capacity);
        
        // The byte offset of the origin of the coded data in the buffer.
        final int O_origin = buf.pos();

        // version
        assert buf.pos() == O_VERSION + O_origin;
        buf.putByte(VERSION0);

        // #of entries.
        assert buf.pos() == O_SIZE + O_origin;
        buf.putInt(size);
        
        // bit flag nulls
        {
            
            assert buf.pos() == O_NULLS + O_origin;
            
            for (int i = 0; i < size;) {

                byte bits = 0;

                for (int j = 0; j < 8 && i < size; j++, i++) {

                    if (raba.isNull(i)) {

                        // Note: bit order is per BitInputStream & BytesUtil!
                        bits |= 1 << (7 - j);

                    } else if (raba.length(i) != len) {

                        /*
                         * All non-null values must have the length specified to
                         * the constructor, so this is a runtime error.
                         */

                        throw new UnsupportedOperationException(
                                "Value has wrong length: index=" + i
                                        + ", expected=" + len + ", actual="
                                        + raba.length(i));

                    }

                }

                buf.putByte(bits);

            }

        }

        // value[]s
        for (int i = 0; i < size; i++) {//, offset += len) {

            if (raba.isNull(i)) {
                
                buf.advancePosAndLimit(len);
                
            } else {
                
                buf.put(raba.get(i));
                
            }
            
        }

        assert buf.pos() == buf.limit() : buf.toString() + " : src=" + raba;

        final AbstractFixedByteArrayBuffer slice = buf.slice(//
                O_origin, buf.pos() - O_origin);

        return new CodedRabaImpl(len, slice, size);

    }

    public AbstractFixedByteArrayBuffer encode(final IRaba raba,
            final DataOutputBuffer buf) {

        return encodeLive(raba, buf).data();

    }
    
    public ICodedRaba decode(final AbstractFixedByteArrayBuffer data) {

        return new CodedRabaImpl(len, data);

    }

    /**
     * Class provides in place access to the "coded" logical byte[][].
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class CodedRabaImpl extends AbstractCodedRaba {

        /**
         * The byte length of each non-null entry (from the constructor on the
         * outer class).
         */
        private final int len;

        /**
         * The #of entries (cached).
         */
        private final int size;

        private final AbstractFixedByteArrayBuffer data;

        /**
         * 
         * @param data
         */
        public CodedRabaImpl(final int len,
                final AbstractFixedByteArrayBuffer data) {

            if (len <= 0)
                throw new IllegalArgumentException();

            if (data == null)
                throw new IllegalArgumentException();

            this.len = len;

            this.data = data;

            final byte version = data.getByte(O_VERSION);

            switch (version) {
            case VERSION0:
                break;
            default:
                throw new RuntimeException("Unknown version: " + version);
            }

            this.size = data.getInt(O_SIZE);

            if (size < 0)
                throw new IllegalArgumentException();

            // #of bytes required for the nulls bit flags.
            final int bitFlagByteCount = BytesUtil.bitFlagByteLength(size);

            // offset of the value[].
            O_values = O_NULLS + bitFlagByteCount;
            
        }
        
        public CodedRabaImpl(final int len,
                final AbstractFixedByteArrayBuffer data, final int size) {

            this.len = len;
            this.data = data;
            this.size = size;
            
            // #of bytes required for the nulls bit flags.
            final int bitFlagByteCount = BytesUtil.bitFlagByteLength(size);

            // offset of the value[].
            O_values = O_NULLS + bitFlagByteCount;
            
        }

        /**
         * The offset into the buffer of the value[].
         */
        private final int O_values;
        
        final public AbstractFixedByteArrayBuffer data() {
            
            return data;
            
        }

        public boolean isKeys() {

            return false;
            
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

            return data.getBit((O_NULLS << 3) + index);

        }

        public int length(final int index) {
            
            if (isNull(index))
                throw new NullPointerException();
            
            return len;
            
        }

        public byte[] get(final int index) {

            if (isNull(index))
                return null;
            
            final int offset = O_values + index * len;

            final byte[] a = new byte[len];

            // Copy the byte[] from the buffer.
            data.get(offset, a, 0/* dstoff */, len);
            
            return a;

        }

        public int copy(final int index, final OutputStream os) {

            if (isNull(index))
                throw new NullPointerException();
            
            final int offset = O_values + index * len;

            try {
            
                data.writeOn(os, offset, len);

            } catch (IOException ex) {
                
                throw new RuntimeException(ex);
                
            }

            return len;
            
        }

        /*
         * Search
         */
        
        public int search(final byte[] key) {

            throw new UnsupportedOperationException();
            
        }
        
    }

}
