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
 * Created on Aug 6, 2009
 */

package com.bigdata.util;

import it.unimi.dsi.bits.AbstractBitVector;

import java.nio.ByteBuffer;

import cern.colt.bitvector.BitVector;

/**
 * Wraps a {@link ByteBuffer} as a read-only {@link BitVector}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ByteBufferBitVector extends AbstractBitVector {

    /**
     * The {@link ByteBuffer} containing the backing data.
     */
    final private ByteBuffer b;
    
    /**
     * The #of bits in the vector. 
     */
    private final long len;
    
    /**
     * The bit offset into the {@link ByteBuffer} of the first bit in the
     * vector.
     */
    private final long off;

    final public long length() {

        return len;

    }

    /**
     * Ctor assumes that all bits in the buffer are used.
     * 
     * @param b
     *            The buffer.
     */
    public ByteBufferBitVector(final ByteBuffer b) {
        
        this(b, 0/* offset */, b == null ? 0 : b.capacity() * 8/* len */);

    }

    /**
     * 
     * @param b
     *            The buffer.
     * @param off
     *            The offset from the start of the buffer for the view.
     * @param len
     *            The #of bits which will be included in the view.
     */
    public ByteBufferBitVector(final ByteBuffer b, final long off,
            final long len) {

        if (b == null)
            throw new IllegalArgumentException();
        
        if (len < 0)
            throw new IllegalArgumentException();

        if (len < 0)
            throw new IllegalArgumentException();

        if (off + len > b.capacity() * 8L)
            throw new IllegalArgumentException();

        this.b = b;
        
        this.len = len;

        this.off = off;
        
    }

    /**
     * Return the index of the byte in which the bit with the given index is
     * encoded.
     * 
     * @param bitIndex
     *            The bit index.
     *            
     * @return The byte index.
     */
    final protected int byteIndexForBit(final long bitIndex) {

        return ((int) ((bitIndex + off) / 8));

    }

    /**
     * Return the offset within the byte in which the bit is coded of the bit
     * (this is just the remainder <code>bitIndex % 8</code>).
     * 
     * @param bitIndex
     *            The bit index into the byte[].
     * 
     * @return The offset of the bit in the appropriate byte.
     */
    final protected int withinByteIndexForBit(final long bitIndex) {

        return (int) ((bitIndex + off) % 8);

    }

    /**
     * Extract and return a bit coded flag.
     * 
     * @param offset
     *            The offset in the buffer of the start of the byte[] sequence
     *            in which the bit coded flags are stored.
     * @param index
     *            The index of the bit.
     * 
     * @return The value of the bit.
     */
    public boolean getBoolean(final long index) {

        if (index < 0 || index >= len)
            throw new IndexOutOfBoundsException();
        
        return (b.get(byteIndexForBit(index)) & (1 << withinByteIndexForBit(index))) != 0;

    }

//    // @todo override for mutation.
//    public boolean set(final long index, final boolean value) {
//
//        throw new UnsupportedOperationException();
//
//    }

}
