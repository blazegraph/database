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
 * Created on Aug 26, 2009
 */

package com.bigdata.io;

import java.nio.ByteBuffer;

/**
 * An interface for absolute get/put operations on a slice of a byte[].
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRawRecord extends IByteArrayAccess {

    /**
     * The start of the slice in the {@link #array()}.
     */
    public int off();

    /**
     * The length of the slice in the {@link #array()}.
     */
    public int len();

    /**
     * Absolute bulk <i>put</i> copies all bytes in the caller's array into this
     * buffer starting at the specified position within the slice defined by
     * this buffer.
     * 
     * @param pos
     *            The starting position within the slice defined by this buffer.
     * @param src
     *            The source data.
     */
    void put(int pos, byte[] src);

    /**
     * Absolute bulk <i>put</i> copies the specified slice of bytes from the
     * caller's array into this buffer starting at the specified position within
     * the slice defined by this buffer.
     * 
     * @param dstoff
     *            The offset into the slice to which the data will be copied.
     * @param src
     *            The source data.
     * @param srcoff
     *            The offset of the 1st byte in the source data to be copied.
     * @param srclen
     *            The #of bytes to be copied.
     */
    void put(int dstoff, byte[] src, int srcoff, int srclen);

    /**
     * Absolute bulk <i>get</i> copies <code>dst.length</code> bytes from the
     * specified offset into the slice defined by this buffer into the caller's
     * array.
     * 
     * @param srcoff
     *            The offset into the slice of the first byte to be copied.
     * @param dst
     *            The array into which the data will be copied.
     */
    void get(final int srcoff, final byte[] dst);

    /**
     * Absolute bulk <i>get</i> copies the specified slice of bytes from this
     * buffer into the specified slice of the caller's array.
     * 
     * @param srcoff
     *            The offset into the slice defined by this buffer of the first
     *            byte to be copied.
     * @param dst
     *            The array into which the data will be copied.
     * @param dstoff
     *            The offset of the first byte in that array onto which the data
     *            will be copied.
     * @param dstlen
     *            The #of bytes to be copied.
     */
    void get(final int srcoff, final byte[] dst, final int dstoff,
            final int dstlen);

    /**
     * Absolute <i>put</i> method for writing a byte value.
     * 
     * @param pos
     *            The offset into the slice.
     * @param v
     *            The value.
     *            
     * @todo rename as get(int,byte) to conform with {@link ByteBuffer}?
     */
    void putByte(int pos, byte v);

    /**
     * Absolute <i>get</i> for reading a byte value.
     * 
     * @param pos
     *            The offset into the slice.
     *            
     * @return The byte value at that offset.
     * 
     * @todo rename as get(int,byte) to conform with {@link ByteBuffer}?
     */
    byte getByte(int pos);

    void putShort(int pos, short v);

    short getShort(int pos);

    void putInt(int pos, int v);

    int getInt(int pos);

    void putFloat(int pos, float f);

    float getFloat(int pos);

    void putLong(int pos, long v);

    void putDouble(int pos, double d);

    long getLong(int pos);

    double getDouble(int pos);


    /**
     * Return a slice of the backing buffer.
     * 
     * @param aoff
     *            The starting offset of the slice into this slice.
     * @param alen
     *            The #of bytes in the slice.
     * 
     * @return The slice.
     */
    IRawRecord slice(final int aoff, final int alen);

    /**
     * Get the value of a bit.
     * 
     * @param bitIndex
     *            The index of the bit, counting from the first bit position in
     *            the slice.
     * 
     * @return The value of the bit.
     */
    boolean getBit(final long bitIndex);

    /**
     * Set the value of a bit.
     * 
     * @param bitIndex
     *            The index of the bit, counting from the first bit position in
     *            the slice.
     * 
     * @return The old value of the bit.
     */
    boolean setBit(final long bitIndex, final boolean value);

}
