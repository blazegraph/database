/*

 Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Dec 23, 2007
 */

package com.bigdata.io;

import it.unimi.dsi.io.InputBitStream;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;

/**
 * Efficient absolute get/put operations on a slice of a byte[]. This class is
 * not thread-safe under mutation because the operations are not atomic.
 * Concurrent operations on the same region of the slice can reveal partial
 * updates. This class is abstract. A concrete implementation need only
 * implement {@link #array()} and an appropriate constructor. This allows for
 * use cases where the backing byte[] is extensible. E.g., a fixed slice onto an
 * extensible {@link ByteArrayBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractFixedByteArrayBuffer implements IFixedByteArrayBuffer {

    protected static final Logger log = Logger.getLogger(AbstractFixedByteArrayBuffer.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * The start of the slice in the {@link #array()}.
     */
    private final int off;
    
    /**
     * The length of the slice in the {@link #array()}.
     */
    private final int len;
    
    final public int off() {
        
        return off;
        
    }
    
    final public int len() {
        
        return len;
        
    }

    /**
     * A slice wrapping the entire array.
     * 
     * @param array
     *            The array.
     */
    public static FixedByteArrayBuffer wrap(final byte[] array) {

        return new FixedByteArrayBuffer(array, 0/* off */, array.length/* len */);

    }

    /**
     * Protected constructor used to create a slice. The caller is responsible
     * for verifying that the slice is valid for the backing byte[] buffer.
     * 
     * @param off
     *            The offset of the start of the slice.
     * @param len
     *            The length of the slice.
     */
    protected AbstractFixedByteArrayBuffer(final int off, final int len) {

        if (off < 0)
            throw new IllegalArgumentException("off<0");

        if (len < 0)
            throw new IllegalArgumentException("len<0");

        this.off = off;

        this.len = len;

    }

    /*
     * Absolute get/put operations.
     */
    
    /**
     * Verify that an operation starting at the specified offset into the slice
     * and having the specified length is valid against the slice.
     * 
     * @param aoff
     *            The offset into the slice.
     * @param alen
     *            The #of bytes to be addressed starting from that offset.
     * 
     * @return <code>true</code>.
     * 
     * @throws IllegalArgumentException
     *             if the operation is not valid.
     */
    protected boolean rangeCheck(final int aoff, final int alen) {

        if (aoff < 0)
            throw new IndexOutOfBoundsException();

        if (alen < 0)
            throw new IndexOutOfBoundsException();

        if ((aoff + alen) > len) {

            /*
             * The operation run length at that offset would extend beyond the
             * end of the slice.
             */
            
            throw new IndexOutOfBoundsException();
            
        }

        return true;
        
    }

    final public void put(final int pos, final byte[] b) {

        put(pos, b, 0, b.length);

    }

    final public void put(final int dstoff,//
            final byte[] src, final int srcoff, final int srclen) {

        assert rangeCheck(dstoff, srclen);
        
        System.arraycopy(src, srcoff, array(), off + dstoff, srclen);

    }

    final public void get(final int srcoff, final byte[] dst) {
        
        get(srcoff, dst, 0/* dstoff */, dst.length);
        
    }
    
    final public void get(final int srcoff, final byte[] dst, final int dstoff,
            final int dstlen) {

        assert rangeCheck(srcoff, dstlen);

        System.arraycopy(array(), off + srcoff, dst, dstoff, dstlen);

    }

    final public void putByte(int pos, final byte v) {

        assert rangeCheck(pos, 1);

        pos += off;
        
        array()[pos] = v;

    }

    final public byte getByte(int pos) {

        assert rangeCheck(pos, 1);

        pos += off;
        
        return array()[pos];
        
    }
    
    final public void putShort(int pos, final short v) {

        assert rangeCheck(pos, 2);

        pos += off;
        
        // big-endian
        array()[pos++] = (byte) (v >>> 8);
        array()[pos  ] = (byte) (v >>> 0);

    }

    final public short getShort(int pos) {

        assert rangeCheck(pos, 2);

        pos += off;
        
        short v = 0;
        
        // big-endian.
        v += (0xff & array()[pos++]) <<  8;
        v += (0xff & array()[pos  ]) <<  0;

        return v;

    }

    final public void putInt(int pos, final int v){

        assert rangeCheck(pos, 4);
        
        pos += off;

        array()[pos++] = (byte) (v >>> 24);
        array()[pos++] = (byte) (v >>> 16);
        array()[pos++] = (byte) (v >>> 8);
        array()[pos  ] = (byte) (v >>> 0);

    }

    final public int getInt( int pos ) {

        assert rangeCheck(pos, 4);

        pos += off;
        
        int v = 0;
        
        // big-endian.
        v += (0xff & array()[pos++]) << 24;
        v += (0xff & array()[pos++]) << 16;
        v += (0xff & array()[pos++]) <<  8;
        v += (0xff & array()[pos  ]) <<  0;

        return v;
        
    }
    
    final public void putFloat(final int pos, final float f) {

        putInt( pos, Float.floatToIntBits(f) );

    }

    final public float getFloat( final int pos ) {
        
        return Float.intBitsToFloat( getInt( pos ) );
        
    }
    
    final public void putLong( int pos, final long v) {

        assert rangeCheck(pos, 8);
        
        pos += off;

        // big-endian.
        array()[pos++] = (byte) (v >>> 56);
        array()[pos++] = (byte) (v >>> 48);
        array()[pos++] = (byte) (v >>> 40);
        array()[pos++] = (byte) (v >>> 32);
        array()[pos++] = (byte) (v >>> 24);
        array()[pos++] = (byte) (v >>> 16);
        array()[pos++] = (byte) (v >>> 8);
        array()[pos  ] = (byte) (v >>> 0);

    }

    final public void putDouble( final int pos, final double d) {

        putLong( pos, Double.doubleToLongBits(d) );
        
    }

    final public long getLong( int pos ) {

        assert rangeCheck(pos, 8);
        
        pos += off;
        
        long v = 0L;
        
        // big-endian.
        v += (0xffL & array()[pos++]) << 56;
        v += (0xffL & array()[pos++]) << 48;
        v += (0xffL & array()[pos++]) << 40;
        v += (0xffL & array()[pos++]) << 32;
        v += (0xffL & array()[pos++]) << 24;
        v += (0xffL & array()[pos++]) << 16;
        v += (0xffL & array()[pos++]) <<  8;
        v += (0xffL & array()[pos  ]) <<  0;

        return v;
        
    }
    
    final public double getDouble( final int pos ) {
        
        return Double.longBitsToDouble( getLong( pos ) );
        
    }

    final public byte[] toByteArray() {
        
        final byte[] tmp = new byte[len];

        System.arraycopy(array(), off/* srcPos */, tmp/* dst */,
                0/* destPos */, len);
        
        return tmp;
        
    }

    final public ByteBuffer wrap() {

        return ByteBuffer.wrap(array(), off, len);
        
    }

    final public boolean getBit(final long bitIndex) {

        assert rangeCheck(BytesUtil.byteIndexForBit(bitIndex), 1);

        // convert off() to a bit offset and then address the bit at the
        // caller's index
        return BytesUtil.getBit(array(), (off << 3) + bitIndex);

    }

    final public boolean setBit(final long bitIndex, final boolean value) {

        assert rangeCheck(BytesUtil.byteIndexForBit(bitIndex), 1);

        // convert off() to a bit offset and then address the bit at the
        // caller's index
        return BytesUtil.setBit(array(), (off << 3) + bitIndex, value);

    }

    /**
     * Return an input stream that will read from the slice.
     */
    public DataInputBuffer getDataInput() {

        return new DataInputBuffer(array(), off, len);
        
    }
    
    /**
     * Return a bit stream that will read from the slice.
     */
    public InputBitStream getInputBitStream() {

        /*
         * We have to double-wrap the buffer to ensure that it reads from just
         * the slice since InputBitStream does not have a constructor which
         * accepts a slice of the form (byte[], off, len). [It would be nice if
         * InputBitStream handled the slice natively since should be faster per
         * its own javadoc.]
         * 
         * Note: The reflection test semantics are not quite what I would want.
         * If you specify [false] then the code does not even test for the
         * RepositionableStream interface. Ideally, it would always do that but
         * skip the reflection on the getChannel() method when it was false.
         */
//        return new InputBitStream(getDataInput(), 0/* unbuffered */, true/* reflectionTest */);
        
        /*
         * This directly wraps the slice.  This is much faster.
         */
        return new InputBitStream(array(), off, len);

    }

    /**
     * Write the slice on the output stream.
     * 
     * @param os
     *            The output stream.
     * 
     * @throws IOException
     */
    final public void writeOn(final OutputStream os) throws IOException {
        
        os.write(array(), off, len);
        
    }
    
    /**
     * Write the slice on the output stream.
     * 
     * @param os
     *            The output stream.
     *            
     * @throws IOException
     */
    final public void writeOn(final DataOutput out) throws IOException {
        
        out.write(array(), off, len);

    }

    /**
     * Write part of the slice on the output stream.
     * 
     * @param os
     *            The output stream.
     *            
     * @throws IOException
     */
    final public void writeOn(final OutputStream os, final int aoff,
            final int alen) throws IOException {

        if (aoff < 0) // check starting pos.
            throw new IllegalArgumentException();

        if (aoff + alen > this.len) // check run length.
            throw new IllegalArgumentException();

        os.write(array(), off + aoff, alen);

    }

    public AbstractFixedByteArrayBuffer slice(final int aoff, final int alen) {

        assert rangeCheck(aoff, alen);

        return new AbstractFixedByteArrayBuffer(off() + aoff, alen) {

            public byte[] array() {

                return AbstractFixedByteArrayBuffer.this.array();

            }

        };

    }

}
