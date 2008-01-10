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

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.bigdata.btree.KeyBuilder;

/**
 * A view on a mutable byte[] that may be extended.
 * <p>
 * Note: This class is designed for uses where {@link ByteBuffer} is not
 * appropriate, primarily those which require transparent extension of the
 * backing buffer. Also note that the facility for native data operations in
 * {@link ByteBuffer} is not meaningful unless you are going to read or write
 * directly on the file system or network channels - and we are always reading
 * and writing on a buffered disk cache rather than directly on the channel.
 * 
 * @todo consider method signature conformance with {@link ByteBuffer} for ease
 *       of migrating code (misaligned only on getByte/putByte).
 * 
 * @todo packed coding for short, int, and long only makes sense in the context
 *       of variable length fields. should those methods be introduced here or
 *       in a derived class?
 * 
 * @todo add put/get char methods?
 * 
 * @todo support insertion sort maintenance on a region with fields of a fixed
 *       length. _if_ it is allowable for the region size to change then an
 *       extension may require updates of other offsets into the byte[]. the
 *       adaptive packed memory array is another extreme in which holes are left
 *       in the buffer to minimize copying.
 * 
 * @todo test suite for get/set methods - refactor from {@link DataOutputBuffer}
 *       and {@link KeyBuilder} test suites.
 * 
 * @todo consider an offset (aka base) for the absolute positioning so that a
 *       view on a larger array may be used.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ByteArrayBuffer {

    protected static Logger log = Logger.getLogger(ByteArrayBuffer.class);

    /**
     * The default capacity of the buffer.
     */
    final public static int DEFAULT_INITIAL_CAPACITY = 1024;

    /**
     * The buffer. This is re-allocated whenever the capacity of the buffer
     * is too small and reused otherwise.
     * 
     * @todo make this protected?
     */
    protected byte[] buf;

    /**
     * The backing byte[] buffer. This is re-allocated whenever the capacity of
     * the buffer is too small and reused otherwise.
     */
    final public byte[] array() {
        
        return buf;
        
    }
    
    /**
     * Throws exception unless the value is non-negative.
     * 
     * @param msg
     *            The exception message.
     * @param v
     *            The value.
     * 
     * @return The value.
     * 
     * @exception IllegalArgumentException
     *                unless the value is non-negative.
     */
    protected static int assertNonNegative(String msg, final int v) {
       
        if(v<0) throw new IllegalArgumentException(msg);
        
        return v;
        
    }
    
    /**
     * Uses an initial buffer capacity of <code>1024</code> bytes.
     */
    public ByteArrayBuffer() {
        
        this(DEFAULT_INITIAL_CAPACITY);
        
    }
    
    /**
     * @param initialCapacity
     *            The initial capacity of the internal byte[].
     */
    public ByteArrayBuffer(final int initialCapacity) {
        
        this(new byte[assertNonNegative("initialCapacity", initialCapacity)]);
        
    }

    /**
     * @param buf
     *            The buffer, which may have pre-existing data. The buffer
     *            reference is used directly rather than making a copy of the
     *            data.
     * 
     * @throws IllegalArgumentException
     *             if the <i>buf</i> is <code>null</code>.
     */
    public ByteArrayBuffer(final byte[] buf) {

        if (buf == null)
            throw new IllegalArgumentException();

        this.buf = buf;

    }
    
    /**
     * Ensure that at least <i>len</i> bytes are free in the buffer starting at
     * <i>pos</i>. The {@link #buf buffer} may be grown by this operation but
     * it will not be truncated.
     * <p>
     * This operation is equivilent to
     * 
     * <pre>
     * ensureCapacity(pos + len)
     * </pre>
     * 
     * and the latter is often used as an optimization.
     * 
     * @param pos
     *            The position in the buffer.
     * @param len
     *            The minimum #of free bytes.
     */
    final public void ensureFree(final int pos, final int len) {
        
        ensureCapacity(pos + len);

    }

    /**
     * Ensure that the buffer capacity is a least <i>capacity</i> total bytes.
     * The {@link #buf buffer} may be grown by this operation but it will not be
     * truncated.
     * 
     * @param capacity
     *            The minimum #of bytes in the buffer.
     * 
     * @todo this can be potentially overriden in a derived class to only copy
     *       those bytes up to the current position.
     */
    /*final*/ public void ensureCapacity(final int capacity) {
        
        if(capacity<0) throw new IllegalArgumentException();
//        assert capacity >= 0;
        
        final int overflow = capacity - buf.length;
        
        if(overflow>0) {
        
            /*
             * extend to at least the target capacity.
             */
            final byte[] tmp = new byte[extend(capacity)];
            
//            // copy only the defined bytes.
//            System.arraycopy(buf, 0, tmp, 0, this.len);
            
            // copy all bytes to the new byte[].
            System.arraycopy(buf, 0, tmp, 0, buf.length);

            // update the reference to use the new byte[].
            buf = tmp;
            
        }

    }

    /**
     * The capacity of the buffer.
     */
    final public int capacity() {
        
        return buf.length;
        
    }
    
    /**
     * Return the new capacity for the buffer (default is always large enough
     * and will normally double the buffer capacity each time it overflows).
     * 
     * @param required
     *            The minimum required capacity.
     * 
     * @return The new capacity.
     * 
     * @todo this does not need to be final. also, caller's could set the policy
     *       including a policy that refuses to extend the capacity.
     */
    final protected int extend(final int required) {

        int capacity = Math.max(required, buf.length * 2);

        log.info("Extending buffer to capacity=" + capacity + " bytes.");

        return capacity;
        
    }

//    /**
//     * Copy the data from the internal buffer into the supplied buffer.
//     * 
//     * @param b
//     *            A byte[].
//     * 
//     * @exception IndexOutOfBoundsException
//     *                if the supplied buffer is not large enough.
//     */
//    final public void copy(byte[] b) {
//    
//        System.arraycopy(this.buf, 0, b, 0, this.len);
//        
//    }
    
    /**
     * Copy bytes into the buffer starting at the specified position.
     * 
     * @param pos
     *            The position.
     * @param b
     *            The data.
     */
    final public void put(final int pos, //
            final byte[] b) {

        put(pos, b, 0, b.length);

    }

    /**
     * Copy bytes into the buffer starting at the specified position.
     * 
     * @param pos
     *            The position.
     * @param b
     *            The data.
     * @param off
     *            The offset of the 1st byte in the source data to be copied.
     * @param len
     *            The #of bytes to be copied.
     */
    final public void put(final int pos,//
            final byte[] b, final int off, final int len) {

        ensureCapacity(pos + len);
        
        System.arraycopy(b, off, buf, pos, len);

    }

    /*
     * @todo rewrite as get/put bits with pos, bit offset from pos (may be
     * greater than 8), and bit length (must be less than 31 since we have
     * trouble otherwise with the sign bit). The value in/out will be a 32-bit
     * integer.
     */
//    final public void putBoolean(final int pos, final boolean v) {
//
//        if (len + 1 > buf.length)
//            ensureCapacity(len + 1);
//
//        buf[len++] = v ? (byte)1 : (byte)0;
//
//    }

    /**
     * Absolute <i>put</i> method for writing a byte value.
     * 
     * @param v
     *            The value.
     */
    final public void putByte(int pos, final byte v) {

        if (pos + 1 > buf.length)
            ensureCapacity(pos + 1);

        buf[pos] = v;

    }

    final public byte getByte(int pos) {
        
        return buf[pos];
        
    }
    
    final public void putShort(int pos, final short v) {

        if (pos + 2 > buf.length)
            ensureCapacity(pos + 2);

        // big-endian
        buf[pos++] = (byte) (v >>> 8);
        buf[pos  ] = (byte) (v >>> 0);

    }

    final public short getShort(int pos) {

        short v = 0;
        
        // big-endian.
        v += (0xff & buf[pos++]) <<  8;
        v += (0xff & buf[pos  ]) <<  0;

        return v;

    }

    final public void putInt(int pos, final int v){

        if (pos + 4 > buf.length)
            ensureCapacity(pos + 4);

        buf[pos++] = (byte) (v >>> 24);
        buf[pos++] = (byte) (v >>> 16);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos  ] = (byte) (v >>> 0);

    }

    final public int getInt( int pos ) {
        
        int v = 0;
        
        // big-endian. @todo verify 0xffL not required.
        v += (0xff & buf[pos++]) << 24;
        v += (0xff & buf[pos++]) << 16;
        v += (0xff & buf[pos++]) <<  8;
        v += (0xff & buf[pos  ]) <<  0;

        return v;
        
    }
    
    final public void putFloat(int pos, final float f) {

        putInt( pos, Float.floatToIntBits(f) );

    }

    final public float getFloat( int pos ) {
        
        return Float.intBitsToFloat( getInt( pos ) );
        
    }
    
    final public void putLong( int pos, final long v) {

        if (pos + 8 > buf.length)
            ensureCapacity(pos + 8);

        // big-endian.
        buf[pos++] = (byte) (v >>> 56);
        buf[pos++] = (byte) (v >>> 48);
        buf[pos++] = (byte) (v >>> 40);
        buf[pos++] = (byte) (v >>> 32);
        buf[pos++] = (byte) (v >>> 24);
        buf[pos++] = (byte) (v >>> 16);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos  ] = (byte) (v >>> 0);

    }

    final public void putDouble( int pos, final double d) {

        putLong( pos, Double.doubleToLongBits(d) );
        
    }

    final public long getLong( int pos ) {
        
        long v = 0L;
        
        // big-endian.
        v += (0xffL & buf[pos++]) << 56;
        v += (0xffL & buf[pos++]) << 48;
        v += (0xffL & buf[pos++]) << 40;
        v += (0xffL & buf[pos++]) << 32;
        v += (0xffL & buf[pos++]) << 24;
        v += (0xffL & buf[pos++]) << 16;
        v += (0xffL & buf[pos++]) <<  8;
        v += (0xffL & buf[pos  ]) <<  0;

        return v;
        
    }
    
    final public double getDouble( int pos ) {
        
        return Double.longBitsToDouble( getLong( pos ) );
        
    }
    
}
