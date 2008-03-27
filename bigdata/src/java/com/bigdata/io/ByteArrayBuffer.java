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

import it.unimi.dsi.fastutil.io.RepositionableStream;
import it.unimi.dsi.mg4j.io.OutputBitStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

/**
 * A view on a mutable byte[] that may be extended.
 * <p>
 * Note: This class is designed for uses where {@link ByteBuffer} is not
 * appropriate, primarily those which require transparent extension of the
 * backing buffer. Also note that the facility for native data operations in
 * {@link ByteBuffer} is not meaningful unless you are going to read or write
 * directly on the file system or network channels - and we are always reading
 * and writing on a buffered disk cache rather than directly on the channel.
 * <p>
 * Note: This class implements {@link OutputStream} so that it may be wrapped by
 * an {@link OutputBitStream}. Likewise it implements
 * {@link RepositionableStream} so that you can rewind the
 * {@link OutputBitStream}.
 * <p>
 * Note: The concept of {@link #limit()} and {@link #capacity()} are slightly
 * different than for {@link ByteBuffer} since the limit and the capacity may be
 * transparently extended on write. However, neither the limit nor the capacity
 * will be extended on read (this prevents you from reading bad data). The
 * relative <i>put</i> operations always set the limit to the position as a
 * post-condition of the operation. The absolute get/put operations ignore the
 * limit entirely.
 * 
 * @todo consider method signature conformance with {@link ByteBuffer} for ease
 *       of migrating code (misaligned only on {@link #getByte()}/{@link #putByte(byte)},
 *       and {@link #pos()}).
 * 
 * @todo packed coding for short, int, and long only makes sense in the context
 *       of variable length fields. should those methods be introduced here or
 *       in a derived class?
 * 
 * @todo add put/get char methods?
 * 
 * @todo consider an offset (aka base) for the absolute positioning so that a
 *       view on a larger array may be used.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ByteArrayBuffer extends OutputStream implements IByteArrayBuffer,
        RepositionableStream {

    protected static Logger log = Logger.getLogger(ByteArrayBuffer.class);

    /**
     * The default capacity of the buffer.
     */
    final public static int DEFAULT_INITIAL_CAPACITY = 1024;

    /**
     * The buffer. This is re-allocated whenever the capacity of the buffer is
     * too small and reused otherwise.
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
     * Creates a buffer with an initial capacity of <code>1024</code> bytes.
     * The position and the read limit will zero. The capacity of the buffer
     * will be automatically extended as required.
     */
    public ByteArrayBuffer() {
        
        this(DEFAULT_INITIAL_CAPACITY);
        
    }
    
    /**
     * Creates a buffer with the specified initial capacity. The position and
     * the read limit will be zero. The capacity of the buffer will be
     * automatically extended as required.
     * 
     * @param initialCapacity
     *            The initial capacity.
     */
    public ByteArrayBuffer(final int initialCapacity) {
        
        this(//
                0, // pos
                0, // readLimit
                new byte[assertNonNegative("initialCapacity", initialCapacity)]//
        );
        
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

    /**
     * Return a copy of the data written on the buffer (the bytes in [0:pos]).
     * 
     * @return A new array containing data in the buffer.
     * 
     * @see #wrap()
     * 
     * @todo this returns the data in [0:pos], which is essentially the data
     *       written on the buffer using relative put operations but without
     *       requiring a {@link #flip()} to set the pos to zero while leaving
     *       the read limit alone. that is at odds with the rest of the relative
     *       api but there is a lot of use of this method already. perhaps it
     *       can be moved to the {@link DataOutputBuffer} where it would share
     *       semantics with {@link ByteArrayOutputStream#toByteArray()}?
     */
    final public byte[] toByteArray() {
        
        byte[] tmp = new byte[this.pos];
        
        System.arraycopy(buf, 0, tmp, 0, this.pos);
        
        return tmp;
        
    }

    /*
     * Sequential operations (position, limit, capacity)
     */

    /**
     * A non-negative integer specifying the #of bytes of data in the buffer
     * that contain valid data starting from position zero(0).
     */
    protected int pos = 0;

    /**
     * The read limit (there is no write limit on the buffer since the capacity
     * will be automatically extended on overflow).
     */
    protected int limit;
    
    /**
     * An optional mark to which the buffer can be rewound and <code>0</code>
     * if the mark has never been set.
     */
    private int mark = 0;
    
    /**
     * Create a new buffer backed by the given array. The initial capacity will
     * be the size of the given byte[]. The mark will be zero. The capacity of
     * the buffer will be automatically extended as required.
     * <p>
     * Note: The buffer reference is used directly rather than making a copy of
     * the data.
     * 
     * @param pos
     *            The initial {@link #pos() position}.
     * @param limit
     *            The initial {@link #limit()}.
     * @param buf
     *            The backing byte[].
     */
    public ByteArrayBuffer(final int pos, final int limit, final byte[] buf) {

        if (pos < 0)
            throw new IllegalArgumentException("pos<0");
        
        if (pos > limit)
            throw new IllegalArgumentException("pos>limit");

        if (limit > buf.length)
            throw new IllegalArgumentException("limit>buf.length");

        if (buf == null)
            throw new IllegalArgumentException("buf");
        
        this.buf = buf;

        this.pos = pos;
        
        this.limit = limit;

    }
    
    /**
     * The #of bytes remaining in the buffer for relative read operations (limit -
     * pos).
     */
    final public int remaining() {
       
        return limit - pos;
        
    }

    /**
     * The current position in the buffer.
     */
    final public int pos() {
        
        return pos;
        
    }

    /**
     * Set the position in the buffer (does not change the limit).
     * 
     * @param pos
     *            The new position, must be in [0:{@link #capacity()}).
     * 
     * @return The old position.
     * 
     * @throws IllegalArgumentException
     *             if <i>pos</i> is less than ZERO (0).
     * @throws IllegalArgumentException
     *             if <i>pos</i> is greater than or equal to the current
     *             capacity of the buffer.
     */
    final public int pos(final int pos) {

        if (pos < 0 || pos >= buf.length)
            throw new IllegalArgumentException();

        int v = this.pos;

        this.pos = pos;

        return v;

    }

    /**
     * The read limit (there is no write limit on the buffer since the capacity
     * will be automatically extended on overflow).
     */
    final public int limit() {
        
        return limit;
        
    }

    /**
     * Sets the position to zero but leaves the read limit at the old position.
     * After invoking this method you can use relative get methods to read all
     * data up to the read limit.
     */
    final public void flip() {
        
        /*
         * @todo remove this assertion -- it's here to be triggered if relative
         * put methods fail to update the limit with the position, but it is not
         * an absolute requirement since you can change the position
         * independently of the limit using pos().
         */  
        assert limit == pos : "pos="+pos+", limit="+limit;
        
        pos = 0;
        
    }
    
    /**
     * Prepares the buffer for new data by resetting the position and limit to
     * zero.
     * 
     * @return This buffer.
     */
    public ByteArrayBuffer reset() {
        
        pos = limit = 0;
        
        return this;
        
    }

    /**
     * Ensure that at least <i>len</i> bytes are free in the buffer. The
     * {@link #buf buffer} may be grown by this operation but it will not be
     * truncated.
     * <p>
     * This operation is equivilent to
     * 
     * <pre>
     * ensureCapacity(this.len + len)
     * </pre>
     * 
     * and the latter is often used as an optimization.
     * 
     * @param len
     *            The minimum #of free bytes.
     */
    final public void ensureFree(final int len) {
        
        ensureCapacity(this.pos + len);
        
    }

    /**
     * Sets the mark.
     * 
     * @return the old mark (initially zero).
     */
    final public int mark() {
        
        final int tmp = mark;
        
        mark = pos;
        
        return tmp;
        
    }

    /**
     * Rewinds the buffer to the mark.  Does not change the limit.
     * 
     * @return The new {@link #pos()}.
     */
    final public int rewind() {
        
        pos = mark;
        
        return pos;
        
    }

    /**
     * Relative copy of data into <i>this</i> buffer. 
     * 
     * @param src
     *            The source.
     * 
     * @return The #of bytes copied.
     */
    final public int copy(ByteBuffer src) {
        
        final int n = src.remaining();

        if (n > 0) {

            ensureFree(n);

            src.get(buf, pos, n);

            this.pos += n;

            this.limit = this.pos;
            
        }
        
        return n;
        
    }
    
    /**
     * Wraps up a reference to the data in a {@link ByteBuffer} between the
     * position and the limit.
     * 
     * @return A {@link ByteBuffer} encapsulating a reference to the data in the
     *         current buffer. The data will be overwritten if {@link #reset()}
     *         is invoked followed by any operations that write on the buffer.
     */
    final public ByteBuffer wrap() {

        return ByteBuffer.wrap(buf, pos, limit);
        
    }

    /**
     * Relative <i>put</i> method for writing a byte[] on the buffer.
     * 
     * @param b
     *            The byte[].
     */
    final public void put(final byte[] b) {

        put(this.pos/*pos*/, b, 0, b.length);
        
        this.pos += b.length;

        this.limit = this.pos;
        
    }
    
    /**
     * Relative <i>put</i> method for writing a byte[] on the buffer.
     * 
     * @param b
     *            The byte[].
     * @param off
     *            The offset of the first byte in <i>b</i> to be written on the
     *            buffer.
     * @param len
     *            The #of bytes in <i>b</i> to be written on the buffer.
     */
    final public void put(final byte[] b, final int off, final int len) {

        put(this.pos/*pos*/, b, off, len);
        
        this.pos += len;
        
        this.limit = pos;
        
    }
    
    /**
     * Relative <i>put</i> method for writing a byte value.
     * 
     * @param v
     *            The value.
     */
    final public void putByte(final byte v) {

        putByte(pos, v);
        
        limit = ++pos;

    }

    /**
     * Relative <i>get</i> method for reading a byte value.
     * 
     * @return The value.
     * 
     * @exception IndexOutOfBoundsException
     *                if the position is greater than or equal to the limit.
     */
    final public byte getByte() {
        
        if (pos >= limit)
            throw new IndexOutOfBoundsException();
        
        final byte v = getByte(pos);
        
        pos++;
        
        return v;
        
    }
    
    final public void putShort(final short v) {

        putShort(pos, v);
        
        pos += 2;
        
        limit = pos;

    }

    final public short getShort() {
        
        if (pos +2 >= limit)
            throw new IndexOutOfBoundsException();

        final short v = getShort(pos);
        
        pos += 2;
        
        return v;
        
    }
    
    final public void putInt(final int v) {

        putInt(pos, v);
        
        pos += 4;

        limit = pos;
        
    }
    
    final public int getInt() {
        
        if (pos + 4 >= limit)
            throw new IndexOutOfBoundsException();
        
        final int v = getInt(pos);
        
        pos += 4;
        
        return v;
        
    }
    
    final public void putFloat(final float v) {

        putFloat(pos, v);
        
        pos += 4;

        limit = pos;
        
    }

    final public float getFloat() {
        
        if (pos + 4 >= limit)
            throw new IndexOutOfBoundsException();

        final float v = getFloat(pos);
        
        pos += 4;
        
        return v;
        
    }
    
    final public void putLong(final long v) {

        putLong(pos, v);
        
        pos += 8;
        
        limit = pos;

    }

    final public long getLong() {
        
        if (pos + 8 >= limit)
            throw new IndexOutOfBoundsException();
        
        final long v = getLong(pos);
        
        pos += 8;
        
        return v;
        
    }
    
    final public void putDouble(final double v) {

        putDouble(pos, v);
        
        pos += 8;
        
        limit = pos;

    }

    final public double getDouble() {
        
        if (pos + 8 >= limit)
            throw new IndexOutOfBoundsException();
        
        final double v = getDouble(pos);
        
        pos += 8;
        
        return v;
        
    }
    
    /*
     * Pack unsigned long integer.
     */
    
    /**
     * Packs a non-negative long value into the minimum #of bytes in which the
     * value can be represented and writes those bytes onto the output stream.
     * The first byte determines whether or not the long value was packed and,
     * if packed, how many bytes were required to represent the packed long
     * value. When the high bit of the first byte is a one (1), then the long
     * value could not be packed and the long value is found by clearing the
     * high bit and interpreting the first byte plus the next seven (7) bytes as
     * a long. Otherwise the next three (3) bits are interpreted as an unsigned
     * integer giving the #of bytes (nbytes) required to represent the packed
     * long value. To recover the long value the high nibble is cleared and the
     * first byte together with the next nbytes are interpeted as an unsigned
     * long value whose leading zero bytes were not written.
     * 
     * <pre>
     *    
     * [0|1|2|3|4|5|6|7]
     *  1 - - -   nbytes = 8, clear high bit and interpret this plus the next 7 bytes as a long.
     *  0 1 1 1   nbytes = 7, clear high nibble and interpret this plus the next 6 bytes as a long. 
     *  0 1 1 0   nbytes = 6, clear high nibble and interpret this plus the next 5 bytes as a long. 
     *  0 1 0 1   nbytes = 5, clear high nibble and interpret this plus the next 4 bytes as a long.
     *  0 1 0 0   nbytes = 4, clear high nibble and interpret this plus the next 3 bytes as a long.
     *  0 0 1 1   nbytes = 3, clear high nibble and interpret this plus the next 3 bytes as a long.
     *  0 0 1 0   nbytes = 2, clear high nibble and interpret this plus the next byte as a long.
     *  0 0 0 1   nbytes = 1, clear high nibble.  value is the low nibble.
     *  
     * </pre>
     * 
     * @param v The unsigned long value.
     * 
     * @return The #of bytes onto which the unsigned long value was packed.
     */
    final public int packLong( final long v ) throws IOException {
        
        /*
         * You can only pack non-negative long values with this method.
         */
        
        if (v < 0) {
            
            throw new IllegalArgumentException("negative value: v=" + v);
            
        }
        
        /*
         * If the high byte is non-zero then we will write the value as a normal
         * long and return nbytes == 8. This case handles large positive long
         * values.
         */
        if( ( v >> 56 ) != 0 ) {
            pbuf[0] = ( (byte)((0xff & (v >> 56))|0x80) ); // note: set the high bit.
            pbuf[1] = ( (byte)(0xff & (v >> 48)) );
            pbuf[2] = ( (byte)(0xff & (v >> 40)) );
            pbuf[3] = ( (byte)(0xff & (v >> 32)) );
            pbuf[4] = ( (byte)(0xff & (v >> 24)) );
            pbuf[5] = ( (byte)(0xff & (v >> 16)) );
            pbuf[6] = ( (byte)(0xff & (v >>  8)) );
            pbuf[7] = ( (byte)(0xff & v) );
            put(pbuf, 0, 8);
            return 8;
        }
        
        // #of nibbles required to represent the long value.
        final int nnibbles = getNibbleLength( v );

        /*
         * Is [nnibbles] even? (If it is even then we need to pad out an extra
         * zero nibble in the first byte.)
         */
        final boolean evenNibbleCount = ( nnibbles == ( ( nnibbles >> 1 ) << 1 ) );
        
        // #of bytes required to represent the long value (plus the header nibble).
        final int nbytes = ( ( nnibbles +1 ) >> 1 ) + (evenNibbleCount?1:0);
        
        int nwritten = 0;
        
        if( evenNibbleCount ) {
            
            /*
             * An even nibble count requires that we pad the low nibble of the
             * first byte with zeros.
             */
        
            // header byte. low nibble is empty.
            byte b = (byte) ( nbytes << 4 );
            
            pbuf[nwritten++] = b;
            
            // remaining bytes containing the packed value.
            for( int i=(nnibbles-2)<<2; i>=0; i-=8 ) {
                
                b = (byte) (0xff & (v >> i));
                
                pbuf[nwritten++] = b;
                
            }
            
        } else {
            
            /*
             * An odd nibble count means that we pack the first nibble of the
             * long value into the low nibble of the header byte. In this case
             * the first nibble will always be the low nibble of the first
             * non-zero byte in the long value (the high nibble of that byte
             * must be zero since there is an odd nibble count).
             */
            
            byte highByte = (byte) (0xff & (v >> ((nbytes-1)*8) ));
            
            byte b = (byte) ( ( nbytes << 4 ) | highByte );
            
            pbuf[nwritten++] = b;
            
            for( int i=(nnibbles-3)<<2; i>=0; i-=8 ) {
            
                b = (byte) (0xff & (v >> i));
                
                pbuf[nwritten++] = b;
                
            }
            
        }
        
        put(pbuf,0,nwritten);
        
        return nwritten;
        
    }

    /**
     * Private buffer for packing long integers.
     */
    private byte[] pbuf = new byte[8];
    
    /**
     * Return the #of non-zero nibbles, counting from the first non-zero nibble
     * in the long value. A value of <code>0L</code> is considered to be one
     * nibble for our purposes.
     * 
     * @param v
     *            The long value.
     * 
     * @return The #of nibbles in [1:16].
     */
    static protected final int getNibbleLength( final long v )
    {

        for( int i=56, j=16; i>=0; i-=8, j-=2 ) {
            
            if( (0xf0 & (v >> i)) != 0 ) return j;
            
            if( (0x0f & (v >> i)) != 0 ) return j-1;
            
        }
        
        if (v != 0)
            throw new AssertionError("v=" + v);
     
        /*
         * value is zero, which is considered to be one nibble for our purposes.
         */

        return 1;
        
    }
    
    /*
     * Pack unsigned short integer.
     */
    
    /**
     * Packs a non-negative short value into one or two bytes and writes them on
     * <i>os </i>. A short in [0:127] is packed into one byte. Larger values are
     * packed into two bytes. The high bit of the first byte is set if the value
     * was packed into two bytes. If the bit is set, clear the high bit, read
     * the next byte, and interpret the two bytes as a short value. Otherwise
     * interpret the byte as a short value.
     * 
     * @param v The unsigned short integer.
     * 
     * @return The #of bytes into which the value was packed.
     */ 
    final public int packShort( final short v ) throws IOException
    {
    
        /*
         * You can only pack non-negative values with this method.
         */
        if( v < 0 ) {
            throw new IllegalArgumentException( "negative value: v="+v );
        }
        if( v > 127 ) {
            // the value requires two bytes.
            if (pos + 2 > buf.length)
                ensureCapacity(pos + 2);
            buf[pos++] = ( (byte)((0xff & (v >> 8))|0x80) ); // note: set the high bit.
            buf[pos++] = ( (byte)(0xff & v) );
            return 2;
        } else {
            // the value fits in one byte.
            if (pos + 1 > buf.length)
                ensureCapacity(pos + 1);
            buf[pos++] = ( (byte)(0xff & v) );
            return 1;
        }
    }

    /*
     * OutputStream integration.  Operations all set the read limit to the
     * position as a post-condition (they are treated as relative puts).
     */
    
    final public void write(final int b) throws IOException {

        if (pos + 1 > buf.length)
            ensureCapacity(pos + 1);

        buf[pos++] = (byte) (b & 0xff);
        
        limit = pos;

    }

    final public void write(final byte[] b) throws IOException {

        write(b, 0, b.length);

    }

    final public void write(final byte[] b, final int off, final int len)
            throws IOException {

        if (len == 0)
            return;

        ensureFree(len);

        System.arraycopy(b, off, buf, this.pos, len);

        this.pos += len;

        this.limit = this.pos;

    }

    /*
     * RepositionableStream.
     * 
     * Note: This interface allows us to use OutputBitStream and reset the
     * position to zero (0L) so that we can reuse the buffer, e.g., to serialize
     * nodes in the B+Tree.
     */

    public long position() throws IOException {
    
        return (int) pos;
        
    }
    
    public void position(long v) throws IOException {

        if (v < 0 || v > buf.length) {

            throw new IOException();

        }

        this.pos = (int) v;
        
    }

}
