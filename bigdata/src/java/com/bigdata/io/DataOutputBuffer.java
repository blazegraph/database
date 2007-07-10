/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Apr 7, 2007
 */

package com.bigdata.io;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.nio.ByteBuffer;

/**
 * Fast special purpose serialization onto a managed byte[] buffer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataOutputBuffer implements DataOutput {

    /**
     * The default capacity of the buffer.
     */
    final public static int DEFAULT_INITIAL_CAPACITY = 1024;

    /**
     * A non-negative integer specifying the #of bytes of data in the buffer
     * that contain valid data starting from position zero(0).
     */
    public int len;
    
    /**
     * The buffer. This is re-allocated whenever the capacity of the buffer
     * is too small and reused otherwise.
     */
    public byte[] buf;
    
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
    public DataOutputBuffer() {
        
        this(DEFAULT_INITIAL_CAPACITY);
        
    }
    
    /**
     * @param initialCapacity
     *            The initial capacity of the internal byte[].
     */
    public DataOutputBuffer(final int initialCapacity) {
        
        this(0, new byte[assertNonNegative("initialCapacity", initialCapacity)]);
        
    }

    /**
     * @param len
     *            The #of bytes of data in the provided buffer.
     * @param buf
     *            The buffer, with <i>len</i> pre-existing bytes of valid data.
     *            The buffer reference is used directly rather than making a
     *            copy of the data.
     */
    public DataOutputBuffer(final int len, byte[] buf) {

        if (len < 0)
            throw new IllegalArgumentException("len");

        if (buf == null)
            throw new IllegalArgumentException("buf");

        if (len > buf.length)
            throw new IllegalArgumentException("len>buf.length");

        this.len = len;

        this.buf = buf;

    }

    /**
     * Reads the entire input stream into the buffer. The data are then
     * available in {@link #buf} from position 0 (inclusive) through position
     * {@link #len} (exclusive).
     */
    public DataOutputBuffer(InputStream in) throws IOException {

        this();
        
        // temporary buffer for read from the input stream.
        final byte[] b = new byte[remaining()];
        
        while(true) {
            
            int nread = in.read(b);
            
            if( nread == -1 ) break;
            
            write(b,0,nread);
            
        }
        
    }
    
    /**
     * Reads the entire input stream into the buffer. The data are then
     * available in {@link #buf} from position 0 (inclusive) through position
     * {@link #len} (exclusive).
     */
    public DataOutputBuffer(ObjectInput in) throws IOException {

        this();

        // temporary buffer for read from the input stream.
        byte[] b = new byte[remaining()];
        
        while(true) {
            
            int nread = in.read(b);
            
            if( nread == -1 ) break;
            
            write(b,0,nread);
            
        }
        
    }
    
    /**
     * The current position in the buffer.
     */
    final public int position() {
        
        return len;
        
    }

    /**
     * Set the position in the buffer.
     * 
     * @param pos
     *            The new position, must be in [0:capacity).
     * 
     * @return The old position.
     */
    final public int position(final int pos) {

        if(pos<0 || pos>=buf.length) throw new IllegalArgumentException();
        
        int v = this.len;
        
        this.len = pos;
        
        return v;
        
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
        
        ensureCapacity(this.len + len );
        
    }

    /**
     * Ensure that the buffer capacity is a least <i>capacity</i> total bytes.
     * The {@link #buf buffer} may be grown by this operation but it will not be
     * truncated.
     * 
     * @param capacity
     *            The minimum #of bytes in the buffer.
     */
    final public void ensureCapacity(final int capacity) {
        
        if(capacity<0) throw new IllegalArgumentException();
//        assert capacity >= 0;
        
        final int overflow = capacity - buf.length;
        
        if(overflow>0) {
        
            /*
             * extend to at least the target capacity.
             */
            final byte[] tmp = new byte[extend(capacity)];
            
            // copy only the defined bytes.
            System.arraycopy(buf, 0, tmp, 0, this.len);
            
            buf = tmp;
            
        }

    }
    
    /**
     * The #of bytes remaining in the buffer before it would overflow.
     */
    final public int remaining() {
       
        return buf.length - len;
        
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
     */
    protected int extend(final int required) {

        int capacity = Math.max(required, buf.length * 2);

        System.err.println("Extending buffer to capacity=" + capacity
                + " bytes.");

        return capacity;
        
    }

    /**
     * Return a copy of the buffer.
     * 
     * @return A new array containing data in the buffer.
     * 
     * @see #wrap()
     */
    final public byte[] toByteArray() {
        
        byte[] tmp = new byte[this.len];
        
        System.arraycopy(buf, 0, tmp, 0, this.len);
        
        return tmp;
        
    }

    /**
     * Wraps up a reference to the data in a {@link ByteBuffer}
     * 
     * @return A {@link ByteBuffer} encapsulating a reference to the data in the
     *         current buffer. The data will be overwritten if {@link #reset()}
     *         is invoked followed by any operations that write on the buffer.
     */
    final public ByteBuffer wrap() {

        return ByteBuffer.wrap(buf, 0, len);
        
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
     * Prepares the buffer for new data by resetting the length to zero.
     * 
     * @return This buffer.
     */
    final public DataOutputBuffer reset() {
        
        len = 0;
        
        return this;
        
    }

    /**
     * Read <i>len</i> bytes into the buffer.
     * 
     * @param in
     *            The input source.
     * @param len
     *            The #of bytes to read.
     *            
     * @throws EOFException
     *             if the EOF is reached before <i>len</i> bytes have been
     *             read.
     * @throws IOException
     *             if an I/O error occurs.
     * 
     * @todo read many bytes at a time.
     * @todo write test.
     */
    final public void write(DataInput in, final int len) throws IOException {

        ensureCapacity(len);
        
        int c = 0;
        
        byte b;
        
        while (c < len) {

            b = in.readByte();

            buf[this.len++] = (byte) (b & 0xff);
            
            c++;

        }

    }
    
    final public void write(final int b) throws IOException {

        if (len + 1 > buf.length)
            ensureCapacity(len + 1);

        buf[len++] = (byte) (b & 0xff);

    }

    final public void write(final byte[] b) throws IOException {

        write(b,0,b.length);

    }

    final public void write(final byte[] b, final int off, final int len) throws IOException {

      ensureFree(len);
      
      System.arraycopy(b, off, buf, this.len, len);
      
      this.len += len;

    }

    final public void writeBoolean(final boolean v) throws IOException {

        if (len + 1 > buf.length)
            ensureCapacity(len + 1);

        buf[len++] = v ? (byte)1 : (byte)0;

    }

    final public void writeByte(final int v) throws IOException {

        if (len + 1 > buf.length)
            ensureCapacity(len + 1);

        buf[len++] = (byte) (v & 0xff);

    }

    final public void writeDouble(final double d) throws IOException {

        if (len + 8 > buf.length)
            ensureCapacity(len + 8);

        long v = Double.doubleToLongBits(d);

        // big-endian.
        buf[len++] = (byte) (v >>> 56);
        buf[len++] = (byte) (v >>> 48);
        buf[len++] = (byte) (v >>> 40);
        buf[len++] = (byte) (v >>> 32);
        buf[len++] = (byte) (v >>> 24);
        buf[len++] = (byte) (v >>> 16);
        buf[len++] = (byte) (v >>> 8);
        buf[len++] = (byte) (v >>> 0);

    }

    final public void writeFloat(final float f) throws IOException {

        if (len + 4 > buf.length)
            ensureCapacity(len + 4);

        int v = Float.floatToIntBits(f);

        buf[len++] = (byte) (v >>> 24);
        buf[len++] = (byte) (v >>> 16);
        buf[len++] = (byte) (v >>> 8);
        buf[len++] = (byte) (v >>> 0);

    }

    final public void writeInt(final int v) throws IOException {

        if (len + 4 > buf.length)
            ensureCapacity(len + 4);

        buf[len++] = (byte) (v >>> 24);
        buf[len++] = (byte) (v >>> 16);
        buf[len++] = (byte) (v >>> 8);
        buf[len++] = (byte) (v >>> 0);

    }

    final public void writeLong(long v) throws IOException {

        if (len + 8 > buf.length)
            ensureCapacity(len + 8);

        // big-endian.
        buf[len++] = (byte) (v >>> 56);
        buf[len++] = (byte) (v >>> 48);
        buf[len++] = (byte) (v >>> 40);
        buf[len++] = (byte) (v >>> 32);
        buf[len++] = (byte) (v >>> 24);
        buf[len++] = (byte) (v >>> 16);
        buf[len++] = (byte) (v >>> 8);
        buf[len++] = (byte) (v >>> 0);

    }

    final public void writeShort(int v) throws IOException {

        if (len + 2 > buf.length)
            ensureCapacity(len + 2);

        // big-endian
        buf[len++] = (byte) (v >>> 8);
        buf[len++] = (byte) (v >>> 0);

    }

    final public void writeChar(int v) throws IOException {

        if (len + 2 > buf.length)
            ensureCapacity(len + 2);

        buf[len++] = (byte) (v >>> 8);
        buf[len++] = (byte) (v >>> 0);

    }

    public void writeBytes(final String s) throws IOException {

        int len = s.length();
        
        for (int i = 0 ; i < len ; i++) {
            
            write((byte)s.charAt(i));
            
        }

    }

    public void writeChars(final String s) throws IOException {

        int len = s.length();
        
        for (int i = 0 ; i < len ; i++) {
        
            int v = s.charAt(i);
            
            write((v >>> 8) & 0xFF); 
            
            write((v >>> 0) & 0xFF); 
        
        }
        
    }
    
    /**
     * @todo This is not wildly efficient (it would be fine if
     *       DataOutputStream#writeUTF(String str, DataOutput out)} was public)
     *       but the use cases for serializing the nodes and leaves of a btree
     *       do not suggest any requirement for Unicode (if you assume that the
     *       application values are already being serialized as byte[]s - which
     *       is always true when there is a client-server divide). The RDF value
     *       serializer does use this method right now, but that will be client
     *       side code as soon we as refactor to isolate the client and the
     *       server.
     */
    public void writeUTF(final String str) throws IOException {
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        DataOutputStream dos = new DataOutputStream(baos);
        
        dos.writeUTF(str);
        
        dos.flush();
        
        write(baos.toByteArray());

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
            write(pbuf, 0, 8);
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
        
        write(pbuf,0,nwritten);
        
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
            buf[len++] = ( (byte)((0xff & (v >> 8))|0x80) ); // note: set the high bit.
            buf[len++] = ( (byte)(0xff & v) );
            return 2;
        } else {
            // the value fits in one byte.
            buf[len++] = ( (byte)(0xff & v) );
            return 1;
        }
    }

}
