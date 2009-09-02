/**

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
 * Created on Apr 9, 2007
 */

package com.bigdata.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * A fast implementation of DataInput designed to read from a byte[].
 * 
 * @see DataOutputBuffer
 * 
 * @see DataInputStream
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataInputBuffer extends InputStream implements DataInput {

    /**
     * The buffer whose contents are being read.
     */
    protected byte[] buf;
    
    /**
     * The current offset in the buffer.  This is incremented each time
     * any data is read from the buffer.
     */
    protected int off;
    
    /**
     * The exclusive index of the last byte in the buffer having valid date (up
     * to limit-1 is valid).
     */
    protected int limit;
    
    /**
     * Prepare for reading from the byte[].
     * 
     * @param buf
     *            The source data.
     */
    public DataInputBuffer(byte[] buf) {
        
        if(buf==null) throw new IllegalArgumentException();
        
        this.buf = buf;
        
        this.off = 0;
        
        this.limit = buf.length;
        
    }
    
    /**
     * Prepare for reading from the byte[].
     * 
     * @param buf
     *            The source data.
     * @param off
     *            The offset of the first byte to be read.
     * @param len
     *            The #of bytes available.
     */
    public DataInputBuffer(byte[] buf,int off, int len) {
        
        if (buf == null)
            throw new IllegalArgumentException();
        
        assert off >= 0;

        assert len >= 0;
        
        assert off + len <= buf.length;
        
        this.buf = buf;
        
        this.off = off;
        
        this.limit = off + len;
        
    }

    /**
     * Prepare for reading from the buffer. The bytes between the
     * {@link ByteArrayBuffer#pos()} and the {@link ByteArrayBuffer#limit()}
     * will be read.
     * 
     * @param buf
     *            The buffer.
     */
    public DataInputBuffer(ByteArrayBuffer buf) {
        
        if (buf == null)
            throw new IllegalArgumentException();
        
        this.buf = buf.array();
        
        this.off = buf.pos;
        
        this.limit = buf.limit;
        
    }
    
    /**
     * Replaces the buffer and resets the offset to zero (0).
     * 
     * @param buf
     *            The new buffer.
     */
    public void setBuffer(byte[] buf) {
        
        if (buf == null)
            throw new IllegalArgumentException();

        setBuffer(buf,0,buf.length);
        
    }
    
    /**
     * Replaces the buffer and reset the offset and length to the specified
     * values.
     * 
     * @param buf
     *            The new buffer.
     * @param off
     * @param len
     */
    public void setBuffer(byte[] buf, int off, int len) {

        if (buf == null)
            throw new IllegalArgumentException();

        if (off + len > buf.length)
            throw new IllegalArgumentException();

        this.buf = buf;

        this.off = off;

        this.limit = off + len;
        
    }
    
    /**
     * Replaces the buffer reference with {@link ByteArrayBuffer#array()} and
     * resets the offset and length to the {@link ByteArrayBuffer#pos()} and the
     * {@link ByteArrayBuffer#limit()} respectively.
     * 
     * @param buf
     *            The buffer.
     */
    public void setBuffer(ByteArrayBuffer buf) {
        
        if (buf == null)
            throw new IllegalArgumentException();

        setBuffer(buf.array(), buf.pos, buf.limit);
        
    }
    
    /*
     * DataInput
     */
    
    public boolean readBoolean() throws IOException {

        if (off >= limit)
            throw new EOFException();

        return buf[off++] == 0 ? false : true;

    }

    @Override
    public int read() throws IOException {

        if (off >= limit)
            return -1; // EOF
        
        return 0xff & buf[off++];
        
    }
    
    public byte readByte() throws IOException {

        if (off >= limit)
            throw new EOFException();
        
        return buf[off++];
        
    }

    public char readChar() throws IOException {

        if (off + 2 > limit)
            throw new EOFException();

        int ch1 = buf[off++];

        int ch2 = buf[off++];
        
        return (char) ((ch1 << 8) + (ch2 << 0));
        
    }

    public double readDouble() throws IOException {

        return Double.longBitsToDouble(readLong());

    }

    public float readFloat() throws IOException {

        return Float.intBitsToFloat(readInt());
        
    }

    final public void readFully(byte[] b) throws IOException {

        readFully(b, 0, b.length);
        
    }

    final public void readFully(byte[] b, final  int off, final int len) throws IOException {

        if (this.off + len > this.limit)
            throw new EOFException();
        
        System.arraycopy(buf, this.off, b, off, len);
        
        this.off += len;
        
    }

    public int readInt() throws IOException {

        if(off+4>limit) throw new EOFException();
        
        int v = 0;
        
        // big-endian.
        v += (0xff & buf[off++]) << 24;
        v += (0xff & buf[off++]) << 16;
        v += (0xff & buf[off++]) <<  8;
        v += (0xff & buf[off++]) <<  0;
        
        return v;

    }

    public String readLine() throws IOException {
        
        throw new UnsupportedOperationException();
        
    }

    public long readLong() throws IOException {

        if(off+8>limit) throw new EOFException();
        
        long v = 0L;
        
        // big-endian.
        v += (0xffL & buf[off++]) << 56;
        v += (0xffL & buf[off++]) << 48;
        v += (0xffL & buf[off++]) << 40;
        v += (0xffL & buf[off++]) << 32;
        v += (0xffL & buf[off++]) << 24;
        v += (0xffL & buf[off++]) << 16;
        v += (0xffL & buf[off++]) <<  8;
        v += (0xffL & buf[off++]) <<  0;
        
        return v;
 
    }

    public short readShort() throws IOException {
        
        if (off + 2 > limit)
            throw new EOFException();

        int ch1 = buf[off++];

        int ch2 = buf[off++];
        
        return (short) ((ch1 << 8) + (ch2 << 0));
        
    }

    public String readUTF() throws IOException {

        return DataInputStream.readUTF(this);
        
    }

    public int readUnsignedByte() throws IOException {
        
        if (off >= limit)
            throw new EOFException();
        
        return buf[off++];
        
    }

    public int readUnsignedShort() throws IOException {

        if (off + 2 > limit)
            throw new EOFException();

        int a = buf[off++];

        int b = buf[off++];
        
        return (((a & 0xff) << 8) | (b & 0xff));

//        return ((ch1 << 8) + (ch2 << 0));
        
    }

    public int skipBytes(int n) throws IOException {

        off += n;
        
        if(off>limit) throw new IOException();
        
        return n;
        
    }

    /*
     * unpack unsigned long integer.
     */

    /**
     * Unpack a long value from the current buffer position.
     * 
     * @return The long value.
     * 
     * @throws IOException
     */
    final public long unpackLong() throws IOException {
        if (off + 1 > limit)
            throw new EOFException();
        int b = buf[off++];
        int nbytes;
        long l;
        if ((b & 0x80) != 0) {
            // high bit is set.
            nbytes = 8; // use 8 bytes (this one plus the next 7).
            l = b & 0x7f; // clear the high bit - the rest of the byte is the
                            // start value.
        } else {
            // high bit is clear.
            nbytes = b >> 4; // nbytes is the upper nibble. (right shift one
                                // nibble).
            l = b & 0x0f; // starting value is lower nibble (clear the upper
                            // nibble).
        }
        if (off + nbytes - 1 > limit)
            throw new EOFException();
        for (int i = 1; i < nbytes; i++) {
            // Read the next byte.
            b = buf[off++];
            // Shift the existing value one byte left and add into the low
            // (unsigned) byte.
            l = (l << 8) + (0xff & b);
        }
        return l;
    }

    /*
     * unpack unsigned short integer.
     */
    
    /**
     * Unpack a non-negative short value from the input stream.
     * 
     * @param is
     *            The input stream.
     * 
     * @return The short value.
     * 
     * @throws IOException
     */
    final public short unpackShort() throws IOException {
        if (off + 1 > limit)
            throw new EOFException();
        short b = (short) buf[off++];
        short v;
        if ((b & 0x80) != 0) { // high bit is set.
            /*
             * clear the high bit and shift over one byte.
             */
            v = (short) ((b & 0x7f) << 8);
            if (off + 1 > limit)
                throw new EOFException();
            b = buf[off++]; // read the next byte.
            v |= (b & 0xff); // and combine it together with the high byte.
        } else {
            // high bit is clear.
            v = b; // interpret the byte as a short value.
        }
        return (short) v;
    }

}
