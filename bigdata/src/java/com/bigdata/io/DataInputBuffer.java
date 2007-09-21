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
 * Created on Apr 9, 2007
 */

package com.bigdata.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

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
public class DataInputBuffer implements DataInput {

    /**
     * The buffer whose contents are being read.
     */
    final protected byte[] buf;
    
    /**
     * The current offset in the buffer.  This is incremented each time
     * any data is read from the buffer.
     */
    protected int off;
    
    /**
     * The index of the last byte in the buffer having valid data.
     */
    final protected int len;
    
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
        
        this.len = buf.length;
        
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
        
        this.len = len;
        
    }
    
    /*
     * DataInput
     */
    
    public boolean readBoolean() throws IOException {

        if(off>=len) throw new EOFException();
        
        return buf[off++] == 1 ? true : false;
        
    }

    public byte readByte() throws IOException {

        if (off >= len)
            throw new EOFException();
        
        return buf[off++];
        
    }

    public char readChar() throws IOException {

        if (off + 2 > len)
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

        if (this.off + len > this.len)
            throw new EOFException();
        
        System.arraycopy(buf, this.off, b, off, len);
        
        this.off += len;
        
    }

    public int readInt() throws IOException {

        if(off+4>len) throw new EOFException();
        
        int v = 0;
        
        // big-endian.
        v += (0xffL & buf[off++]) << 24;
        v += (0xffL & buf[off++]) << 16;
        v += (0xffL & buf[off++]) <<  8;
        v += (0xffL & buf[off++]) <<  0;
        
        return v;

    }

    public String readLine() throws IOException {
        
        throw new UnsupportedOperationException();
        
    }

    public long readLong() throws IOException {

        if(off+8>len) throw new EOFException();
        
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
        
        if (off + 2 > len)
            throw new EOFException();

        int ch1 = buf[off++];

        int ch2 = buf[off++];
        
        return (short) ((ch1 << 8) + (ch2 << 0));
        
    }

    public String readUTF() throws IOException {

        return DataInputStream.readUTF(this);
        
    }

    public int readUnsignedByte() throws IOException {
        
        if (off >= len)
            throw new EOFException();
        
        return buf[off++];
        
    }

    public int readUnsignedShort() throws IOException {

        if (off + 2 > len)
            throw new EOFException();

        int ch1 = buf[off++];

        int ch2 = buf[off++];
        
        return ((ch1 << 8) + (ch2 << 0));
        
    }

    public int skipBytes(int n) throws IOException {

        off += n;
        
        if(off>len) throw new IOException();
        
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
        if (off + 1 > len)
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
        if (off + nbytes - 1 > len)
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
        if (off + 1 > len)
            throw new EOFException();
        short b = (short) buf[off++];
        short v;
        if ((b & 0x80) != 0) { // high bit is set.
            /*
             * clear the high bit and shift over one byte.
             */
            v = (short) ((b & 0x7f) << 8);
            if (off + 1 > len)
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
