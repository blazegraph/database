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
 * Created on Apr 7, 2007
 */

package com.bigdata.io;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;

import org.apache.log4j.Logger;

/**
 * Fast special purpose serialization onto a managed byte[] buffer conforming to
 * the {@link DataOutput} API.
 * <p>
 * Note: The base classes provide all of the same functionality without
 * declaring {@link IOException} as a thrown exception.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataOutputBuffer extends ByteArrayBuffer implements DataOutput {

    protected static Logger log = Logger.getLogger(DataOutputBuffer.class);
    
    /**
     * Uses {@link ByteArrayBuffer#DEFAULT_INITIAL_CAPACITY}.
     */
    public DataOutputBuffer() {
        
        super();
        
    }
    
    /**
     * @param initialCapacity
     *            The initial capacity of the internal byte[].
     */
    public DataOutputBuffer(int initialCapacity) {
        
        super(initialCapacity);
        
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

        super(len,buf);
        
    }

    /**
     * Reads the entire input stream into the buffer. The data are then
     * available in {@link #buf} from position 0 (inclusive) through position
     * {@link #len} (exclusive).
     */
    public DataOutputBuffer(InputStream in) throws IOException {

        super();
        
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

        super();

        // temporary buffer for read from the input stream.
        byte[] b = new byte[remaining()];
        
        while(true) {
            
            int nread = in.read(b);
            
            if( nread == -1 ) break;
            
            write(b,0,nread);
            
        }
        
    }

    /**
     * Conforms the return type to an instance of this class.
     */
    public DataOutputBuffer reset() {
        
        return (DataOutputBuffer)super.reset();
        
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

    final public void writeDouble(final double v) throws IOException {

        putDouble( v );
        
    }

    final public void writeFloat(final float v) throws IOException {

        putFloat( v );

    }

    final public void writeInt(final int v) throws IOException {

        putInt( v );

    }

    final public void writeLong(final long v) throws IOException {

        putLong(v);
        
    }

    final public void writeShort(final int v) throws IOException {

//        if (len + 2 > buf.length)
//            ensureCapacity(len + 2);
//
//        // big-endian
//        buf[len++] = (byte) (v >>> 8);
//        buf[len++] = (byte) (v >>> 0);

        putShort( (short)v );

    }

    final public void writeChar(final int v) throws IOException {

        if (len + 2 > buf.length)
            ensureCapacity(len + 2);

        buf[len++] = (byte) (v >>> 8);
        buf[len++] = (byte) (v >>> 0);

    }

    public void writeBytes(final String s) throws IOException {

        // #of bytes == #of characters (writes only the low bytes).
        final int len = s.length();

        if (this.len + len > buf.length)
            ensureCapacity(this.len + len);

        for (int i = 0 ; i < len ; i++) {
            
            write((byte)s.charAt(i));
            
        }

    }

    public void writeChars(final String s) throws IOException {

        // #of characters (twice as many bytes).
        final int len = s.length();
        
        if (this.len + (len * 2) > buf.length)
            ensureCapacity(this.len + (len * 2));

        for (int i = 0 ; i < len ; i++) {

            final char v = s.charAt(i);
            
            buf[this.len++] = (byte) (v >>> 8);
            buf[this.len++] = (byte) (v >>> 0);

//            write((v >>> 8) & 0xFF); 
//            
//            write((v >>> 0) & 0xFF); 
        
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

}
