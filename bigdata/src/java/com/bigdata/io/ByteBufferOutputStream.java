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
package com.bigdata.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Writes bytes onto a {@link ByteBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ByteBufferOutputStream extends OutputStream {

    final protected ByteBuffer buf;
    
    final public ByteBuffer getByteBuffer() {
        
        return buf;
        
    }

    public ByteBufferOutputStream(ByteBuffer buf) {

        assert buf != null;

        this.buf = buf;

    }

    /**
     * Write a byte on the buffer. The {@link ByteBuffer#position()} is advanced
     * as a side effect.
     * 
     * @param b
     *            A byte whose value is in [-128:127].
     * 
     * @exception EOFException
     *                if the buffer would overflow.
     */
    public void write(int b) throws IOException {

        if (buf.remaining() == 0) {

            throw new EOFException();

        }

        //            buf.put( (byte) (0x000000ff & b) );

        buf.put((byte) b);

    }

    /**
     * Write an array of bytes on the buffer. The {@link ByteBuffer#position()}
     * is advanced as a side effect.
     * 
     * @param b
     *            the data.
     * @param off
     *            the start offset in the data.
     * @param len
     *            the number of bytes to write.
     * 
     * @exception EOFException
     *                if the buffer would overflow.
     * 
     */
    public void write(byte b[], int off, int len) throws IOException {
    
        if(buf.remaining()<len) {

            throw new EOFException();

        }
        
        buf.put(b,off,len);
        
    }
    
}
