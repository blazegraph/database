package com.bigdata.objndx;

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
class ByteBufferOutputStream extends OutputStream {

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

}
