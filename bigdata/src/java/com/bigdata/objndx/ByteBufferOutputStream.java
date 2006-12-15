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

    final ByteBuffer buf;

    public ByteBufferOutputStream(ByteBuffer buf) {

        assert buf != null;

        this.buf = buf;

    }

    /**
     * Write a byte on the buffer.
     * 
     * @param b
     *            A byte whose value is in [-128:127].
     */
    public void write(int b) throws IOException {

        if (buf.remaining() == 0) {

            throw new EOFException();

        }

        //            buf.put( (byte) (0x000000ff & b) );

        buf.put((byte) b);

    }

}
