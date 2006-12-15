package com.bigdata.objndx;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Reads bytes from a {@link ByteBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class ByteBufferInputStream extends InputStream {

    final ByteBuffer buf;

    public ByteBufferInputStream(ByteBuffer buf) {

        assert buf != null;

        this.buf = buf;

    }

    /**
     * Read the next byte from the buffer.
     * 
     * @return The byte as a value in [0:255].
     */
    public int read() throws IOException {

        if (buf.remaining() == 0) {

            return -1;

        }

        // A byte whose value is in [-128:127].
        byte b = buf.get();

        return (0xff & b);
        //            return ((int) b) + 128;
        //            int v = ((int)b) + 128;
        //            assert v>=0 && v<=255;
        //            return v;
        //            return b;

    }

}
