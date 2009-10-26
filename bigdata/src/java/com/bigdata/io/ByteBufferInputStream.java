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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Reads bytes from a {@link ByteBuffer}. This class IS NOT thread-safe.
 * Caller's MUST {@link ByteBuffer#duplicate()} the {@link ByteBuffer} before
 * creating an instance of this class if they wish to avoid side-effects on the
 * position and limit of the original {@link ByteBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo document that this reads the remaining() bytes and has a side effect on
 *       the position. if that is not the desired behavior then either modify
 *       this class or invoke {@link ByteBuffer#asReadOnlyBuffer()} and pass the
 *       result into the constructor instead.
 * 
 * @todo dsiutils now defines its own implementation of this but does not define
 *       a ByteBufferOutputStream class. if it did, then we might just use its
 *       implementations instead.
 */
public class ByteBufferInputStream extends InputStream {

    private final ByteBuffer buf;

    public ByteBufferInputStream(final ByteBuffer buf) {

        if (buf == null)
            throw new IllegalArgumentException();

        this.buf = buf;

    }

    /**
     * Read the next byte from the buffer.
     * 
     * @return The byte as a value in [0:255].
     */
    synchronized
    public int read() throws IOException {

        if (buf.remaining() == 0) {

            return -1;

        }

        // A byte whose value is in [-128:127].
        final byte b = buf.get();

        return (0xff & b);
        //            return ((int) b) + 128;
        //            int v = ((int)b) + 128;
        //            assert v>=0 && v<=255;
        //            return v;
        //            return b;

    }

    synchronized
    public int read(final byte[] a, final int off, final int len)
            throws IOException {

        if (len == 0)
            return 0;
        
        final int remaining = buf.remaining();
        
        if (remaining == 0)
            return -1;
        
        final int readLength = Math.min(remaining, len);
        
        // current limit.
        final int climit = buf.limit();

        // new limit.
        buf.limit(buf.position() + readLength);
        
        // bulk copy.
        buf.get(a, off, readLength);

        // restore the limit (position was updated as a side effect).
        buf.limit(climit);

        // #of bytes transferred.
        return readLength;

    }

}
