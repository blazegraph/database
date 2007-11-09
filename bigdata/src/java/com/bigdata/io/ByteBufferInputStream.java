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
 * Reads bytes from a {@link ByteBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo override {@link InputStream#read(byte[], int, int)} for better
 *       performance.
 * 
 * @todo document that this reads the remaining() bytes and has a side effect on
 *       the position. if that is not the desired behavior then either modify
 *       this class or invoke {@link ByteBuffer#asReadOnlyBuffer()} and pass the
 *       result into the constructor instead.
 */
public class ByteBufferInputStream extends InputStream {

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
