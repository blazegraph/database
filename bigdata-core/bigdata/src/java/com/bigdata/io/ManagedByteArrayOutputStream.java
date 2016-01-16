/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on May 25, 2011
 */

package com.bigdata.io;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Wraps an {@link IManagedByteArray} as an {@link OutputStream}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ManagedByteArrayOutputStream extends OutputStream {

    private final IManagedByteArray buf;
    
    /**
     * @param buf
     *            The data written onto the stream will be appended to the
     *            caller's buffer.
     */
    public ManagedByteArrayOutputStream(final IManagedByteArray buf) {
        
        if(buf == null)
            throw new IllegalArgumentException();
        
        this.buf = buf;
        
    }

    @Override
    public void write(int b) throws IOException {
        
        buf.append((byte) (b & 0xff));
        
    }

    @Override
    public void write(final byte[] b) {
        
        write(b, 0/* off */, b.length);
        
    }

    @Override
    public void write(final byte[] b,final int off, final int len) {

        buf.append(b, off, len);
        
    }

}
