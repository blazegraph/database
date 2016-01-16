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
 * Created on May 26, 2011
 */

package com.bigdata.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Output stream which reports the #of bytes read from the underlying stream.
 * <p>
 * Note: All <em>write</em> methods MUST be overridden if you subclass this
 * filter as it overrides them all for better performance.

 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * TODO test suite.
 */
public class ByteCountOutputStream extends FilterOutputStream {

    private final OutputStream out;
    private int nwritten = 0;

    public int getNWritten() {
        return nwritten;
    }
    
    /**
     * @param out
     */
    public ByteCountOutputStream(final OutputStream out) {
        super(out);
        this.out = out;
    }

    public void write(final int b) throws IOException {
        super.write(b);
        nwritten++;
    }

    public void write(final byte b[]) throws IOException {
        out.write(b); // efficiency!
        nwritten += b.length;
    }

    public void write(final byte b[], final int off, final int len)
            throws IOException {
        out.write(b, off, len); // efficiency!
        nwritten += len;
    }
    
}
