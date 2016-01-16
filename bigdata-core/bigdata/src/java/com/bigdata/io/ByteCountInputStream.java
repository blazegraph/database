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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Input stream which reports the #of bytes read from the underlying stream.
 * <p>
 * Note: All <em>read</em> methods MUST be overridden if you subclass this
 * filter as it overrides them all for better performance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO test suite.
 */
public class ByteCountInputStream extends FilterInputStream {

    private final InputStream in;
    private int nread = 0;

    /**
     * The #of bytes read from the underlying stream.
     */
    public int getNRead() {
        return nread;
    }
    
    /**
     * @param in
     */
    public ByteCountInputStream(final InputStream in) {
        super(in);
        this.in = in;
    }

    public int read() throws IOException {
        final int b = super.read();
        if (b != -1)
            nread++;
        return b;
    }

    public int read(final byte[] a) throws IOException {
        final int n = in.read(a); // efficiency!
        if (n != -1)
            nread += n;
        return n;
    }

    public int read(final byte[] a, final int off, final int len)
            throws IOException {
        final int n = in.read(a, off, len); // efficiency!
        if (n != -1)
            nread += n;
        return n;
    }

}
