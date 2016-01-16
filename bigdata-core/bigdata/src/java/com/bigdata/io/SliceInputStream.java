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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A stream which only lets you read a fixed #of bytes from an underlying
 * stream.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SliceInputStream extends FilterInputStream {

    final private int limit;
    private int nread = 0;

    /**
     * @param in
     *            The source stream.
     * @param limit
     *            The maximum #of bytes which may be read.
     */
    public SliceInputStream(final InputStream in, final int limit) {
      
        super(in);

        if (in == null)
            throw new IllegalArgumentException();
        
        if (limit < 0)
            throw new IllegalArgumentException();
        
        this.limit = limit;
        
    }

    @Override
    public int read() throws IOException {
        
        if (nread >= limit)
            return -1;
        
        final int b = in.read();
        
        if (b == -1)
            return -1;
    
        nread++;
        
        return b;

    }

    @Override
    public int read(final byte[] b, final int off, final int len)
            throws IOException {
        
        if (off < 0)
            throw new IllegalArgumentException();
        
        if (len < 0)
            throw new IllegalArgumentException();
        
        if (off + len > b.length)
            throw new IllegalArgumentException();
        
        final int nremaining = limit - nread;
        
        final int nrequested = len - off;
        
        if (nremaining == 0 && nrequested > 0) {
            // EOF
            return -1;
        }

        final int nallowed = Math.min(nremaining, nrequested);

        final int n = in.read(b, off, nallowed);

        if (n == -1)
            return -1;

        nread += n;

        return n;

    }
}
