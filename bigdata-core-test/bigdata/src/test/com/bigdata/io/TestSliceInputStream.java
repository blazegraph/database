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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase2;

/**
 * Test suite for {@link SliceInputStream}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSliceInputStream extends TestCase2 {

    /**
     * 
     */
    public TestSliceInputStream() {
    }

    /**
     * @param name
     */
    public TestSliceInputStream(String name) {
        super(name);
    }

    public void test_ctor_null_arg() {
        try {
            new SliceInputStream(null, 10);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
    }

    public void test_ctor_negative_limit() {
        try {
            new SliceInputStream(new ByteArrayInputStream(new byte[10]), -1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
    }

    /*
     * TODO read(byte[])
     * 
     * TODO read(byte[],off,len)
     */
    public void test_slice_read() throws IOException {

        final byte[] b = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

        final InputStream is = new SliceInputStream(
                new ByteArrayInputStream(b), 3/* limit */);

        assertEquals(0, is.read());
        assertEquals(1, is.read());
        assertEquals(2, is.read());
        assertEquals(-1, is.read());
        
    }
    
    public void test_slice_read_byteArray() throws IOException {

        final byte[] b = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        
        final SliceInputStream is = new SliceInputStream(
                new ByteArrayInputStream(b), 3/* limit */);

        final byte[] a = new byte[3];

        // empty read is allowed and reads nothing.
        assertEquals(0, is.read(a, 0, 0));

        // read 3 bytes is allowed.
        assertEquals(3, is.read(a)); // TODO validate args.
        assertEquals(new byte[] { 0, 1, 2 }, a);// verify data.

        // next byte is not allowed.
        assertEquals(-1, is.read());

        // empty read is still allowed.
        assertEquals(0, is.read(a, 0, 0));

        // non-empty read is not allowed.
        assertEquals(-1, is.read(a, 0, 1));

    }
    
}
