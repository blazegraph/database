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
/*
 * Created on Nov 5, 2006
 */

package com.bigdata.util;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.Adler32;

import junit.framework.TestCase;

/**
 * Test suite for {@link ChecksumUtility}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestChecksumUtility extends TestCase {

    Random r = new Random();
    
    ChecksumUtility chk = new ChecksumUtility();
    
    /**
     * 
     */
    public TestChecksumUtility() {
    }

    /**
     * @param arg0
     */
    public TestChecksumUtility(String arg0) {
        super(arg0);
    }

    
    /**
     * Test verifies that the checksum of the buffer is being computed
     * correctly.
     */
    public void test_checksum01() {
        
        byte[] data = new byte[100];
        r.nextBytes(data);

        Adler32 adler32 = new Adler32();
        adler32.update(data);
        final int expectedChecksum = (int) adler32.getValue();

        assertEquals(expectedChecksum, chk.checksum(ByteBuffer.wrap(data), 0,
                data.length));

    }

    /**
     * Test verifies that only the specified region of the buffer is used to
     * compute the checksum.
     */
    public void test_checksum02() {

        byte[] data = new byte[100];
        r.nextBytes(data);

        Adler32 adler32 = new Adler32();
        adler32.update(data, 10, 90);
        final int expectedChecksum = (int) adler32.getValue();

        assertEquals(expectedChecksum, chk.checksum(ByteBuffer.wrap(data), 10,
                data.length));
        
    }

    /**
     * Test verifies that the mark, position and limit are unchanged by the
     * checksum operation.
     */
    public void test_checksum03() {

        byte[] data = new byte[100];
        r.nextBytes(data);

        ByteBuffer buf = ByteBuffer.wrap(data);
        // set the limit.
        buf.limit(20);
        /*
         * set the mark (we have to choose a mark less than the position we will
         * set and test below or the mark will be discarded when we set the
         * position).
         */
        buf.position(9);
        buf.mark();
        // set the position.
        buf.position(12);
        chk.checksum(buf, 0, data.length);
        // verify limit unchanged.
        assertEquals(20,buf.limit());
        // verify position unchanged.
        assertEquals(12,buf.position());
        // reset the buffer to the mark and verify the mark was not changed.
        buf.reset();
        assertEquals(9,buf.position());
        
    }

    /**
     * Verify that the computed checksum is the same whether the buffer is
     * backed by an array or not.
     */
    public void test_checksum04() {
        
        byte[] data = new byte[100];
        r.nextBytes(data);

        Adler32 adler32 = new Adler32();
        adler32.update(data);
        final int expectedChecksum = (int) adler32.getValue();

        assertEquals(expectedChecksum, chk.checksum(ByteBuffer.wrap(data), 0,
                data.length));
        
        ByteBuffer direct = ByteBuffer.allocate(data.length);
        direct.put(data);
        assertEquals(expectedChecksum, chk.checksum(direct, 0,
                data.length));
        
    }

    /**
     * Verify that the computed checksum is the same whether the buffer is
     * backed by an array or not when the checksum is computed for only a region
     * of the buffer.
     */
    public void test_checksum05() {
        
        byte[] data = new byte[100];
        r.nextBytes(data);

        Adler32 adler32 = new Adler32();
        adler32.update(data,20,100-10-20);
        final int expectedChecksum = (int) adler32.getValue();

        assertEquals(expectedChecksum, chk.checksum(ByteBuffer.wrap(data), 20,
                data.length-10));
        
        ByteBuffer direct = ByteBuffer.allocate(data.length);
        direct.put(data);
        assertEquals(expectedChecksum, chk.checksum(direct, 20,
                data.length-10));
        
    }

}
