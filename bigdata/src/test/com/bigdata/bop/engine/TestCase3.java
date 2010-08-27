/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 24, 2010
 */

package com.bigdata.bop.engine;

import java.nio.ByteBuffer;
import java.util.Random;

import junit.framework.TestCase;
import junit.framework.TestCase2;

/**
 * Base class for some <code>assertEquals</code> methods not covered by
 * {@link TestCase} or {@link TestCase2}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated This is currently duplicating a class by the same name in
 *             com.bigdata.io in the JOURNAL_HA_BRANCH. Those classes should be
 *             reconciled.
 *             <p>
 *             I have added {@link #fillBufferWithRandomData(ByteBuffer)} to
 *             this version of the class. That method should make it into the
 *             original version of this class when they are reconciled.
 */
public class TestCase3 extends TestCase2 {

    /**
     * 
     */
    public TestCase3() {
     
    }

    /**
     * @param name
     */
    public TestCase3(String name) {
        super(name);
     
    }

    /**
     * Helper method verifies that the contents of <i>actual</i> from
     * position() to limit() are consistent with the expected byte[]. A
     * read-only view of <i>actual</i> is used to avoid side effects on the
     * position, mark or limit properties of the buffer.
     * 
     * @param expected
     *            Non-null byte[].
     * @param actual
     *            Buffer.
     */
    public static void assertEquals(ByteBuffer expectedBuffer,
            final ByteBuffer actual) {

        if (expectedBuffer == null)
            throw new IllegalArgumentException();

        if (actual == null)
            fail("actual is null");

        if (expectedBuffer.hasArray() && expectedBuffer.arrayOffset() == 0
                && expectedBuffer.position() == 0
                && expectedBuffer.limit() == expectedBuffer.capacity()) {

            // evaluate byte[] against actual.
            assertEquals(expectedBuffer.array(), actual);

            return;

        }
        
        /*
         * Copy the expected data into a byte[] using a read-only view on the
         * buffer so that we do not mess with its position, mark, or limit.
         */
        final byte[] expected;
        {

            expectedBuffer = expectedBuffer.asReadOnlyBuffer();

            final int len = expectedBuffer.remaining();

            expected = new byte[len];

            expectedBuffer.get(expected);

        }

        // evaluate byte[] against actual.
        assertEquals(expected, actual);

    }

    /**
     * Helper method verifies that the contents of <i>actual</i> from
     * position() to limit() are consistent with the expected byte[]. A
     * read-only view of <i>actual</i> is used to avoid side effects on the
     * position, mark or limit properties of the buffer.
     * 
     * @param expected
     *            Non-null byte[].
     * @param actual
     *            Buffer.
     */
    public static void assertEquals(final byte[] expected, ByteBuffer actual) {

        if (expected == null)
            throw new IllegalArgumentException();

        if (actual == null)
            fail("actual is null");

        if (actual.hasArray() && actual.arrayOffset() == 0
                && actual.position() == 0
                && actual.limit() == actual.capacity()) {

            assertEquals(expected, actual.array());

            return;

        }

        /*
         * Create a read-only view on the buffer so that we do not mess with its
         * position, mark, or limit.
         */
        actual = actual.asReadOnlyBuffer();

        final int len = actual.remaining();

        final byte[] actual2 = new byte[len];

        actual.get(actual2);

        // compare byte[]s.
        assertEquals(expected, actual2);

    }

    /**
     * Fill the buffer with a random run length of random data starting at a
     * random offset.
     * 
     * @param b
     *            The buffer.
     */
    protected void fillBufferWithRandomData(final ByteBuffer b) {
    
        final int capacity = b.capacity();

        b.clear();
        
        // starting offset.
        final int off = r.nextInt(capacity / 2);
        
        // run length (may be zero).
        final int len = r.nextInt(capacity - off + 1) - 1;
        
        if (log.isInfoEnabled())
            log.info("off=" + off + ", len=" + len + ", off+len=" + (off + len)
                    + ", capacity=" + capacity);

        final byte[] a = new byte[len];
        
        // random byte[] of that length.
        r.nextBytes(a);
        
        // setup the view of the slice on the buffer.
        b.limit(off + len);
        b.position(off);
        
        // copy random byte[] into the buffer.
        b.put(a);
        
        // prepare the buffer for reading.
        b.flip();
        
    }
    
    private final Random r = new Random();
    
}
