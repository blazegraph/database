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
 * Created on Apr 9, 2007
 */

package com.bigdata.io;

import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.btree.BytesUtil;

/**
 * Test suite for {@link DataOutputBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDataOutputBuffer extends TestCase2
{

    /**
     * 
     */
    public TestDataOutputBuffer() {
    }

    /**
     * @param arg0
     */
    public TestDataOutputBuffer(String arg0) {
        super(arg0);
    }

    /**
     * ctor tests, including correct rejection.
     */
    public void test_ctor() {

        {
            DataOutputBuffer buf = new DataOutputBuffer();

            assertNotNull(buf.buf);
            assertEquals(DataOutputBuffer.DEFAULT_INITIAL_CAPACITY,
                    buf.buf.length);
            assertEquals(0, buf.len);

        }

        {
            DataOutputBuffer buf = new DataOutputBuffer(0);
            assertNotNull(buf.buf);
            assertEquals(0, buf.buf.length);
            assertEquals(0, buf.len);
        }

        {
            DataOutputBuffer buf = new DataOutputBuffer(20);
            assertNotNull(buf.buf);
            assertEquals(20, buf.buf.length);
            assertEquals(0, buf.len);
        }

        {
            final byte[] expected = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
            DataOutputBuffer buf = new DataOutputBuffer(4, expected);
            assertNotNull(buf.buf);
            assertEquals(4, buf.len);
            assertEquals(10, buf.buf.length);
            assertTrue(expected == buf.buf);
        }

    }

    /**
     * correct rejection tests.
     */
    public void test_ctor_correctRejection() {

        try {
            new DataOutputBuffer(-1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        try {
            new DataOutputBuffer(20, new byte[10]);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

    }

    public void test_ensureCapacity() {

        DataOutputBuffer DataOutputBuffer = new DataOutputBuffer(0);

        assertEquals(0, DataOutputBuffer.len);
        assertNotNull(DataOutputBuffer.buf);
        assertEquals(0, DataOutputBuffer.buf.length);

        final byte[] originalBuffer = DataOutputBuffer.buf;

        // correct rejection.
        try {
            DataOutputBuffer.ensureCapacity(-1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        assertTrue(originalBuffer == DataOutputBuffer.buf); // same buffer.

        // no change.
        DataOutputBuffer.ensureCapacity(0);
        assertEquals(0, DataOutputBuffer.len);
        assertNotNull(DataOutputBuffer.buf);
        assertEquals(0, DataOutputBuffer.buf.length);
        assertTrue(originalBuffer == DataOutputBuffer.buf); // same buffer.
    }

    public void test_ensureCapacity02() {

        DataOutputBuffer DataOutputBuffer = new DataOutputBuffer(0);

        assertEquals(0, DataOutputBuffer.len);
        assertNotNull(DataOutputBuffer.buf);
        assertEquals(0, DataOutputBuffer.buf.length);

        final byte[] originalBuffer = DataOutputBuffer.buf;

        // extends buffer.
        DataOutputBuffer.ensureCapacity(100);
        assertEquals(0, DataOutputBuffer.len);
        assertNotNull(DataOutputBuffer.buf);
        assertEquals(100, DataOutputBuffer.buf.length);
        assertTrue(originalBuffer != DataOutputBuffer.buf); // same buffer.
    }

    /**
     * verify that existing data is preserved if the capacity is extended.
     */
    public void test_ensureCapacity03() {

        Random r = new Random();
        byte[] expected = new byte[20];
        r.nextBytes(expected);

        DataOutputBuffer DataOutputBuffer = new DataOutputBuffer(20, expected);

        assertEquals(20, DataOutputBuffer.len);
        assertNotNull(DataOutputBuffer.buf);
        assertTrue(expected == DataOutputBuffer.buf);

        DataOutputBuffer.ensureCapacity(30);
        assertEquals(20, DataOutputBuffer.len);
        assertTrue(DataOutputBuffer.buf.length >= 30);

        assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(0,
                expected.length, expected, 0, expected.length,
                DataOutputBuffer.buf));

        for (int i = 21; i < 30; i++) {
            assertEquals(0, DataOutputBuffer.buf[i]);
        }

    }

    public void test_ensureFree() {

        DataOutputBuffer DataOutputBuffer = new DataOutputBuffer(0);

        assertEquals(0, DataOutputBuffer.len);
        assertNotNull(DataOutputBuffer.buf);
        assertEquals(0, DataOutputBuffer.buf.length);

        DataOutputBuffer.ensureFree(2);

        assertEquals(0, DataOutputBuffer.len);
        assertNotNull(DataOutputBuffer.buf);
        assertTrue(DataOutputBuffer.buf.length >= 2);

    }

    /**
     * Tests ability to append to the buffer, including with overflow of the
     * buffer capacity.
     */
    public void test_append_bytes() throws IOException {

        // setup buffer with some data and two(2) free bytes.
        DataOutputBuffer DataOutputBuffer = new DataOutputBuffer(5, new byte[] {
                1, 2, 3, 4, 5, 0, 0 });

        /*
         * fill to capacity by copying two bytes from the middle of another
         * array. since this does not overflow we know the exact capacity of the
         * internal buffer (it is not reallocated).
         */
        byte[] tmp = new byte[] { 4, 5, 6, 7, 8, 9 };
        DataOutputBuffer.write(tmp, 2, 2);
        assertEquals(7, DataOutputBuffer.len);
        assertEquals(new byte[] { 1, 2, 3, 4, 5, 6, 7 }, DataOutputBuffer.buf);
        assertEquals(0, BytesUtil.compareBytes(
                new byte[] { 1, 2, 3, 4, 5, 6, 7 }, DataOutputBuffer.buf));

        // overflow capacity (new capacity is not known in advance).
        tmp = new byte[] { 8, 9, 10 };
        byte[] expected = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        DataOutputBuffer.write(tmp);
        assertEquals(10, DataOutputBuffer.len);
        assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(0,
                expected.length, expected, 0, DataOutputBuffer.len,
                DataOutputBuffer.buf));

        // possible overflow (old and new capacity are unknown).
        tmp = new byte[] { 11, 12 };
        expected = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        DataOutputBuffer.write(tmp);
        assertEquals(12, DataOutputBuffer.len);
        assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(0,
                expected.length, expected, 0, DataOutputBuffer.len,
                DataOutputBuffer.buf));

    }

    /**
     * Test ability to extract and return a key.
     */
    public void test_getKey() {

        DataOutputBuffer DataOutputBuffer = new DataOutputBuffer(5, new byte[] {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });

        byte[] key = DataOutputBuffer.toByteArray();

        assertEquals(5, key.length);
        assertEquals(new byte[] { 1, 2, 3, 4, 5 }, key);

    }

    /**
     * Verify returns zero length byte[] when the key has zero bytes.
     */
    public void test_getKey_len0() {

        DataOutputBuffer DataOutputBuffer = new DataOutputBuffer();

        byte[] key = DataOutputBuffer.toByteArray();

        assertEquals(0, key.length);

    }

    /**
     * Test ability to reset the key buffer (simply zeros the #of valid bytes in
     * the buffer without touching the buffer itself).
     */
    public void test_reset() {

        byte[] expected = new byte[10];

        DataOutputBuffer DataOutputBuffer = new DataOutputBuffer(5, expected);

        assertEquals(5, DataOutputBuffer.len);
        assertTrue(expected == DataOutputBuffer.buf);

        assertTrue(DataOutputBuffer == DataOutputBuffer.reset());

        assertEquals(0, DataOutputBuffer.len);
        assertTrue(expected == DataOutputBuffer.buf);

    }

    public void test_roundTrip() {
        fail("write tests");
    }

    public void test_fencePosts() {
        fail("write tests");
    }

}
