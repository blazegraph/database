/*

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
 * Created on Dec 27, 2007
 */

package com.bigdata.io;

import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.btree.BytesUtil;
import com.bigdata.rawstore.Bytes;

/**
 * Test suite for {@link FixedByteArrayBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFixedByteArrayBuffer extends TestCase2 {

    /**
     * 
     */
    public TestFixedByteArrayBuffer() {
    }

    /**
     * @param arg0
     */
    public TestFixedByteArrayBuffer(String arg0) {
        super(arg0);
    }

    /*
     * ctor tests.
     */
    
    /**
     * ctor tests.
     */
    public void test_ctor() {

        {
            final IFixedByteArrayBuffer buf = new FixedByteArrayBuffer(10);
            assertNotNull(buf.array());
            assertEquals(0, buf.off());
            assertEquals(10, buf.len());
        }

        {
            final IFixedByteArrayBuffer buf = new FixedByteArrayBuffer(0);
            assertNotNull(buf.array());
            assertEquals(0, buf.off());
            assertEquals(0, buf.len());
        }

        {
            final IFixedByteArrayBuffer buf = new FixedByteArrayBuffer(
                    new byte[12], 2, 8);
            assertNotNull(buf.array());
            assertEquals(12, buf.array().length);
            assertEquals(2, buf.off());
            assertEquals(8, buf.len());
        }

    }

    /**
     * correct rejection tests.
     */
    public void test_ctor_correctRejection() {

        try {
            new FixedByteArrayBuffer(-1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if(log.isInfoEnabled()) log.info("Ignoring expected exception: " + ex);
        }

        try {
            new FixedByteArrayBuffer(null,0,0);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if(log.isInfoEnabled()) log.info("Ignoring expected exception: " + ex);
        }

        try {
            new FixedByteArrayBuffer(new byte[2],3,0);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if(log.isInfoEnabled()) log.info("Ignoring expected exception: " + ex);
        }

        try {
            new FixedByteArrayBuffer(new byte[2],1,3);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if(log.isInfoEnabled()) log.info("Ignoring expected exception: " + ex);
        }

    }

    /*
     * buffer extension tests.
     */

    public void test_rangeCheck() {

        final FixedByteArrayBuffer buf = new FixedByteArrayBuffer(new byte[10],
                2, 8);
        
        assertEquals(2, buf.off());
        assertEquals(8, buf.len());
        
        assertRangeCheckAccepts(buf, 0, 0);
        assertRangeCheckRejects(buf, -1, 0);
        assertRangeCheckRejects(buf, 0, -1);

        assertRangeCheckAccepts(buf, 0, 8);
        assertRangeCheckRejects(buf, 1, 8);
        assertRangeCheckAccepts(buf, 1, 7);
        assertRangeCheckRejects(buf, 2, 7);
        assertRangeCheckAccepts(buf, 2, 6);
        assertRangeCheckAccepts(buf, 7, 1);
        assertRangeCheckAccepts(buf, 8, 0);

    }

    /**
     * Verify that the range check logic will accept the given arguments.
     */
    protected void assertRangeCheckAccepts(final FixedByteArrayBuffer buf,
            final int aoff, final int alen) {

        buf.rangeCheck(aoff, alen);

    }

    /**
     * Verify that the range check logic will reject the given arguments.
     */
    protected void assertRangeCheckRejects(final FixedByteArrayBuffer buf,
            final int aoff, final int alen) {

        try {
         
            buf.rangeCheck(aoff, alen);
            
            fail("Expecting: " + IllegalArgumentException.class);
            
        } catch (IndexOutOfBoundsException ex) {
            
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);

        }

    }

    /*
     * get/put
     * 
     * @todo verify all methods are tested.
     */

    final Random r = new Random();

    final int LIMIT = 1000;

    public void test_get_correctRejection() {

        final int size = 20;

        final IFixedByteArrayBuffer buf = new FixedByteArrayBuffer(size);

        assertEquals((byte) 0, buf.getByte(0));

        try {
            buf.getByte(-1);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            buf.getByte(size);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        assertEquals(0, buf.getByte(size - 1));

    }

    /**
     * Test of the simple forms of the bulk get/put methods.
     */
    public void test_getPutByteArray() {
        
        final int size = 200;
        
        final IFixedByteArrayBuffer buf = new FixedByteArrayBuffer(size);
        
        assertEquals((byte) 0, buf.getByte(0));
        assertEquals((byte) 0, buf.getByte(size - 1));

        final int pos = 1;

        for (int i = 0; i < LIMIT; i++) {

            final byte[] expected = new byte[r.nextInt(size - 2)];

            r.nextBytes(expected);

            buf.put(pos, expected);

            assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(0,
                    expected.length, expected, pos, expected.length, buf.array()));

            final byte[] actual = new byte[expected.length];

            buf.get(pos, actual);

            assertTrue(BytesUtil.bytesEqual(expected, actual));
      
        }

        assertEquals((byte) 0, buf.getByte(0));

        assertEquals((byte) 0, buf.getByte(pos + size - 2));
        
    }

    /**
     * Test of the bulk get/put byte[] methods which accept a slice into the
     * caller's array.
     */
    public void test_getPutByteArrayWithOffsetAndLength() {
        
        final int size = 200;
        
        final IFixedByteArrayBuffer buf = new FixedByteArrayBuffer(size);

        assertEquals((byte) 0, buf.getByte(0));
        assertEquals((byte) 0, buf.getByte(size - 1));

        final int pos = 1;
        
        for (int i = 0; i < LIMIT; i++) {

            final byte[] expected = new byte[r.nextInt(size - 2)];

            final int off = (expected.length / 2 == 0 ? 0 : r
                    .nextInt(expected.length / 2));

            final int len = (expected.length == 0 ? 0 : r
                    .nextInt(expected.length - off));

            r.nextBytes(expected);

            buf.put(pos, expected, off, len);

            assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(off, len,
                    expected, pos, len, buf.array()));

            final int dstoff = r.nextInt(10);
            
            final byte[] actual = new byte[expected.length+dstoff];

            buf.get(pos, actual, dstoff, expected.length);

            assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(off, len,
                    expected, dstoff, len, actual));

        }

        assertEquals((byte) 0, buf.getByte(0));

        assertEquals((byte) 0, buf.getByte(pos + size - 2));

    }

    public void test_getByte_putByte() {
        
        final IFixedByteArrayBuffer buf = new FixedByteArrayBuffer(
                Bytes.SIZEOF_BYTE * 3);

        final int pos = Bytes.SIZEOF_BYTE;

        assertEquals((byte) 0, buf.getByte(pos));

        final byte[] tmp = new byte[1];

        for (int i = 0; i < LIMIT; i++) {

            r.nextBytes(tmp);

            final byte expected = tmp[0];

            buf.putByte(pos, expected);

            assertEquals(expected, buf.getByte(pos));

        }

        assertEquals((byte) 0, buf.getByte(pos - Bytes.SIZEOF_BYTE));

        assertEquals((byte) 0, buf.getByte(pos + Bytes.SIZEOF_BYTE));

    }
    
    public void test_getShort_putShort() {
        
        final IFixedByteArrayBuffer buf = new FixedByteArrayBuffer(
                Bytes.SIZEOF_SHORT * 3);

        final int pos = Bytes.SIZEOF_SHORT;

        assertEquals((short) 0, buf.getShort(pos));

        for (int i = 0; i < LIMIT; i++) {

            final short expected = (short) r.nextInt();

            buf.putShort(pos, expected);

            assertEquals(expected, buf.getShort(pos));

        }

        assertEquals((short) 0, buf.getShort(pos - Bytes.SIZEOF_SHORT));

        assertEquals((short) 0, buf.getShort(pos + Bytes.SIZEOF_SHORT));

    }
    
    public void test_getInt_putInt() {

        final IFixedByteArrayBuffer buf = new FixedByteArrayBuffer(
                Bytes.SIZEOF_INT * 3);

        final int pos = Bytes.SIZEOF_INT;

        assertEquals(0, buf.getInt(pos));

        for (int i = 0; i < LIMIT; i++) {

            final int expected = r.nextInt();

            buf.putInt(pos, expected);

            assertEquals(expected, buf.getInt(pos));

        }

        assertEquals(0, buf.getInt(pos - Bytes.SIZEOF_INT));

        assertEquals(0, buf.getInt(pos + Bytes.SIZEOF_INT));

    }

    public void test_getFloat_putFloat() {

        final IFixedByteArrayBuffer buf = new FixedByteArrayBuffer(
                Bytes.SIZEOF_FLOAT * 3);

        final int pos = Bytes.SIZEOF_FLOAT;

        assertEquals(0f, buf.getFloat(pos));

        for (int i = 0; i < LIMIT; i++) {

            final float expected = r.nextFloat();

            buf.putFloat(pos, expected);

            assertEquals(expected, buf.getFloat(pos));
        }

        assertEquals(0f, buf.getFloat(pos - Bytes.SIZEOF_FLOAT));

        assertEquals(0f, buf.getFloat(pos + Bytes.SIZEOF_FLOAT));

    }
    
    public void test_getLong_putLong() {
        
        final IFixedByteArrayBuffer buf = new FixedByteArrayBuffer(
                Bytes.SIZEOF_LONG * 3);

        final int pos = Bytes.SIZEOF_LONG;

        assertEquals(0L, buf.getLong(pos));

        for (int i = 0; i < LIMIT; i++) {

            final long expected = r.nextLong();

            buf.putLong(pos, expected);

            assertEquals(expected, buf.getLong(pos));

        }

        assertEquals(0L, buf.getLong(pos - Bytes.SIZEOF_LONG));

        assertEquals(0L, buf.getLong(pos + Bytes.SIZEOF_LONG));

    }

    public void test_getDouble_putDouble() {

        final IFixedByteArrayBuffer buf = new FixedByteArrayBuffer(
                Bytes.SIZEOF_DOUBLE * 3);

        final int pos = Bytes.SIZEOF_DOUBLE;

        assertEquals(0d, buf.getDouble(pos));

        for (int i = 0; i < LIMIT; i++) {

            final double expected = r.nextDouble();

            buf.putDouble(pos, expected);

            assertEquals(expected, buf.getDouble(pos));
        }

        assertEquals(0d, buf.getDouble(pos - Bytes.SIZEOF_DOUBLE));

        assertEquals(0d, buf.getDouble(pos + Bytes.SIZEOF_DOUBLE));

    }

}
