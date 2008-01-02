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

import com.bigdata.btree.BytesUtil;
import com.bigdata.rawstore.Bytes;

import junit.framework.TestCase;

/**
 * Test suite for {@link ByteArrayBuffer}.
 * 
 * @todo test absolute get/put
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestByteArrayBuffer extends TestCase {

    /**
     * 
     */
    public TestByteArrayBuffer() {
    }

    /**
     * @param arg0
     */
    public TestByteArrayBuffer(String arg0) {
        super(arg0);
    }

    /*
     * ctor tests.
     */
    
    /**
     * ctor tests, including correct rejection.
     */
    public void test_ctor() {

        {
            ByteArrayBuffer buf = new ByteArrayBuffer();
            assertNotNull(buf.buf);
            assertEquals(ByteArrayBuffer.DEFAULT_INITIAL_CAPACITY,
                    buf.buf.length);

        }

        {
            ByteArrayBuffer buf = new ByteArrayBuffer(0);
            assertNotNull(buf.buf);
            assertEquals(0, buf.buf.length);
        }

        {
            ByteArrayBuffer buf = new ByteArrayBuffer(20);
            assertNotNull(buf.buf);
            assertEquals(20, buf.buf.length);
        }

    }

    /**
     * correct rejection tests.
     */
    public void test_ctor_correctRejection() {

        try {
            new ByteArrayBuffer(-1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

    }

    /*
     * buffer extension tests.
     */

    public void test_ensureCapacity() {

        ByteArrayBuffer buf = new ByteArrayBuffer(0);

//        assertEquals(0, buf.len);
        assertNotNull(buf.buf);
        assertEquals(0, buf.buf.length);

        final byte[] originalBuffer = buf.buf;

        // correct rejection.
        try {
            buf.ensureCapacity(-1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        assertTrue(originalBuffer == buf.buf); // same buffer.

        // no change.
        buf.ensureCapacity(0);
//        assertEquals(0, buf.len);
        assertNotNull(buf.buf);
        assertEquals(0, buf.buf.length);
        assertTrue(originalBuffer == buf.buf); // same buffer.
    }

    public void test_ensureCapacity02() {

        ByteArrayBuffer buf = new ByteArrayBuffer(0);

//        assertEquals(0, buf.len);
        assertNotNull(buf.buf);
        assertEquals(0, buf.buf.length);

        final byte[] originalBuffer = buf.buf;

        // extends buffer.
        buf.ensureCapacity(100);
//        assertEquals(0, buf.len);
        assertNotNull(buf.buf);
        assertEquals(100, buf.buf.length);
        assertTrue(originalBuffer != buf.buf); // same buffer.
    }

    /**
     * verify that existing data is preserved if the capacity is extended.
     */
    public void test_ensureCapacity03() {

        Random r = new Random();
        byte[] expected = new byte[20];
        r.nextBytes(expected);

        ByteArrayBuffer buf = new ByteArrayBuffer(20);

        // copy in random data.
        buf.put(0, expected);
        
        // verify buffer state.
        {
        assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(0,
                expected.length, expected, 0, expected.length,
                buf.buf));

//        assertEquals(20, buf.len);
        assertNotNull(buf.buf);
        assertTrue(expected != buf.buf); // data was copied.
        }
        
        // extend capacity.
        buf.ensureCapacity(30);
        
        // verify buffer state.
        {
//        assertEquals(20, buf.len);
        assertTrue(buf.buf.length >= 30);

        assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(0,
                expected.length, expected, 0, expected.length,
                buf.buf));

        for (int i = 21; i < 30; i++) {
            assertEquals(0, buf.buf[i]);
        }
        }
        
    }

    /*
     * get/put
     * 
     * @todo verify all methods are tested.
     * 
     * @todo verify transparent extension for all methods.
     */

    Random r = new Random();
    
    final int LIMIT = 1000;

    public void test_get_correctRejection() {

        final int capacity = 20;
        
        ByteArrayBuffer buf = new ByteArrayBuffer(capacity);

        assertEquals((byte)0,buf.getByte(0));

        try {
            buf.getByte(-1);
            fail("Expecting: "+ArrayIndexOutOfBoundsException.class);
        } catch(ArrayIndexOutOfBoundsException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        try {
            buf.getByte(capacity);
            fail("Expecting: "+ArrayIndexOutOfBoundsException.class);
        } catch(ArrayIndexOutOfBoundsException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        buf.getByte(capacity-1);

    }
    
    /**
     * @todo no method has been defined yet to read a byte[] out of the buffer
     *       so this merely verifies the data in the buffer.
     */
    public void test_getPutByteArray() {
        
        final int capacity = 200;
        
        ByteArrayBuffer buf = new ByteArrayBuffer(capacity);
        
        assertEquals((byte)0,buf.getByte(0));
        assertEquals((byte)0,buf.getByte(capacity-1));

        final int pos = 1;
        
        for(int i=0; i<LIMIT; i++) {

            final byte[] expected = new byte[r.nextInt(capacity-2)];            

            r.nextBytes(expected);
            
            buf.put(pos, expected);
            
            assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(0,
                    expected.length, expected, pos, expected.length,
                    buf.buf));
            
        }

        assertEquals((byte)0,buf.getByte(0));

        assertEquals((byte)0,buf.getByte(pos+capacity-2));
        
    }
    
    /**
     * 
     * @todo no method has been defined yet to read a byte[] out of the buffer
     *       so this merely verifies the data in the buffer.
     */
    public void test_getPutByteArrayWithOffsetAndLength() {
        
        final int capacity = 200;
        
        ByteArrayBuffer buf = new ByteArrayBuffer(capacity);
        
        assertEquals((byte)0,buf.getByte(0));
        assertEquals((byte)0,buf.getByte(capacity-1));

        final int pos = 1;
        
        for(int i=0; i<LIMIT; i++) {

            final byte[] expected = new byte[r.nextInt(capacity-2)];            

            final int off = (expected.length / 2 == 0 ? 0 : r
                    .nextInt(expected.length / 2));

            final int len = (expected.length == 0 ? 0 : r
                    .nextInt(expected.length - off));
            
            r.nextBytes(expected);
            
            buf.put(pos, expected, off, len);
            
            assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(off, len,
                    expected, pos, len, buf.buf));
            
        }

        assertEquals((byte)0,buf.getByte(0));

        assertEquals((byte)0,buf.getByte(pos+capacity-2));
        
    }

    public void test_getByte_putByte() {
        
        ByteArrayBuffer buf = new ByteArrayBuffer(Bytes.SIZEOF_BYTE*3);
        
        final int pos = Bytes.SIZEOF_BYTE;
        
        assertEquals((byte)0,buf.getByte(pos));

        final byte[] tmp = new byte[1];
        
        for(int i=0; i<LIMIT; i++) {
        
            r.nextBytes(tmp);
            
            final byte expected = tmp[0];
        
            buf.putByte(pos, expected);
            
            assertEquals(expected,buf.getByte(pos));
            
        }

        assertEquals((byte)0,buf.getByte(pos-Bytes.SIZEOF_BYTE));

        assertEquals((byte)0,buf.getByte(pos+Bytes.SIZEOF_BYTE));

    }
    
    public void test_getShort_putShort() {
        
        ByteArrayBuffer buf = new ByteArrayBuffer(Bytes.SIZEOF_SHORT*3);
        
        final int pos = Bytes.SIZEOF_SHORT;
        
        assertEquals((short)0,buf.getShort(pos));

        for(int i=0; i<LIMIT; i++) {
        
            final short expected = (short)r.nextInt();
        
            buf.putShort(pos, expected);
        
            assertEquals(expected,buf.getShort(pos));
            
        }

        assertEquals((short)0,buf.getShort(pos-Bytes.SIZEOF_SHORT));

        assertEquals((short)0,buf.getShort(pos+Bytes.SIZEOF_SHORT));

    }
    
    public void test_getInt_putInt() {
        
        ByteArrayBuffer buf = new ByteArrayBuffer(Bytes.SIZEOF_INT*3);
        
        final int pos = Bytes.SIZEOF_INT;
        
        assertEquals(0,buf.getInt(pos));

        for(int i=0; i<LIMIT; i++) {

            final int expected = r.nextInt();
        
            buf.putInt(pos, expected);
        
            assertEquals(expected,buf.getInt(pos));
            
        }

        assertEquals(0,buf.getInt(pos-Bytes.SIZEOF_INT));

        assertEquals(0,buf.getInt(pos+Bytes.SIZEOF_INT));

    }

    public void test_getFloat_putFloat() {
        
        ByteArrayBuffer buf = new ByteArrayBuffer(Bytes.SIZEOF_FLOAT*3);
        
        final int pos = Bytes.SIZEOF_FLOAT;
        
        assertEquals(0f,buf.getFloat(pos));

        for (int i = 0; i < LIMIT; i++) {

            final float expected = r.nextFloat();

            buf.putFloat(pos, expected);

            assertEquals(expected, buf.getFloat(pos));
        }
        
        assertEquals(0f,buf.getFloat(pos-Bytes.SIZEOF_FLOAT));

        assertEquals(0f,buf.getFloat(pos+Bytes.SIZEOF_FLOAT));

    }
    
    public void test_getLong_putLong() {
        
        ByteArrayBuffer buf = new ByteArrayBuffer(Bytes.SIZEOF_LONG*3);
        
        final int pos = Bytes.SIZEOF_LONG;
        
        assertEquals(0L,buf.getLong(pos));

        for(int i=0; i<LIMIT; i++) {

            final long expected = r.nextLong();
        
            buf.putLong(pos, expected);
        
            assertEquals(expected,buf.getLong(pos));
            
        }

        assertEquals(0L,buf.getLong(pos-Bytes.SIZEOF_LONG));

        assertEquals(0L,buf.getLong(pos+Bytes.SIZEOF_LONG));

    }

    public void test_getDouble_putDouble() {
        
        ByteArrayBuffer buf = new ByteArrayBuffer(Bytes.SIZEOF_DOUBLE*3);
        
        final int pos = Bytes.SIZEOF_DOUBLE;
        
        assertEquals(0d,buf.getDouble(pos));

        for (int i = 0; i < LIMIT; i++) {

            final double expected = r.nextDouble();

            buf.putDouble(pos, expected);

            assertEquals(expected, buf.getDouble(pos));
        }
        
        assertEquals(0d,buf.getDouble(pos-Bytes.SIZEOF_DOUBLE));

        assertEquals(0d,buf.getDouble(pos+Bytes.SIZEOF_DOUBLE));

    }
    
}
