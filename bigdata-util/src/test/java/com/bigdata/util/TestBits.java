/*

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
 * Created on Aug 6, 2009
 */

package com.bigdata.util;

import junit.framework.TestCase2;

/**
 * Test suite for {@link Bits}.
 */
public class TestBits extends TestCase2 {

    /**
     * 
     */
    public TestBits() {
    }

    /**
     * @param name
     */
    public TestBits(String name) {
        super(name);
    }

    public void test_ctor1() {

//        final byte[] d = new byte[1];
//        final ByteBuffer b = ByteBuffer.wrap(d);
//        final BitVector v = new ByteBufferBitVector(b);
//
//        assertEquals("length", 8L, v.length());
    	
    	byte v = 0;

        // verify range check.
        try {
            Bits.get(v, -1);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // verify range check.
        try {
            Bits.get(v, 8);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        for (int i = 0; i < 8; i++)
            assertEquals(false, Bits.get(v, i));

        // set bit zero.
//        d[0] |= (1 << 0);
        v = Bits.set(v, 0, true);
        
        if (log.isInfoEnabled())
            log.info(Bits.toString(v));
        assertEquals(true, Bits.get(v, 0));

        // clear bit zero.
//        d[0] &= ~(1 << 0);
        v = Bits.set(v, 0, false);
        
        if (log.isInfoEnabled())
            log.info(Bits.toString(v));
        assertEquals(false, Bits.get(v, 0));

    }

    /**
     * Verify set/clear of each bit in the first byte.
     */
    public void test_getBoolean() {

//        final byte[] d = new byte[1];
//        final ByteBuffer b = ByteBuffer.wrap(d);
//        final BitVector v = new ByteBufferBitVector(b);
    	
    	byte v = 0;

        // verify all bits are zero.
        for (int i = 0; i < 8; i++)
            assertEquals(false, Bits.get(v, i));

        // set/clear each bit in the first byte in turn.
        for (int i = 0; i < 8; i++) {

            // set bit
//            d[0] |= (1 << i);
        	v = Bits.set(v, i, true);
            
            if (log.isInfoEnabled())
                log.info(Bits.toString(v) + " : i=" + i + ", (1<<" + i + ")="
                        + (1 << i));
            assertEquals(true, Bits.get(v, i));

            // clear bit
//            d[0] &= ~(1 << i);
        	v = Bits.set(v, i, false);
            
            if (log.isInfoEnabled())
                log.info(Bits.toString(v));
            assertEquals(false, Bits.get(v, i));

        }

    }

    /**
     * Verify set/clear of each bit in the first byte.
     */
    public void test_getMask() {

    	byte v = 0;

        // verify all bits are zero.
        for (int i = 0; i < 8; i++)
            assertEquals(false, Bits.get(v, i));

        // set each bit in the byte
        for (int i = 0; i < 8; i++) {

            // set bit
        	v = Bits.set(v, i, true);
            assertEquals(true, Bits.get(v, i));
            
        }
        
        // mask off all but the 0 and 1 bits
        v = Bits.mask(v, 0, 1);
        if (log.isInfoEnabled())
            log.info(Bits.toString(v));
        assertEquals(3, v);

    }

}
