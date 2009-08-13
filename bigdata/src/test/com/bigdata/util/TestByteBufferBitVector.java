/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 6, 2009
 */

package com.bigdata.util;

import it.unimi.dsi.bits.BitVector;

import java.nio.ByteBuffer;

import com.bigdata.util.ByteBufferBitVector;

import junit.framework.TestCase2;

/**
 * Test suite for {@link ByteBufferBitVector}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestByteBufferBitVector extends TestCase2 {

    /**
     * 
     */
    public TestByteBufferBitVector() {
    }

    /**
     * @param name
     */
    public TestByteBufferBitVector(String name) {
        super(name);
    }

    /** Correct rejection test for ctor1. */
    public void test_ctor1_correct_rejection() {

        try {
            new ByteBufferBitVector(null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("ignoring expected exception: " + ex);
        }
        
    } 
    
    public void test_ctor1() {

        final byte[] d = new byte[1];
        final ByteBuffer b = ByteBuffer.wrap(d);
        final BitVector v = new ByteBufferBitVector(b);

        assertEquals("length", 8L, v.length());

        // verify range check.
        try {
            v.getBoolean(-1);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // verify range check.
        try {
            v.getBoolean(8);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        for (long i = 0; i < 8L; i++)
            assertEquals(false, v.getBoolean(i));

        // set bit zero.
        d[0] |= (1 << 0);
        if (log.isInfoEnabled())
            log.info(v.toString());
        assertEquals(true, v.getBoolean(0));

        // clear bit zero.
        d[0] &= ~(1 << 0);
        if (log.isInfoEnabled())
            log.info(v.toString());
        assertEquals(false, v.getBoolean(0));

    }

    /**
     * Correct rejection and assumptions for ctor accepting offset and length
     * options.
     * 
     * @todo this tests with an even byte offset. Try w/ only a few bits offset.
     */
    public void test_ctor2() {

        final byte[] d = new byte[3];
        final ByteBuffer b = ByteBuffer.wrap(d);
        final BitVector v = new ByteBufferBitVector(b, 8, 8);

        assertEquals("length", 8L, v.length());

        // verify range check.
        try {
            v.getBoolean(-1);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // verify range check.
        try {
            v.getBoolean(8);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        for (long i = 0; i < 8L; i++)
            assertEquals(false, v.getBoolean(i));

        // set bit zero.
        d[1] |= (1 << 0);
        if (log.isInfoEnabled())
            log.info(v.toString());
        assertEquals(true, v.getBoolean(0));

        // clear bit zero.
        d[1] &= ~(1 << 0);
        if (log.isInfoEnabled())
            log.info(v.toString());
        assertEquals(false, v.getBoolean(0));
        
    }

    /**
     * Verify set/clear of each bit in the first byte.
     */
    public void test_getBoolean() {

        final byte[] d = new byte[1];
        final ByteBuffer b = ByteBuffer.wrap(d);
        final BitVector v = new ByteBufferBitVector(b);

        // verify all bits are zero.
        for (long i = 0; i < 8L; i++)
            assertEquals(false, v.getBoolean(i));

        // set/clear each bit in the first byte in turn.
        for (int i = 0; i < 8; i++) {

            // set bit
            d[0] |= (1 << i);
            if (log.isInfoEnabled())
                log.info(v.toString() + " : i=" + i + ", (1<<" + i + ")="
                        + (i << i));
            assertEquals(true, v.getBoolean(i));

            // clear bit
            d[0] &= ~(1 << i);
            if (log.isInfoEnabled())
                log.info(v.toString());
            assertEquals(false, v.getBoolean(i));

        }

    }

}
