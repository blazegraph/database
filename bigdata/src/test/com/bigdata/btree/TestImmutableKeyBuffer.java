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
 * Created on Jan 16, 2007
 */

package com.bigdata.btree;

import java.util.Arrays;

/**
 * Test of an immutable representation of keys that is both compact and
 * efficient for search.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestImmutableKeyBuffer extends AbstractKeyBufferTestCase {

    /**
     * 
     */
    public TestImmutableKeyBuffer() {
    }

    /**
     * @param name
     */
    public TestImmutableKeyBuffer(String name) {
        super(name);
    }

    /**
     * Tests construction and access to an {@link ImmutableKeyBuffer} and the
     * conversion of immutable keys into a mutable byte[][].
     */
    public void test_immutableKeyBuffer() {
        
        final int nkeys = 4;
        final int maxKeys = 6;
        ImmutableKeyBuffer kbuf = new ImmutableKeyBuffer(nkeys,maxKeys,new byte[][]{//
                new byte[]{1,2},//
                new byte[]{1,2,2},//
                new byte[]{1,2,4,1},//
                new byte[]{1,2,5},//
                new byte[]{1,3}, // Note: ignored at nkeys=4
                new byte[]{2} // Note: ignored at nkeys=5
        });
        System.err.println("offsets="+Arrays.toString(kbuf.offsets));
        System.err.println("buf="+BytesUtil.toString(kbuf.buf)); // as unsigned byte[].
        // verify #of keys.
        assertEquals("nkeys",nkeys,kbuf.nkeys);
        assertEquals("nkeys",nkeys,kbuf.getKeyCount());
        // verify maximum #of keys.
        assertEquals("maxKeys",maxKeys,kbuf.maxKeys);
        assertEquals("maxKeys",maxKeys,kbuf.getMaxKeys());
        // verify prefix.
        assertEquals(2,kbuf.getPrefixLength());
        // verify prefix bytes.
        assertTrue(BytesUtil.compareBytesWithLenAndOffset(0,2,new byte[]{1,2},0,2,kbuf.buf)==0);
        assertEquals(new byte[]{1,2},kbuf.getPrefix());
        // verify offsets for each key.
        assertEquals(nkeys,kbuf.offsets.length);
        assertEquals(2,kbuf.offsets[0]);
        assertEquals(2,kbuf.offsets[1]);
        assertEquals(3,kbuf.offsets[2]);
        assertEquals(5,kbuf.offsets[3]);
        // verify getRemainderLength.
        assertEquals(0,kbuf.getRemainderLength(0));
        assertEquals(1,kbuf.getRemainderLength(1));
        assertEquals(2,kbuf.getRemainderLength(2));
        assertEquals(1,kbuf.getRemainderLength(3));
        // verify getKey
        assertEquals(new byte[]{1,2},kbuf.getKey(0));
        assertEquals(new byte[]{1,2,2},kbuf.getKey(1));
        assertEquals(new byte[]{1,2,4,1},kbuf.getKey(2));
        assertEquals(new byte[]{1,2,5},kbuf.getKey(3));

        /*
         * verify convertion to a mutable byte[][].
         */
        byte[][] mutableKeys = kbuf.toKeyArray();
        assertEquals("maxKeys",maxKeys, mutableKeys.length);
        assertEquals(new byte[] { 1, 2 }, mutableKeys[0]);
        assertEquals(new byte[] { 1, 2, 2 }, mutableKeys[1]);
        assertEquals(new byte[] { 1, 2, 4, 1 }, mutableKeys[2]);
        assertEquals(new byte[] { 1, 2, 5 }, mutableKeys[3]);
        assertNull(mutableKeys[4]);
        assertNull(mutableKeys[5]);
        
    }

    /**
     * Test case for construction of an immutable key buffer with one key
     * (degerate case allowed for a root leaf).
     */
    public void test_ctor_oneKey() {
        
        final int nkeys = 1;
        final int maxKeys = 3;
        final byte[][] keys = new byte[][]{
                new byte[]{1}
        };

        ImmutableKeyBuffer kbuf = new ImmutableKeyBuffer(nkeys, maxKeys, keys);
        
        assertEquals("nkeys",nkeys,kbuf.nkeys);
        assertEquals("nkeys",maxKeys,kbuf.maxKeys);
        assertEquals("prefixLength",1,kbuf.getPrefixLength());
        assertEquals("prefix",new byte[]{1},kbuf.getPrefix());
        assertEquals("keys[0]",new byte[]{1},kbuf.getKey(0));
        
    }
    
    public void test_ctor_zeroKeys() {

        ImmutableKeyBuffer kbuf = new ImmutableKeyBuffer(0,5,new byte[][] {});

        assertEquals("nkeys",0,kbuf.nkeys);
        assertEquals("prefixLength",0,kbuf.getPrefixLength());
        assertEquals("prefix",new byte[]{},kbuf.getPrefix());

    }

    /**
     * Correct rejection test case for the nkeys and maxKeys constructor
     * parameters.
     */
    public void test_ctor_correctRejection_nkeys_maxKeys() {

        try { 
            new ImmutableKeyBuffer(-1,5,new byte[][]{});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        try { 
            new ImmutableKeyBuffer(5,4,new byte[][]{});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        try { 
            new ImmutableKeyBuffer(5,-4,new byte[][]{});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
    }
    
    /**
     * Correct rejection test case for an immutable key buffer with null passed
     * as the byte[][].
     */
    public void test_ctor_correctRejection_keys() {
        
        try {
            new ImmutableKeyBuffer(1,5,null);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
    }

}
