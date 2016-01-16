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
 * Created on Jan 22, 2007
 */

package com.bigdata.htree.raba;

import java.io.IOException;

import junit.framework.TestCase2;

import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.htree.HTree;
import com.bigdata.io.DataInputBuffer;

/**
 * Test suite for {@link MutableKeyBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMutableKeyBuffer extends TestCase2 {
    
    /**
     * 
     */
    public TestMutableKeyBuffer() {
    }

    /**
     * @param name
     */
    public TestMutableKeyBuffer(String name) {
        super(name);
    }

    /**
     * Unit tests for constructor accepting only the capacity of the buffer.
     */
    public void test_ctor_capacity_correct_rejection() {
        
        // Capacity must be positive and a (positive) power of two (not 2^0).
        try {
            new MutableKeyBuffer(0);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        try {
            new MutableKeyBuffer(-1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        try {
            new MutableKeyBuffer(1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        try {
            new MutableKeyBuffer(3);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        try {
            new MutableKeyBuffer(17);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        
    }

    /**
     * These are legal invocations.
     */
    public void test_ctor_capacity_validInvocations() {
       
        new MutableKeyBuffer(2);
        new MutableKeyBuffer(4);
        new MutableKeyBuffer(8);
        new MutableKeyBuffer(16);
    }
    
    /**
     * Check the post-conditions for a legal invocation of the constructor.
     */
    public void test_ctor_capacity_postConditions() {
       
        final int m = 2;
        final MutableKeyBuffer t = new MutableKeyBuffer(m);
        assertEquals(0, t.size());
        assertEquals(m, t.capacity());
        assertNotNull(t.keys);
        assertEquals(m, t.keys.length);
        for (int i = 0; i < m; i++) {
            assertNull(t.keys[i]);
        }

    }
    
    public void test_ctor_wrap_correct_rejection() {

        try {
            new MutableKeyBuffer(0/* nkeys */, (byte[][]) null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

        // not a valid power of 2.
        try {
            new MutableKeyBuffer(0/* nkeys */, new byte[1][]);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        
        // not a valid power of 2.
        try {
            new MutableKeyBuffer(0/* nkeys */, new byte[3][]);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

        // size is negative
        try {
            new MutableKeyBuffer(-1/* nkeys */, new byte[2][]);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        
        // size is too large
        try {
            new MutableKeyBuffer(3/* nkeys */, new byte[2][]);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        
    }

    /**
     * Unit test constructor wrapping the caller's data.  For this constructor,
     * the new instance should use the same references and the capacity should
     * be the dimension of the source byte[][].
     */
    public void test_ctor_wrap_postConditions() {
        final byte[] k0 = null;
        final byte[] k1 = new byte[]{1};
        final byte[] k2 = new byte[]{2};
        final byte[] k3 = null;
        final byte[][] keys = new byte[][] {
                k0,k1,k2,k3
        };
        final MutableKeyBuffer buf = new MutableKeyBuffer(2/* nkeys */, keys);
        assertEquals(keys.length, buf.capacity());
        assertEquals(2, buf.size());
        // same data
        assertEquals(k0, buf.keys[0]);
        assertEquals(k1, buf.keys[1]);
        assertEquals(k2, buf.keys[2]);
        assertEquals(k3, buf.keys[3]);
        // same references.
        assertTrue(k0 == buf.keys[0]);
        assertTrue(k1 == buf.keys[1]);
        assertTrue(k2 == buf.keys[2]);
        assertTrue(k3 == buf.keys[3]);
        // same byte[][] reference.
        assertTrue(keys == buf.keys);
    }
    
    public void test_ctor_copy_correct_rejection() {
        try {
            new MutableKeyBuffer(null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
    }

    /**
     * Unit test the copy constructor.
     */
    public void test_ctor_copy() {
        final byte[] k0 = null;
        final byte[] k1 = new byte[]{1};
        final byte[] k2 = new byte[]{2};
        final byte[] k3 = null;
        final byte[][] keys = new byte[][] {
                k0,k1,k2,k3
        };
        final MutableKeyBuffer src = new MutableKeyBuffer(2/* nkeys */, keys);
        final MutableKeyBuffer buf = new MutableKeyBuffer(src);

        assertEquals(keys.length, buf.capacity());
        assertEquals(2, buf.size());
        // same data
        assertEquals(k0, buf.keys[0]);
        assertEquals(k1, buf.keys[1]);
        assertEquals(k2, buf.keys[2]);
        assertEquals(k3, buf.keys[3]);
        // same references.
        assertTrue(k0 == buf.keys[0]);
        assertTrue(k1 == buf.keys[1]);
        assertTrue(k2 == buf.keys[2]);
        assertTrue(k3 == buf.keys[3]);
        // NOT the same byte[][] reference.
        assertTrue(keys != buf.keys);
    }

    public void test_ctor_raba_with_explicit_capacity_correct_rejection() {

        final byte[] k0 = null;
        final byte[] k1 = new byte[]{1};
        final byte[] k2 = new byte[]{2};
        final byte[] k3 = null;
        final byte[][] keys = new byte[][] {
                k0,k1,k2,k3
        };
        final IRaba src = new ReadOnlyValuesRaba(keys);

        // raba may not be null
        try {
            new MutableKeyBuffer(2/* capacity */, (IRaba) null/* src */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        
        // capacity must be 2^n where n is positive.
        try {
            new MutableKeyBuffer(3/* capacity */, (IRaba) src);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

        // capacity must not be LT the capacity of the raba.
        try {
            new MutableKeyBuffer(2/* capacity */, (IRaba) src);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

    }

    /**
     * Unit test the constructor variant which accepts an {@link IRaba} and
     * an explicitly given capacity.
     */
    public void test_ctor_raba_with_explicit_capacity() {

        final byte[] k0 = new byte[]{0};
        final byte[] k1 = new byte[]{1};
        final byte[] k2 = new byte[]{2};
        final byte[] k3 = new byte[]{3};
        final byte[][] keys = new byte[][] {
                k0,k1,k2,k3
        };

        /*
         * Note: The ReadOnlyValuesRaba assumes that the 1st size values have
         * valid data. Thus it is not really suited to sparse random access
         * pattern of the HTree keys and values.
         */
        final IRaba src = new ReadOnlyValuesRaba(4/*size*/,keys);

        final MutableKeyBuffer buf = new MutableKeyBuffer(8/* capacity */, src);

        assertEquals(8,buf.capacity()); // NB: As given!
        assertEquals(4, buf.size());
        // same data
        assertEquals(k0, buf.keys[0]);
        assertEquals(k1, buf.keys[1]);
        assertEquals(k2, buf.keys[2]);
        assertEquals(k3, buf.keys[3]);
        assertNull(buf.keys[4]);
        assertNull(buf.keys[5]);
        assertNull(buf.keys[6]);
        assertNull(buf.keys[7]);
        // NOT the same byte[][] reference.
        assertTrue(keys != buf.keys);

    }
    
    /**
     * The keys of the {@link HTree} do not have any of the requirements of the
     * B+Tree keys. The keys in a bucket page may contain duplicates but are now
     * ordered and therefore searchable. Therefore this class reports
     * <code>true</code> for {@link IRaba#isKeys()}.
     */
    public void test_isKeys() {

        // Note: Must be a power of 2 for an HTree.
        final int m = 4;

        final MutableKeyBuffer kbuf = new MutableKeyBuffer(m);

        assertTrue(kbuf.isKeys());
        
    }
    
    /**
     * The various methods "add()" methods are not supported because they do not
     * specify the index of the tuple and we need to have that since the tuples
     * are not dense and the bucket page is divided into buddy bucket regions
     * which must be managed separately.
     * 
     * @throws IOException 
     */
    public void test_add_not_supported() throws IOException {

        // Note: Must be a power of 2 for an HTree.
        final int m = 4;

        final MutableKeyBuffer kbuf = new MutableKeyBuffer(m);

        try {
            kbuf.add(new byte[] {1,2});
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            kbuf.add(new byte[] {1,2},0/*off*/,1/*len*/);
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            kbuf.add(new DataInputBuffer(new byte[2]), 1/* len */);
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

    /**
     * {@link IRaba#search(byte[])} is now supported because a BucketPage
     * contains ordered keys. Note that there can be
     * duplicate keys within a buddy hash table.
     */
    public void test_search_not_supported() {

        // Note: Must be a power of 2 for an HTree.
        final int m = 4;

        final MutableKeyBuffer kbuf = new MutableKeyBuffer(m);
        try {
            kbuf.search(new byte[]{1,2});
        } catch (UnsupportedOperationException ex) {
            fail("Search should be supported if keys are ordered");
        }

    }

    /**
     * Test for key mutation operations (adding, removing, etc).
     */
    public void test_mutation() {

        // Note: Must be a power of 2 for an HTree.
        final int m = 4;
        
        final MutableKeyBuffer kbuf = new MutableKeyBuffer(m);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("keys.length", m, kbuf.keys.length);
        assertEquals("maxKeys", m, kbuf.capacity());
        assertEquals("nkeys", 0, kbuf.nkeys);
        assertNull(kbuf.keys[0]);

        final byte[] k0 = new byte[] { 1, 2, 0 };
        final byte[] k1 = new byte[] { 1, 2, 1 };
        final byte[] k2 = new byte[] { 1, 2, 2 };
        final byte[] k3 = new byte[] { 1, 2, 3 };

        kbuf.set(0/* index */, k0);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nkeys", 1, kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k0, kbuf.keys[0]);

        kbuf.set(2/*index*/,k2); // Note leaves a "hole" at index 1.
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nkeys", 2, kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k0, kbuf.keys[0]);
        assertEquals(k2, kbuf.keys[2]);

        kbuf.set(3/*index*/,k3);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nkeys", 3, kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k0, kbuf.keys[0]);
        assertEquals(k2, kbuf.keys[2]);
        assertEquals(k3, kbuf.keys[3]);

        kbuf.set(1/*index*/,k1);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nkeys", 4, kbuf.nkeys);
        assertTrue(kbuf.isFull());
        assertEquals(k0, kbuf.keys[0]);
        assertEquals(k1, kbuf.keys[1]);
        assertEquals(k2, kbuf.keys[2]);
        assertEquals(k3, kbuf.keys[3]);

        // remove the 1st key, leaving 3 keys.
        assertEquals("nkeys", 3, kbuf.remove(0));
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nkeys", 3, kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k1, kbuf.keys[0]);
        assertEquals(k2, kbuf.keys[1]);
        assertEquals(k3, kbuf.keys[2]);
        assertNull(kbuf.keys[3]);

        // remove the last key, leaving 2 keys.
        assertEquals("nkeys", 2, kbuf.remove(2));
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nkeys", 2, kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k1, kbuf.keys[0]);
        assertEquals(k2, kbuf.keys[1]);
        assertNull(kbuf.keys[2]);
        assertNull(kbuf.keys[3]);

        // insert a key in the 1st position (3 keys in the buffer).
        kbuf.insert(0, k1);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nkeys", 3, kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k1, kbuf.keys[0]); // note: duplicate key.
        assertEquals(k1, kbuf.keys[1]);
        assertEquals(k2, kbuf.keys[2]);
        assertNull(kbuf.keys[3]);

        // set a key in the last position (buffer is full again).
        kbuf.set(3, k2);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nkeys", 4, kbuf.nkeys);
        assertTrue(kbuf.isFull());
        assertEquals(k1, kbuf.keys[0]);
        assertEquals(k1, kbuf.keys[1]);
        assertEquals(k2, kbuf.keys[2]);
        assertEquals(k2, kbuf.keys[3]); // note: another duplicate key.

    }

}
