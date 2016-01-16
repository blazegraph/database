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
 * Created on Jul 1, 2011
 */

package com.bigdata.htree.raba;

import java.io.IOException;

import junit.framework.TestCase2;

import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.htree.HTree;
import com.bigdata.io.DataInputBuffer;

/**
 * Test suite for {@link MutableValuesBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMutableValueBuffer extends TestCase2 {

    /**
     * 
     */
    public TestMutableValueBuffer() {
    }

    /**
     * @param name
     */
    public TestMutableValueBuffer(String name) {
        super(name);
    }

    /**
     * Unit tests for constructor accepting only the capacity of the buffer.
     */
    public void test_ctor_capacity_correct_rejection() {

        // Capacity must be positive and a (positive) power of two (not 2^0).
        try {
            new MutableValueBuffer(0);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        try {
            new MutableValueBuffer(-1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        try {
            new MutableValueBuffer(1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        try {
            new MutableValueBuffer(3);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }
        try {
            new MutableValueBuffer(17);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

    }

    /**
     * These are legal invocations.
     */
    public void test_ctor_capacity_validInvocations() {

        new MutableValueBuffer(2);
        new MutableValueBuffer(4);
        new MutableValueBuffer(8);
        new MutableValueBuffer(16);
    }

    /**
     * Check the post-conditions for a legal invocation of the constructor.
     */
    public void test_ctor_capacity_postConditions() {

        final int m = 2;
        final MutableValueBuffer t = new MutableValueBuffer(m);
        assertEquals(0, t.size());
        assertEquals(m, t.capacity());
        assertNotNull(t.values);
        assertEquals(m, t.values.length);
        for (int i = 0; i < m; i++) {
            assertNull(t.values[i]);
        }

    }

    public void test_ctor_wrap_correct_rejection() {

        try {
            new MutableValueBuffer(0/* nvalues */, (byte[][]) null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

        // not a valid power of 2.
        try {
            new MutableValueBuffer(0/* nvalues */, new byte[1][]);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

        // not a valid power of 2.
        try {
            new MutableValueBuffer(0/* nvalues */, new byte[3][]);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

        // size is negative
        try {
            new MutableValueBuffer(-1/* nvalues */, new byte[2][]);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

        // size is too large
        try {
            new MutableValueBuffer(3/* nvalues */, new byte[2][]);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

    }

    /**
     * Unit test constructor wrapping the caller's data. For this constructor,
     * the new instance should use the same references and the capacity should
     * be the dimension of the source byte[][].
     */
    public void test_ctor_wrap_postConditions() {
        final byte[] k0 = null;
        final byte[] k1 = new byte[] { 1 };
        final byte[] k2 = new byte[] { 2 };
        final byte[] k3 = null;
        final byte[][] values = new byte[][] { k0, k1, k2, k3 };
        final MutableValueBuffer buf = new MutableValueBuffer(2/* nvalues */, values);
        assertEquals(values.length, buf.capacity());
        assertEquals(2, buf.size());
        // same data
        assertEquals(k0, buf.values[0]);
        assertEquals(k1, buf.values[1]);
        assertEquals(k2, buf.values[2]);
        assertEquals(k3, buf.values[3]);
        // same references.
        assertTrue(k0 == buf.values[0]);
        assertTrue(k1 == buf.values[1]);
        assertTrue(k2 == buf.values[2]);
        assertTrue(k3 == buf.values[3]);
        // same byte[][] reference.
        assertTrue(values == buf.values);
    }

    public void test_ctor_copy_correct_rejection() {
        try {
            new MutableValueBuffer(null);
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
        final byte[] k1 = new byte[] { 1 };
        final byte[] k2 = new byte[] { 2 };
        final byte[] k3 = null;
        final byte[][] values = new byte[][] { k0, k1, k2, k3 };
        final MutableValueBuffer src = new MutableValueBuffer(2/* nvalues */, values);
        final MutableValueBuffer buf = new MutableValueBuffer(src);

        assertEquals(values.length, buf.capacity());
        assertEquals(2, buf.size());
        // same data
        assertEquals(k0, buf.values[0]);
        assertEquals(k1, buf.values[1]);
        assertEquals(k2, buf.values[2]);
        assertEquals(k3, buf.values[3]);
        // same references.
        assertTrue(k0 == buf.values[0]);
        assertTrue(k1 == buf.values[1]);
        assertTrue(k2 == buf.values[2]);
        assertTrue(k3 == buf.values[3]);
        // NOT the same byte[][] reference.
        assertTrue(values != buf.values);
    }

    public void test_ctor_raba_with_explicit_capacity_correct_rejection() {

        final byte[] k0 = null;
        final byte[] k1 = new byte[] { 1 };
        final byte[] k2 = new byte[] { 2 };
        final byte[] k3 = null;
        final byte[][] values = new byte[][] { k0, k1, k2, k3 };
        final IRaba src = new ReadOnlyValuesRaba(values);

        // raba may not be null
        try {
            new MutableValueBuffer(2/* capacity */, (IRaba) null/* src */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

        // capacity must be 2^n where n is positive.
        try {
            new MutableValueBuffer(3/* capacity */, (IRaba) src);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

        // capacity must not be LT the capacity of the raba.
        try {
            new MutableValueBuffer(2/* capacity */, (IRaba) src);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            // ignored.
        }

    }

    /**
     * Unit test the constructor variant which accepts an {@link IRaba} and an
     * explicitly given capacity.
     */
    public void test_ctor_raba_with_explicit_capacity() {

        final byte[] k0 = new byte[] { 0 };
        final byte[] k1 = new byte[] { 1 };
        final byte[] k2 = new byte[] { 2 };
        final byte[] k3 = new byte[] { 3 };
        final byte[][] values = new byte[][] { k0, k1, k2, k3 };

        /*
         * Note: The ReadOnlyValuesRaba assumes that the 1st size values have
         * valid data. Thus it is not really suited to sparse random access
         * pattern of the HTree values and values.
         */
        final IRaba src = new ReadOnlyValuesRaba(4/* size */, values);

        final MutableValueBuffer buf = new MutableValueBuffer(8/* capacity */, src);

        assertEquals(8, buf.capacity()); // NB: As given!
        assertEquals(4, buf.size());
        // same data
        assertEquals(k0, buf.values[0]);
        assertEquals(k1, buf.values[1]);
        assertEquals(k2, buf.values[2]);
        assertEquals(k3, buf.values[3]);
        assertNull(buf.values[4]);
        assertNull(buf.values[5]);
        assertNull(buf.values[6]);
        assertNull(buf.values[7]);
        // NOT the same byte[][] reference.
        assertTrue(values != buf.values);

    }

    /**
     * The values of the {@link HTree} do not have any of the requirements of the
     * B+Tree values. The values in a bucket page may contain duplicates and
     * <code>null</code>s and are not searchable (only scannable, and then only
     * within a logical buddy bucket). Therefore this class reports
     * <code>false</code> for {@link IRaba#isvalues()}.
     */
    public void test_isvalues() {

        // Note: Must be a power of 2 for an HTree.
        final int m = 4;

        final MutableValueBuffer kbuf = new MutableValueBuffer(m);

        assertFalse(kbuf.isKeys());

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

        final MutableValueBuffer kbuf = new MutableValueBuffer(m);

        try {
            kbuf.add(new byte[] { 1, 2 });
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            kbuf.add(new byte[] { 1, 2 }, 0/* off */, 1/* len */);
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
     * {@link IRaba#search(byte[])} is not supported because it must be done
     * within a buddy hash bucket boundary. Since the values are neither ordered
     * nor dense, search must use a simple scan. Also note that there can be
     * duplicate values within a buddy hash table.
     */
    public void test_search_not_supported() {

        // Note: Must be a power of 2 for an HTree.
        final int m = 4;

        final MutableValueBuffer kbuf = new MutableValueBuffer(m);
        try {
            kbuf.search(new byte[] { 1, 2 });
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

    /**
     * Test for key mutation operations (adding, removing, etc).
     */
    public void test_mutation() {

        // Note: Must be a power of 2 for an HTree.
        final int m = 4;

        final MutableValueBuffer kbuf = new MutableValueBuffer(m);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("values.length", m, kbuf.values.length);
        assertEquals("maxvalues", m, kbuf.capacity());
        assertEquals("nvalues", 0, kbuf.nvalues);
        assertNull(kbuf.values[0]);

        final byte[] k0 = new byte[] { 1, 2, 0 };
        final byte[] k1 = new byte[] { 1, 2, 1 };
        final byte[] k2 = new byte[] { 1, 2, 2 };
        final byte[] k3 = new byte[] { 1, 2, 3 };

        kbuf.set(0/* index */, k0);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nvalues", 1, kbuf.nvalues);
        assertFalse(kbuf.isFull());
        assertEquals(k0, kbuf.values[0]);

        kbuf.set(2/* index */, k2); // Note leaves a "hole" at index 1.
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nvalues", 2, kbuf.nvalues);
        assertFalse(kbuf.isFull());
        assertEquals(k0, kbuf.values[0]);
        assertEquals(k2, kbuf.values[2]);

        kbuf.set(3/* index */, k3);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nvalues", 3, kbuf.nvalues);
        assertFalse(kbuf.isFull());
        assertEquals(k0, kbuf.values[0]);
        assertEquals(k2, kbuf.values[2]);
        assertEquals(k3, kbuf.values[3]);

        kbuf.set(1/* index */, k1);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nvalues", 4, kbuf.nvalues);
        assertTrue(kbuf.isFull());
        assertEquals(k0, kbuf.values[0]);
        assertEquals(k1, kbuf.values[1]);
        assertEquals(k2, kbuf.values[2]);
        assertEquals(k3, kbuf.values[3]);

        // remove the 1st key, leaving 3 values.
        assertEquals("nvalues", 3, kbuf.remove(0));
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nvalues", 3, kbuf.nvalues);
        assertFalse(kbuf.isFull());
        assertEquals(k1, kbuf.values[0]);
        assertEquals(k2, kbuf.values[1]);
        assertEquals(k3, kbuf.values[2]);
        assertNull(kbuf.values[3]);

        // remove the last key, leaving 2 values.
        assertEquals("nvalues", 2, kbuf.remove(2));
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nvalues", 2, kbuf.nvalues);
        assertFalse(kbuf.isFull());
        assertEquals(k1, kbuf.values[0]);
        assertEquals(k2, kbuf.values[1]);
        assertNull(kbuf.values[2]);
        assertNull(kbuf.values[3]);

        // insert a key in the 1st position (3 values in the buffer).
        kbuf.insert(0, k1);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nvalues", 3, kbuf.nvalues);
        assertFalse(kbuf.isFull());
        assertEquals(k1, kbuf.values[0]); // note: duplicate key.
        assertEquals(k1, kbuf.values[1]);
        assertEquals(k2, kbuf.values[2]);
        assertNull(kbuf.values[3]);

        // set a key in the last position (buffer is full again).
        kbuf.set(3, k2);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nvalues", 4, kbuf.nvalues);
        assertTrue(kbuf.isFull());
        assertEquals(k1, kbuf.values[0]);
        assertEquals(k1, kbuf.values[1]);
        assertEquals(k2, kbuf.values[2]);
        assertEquals(k2, kbuf.values[3]); // note: another duplicate key.

    }

}
