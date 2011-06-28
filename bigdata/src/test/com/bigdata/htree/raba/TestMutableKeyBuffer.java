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
 * Created on Jan 22, 2007
 */

package com.bigdata.htree.raba;

import java.io.IOException;

import junit.framework.TestCase2;

import com.bigdata.btree.raba.IRaba;
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
     * The keys of the {@link HTree} do not have any of the requirements of the
     * B+Tree keys. The keys in a bucket page may contain duplicates and
     * <code>null</code>s and are not searchable (only scannable, and then only
     * within a logical buddy bucket). Therefore this class reports
     * <code>false</code> for {@link IRaba#isKeys()}.
     */
    public void test_isKeys() {

        // Note: Must be a power of 2 for an HTree.
        final int m = 4;

        final MutableKeyBuffer kbuf = new MutableKeyBuffer(m);

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
     * {@link IRaba#search(byte[])} is not supported because it must be done
     * within a buddy hash bucket boundary. Since the keys are neither ordered
     * nor dense, search must use a simple scan.  Also note that there can be
     * duplicate keys within a buddy hash table.
     */
    public void test_search_not_supported() {

        // Note: Must be a power of 2 for an HTree.
        final int m = 4;

        final MutableKeyBuffer kbuf = new MutableKeyBuffer(m);
        try {
            kbuf.search(new byte[]{1,2});
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
        assertEquals(k1, kbuf.keys[1]);
        assertEquals(k2, kbuf.keys[2]);
        assertEquals(k3, kbuf.keys[3]);
        assertNull(kbuf.keys[0]);

        // remove the last key, leaving 2 keys.
        assertEquals("nkeys", 2, kbuf.remove(3));
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nkeys", 2, kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k1, kbuf.keys[1]);
        assertEquals(k2, kbuf.keys[2]);
        assertNull(kbuf.keys[0]);
        assertNull(kbuf.keys[3]);

        // insert a key in the 1st position (3 keys in the buffer).
        kbuf.set(0, k1);
        if (log.isInfoEnabled())
            log.info(kbuf.toString());
        assertEquals("nkeys", 3, kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k1, kbuf.keys[0]); // note: duplicate key.
        assertEquals(k1, kbuf.keys[1]);
        assertEquals(k2, kbuf.keys[2]);
        assertNull(kbuf.keys[3]);

        // insert a key in the last position (buffer is full again).
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
