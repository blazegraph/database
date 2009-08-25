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
 * Created on Jan 8, 2007
 */

package com.bigdata.btree;

import com.bigdata.btree.raba.MutableKeyBuffer;
import com.bigdata.btree.raba.MutableValueBuffer;



/**
 * Test suite for various utility methods, both static and instance, on
 * {@link AbstractNode}.
 * 
 * @todo review converage now that so much has changed with respect to keys.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestUtilMethods extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestUtilMethods() {
    }

    /**
     * @param name
     */
    public TestUtilMethods(String name) {
        super(name);
    }

//    public void test_keysAsString() {
//        
//        assertEquals("[1, 2, 3]", AbstractNode.keysAsString(1, new int[]{1,2,3}));
//
//        assertEquals("[a, b, c]", AbstractNode.keysAsString(1, new char[]{'a','b','c'}));
//
//        assertEquals("[{1, 2, 3}]", AbstractNode.keysAsString(3, new int[]{1,2,3}));
//
//        assertEquals("[{1, 2, 3}, {4, 5, 6}]", AbstractNode.keysAsString(3, new int[]{1,2,3,4,5,6}));
//
//        assertEquals("[{a, b, c}, {d, e, f}]", AbstractNode.keysAsString(3, new char[]{'a','b','c','d','e','f'}));
//
//    }
//    
//    /**
//     * @todo test routines to get and set individual keys. when stride gt 1 this
//     *       requires creating an array of values. i need to determine how to
//     *       handle stride == 1 which currently autoboxes, e.g., int to Integer
//     *       vs int to int[0].
//     */
//    public void test_getSetKey() {
//        fail("write tests");
//    }
    
//    /**
//     * test routines to copy a key from some index in a node or leaf to another
//     * index in either the same or another node or leaf.
//     * 
//     * @todo move this method and this test to {@link MutableKeyBuffer}?
//     */
//    public void test_copyKey() {
//
//        final int m = 3;
//        final int nkeys = 3;
//        final BTree btree = getBTree(m);
//
//        Leaf leaf1 = new Leaf(btree, 1L, m, nkeys, new int[] { 1, 2, 3,
//                0 }, new Integer[] { 1, 2, 3, 0 });
//
//        Leaf leaf2 = new Leaf(btree, 1L, m, nkeys, new int[] { 4, 5, 6,
//                0 }, new Integer[] { 1, 2, 3, 0 });
//
//        // original state.
//        assertKeys(new int[] { 1, 2, 3 }, leaf1);
//        assertKeys(new int[] { 4, 5, 6 }, leaf2);
//
//        leaf1.copyKey(0, leaf2.keys, 0);
//        assertKeys(new int[] { 4, 2, 3 }, leaf1);
//        assertKeys(new int[] { 4, 5, 6 }, leaf2);
//
//        leaf2.copyKey(0, leaf1.keys, 2);
//        assertKeys(new int[] { 4, 2, 3 }, leaf1);
//        assertKeys(new int[] { 3, 5, 6 }, leaf2);
//
//        leaf1.copyKey(2, leaf2.keys, 1);
//        assertKeys(new int[] { 4, 2, 5 }, leaf1);
//        assertKeys(new int[] { 3, 5, 6 }, leaf2);
//
//    }

    /**
     * Note: the arrays are dimensions to nkeys + 1 for a leaf to provide a slot
     * for overflow during a split.
     */
    public void test_assertKeysMonotonic() {

        final int m = 3;
        final int nkeys = 3;
        final BTree btree = getBTree(m);

        final Leaf leaf = new Leaf(btree, 1L, //
                new MutableLeafData(//
                    new MutableKeyBuffer(nkeys,
                    // keys
                        new byte[][] {//
                        new byte[] { 1 }, //
                        new byte[] { 2 }, //
                        new byte[] { 3 },//
                        null }),//
                    // vals
                    new MutableValueBuffer(nkeys, //
                        // vals
                        new byte[][] { //
                        new byte[] { 1 },//
                        new byte[] { 2 },//
                        new byte[] { 3 },//
                        new byte[] { 0 }} //
                    ),
                    null,// timestamps
                    null// deleteMarkers
                    ));

        leaf.assertKeysMonotonic();

        // access the byte[][].
        final byte[][] keys = ((MutableKeyBuffer)leaf.getKeys()).keys;

        // swap around two keys so that they are out of order.
        final byte[] tmp = keys[0];
        keys[0] = keys[1];
        keys[1] = tmp;

        try {
            leaf.assertKeysMonotonic();
            fail("Expecting " + AssertionError.class);
        } catch (AssertionError ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

    }
    
}
