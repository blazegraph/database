/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jan 8, 2007
 */

package com.bigdata.btree;

import com.bigdata.btree.AbstractNode;
import com.bigdata.btree.BTree;
import com.bigdata.btree.Leaf;
import com.bigdata.btree.MutableKeyBuffer;

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

        Leaf leaf = new Leaf(btree, 1L, m, new MutableKeyBuffer(nkeys,
                new byte[][] { new byte[] { 1 }, new byte[] { 2 },
                        new byte[] { 3 }, null }), new Integer[] { 1, 2, 3, 0 });

        leaf.assertKeysMonotonic();

        // access the byte[][].
        byte[][] keys = ((MutableKeyBuffer)leaf.keys).keys;

        // swap around two keys so that they are out of order.
        byte[] tmp = keys[0];
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
