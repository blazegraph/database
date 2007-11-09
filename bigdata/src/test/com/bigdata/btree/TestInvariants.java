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
 * Created on Nov 27, 2006
 */

package com.bigdata.btree;



/**
 * Test suite for the assertions that detect a violation of the invariants for a
 * node or leaf in the tree.
 * 
 * Note: Assertions MUST be enabled or this test suite will show failures.
 * 
 * Note: The keys[] array MUST be dimensioned to one more than the maximum #of
 * keys allowed in order to permit temporary overflow during insert().
 * 
 * @todo update this test suite, possibly refactoring some of the assertion
 *       method under test into the {@link IKeyBuffer} interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestInvariants extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestInvariants() {
    }

    /**
     * @param name
     */
    public TestInvariants(String name) {
        super(name);
    }

//    /**
//     * Test the invariant assertions for a leaf with branching factor three (3).
//     * These tests validate the correct computation of the minimum and maximum
//     * #of keys for a node.
//     * 
//     * @see AbstractNode#AbstractNode(BTree, int)
//     */
//    public void test_leafInvariantsBranchingFactor3() {
//
//        BTree btree = getBTree(3);
//        
//        // invalid node just serves as parent to our leaf.
//        Node n = new Node(btree);
//        
//        Leaf l = new Leaf(btree);
//        l.parent = new WeakReference<Node>(n);
//
//        assertEquals("branchingFactor",3,l.branchingFactor);
//        assertEquals("minKeys",2,l.minKeys);
//        assertEquals("maxKeys",3,l.maxKeys);
//        
//        // valid leaf (specific values of keys are ignored).
//        {
//            l.nkeys = 2;
//            l.keys = new int[4];
//            l.assertInvariants();
//        }
//
//        // valid leaf.
//        {
//            l.nkeys = 3;
//            l.keys = new int[4];
//            l.assertInvariants();
//        }
//        
//        /*
//         * invalid leaf - too few keys for order 3.
//         */
//        try {
//            l.nkeys = 1;
//            l.keys = new int[4];
//            l.assertInvariants();
//            fail("Expecting: " + AssertionError.class);
//        } catch (AssertionError ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//
//        /*
//         * invalid leaf - too many keys for order 3.
//         */
//        try {
//            l.nkeys = 4;
//            l.keys = new int[4];
//            l.assertInvariants();
//            fail("Expecting: " + AssertionError.class);
//        } catch (AssertionError ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//
//    }
//    
//    /**
//     * Test the invariant assertions for a leaf with branching factor four(4).
//     * These tests validate the correct computation of the minimum and maximum
//     * #of keys for a node.
//     * 
//     * @see AbstractNode#AbstractNode(BTree, int)
//     */
//    public void test_leafInvariantsBranchingFactor4() {
//
//        BTree btree = getBTree(4);
//        
//        // invalid node just serves as parent to our leaf.
//        Node n = new Node(btree);
//        
//        Leaf l = new Leaf(btree);
//        l.parent = new WeakReference<Node>(n);
//
//        assertEquals("branchingFactor",4,l.branchingFactor);
//        assertEquals("minKeys",2,l.minKeys);
//        assertEquals("maxKeys",4,l.maxKeys);        
//
//        // valid leaf (specific values and data type of keys are ignored).
//        {
//            l.nkeys = 2;
//            l.keys = new int[5];
//            l.assertInvariants();
//        }
//
//        // valid leaf.
//        {
//            l.nkeys = 3;
//            l.keys = new int[5];
//            l.assertInvariants();
//        }
//        
//        // valid leaf.
//        {
//            l.nkeys = 4;
//            l.keys = new int[5];
//            l.assertInvariants();
//        }
//        
//        /*
//         * invalid leaf - too few keys for order 4.
//         */
//        try {
//            l.nkeys = 1;
//            l.keys = new int[5];
//            l.assertInvariants();
//            fail("Expecting: " + AssertionError.class);
//        } catch (AssertionError ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//
//        /*
//         * invalid leaf - too many keys for order 4.
//         */
//        try {
//            l.nkeys = 5;
//            l.keys = new int[5];
//            l.assertInvariants();
//            fail("Expecting: " + AssertionError.class);
//        } catch (AssertionError ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//
//    }
    
}
