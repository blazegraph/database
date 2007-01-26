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
 * Created on Nov 27, 2006
 */

package com.bigdata.objndx;

import java.lang.ref.WeakReference;

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
