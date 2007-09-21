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

package com.bigdata.btree;


/**
 * Test code that chooses the child to search during recursive traversal of the
 * separator keys to find a leaf in which a key would be found.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFindChild extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestFindChild() {
    }

    /**
     * @param name
     */
    public TestFindChild(String name) {
        super(name);
    }

    /**
     * A test of {@link Node#findChild(int searchKeyOffset, byte[] searchKey)}
     * with zero offsets.
     */
    public void test_node_findChild01() {
     
        int m = 4;
        
        BTree btree = getBTree(m);

        /*
         * Create a test node.  We do not both to build this up from scratch
         * by inserting keys into the tree.
         */
        // keys[]  : [ 5  9 12    ]
        // child[] : [ a  b  c  d ]

        final int m2 = (m+1)/2;
        final int nentries = m2*4;
        final long[] childAddrs = new long[] { 1, 2, 3, 4, 0 };
        final int[] childEntryCounts = new int[]{m2,m2,m2,m2,0};
        
        IKeyBuffer keys = new MutableKeyBuffer(3, new byte[][] {//
                new byte[] { 5 }, //
                new byte[] { 9 }, //
                new byte[] { 12 }, //
                null });

        Node node = new Node(btree, 1, m, nentries, keys,
                childAddrs,
                childEntryCounts
                );
        
        assertEquals(0,node.findChild(new byte[]{1}));
        assertEquals(0,node.findChild(new byte[]{2}));
        assertEquals(0,node.findChild(new byte[]{3}));
        assertEquals(0,node.findChild(new byte[]{4}));
        assertEquals(1,node.findChild(new byte[]{5}));
        assertEquals(1,node.findChild(new byte[]{6}));
        assertEquals(1,node.findChild(new byte[]{7}));
        assertEquals(1,node.findChild(new byte[]{8}));
        assertEquals(2,node.findChild(new byte[]{9}));
        assertEquals(2,node.findChild(new byte[]{10}));
        assertEquals(2,node.findChild(new byte[]{11}));
        assertEquals(3,node.findChild(new byte[]{12}));
        assertEquals(3,node.findChild(new byte[]{13}));
        
    }
    
//    /**
//     * A test of {@link Node#findChild(int searchKeyOffset, byte[] searchKey)}
//     * with non-zero offsets.
//     */
//    public void test_node_findChild02() {
//
//        int m = 4;
//
//        BTree btree = getBTree(m);
//
//        /*
//         * Create a test node. We do not both to build this up from scratch by
//         * inserting keys into the tree.
//         */
//        // keys[]  : [ 5  9 12    ]
//        // child[] : [ a  b  c  d ]
//        final int m2 = (m + 1) / 2;
//        final int nentries = m2 * 4;
//        final long[] childAddrs = new long[] { 1, 2, 3, 4, 0 };
//        final int[] childEntryCounts = new int[] { m2, m2, m2, m2, 0 };
//
//        IKeyBuffer keys = new MutableKeyBuffer(3, new byte[][] {//
//                new byte[] { 1, 5 }, //
//                new byte[] { 1, 5, 9 }, //
//                new byte[] { 1, 5, 9, 12 }, //
//                null //
//                });
//
//        Node node = new Node(btree, 1, m, nentries, keys, childAddrs,
//                childEntryCounts);
//
//        // verify with searchKeyOffset == 0
//        assertEquals(0, node.findChild(0, new byte[] { 1, 4 }));
//        assertEquals(0, node.findChild(0, new byte[] { 1, 4, 9 }));
//        assertEquals(1, node.findChild(0, new byte[] { 1, 5 }));
//        assertEquals(1, node.findChild(0, new byte[] { 1, 5, 8 }));
//        assertEquals(2, node.findChild(0, new byte[] { 1, 5, 9 }));
//        assertEquals(2, node.findChild(0, new byte[] { 1, 5, 9, 11 }));
//        assertEquals(3, node.findChild(0, new byte[] { 1, 5, 9, 12 }));
//        assertEquals(3, node.findChild(0, new byte[] { 1, 5, 9, 13 }));
//
//        // verify with searchKeyOffset == 1
//        assertEquals(0, node.findChild(1, new byte[] { 1, 4 }));
//        assertEquals(0, node.findChild(1, new byte[] { 1, 4, 9 }));
//        assertEquals(1, node.findChild(1, new byte[] { 1, 5 }));
//        assertEquals(1, node.findChild(1, new byte[] { 1, 5, 8 }));
//        assertEquals(2, node.findChild(1, new byte[] { 1, 5, 9 }));
//        assertEquals(2, node.findChild(1, new byte[] { 1, 5, 9, 11 }));
//        assertEquals(3, node.findChild(1, new byte[] { 1, 5, 9, 12 }));
//        assertEquals(3, node.findChild(1, new byte[] { 1, 5, 9, 13 }));
//
//        // verify with searchKeyOffset == 2
////        assertEquals(0, node.findChild(2, new byte[] { 1, 4 }));
////        assertEquals(0, node.findChild(2, new byte[] { 1, 4, 9 }));
////        assertEquals(1, node.findChild(2, new byte[] { 1, 5 }));
//        assertEquals(1, node.findChild(2, new byte[] { 1, 5, 8 }));
//        assertEquals(2, node.findChild(2, new byte[] { 1, 5, 9 }));
//        assertEquals(2, node.findChild(2, new byte[] { 1, 5, 9, 11 }));
//        assertEquals(3, node.findChild(2, new byte[] { 1, 5, 9, 12 }));
//        assertEquals(3, node.findChild(2, new byte[] { 1, 5, 9, 13 }));
//        
//    }

}
