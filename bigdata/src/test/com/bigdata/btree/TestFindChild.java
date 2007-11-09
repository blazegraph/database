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
