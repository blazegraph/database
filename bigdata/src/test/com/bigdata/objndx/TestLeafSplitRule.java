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

import java.util.Arrays;

/**
 * Test the logic for computing the splitIndex (the first key to move to the
 * rightSibling when a node is split) and the separatorKey (the key that is
 * inserted into the parent when a node is split).
 *
 * @see src/architecture/btree.xls
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLeafSplitRule extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestLeafSplitRule() {
    }

    /**
     * @param name
     */
    public TestLeafSplitRule(String name) {
        super(name);
    }

    /**
     * Test of the leaf split rule for a 2-3 tree.
     */
    public void test_leafSlowSplitRuleBranchingFactor3() {

        doSplitRuleTestBranchingFactor3( new SlowLeafSplitRule(3) );
        
    }

//    public void test_leafFastSplitRuleBranchingFactor3() {
//
//        doSplitRuleTestBranchingFactor3( new FastLeafSplitRule(3) );
//        
//    }
    
    public void doSplitRuleTestBranchingFactor3(ILeafSplitRule splitRule) {

        BTree btree = getBTree(3);

        Leaf a = (Leaf)btree.getRoot();

        /*
         * setup the leaf without using insert(key,val).
         */
        
        a.nkeys = 3;
        
        a.keys = new int[]{3,5,7};

        /*
         * verify that a key that will be inserted into the low leaf results in
         * a split index that puts one value into the low leaf and two values
         * into the high leaf. This way the post-condition of the insert and
         * split will be two laeves each having two values, which is the minimum
         * for a 2-3 tree.
         */
        // postcondition: [ 1 3 - ], [ 5 7 - ]
        assertEquals(1, splitRule.getSplitIndex(a.keys,1));
        assertEquals(5, splitRule.getSeparatorKey());
        // postcondition: [ 3 4 - ], [ 5 7 - ]
        assertEquals(1, splitRule.getSplitIndex(a.keys,4));
        assertEquals(5, splitRule.getSeparatorKey());
        
        /*
         * verify that a key that will be inserted into the high leaf results in
         * a split index that puts two values into the low leaf and one value
         * into the high leaf. This way the post-condition of the insert and
         * split will be two leaves each having two values, which is the minimum
         * for a 2-3 tree.
         */
        // postcondition: [ 3 5 - ], [ 6 7 - ]
        assertEquals(2, splitRule.getSplitIndex(a.keys,6));
        assertEquals(6, splitRule.getSeparatorKey());
        // postcondition: [ 3 5 - ], [ 7 8 - ]
        assertEquals(2, splitRule.getSplitIndex(a.keys,8));
        assertEquals(7, splitRule.getSeparatorKey());

    }

    /**
     * Test of the leaf split rule for a 2-4 tree.
     * 
     * Note: the split rule always chooses the index m/2 when m is even. The
     * complicated case is when m is odd since the insert key must be considered
     * as well.
     */
    public void test_leafSlowSplitRuleBranchingFactor4() {

        doSplitRuleTestBranchingFactor4( new SlowLeafSplitRule(4) );
        
    }

//    public void test_leafFastSplitRuleBranchingFactor4() {
//
//        doSplitRuleTestBranchingFactor4( new FastLeafSplitRule(4) );
//        
//    }
      
    public void doSplitRuleTestBranchingFactor4(ILeafSplitRule splitRule) {
        
        BTree btree = getBTree(4);

        Leaf a = (Leaf)btree.getRoot();
        
        /*
         * setup the leaf without using insert(key,value).
         */
        a.nkeys = 4;

        a.keys = new int[]{3,5,7,9};

        /*
         * verify that a key that will be inserted into the low leaf results in
         * a split index that puts two values into the low leaf and two values
         * into the high leaf. This way the post-condition of the insert and
         * split will be two leaves, one having three and the other having two
         * values.
         */
        // postcondition: [ 1 3 5 - ], [ 7 9 - - ]
        assertEquals(2, splitRule.getSplitIndex(a.keys,1));
        assertEquals(7, splitRule.getSeparatorKey());
        // postcondition: [ 3 4 5 - ], [ 7 9 - - ]
        assertEquals(2, splitRule.getSplitIndex(a.keys,4));
        assertEquals(7, splitRule.getSeparatorKey());
        // postcondition: [ 3 5 6 - ], [ 7 9 - - ]
        assertEquals(2, splitRule.getSplitIndex(a.keys,6));
        assertEquals(7, splitRule.getSeparatorKey());
       
        /*
         * verify that a key that will be inserted into the high leaf results in
         * a split index that puts three values into the low leaf and two values
         * into the high leaf. This way the post-condition of the insert and
         * split will be two leaves, one having three and the other having two
         * values.
         */
        // postcondition: [ 3 5 7 - ], [ 8 9 - - ]
        assertEquals(3, splitRule.getSplitIndex(a.keys,8));
        assertEquals(8, splitRule.getSeparatorKey());
        // postcondition: [ 3 5 7 - ], [ 9 10 - - ]
        assertEquals(3, splitRule.getSplitIndex(a.keys,10));
        assertEquals(9, splitRule.getSeparatorKey());

    }

//    /**
//     * Tests one rule against another looking for agreement in their behavior.
//     */
//    public void test_stress_leafSplitRule() {
//        
//        int ntrials = 10;
//        
//        int ntestkeys = 20;
//        
//        int maxKeyValue = 100;
//        
//        int[] branchingFactors = new int[]{3,4,5,6,7,8,9};
//        
//        for( int i=0; i<branchingFactors.length; i++) {
//            
//            int m = branchingFactors[i];
//
//            int[] keys = new int[m];
//            
//            System.err.println("m="+m);
//            
//            ILeafSplitRule slowSplitRule = new SlowLeafSplitRule(m);
//
//            ILeafSplitRule fastSplitRule = new FastLeafSplitRule(m);
//            
//            for( int trial=0; trial<ntrials; trial++ ) {
//
//                // generate maxKeys random keys
//                for( int k=0; k<m; k++ ) {
//                    
//                    keys[k] = nextKey(maxKeyValue);
//                    
//                }
//                
//                Arrays.sort(keys);
//
//                BTree btree = getBTree(m);
//                
//                Leaf root = (Leaf) btree.root;
//                
//                // set the keys on the root leaf.
//                root.nkeys = m;
//                
//                root.keys = keys;
//                
//                for( int n=0; n<ntestkeys; n++ ) {
//
//                    int insertKey = nextKey(maxKeyValue);
//                    
//                    int splitIndex = slowSplitRule.getSplitIndex(keys, insertKey);
//                    
//                    int separatorKey = slowSplitRule.getSeparatorKey();
//                    
//                    assertEquals("splitIndex",splitIndex,fastSplitRule.getSplitIndex(keys, insertKey));
//
//                    assertEquals("separatorKey",separatorKey,fastSplitRule.getSeparatorKey());
//                    
//                }
//                
//            }
//
//        }
//        
//    }
//
//    /**
//     * Return a random key.
//     * @return
//     */
//    private int nextKey(int max) {
//     
//        return r.nextInt(max) + BTree.NEGINF;
//        
//    }

}
