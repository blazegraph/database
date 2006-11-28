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
public class TestNodeSplitRule extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestNodeSplitRule() {
    }

    /**
     * @param name
     */
    public TestNodeSplitRule(String name) {
        super(name);
    }

    /**
     * Test of the node nsplit rule for a 2-3 tree.
     */
    public void test_nodeSlowSplitRuleBranchingFactor3() {

        int maxKeys = 3 - 1;
        
        doSplitRuleTestBranchingFactor3( new SlowNodeSplitRule(maxKeys) );
        
    }

//    public void test_leafFastSplitRuleBranchingFactor3() {
//
//        doSplitRuleTestBranchingFactor3( new FastLeafSplitRule(3) );
//        
//    }
    
    public void doSplitRuleTestBranchingFactor3(INodeSplitRule splitRule) {

        BTree btree = getBTree(3);

        Node a = new Node(btree);

        /*
         * setup without using insert(key,val).
         */
        
        a.nkeys = 2;
        
        a.keys = new int[]{3,5};

        /*
         * verify that a key that will be inserted into the low leaf results in
         * a split index that puts one value into the low leaf and two values
         * into the high leaf. This way the post-condition of the insert and
         * split will be two laeves each having two values, which is the minimum
         * for a 2-3 tree.
         */
        // postcondition: [ 2 - ], [ 5 - ]
        assertEquals(1, splitRule.getSplitIndex(a.keys,1));
        assertEquals(3, splitRule.getSeparatorKey());
        // postcondition: [ 3 - ], [ 5 - ]
        assertEquals(1, splitRule.getSplitIndex(a.keys,4));
        assertEquals(4, splitRule.getSeparatorKey());
        
        /*
         * verify that a key that will be inserted into the high leaf results in
         * a split index that puts two values into the low leaf and one value
         * into the high leaf. This way the post-condition of the insert and
         * split will be two leaves each having two values, which is the minimum
         * for a 2-3 tree.
         */
        /*
         * postcondition: [ 3 - ], [ 6 - ] : Note that nothing is copied to the
         * rightSibling for this case.  The splitIndex(2) is equal to the keys[]
         * dimension(2).
         */
        assertEquals(2, splitRule.getSplitIndex(a.keys,6));
        assertEquals(5, splitRule.getSeparatorKey());
        
        /*
         * setup some different keys.
         */
        a.nkeys = 2;
        a.keys = new int[]{5,7};
        
        // postcondition: [ 3 - ], [ 7 - ]
        assertEquals(1, splitRule.getSplitIndex(a.keys,3));
        assertEquals(5, splitRule.getSeparatorKey());

    }

    /**
     * Test of the node split rule for a 2-4 tree.
     */
    public void test_nodeSlowSplitRuleBranchingFactor4() {

        int maxKeys = 4 - 1;
        
        doSplitRuleTestBranchingFactor4( new SlowNodeSplitRule(maxKeys) );
        
    }

//    public void test_nodeFastSplitRuleBranchingFactor4() {
//
//        doSplitRuleTestBranchingFactor4( new FastLeafSplitRule(4) );
//        
//    }
    
    
    public void doSplitRuleTestBranchingFactor4(INodeSplitRule splitRule) {
        
        BTree btree = getBTree(4);

        Node a = new Node(btree);

        /*
         * setup without using insert(key,val).
         */

        a.nkeys = 3;

        a.keys = new int[] { 3, 5, 7 };

        /*
         * verify that a key that will be inserted into the low leaf results in
         * a split index that puts one value into the low leaf and two values
         * into the high leaf. This way the post-condition of the insert and
         * split will be two laeves each having two values, which is the minimum
         * for a 2-3 tree.
         */
        // postcondition: [ 2 3 - ], [ 7 - - ]
        assertEquals(2, splitRule.getSplitIndex(a.keys, 2));
        assertEquals(5, splitRule.getSeparatorKey());
        // postcondition: [ 3 4 - ], [ 7 - - ]
        assertEquals(2, splitRule.getSplitIndex(a.keys, 4));
        assertEquals(5, splitRule.getSeparatorKey());

        /*
         * verify that a key that will be inserted into the high leaf results in
         * a split index that puts two values into the low leaf and one value
         * into the high leaf. This way the post-condition of the insert and
         * split will be two leaves each having two values, which is the minimum
         * for a 2-3 tree.
         */
        // postcondition: [ 3 5 - ], [ 7 - - ]
        assertEquals(2, splitRule.getSplitIndex(a.keys, 6));
        assertEquals(6, splitRule.getSeparatorKey());
        // postcondition: [ 3 5 - ], [ 8 - ]
        assertEquals(3, splitRule.getSplitIndex(a.keys, 8));
        assertEquals(7, splitRule.getSeparatorKey());

    }

    /**
     * Test of the node split rule for a 3-5 tree.
     */
    public void test_nodeSlowSplitRuleBranchingFactor5() {

        int maxKeys = 5 - 1;
        
        doSplitRuleTestBranchingFactor5( new SlowNodeSplitRule(maxKeys) );
        
    }


//  public void test_nodeFastSplitRuleBranchingFactor4() {
//
//      doSplitRuleTestBranchingFactor4( new FastLeafSplitRule(4) );
//      
//  }
  
  
  public void doSplitRuleTestBranchingFactor5(INodeSplitRule splitRule) {
      
      BTree btree = getBTree(5);

      Node a = new Node(btree);

      /*
       * setup without using insert(key,val).
       */

      a.nkeys = 4;

      a.keys = new int[] { 3, 5, 7, 9 };

      /*
       * verify that a key that will be inserted into the low leaf results in
       * a split index that puts one value into the low leaf and two values
       * into the high leaf. This way the post-condition of the insert and
       * split will be two laeves each having two values, which is the minimum
       * for a 2-3 tree.
       */
      // postcondition: [ 2 3 - ], [ 7 9 - ]
      assertEquals(2, splitRule.getSplitIndex(a.keys, 2));
      assertEquals(5, splitRule.getSeparatorKey());
      // postcondition: [ 3 4 - ], [ 7 9 - ]
      assertEquals(2, splitRule.getSplitIndex(a.keys, 4));
      assertEquals(5, splitRule.getSeparatorKey());

      /*
       * verify that a key that will be inserted into the high leaf results in
       * a split index that puts two values into the low leaf and one value
       * into the high leaf. This way the post-condition of the insert and
       * split will be two leaves each having two values, which is the minimum
       * for a 2-3 tree.
       */
      // postcondition: [ 3 5 - ], [ 7 9 - ]
      assertEquals(2, splitRule.getSplitIndex(a.keys, 6));
      assertEquals(6, splitRule.getSeparatorKey());
      // postcondition: [ 3 5 - ], [ 8 9 - ]
      assertEquals(3, splitRule.getSplitIndex(a.keys, 8));
      assertEquals(7, splitRule.getSeparatorKey());
      // postcondition: [ 3 5 - ], [ 9 10 - ]
      assertEquals(3, splitRule.getSplitIndex(a.keys, 10));
      assertEquals(7, splitRule.getSeparatorKey());

  }

}
