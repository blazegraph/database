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
 * Created on Nov 28, 2006
 */

package com.bigdata.objndx;

/**
 * FIXME Try writing a test that splits a node that is generated directly
 * (rather than by inserts). The child pointers will all be null, but I can
 * assign my own child pointers and then just test the split logic.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSplitNode extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestSplitNode() {
    }

    /**
     * @param name
     */
    public TestSplitNode(String name) {
        super(name);
    }

    /*
     * FIXME Work through the code for split. This test may or may not help
     * since it is focused on the behavior of split (vs insert) and I either I
     * need to adapt the test to the post-conditions of split (vs insert) or I
     * need to just get the split code right. The former is more noble....
     */
    public void test_splitNode01() {
        
        BTree btree = getBTree(3);
        
        Node a = new Node(btree);
        btree.root = a;
        a.nkeys = 3;
        a.keys = new int[]{3,5,7};
        Node b = (Node) a.split(); // b is the rightSibling of a.
        
        Node c = (Node)btree.root;
        assertEquals(a,c.getChild(0));
        assertEquals(b,c.getChild(1));

        assertEquals(1,a.nkeys);
        assertEquals(new int[]{3,0},a.keys);
        assertEquals(1,b.nkeys);
        assertEquals(new int[]{5,0},b.keys);
        assertEquals(1,c.nkeys);
        assertEquals(new int[]{7,0},c.keys);
        
    }

}
