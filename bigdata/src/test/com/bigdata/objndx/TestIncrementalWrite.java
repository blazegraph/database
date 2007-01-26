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
 * Created on Nov 18, 2006
 */

package com.bigdata.objndx;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.IRawStore;

/**
 * Test suite for the logic performing incremental writes of nodes and leaves
 * onto the store. The actual timing of evictions from the
 * {@link HardReferenceQueue} is essentially unpredictable since evictions are
 * driven by {@link BTree#touch(AbstractNode)} and nodes and leaves are both
 * touched frequently and in a data and code path dependent manner.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIncrementalWrite extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestIncrementalWrite() {
    }

    /**
     * @param name
     */
    public TestIncrementalWrite(String name) {
        super(name);
    }
       
    public BTree getBTree(int branchingFactor, int queueCapacity, int queueScan) {
        
        IRawStore store = new SimpleStore();
        
        BTree btree = new BTree(store,
                branchingFactor,
                new MyHardReferenceQueue<PO>(new DefaultEvictionListener(),
                        queueCapacity, queueScan),
                        SimpleEntry.Serializer.INSTANCE,
                        null // no record compressor
                        );

        return btree;
        
    }

    /**
     * Test verifies that an incremental write of the root leaf may be
     * performed.
     */
    public void test_incrementalWrite() {
        
        /*
         * setup the tree. it uses a queue capacity of two since that is the
         * minimum allowed. it uses scanning to ensure that one a single
         * reference to the root leaf actually enters the queue. that way when
         * we request an incremental write it occurs since the reference counter
         * for the root leaf will be one (1) since there is only one reference
         * to that leaf on the queue.
         */
        BTree btree = getBTree(3,2,1);
        
        final SimpleEntry v3 = new SimpleEntry(3);
        final SimpleEntry v5 = new SimpleEntry(5);
        final SimpleEntry v7 = new SimpleEntry(7);
        
        /*
         * insert some keys into the root leaf.
         */
        final Leaf a = (Leaf)btree.root;
        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        
        /*
         * do an incremental write of the root leaf.
         */
        assertFalse( a.isPersistent() );
        btree.leafQueue.getListener().evicted(btree.leafQueue, btree.root);
        assertTrue( a.isPersistent() );
        
    }

    /**
     * Test verifies that an incremental write of a leaf may be performed, that
     * identity is assigned to the written leaf, and that the childKey[] on the
     * parent node is updated to reflect the identity assigned to the leaf.
     */
    public void test_incrementalWrite02() {

        /*
         * setup the tree with a most queue capacity but set the scan parameter
         * such that we never allow more than a single reference to a node onto
         * the queue.
         */
        BTree btree = getBTree(3,20,20);

        /*
         * insert keys into the root and cause it to split.
         */
        final SimpleEntry v3 = new SimpleEntry(3);
        final SimpleEntry v5 = new SimpleEntry(5);
        final SimpleEntry v7 = new SimpleEntry(7);
        final SimpleEntry v9 = new SimpleEntry(9);
        final Leaf a = (Leaf) btree.root;
        btree.insert(3,v3);
        btree.insert(5,v5);
        btree.insert(7,v7);
        btree.insert(9,v9);
        assertNotSame(a,btree.root);
        final Node c = (Node) btree.root;
        assertKeys(new int[]{7},c);
        assertEquals(a,c.getChild(0));
        final Leaf b = (Leaf) c.getChild(1);
        assertKeys(new int[]{3,5},a);
        assertValues(new Object[]{v3,v5}, a);
        assertKeys(new int[]{7,9},b);
        assertValues(new Object[]{v7,v9}, b);

        /*
         * verify reference counters.
         */
        
        assertEquals(1,a.referenceCount);
        assertEquals(1,b.referenceCount);
        assertEquals(1,c.referenceCount);

        /*
         * verify that all nodes are NOT persistent.
         */
        
        assertFalse(a.isPersistent());
        assertFalse(b.isPersistent());
        assertFalse(c.isPersistent());
        
        /*
         * verify the queue order. we know the queue order since no node is
         * allowed into the queue more than once (because the scan parameter is
         * equal to the queue capacity) and because we know the node creation
         * order (a is created when the tree is created; b is created when a is
         * split; and c is created after the split when we discovert that there
         * is no parent of a and that we need to create one).
         */
        
        assertEquals(new PO[]{a,b,c}, ((MyHardReferenceQueue<PO>)btree.leafQueue).toArray());
        
        /*
         * force (b) to be evicted. since its reference count is one(1) it will
         * be made persistent.
         * 
         * Note: this causes the reference counter for (b) to be reduced to
         * zero(0) even through (b) is on the queue. This is not a legal state
         * so we can not continue with operation that would touch the queue.
         */
        
        btree.leafQueue.getListener().evicted(btree.leafQueue, b);

        // verify that b is now persistent.
        assertTrue(b.isPersistent());
        
        // verify that we set the identity of b on its parent so that it can be
        // recovered from the store if necessary.
        assertEquals(b.getIdentity(), c.childAddr[1]);

    }
    

    /**
     * Test verifies that an incremental write of a node may be performed, that
     * identity is assigned to the written node, and that the childKey[] on the
     * node are updated to reflect the identity assigned to its children (the
     * dirty children are written out when the node is evicted so that the 
     * persistent node knows the persistent identity of each child).
     */
    public void test_incrementalWrite03() {

        /*
         * setup the tree with a most queue capacity but set the scan parameter
         * such that we never allow more than a single reference to a node onto
         * the queue.
         */
        BTree btree = getBTree(3,20,20);

        /*
         * insert keys into the root and cause it to split.
         */
        final SimpleEntry v3 = new SimpleEntry(3);
        final SimpleEntry v5 = new SimpleEntry(5);
        final SimpleEntry v7 = new SimpleEntry(7);
        final SimpleEntry v9 = new SimpleEntry(9);
        final Leaf a = (Leaf) btree.root;
        btree.insert(3,v3);
        btree.insert(5,v5);
        btree.insert(7,v7);
        btree.insert(9,v9);
        assertNotSame(a,btree.root);
        final Node c = (Node) btree.root;
        assertKeys(new int[]{7},c);
        assertEquals(a,c.getChild(0));
        final Leaf b = (Leaf) c.getChild(1);
        assertKeys(new int[]{3,5},a);
        assertValues(new Object[]{v3,v5}, a);
        assertKeys(new int[]{7,9},b);
        assertValues(new Object[]{v7,v9}, b);

        /*
         * verify reference counters.
         */
        
        assertEquals(1,a.referenceCount);
        assertEquals(1,b.referenceCount);
        assertEquals(1,c.referenceCount);

        /*
         * verify that all nodes are NOT persistent.
         */
        
        assertFalse(a.isPersistent());
        assertFalse(b.isPersistent());
        assertFalse(c.isPersistent());
        
        /*
         * verify the queue order. we know the queue order since no node is
         * allowed into the queue more than once (because the scan parameter is
         * equal to the queue capacity) and because we know the node creation
         * order (a is created when the tree is created; b is created when a is
         * split; and c is created after the split when we discovert that there
         * is no parent of a and that we need to create one).
         */
        
        assertEquals(new PO[]{a,b,c}, ((MyHardReferenceQueue<PO>)btree.leafQueue).toArray());
        
        /*
         * force (c) to be evicted. since its reference count is one(1) it will
         * be made persistent.
         * 
         * Note: this causes the reference counter for (c) to be reduced to
         * zero(0) even through (c) is on the queue. This is not a legal state
         * so we can not continue with operations that would touch the queue.
         */
        
        btree.leafQueue.getListener().evicted(btree.leafQueue, c);

        // verify that c and its children (a,b) are now persistent.
        assertTrue(c.isPersistent());
        assertTrue(a.isPersistent());
        assertTrue(b.isPersistent());
        
        // verify that we set the identity of (a,b) on their parent (c).
        assertEquals(a.getIdentity(), c.childAddr[0]);
        assertEquals(b.getIdentity(), c.childAddr[1]);

    }

}
