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
 * Created on Dec 12, 2006
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.btree.AbstractNode;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IValueSerializer;
import com.bigdata.btree.Leaf;
import com.bigdata.btree.Node;
import com.bigdata.btree.PO;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for {@link BTree#touch(AbstractNode)}. None of these tests cause
 * an evicted node to be made persistent, but they do verify the correct
 * tracking of the {@link AbstractNode#referenceCount} and the contract for
 * touching a node.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTouch extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestTouch() {
    }

    /**
     * @param name
     */
    public TestTouch(String name) {
        super(name);
    }

    /**
     * Test verifies that the reference counter is incremented when a node is
     * appended to the hard reference queue (the scan of the tail of the queue
     * is disabled for this test). Finally, verify that we can force the node to
     * be evicted from the queue but that its non-zero reference counter means
     * that it is not made persistent when it is evicted.
     */
    public void test_touch01() {

        /*
         * setup the btree with a queue having two entries and no scanning. The
         * listener initially disallows any evictions and we have to explicitly
         * notify the listener when it should expect an eviction.
         */
        final int branchingFactor = 3;
        final MyEvictionListener listener = new MyEvictionListener();
        final int queueCapacity = 2;
        final int queueScan = 0;
        final MyHardReferenceQueue<PO> leafQueue = new MyHardReferenceQueue<PO>(
                listener, queueCapacity, queueScan);
        assertEquals(queueCapacity,leafQueue.capacity());
        assertEquals(queueScan,leafQueue.nscan());
        assertEquals(listener,leafQueue.getListener());
        
        // The btree.
        final BTree btree = new BTree(
                new SimpleMemoryRawStore(),
                branchingFactor,
                UUID.randomUUID(),
                leafQueue,
                SimpleEntry.Serializer.INSTANCE,
                null // no record compressor
                );
        
        /*
         * verify the initial conditions - the root leaf is on the queue and
         * its reference counter is one (1).
         */
        final Leaf a = (Leaf)btree.root;

        assertEquals(1,a.referenceCount);
        
        assertEquals(new PO[]{a}, leafQueue.toArray());
        
        /*
         * touch the leaf. since we are not scanning the queue, another
         * reference to the leaf will be added to the queue and the reference
         * counter will be incremented.
         */
        btree.touch(a);
        
        assertEquals(2,a.referenceCount);
        
        assertEquals(new PO[]{a,a}, leafQueue.toArray());
        
        /*
         * touch the leaf. since the queue is at capacity, the leaf is evicted.
         * We verify that leaf has a non-zero reference counter when it is
         * evicted, which means that it will not be made persistent since other
         * references to the leaf remain on the queue.
         */
        
        listener.setExpectedRef(a);
        
        btree.touch(a);
        
        assertEquals(2,a.referenceCount);
        
        assertEquals(new PO[]{a,a}, leafQueue.toArray());

        assertFalse(a.isPersistent());
        
    }

    /**
     * Test verifies that the reference counter is unchanged across
     * {@link BTree#touch(AbstractNode)} if a node is already on the hard
     * reference queue.
     */
    public void test_touch02() {

        /*
         * setup the btree with a queue having two entries and scanning. The
         * listener initially disallows any evictions and we have to explicitly
         * notify the listener when it should expect an eviction.
         */
        final int branchingFactor = 3;
        final MyEvictionListener listener = new MyEvictionListener();
        final int queueCapacity = 2;
        final int queueScan = 1;
        final MyHardReferenceQueue<PO> leafQueue = new MyHardReferenceQueue<PO>(
                listener, queueCapacity, queueScan);
        assertEquals(queueCapacity,leafQueue.capacity());
        assertEquals(queueScan,leafQueue.nscan());
        assertEquals(listener,leafQueue.getListener());
        
        // The btree.
        final BTree btree = new BTree(
                new SimpleMemoryRawStore(),
                branchingFactor,
                UUID.randomUUID(),
                leafQueue,
                SimpleEntry.Serializer.INSTANCE,
                null // no record compressor
        );
        
        /*
         * verify the initial conditions - the root leaf is on the queue and
         * its reference counter is one (1).
         */
        final Leaf a = (Leaf)btree.root;

        assertEquals(1,a.referenceCount);
        
        assertEquals(new PO[]{a}, leafQueue.toArray());
        
        /*
         * touch the leaf. since we are scanning the queue, this does NOT cause
         * another reference to the leaf to be added to the queue and the
         * reference counter MUST NOT be incremented across the method call.
         * Nothing is evicted and the leaf is not made persistent.
         */
        btree.touch(a);
        
        assertEquals(1,a.referenceCount);
        
        assertEquals(new PO[]{a}, leafQueue.toArray());

        assertFalse(a.isPersistent());

    }

    /**
     * Test verifies that touching a node when the queue is full and the node is
     * the next reference to be evicted from the queue does NOT cause the node
     * to be made persistent. The test is setup using a queue of capacity one
     * (1) and NO scanning. The root leaf is already on the queue when the btree
     * is created. The test verifies that merely touching the root leaf causes a
     * reference to the leaf to be evicted from the queue, but does NOT cause
     * the leaf to be made persistent. {@link BTree#touch(AbstractNode)} handles
     * this condition by incrementing the reference counter before appending the
     * node to the queue and therefore ensuring that the reference counter for
     * the node that touched is not zero if the node is also selected for
     * eviction. The test also verifies that the reference counter is correctly
     * maintained across the touch. Since the counter was one before the touch
     * and since the root was itself evicted, the counter after the touch is
     * <code>1+1-1 = 1</code>.
     * 
     * FIXME This test needs to use a tree with nodes and leaves or fake another
     * root leaf since the minimum cache size is (2)
     */
    public void test_touch03() {

        /*
         * setup the btree with a queue with surplus capacity and no scanning.
         * The listener initially disallows any evictions and we have to
         * explicitly notify the listener when it should expect an eviction.
         */
        final int branchingFactor = 3;
        final MyEvictionListener listener = new MyEvictionListener();
        final int queueCapacity = 20;
        final int queueScan = 0;
        final MyHardReferenceQueue<PO> leafQueue = new MyHardReferenceQueue<PO>(
                listener, queueCapacity, queueScan);
        assertEquals(queueCapacity,leafQueue.capacity());
        assertEquals(queueScan,leafQueue.nscan());
        assertEquals(listener,leafQueue.getListener());
        
        IValueSerializer valSer = SimpleEntry.Serializer.INSTANCE;

        // The btree.
        final BTree btree = new BTree(
                new SimpleMemoryRawStore(),
                branchingFactor,
                UUID.randomUUID(),
                leafQueue,
                valSer,
                null // no record compressor
        ); 
        
        /*
         * verify the initial conditions - the root leaf is on the queue and
         * its reference counter is one (1).
         */
        final Leaf a = (Leaf)btree.root;

        assertEquals(1,a.referenceCount);
        
        assertEquals(new PO[]{a}, leafQueue.toArray());

        assertFalse(a.isPersistent());

        /*
         * insert keys into the root and cause it to split.
         */
        final SimpleEntry v3 = new SimpleEntry(3);
        final SimpleEntry v5 = new SimpleEntry(5);
        final SimpleEntry v7 = new SimpleEntry(7);
        final SimpleEntry v9 = new SimpleEntry(9);
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
         * bring the queue up to its capacity without causing it to overflow.
         */
        while(leafQueue.size()<leafQueue.capacity()) {
            
            // touch a node - which one does not really matter.
            btree.touch(a);
            
        }
        
        /*
         * examine the queue state and figure out which node or leaf we want to
         * evict. we continue to append a specific node (a) until the reference
         * that would be evicted next has a reference count of one (1). it does
         * not matter which node this is. It will be either (b) or (c) depending
         * on the code paths when we setup the test tree.
         */

        AbstractNode ref;
        
        while(true) {

            ref = (AbstractNode) leafQueue.getTail();

            if(ref.referenceCount == 1 ) break;
            
            listener.setExpectedRef(ref);
            
            btree.touch(a);

        } 

        /*
         * touch the node or leaf that is poised for eviction from the queue and
         * which would be made immutable if it were evicted since its
         * pre-eviction reference count is one (1). since we are not scanning
         * the queue, another reference to the node or leaf will be added to the
         * queue. Since the queue is at capacity, the reference on the queue
         * will be evicted. However, since the reference counter is non-zero in
         * the eviction handler, the leaf will not be made persistent.
         */
        
        assertEquals(1,ref.referenceCount);
        
        listener.setExpectedRef(ref);
        
        btree.touch(ref);
        
        assertEquals(1,ref.referenceCount);
        
        assertFalse(ref.isPersistent());
        
    }

}
