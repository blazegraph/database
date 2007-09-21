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
 * Created on Feb 1, 2007
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for {@link ReadOnlyFusedView}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFusedView extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestFusedView() {
    }

    /**
     * @param name
     */
    public TestFusedView(String name) {
        super(name);
    }

    public void test_ctor() {
        
        IRawStore store = new SimpleMemoryRawStore();
        
        final int branchingFactor = 3;
        
        final UUID indexUUID = UUID.randomUUID();
        
        // two btrees with the same indexUUID.
        BTree btree1 = new BTree(store, branchingFactor, indexUUID,
                SimpleEntry.Serializer.INSTANCE);
        
        BTree btree2 = new BTree(store, branchingFactor, indexUUID,
                SimpleEntry.Serializer.INSTANCE);
        
        // Another btree with a different index UUID.
        BTree btree3 = new BTree(store, branchingFactor, UUID.randomUUID(),
                SimpleEntry.Serializer.INSTANCE);
        
        try {
            new ReadOnlyFusedView(null);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        try {
            new ReadOnlyFusedView(new AbstractBTree[]{});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        try {
            new ReadOnlyFusedView(new AbstractBTree[]{btree1});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
                
        try {
            new ReadOnlyFusedView(new AbstractBTree[]{btree1,null});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
                
        try {
            new ReadOnlyFusedView(new AbstractBTree[]{btree1,btree1});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
                
        try {
            new ReadOnlyFusedView(new AbstractBTree[]{btree1,btree3});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        new ReadOnlyFusedView(new AbstractBTree[]{btree1,btree2});
                
    }

    /**
     * @todo explore rangeCount - should return the sum across the indices in
     * the view.
     * 
     * @todo explore rangeIterator with N == 2 indices.
     *
     * @todo explore rangeIterator with N > 2 indices.
     *
     * @todo explore rangeIterator with TimestampValue.
     * 
     * @todo There is a strong relationship to the {@link IndexSegmentMerger} and
     * its iterator.  Do we need both?
     */
    public void test_rangeIterator() {
        
        byte[] k3 = i2k(3);
        byte[] k5 = i2k(5);
        byte[] k7 = i2k(7);

        Object v3a = "3a";
        Object v5a = "5a";
        Object v7a = "7a";
        
        Object v3b = "3b";
        Object v5b = "5b";
        Object v7b = "7b";
        
        IRawStore store = new SimpleMemoryRawStore();
        
        final int branchingFactor = 3;
        
        final UUID indexUUID = UUID.randomUUID();
        
        // two btrees with the same indexUUID.
        BTree btree1 = new BTree(store, branchingFactor, indexUUID,
                SimpleEntry.Serializer.INSTANCE);
        
        BTree btree2 = new BTree(store, branchingFactor, indexUUID,
                SimpleEntry.Serializer.INSTANCE);

        ReadOnlyFusedView view = new ReadOnlyFusedView(new AbstractBTree[] { btree1, btree2 });
        
        assertEquals(0, btree1.rangeCount(null, null));
        assertEquals(0, btree2.rangeCount(null, null));
        assertEquals(0, view.rangeCount(null, null));
        assertSameIterator(new Object[] {}, btree2.rangeIterator(null, null));
        assertSameIterator(new Object[] {}, btree2.rangeIterator(null, null));
        assertSameIterator(new Object[] {}, view.rangeIterator(null, null));
        assertFalse(view.contains(k3));
        assertFalse(view.contains(k5));
        assertFalse(view.contains(k7));

        btree1.insert(k3, v3a);
        assertEquals(1,btree1.rangeCount(null, null));
        assertEquals(0,btree2.rangeCount(null, null));
        assertEquals(1,view.rangeCount(null, null));
        assertSameIterator(new Object[]{v3a},btree1.rangeIterator(null,null));
        assertSameIterator(new Object[]{},btree2.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3a},view.rangeIterator(null, null));
        assertTrue(view.contains(k3));
        assertFalse(view.contains(k5));
        assertFalse(view.contains(k7));

        btree1.insert(k5, v5a);
        assertEquals(2,btree1.rangeCount(null, null));
        assertEquals(0,btree2.rangeCount(null, null));
        assertEquals(2,view.rangeCount(null, null));
        assertSameIterator(new Object[]{v3a,v5a},btree1.rangeIterator(null,null));
        assertSameIterator(new Object[]{},btree2.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3a,v5a},view.rangeIterator(null, null));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertFalse(view.contains(k7));

        btree1.remove(k5);
        btree2.insert(k5, v5b);
        assertEquals(1,btree1.rangeCount(null, null));
        assertEquals(1,btree2.rangeCount(null, null));
        assertEquals(2,view.rangeCount(null, null));
        assertSameIterator(new Object[]{v3a},btree1.rangeIterator(null,null));
        assertSameIterator(new Object[]{v5b},btree2.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3a,v5b},view.rangeIterator(null, null));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertFalse(view.contains(k7));

        btree1.insert(k5, v5a);
        assertEquals(2,btree1.rangeCount(null, null));
        assertEquals(1,btree2.rangeCount(null, null));
        assertEquals(3,view.rangeCount(null, null));
        assertSameIterator(new Object[]{v3a,v5a},btree1.rangeIterator(null,null));
        assertSameIterator(new Object[]{v5b},btree2.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3a,v5a},view.rangeIterator(null, null));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertFalse(view.contains(k7));

        btree2.insert(k7, v7b);
        assertEquals(2,btree1.rangeCount(null, null));
        assertEquals(2,btree2.rangeCount(null, null));
        assertEquals(4,view.rangeCount(null, null));
        assertSameIterator(new Object[]{v3a,v5a},btree1.rangeIterator(null,null));
        assertSameIterator(new Object[]{v5b,v7b},btree2.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3a,v5a,v7b},view.rangeIterator(null, null));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertTrue(view.contains(k7));

        btree1.insert(k7, v7a);
        assertEquals(3,btree1.rangeCount(null, null));
        assertEquals(2,btree2.rangeCount(null, null));
        assertEquals(5,view.rangeCount(null, null));
        assertSameIterator(new Object[]{v3a,v5a,v7a},btree1.rangeIterator(null,null));
        assertSameIterator(new Object[]{v5b,v7b},btree2.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3a,v5a,v7a},view.rangeIterator(null, null));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertTrue(view.contains(k7));

        btree2.insert(k3, v3b);
        assertEquals(3,btree1.rangeCount(null, null));
        assertEquals(3,btree2.rangeCount(null, null));
        assertEquals(6,view.rangeCount(null, null));
        assertSameIterator(new Object[]{v3a,v5a,v7a},btree1.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3b,v5b,v7b},btree2.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3a,v5a,v7a},view.rangeIterator(null, null));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertTrue(view.contains(k7));

        btree1.remove(k3);
        assertEquals(2,btree1.rangeCount(null, null));
        assertEquals(3,btree2.rangeCount(null, null));
        assertEquals(5,view.rangeCount(null, null));
        assertSameIterator(new Object[]{v5a,v7a},btree1.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3b,v5b,v7b},btree2.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3b,v5a,v7a},view.rangeIterator(null, null));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertTrue(view.contains(k7));

        btree1.remove(k5);
        assertEquals(1,btree1.rangeCount(null, null));
        assertEquals(3,btree2.rangeCount(null, null));
        assertEquals(4,view.rangeCount(null, null));
        assertSameIterator(new Object[]{v7a},btree1.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3b,v5b,v7b},btree2.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3b,v5b,v7a},view.rangeIterator(null, null));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertTrue(view.contains(k7));

        btree1.remove(k7);
        assertEquals(0,btree1.rangeCount(null, null));
        assertEquals(3,btree2.rangeCount(null, null));
        assertEquals(3,view.rangeCount(null, null));
        assertSameIterator(new Object[]{},btree1.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3b,v5b,v7b},btree2.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3b,v5b,v7b},view.rangeIterator(null, null));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertTrue(view.contains(k7));

    }
    
}
