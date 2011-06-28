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
 * Created on Feb 1, 2007
 */

package com.bigdata.btree.view;

import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.AbstractTupleCursorTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BloomFilterFactory;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.NOPTupleSerializer;
import com.bigdata.btree.TestTuple;
import com.bigdata.btree.filter.Advancer;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.striterator.Resolver;
import com.bigdata.striterator.Striterator;

import cutthecrap.utils.striterators.IFilter;

/**
 * Test suite for {@link FusedView}.
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

        // two btrees with the same index UUID.
        final BTree btree1, btree2;
        {
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            md.setBranchingFactor(branchingFactor);
            md.setIsolatable(true);

            btree1 = BTree.create(store, md);
            btree2 = BTree.create(store, md.clone());
        }

        // Another btree with a different index UUID.
        final BTree btree3;
        {
            IndexMetadata md2 = new IndexMetadata(UUID.randomUUID());
            md2.setBranchingFactor(branchingFactor);
            md2.setIsolatable(true);
            btree3 = BTree.create(store, md2);
        }

        try {
            new FusedView(null);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        try {
            new FusedView(new AbstractBTree[]{});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        try {
            new FusedView(new AbstractBTree[]{btree1});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
                
        try {
            new FusedView(new AbstractBTree[]{btree1,null});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
                
        try {
            new FusedView(new AbstractBTree[]{btree1,btree1});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
                
        try {
            new FusedView(new AbstractBTree[]{btree1,btree3});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        new FusedView(new AbstractBTree[] { btree1, btree2 });
                
    }

    /**
     * Test verifies some of the basic principles of the fused view, including
     * that a deleted entry in the first source will mask an undeleted entry in
     * a secondary source. It also verifies that insert() and remove() return
     * the current value under the key from the view (not just from the btree to
     * which the write operations are directed).
     */
    public void test_indexStuff() {
        
        byte[] k3 = i2k(3);
        byte[] k5 = i2k(5);
        byte[] k7 = i2k(7);

        byte[] v3a = new byte[]{3};
        byte[] v5a = new byte[]{5};
        byte[] v7a = new byte[]{7};
        
        byte[] v3b = new byte[]{3,1};
        byte[] v5b = new byte[]{5,1};
        byte[] v7b = new byte[]{7,1};
        
        IRawStore store = new SimpleMemoryRawStore();
        
        // two btrees with the same index UUID.
        final BTree btree1, btree2;
        {
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setBranchingFactor(3);
            
            md.setDeleteMarkers(true);

            md.setTupleSerializer(NOPTupleSerializer.INSTANCE);

            btree1 = BTree.create(store, md);
            
            btree2 = BTree.create(store, md.clone());
            
        }
        
        /*
         * Create an ordered view onto {btree1, btree2}. Keys found in btree1
         * will cause the search to halt. If the key is not in btree1 then
         * btree2 will also be searched. A miss is reported if the key is not
         * found in either btree.
         * 
         * Note: Since delete markers are enabled keys will be recognized when
         * the index entry has been marked as deleted.
         */
        final FusedView view = new FusedView(new AbstractBTree[] { btree1,
                btree2 });
        
        /*
         * Write some data on btree2.
         */
        btree2.insert(k3,v3a);
        btree2.insert(k7,v7a);
        
        /*
         * Verify initial conditions for both source btrees and the view.
         * 
         * btree1 { }
         * 
         * btree2 {k3=v3a; k7=v7a}
         */
        assertEquals(0, btree1.rangeCount(null, null));
        assertEquals(2, btree2.rangeCount(null, null));
        assertEquals(2, view.rangeCount(null, null));
        assertSameIterator(new byte[][] {}, btree1.rangeIterator(null, null));
        assertSameIterator(new byte[][] { v3a, v7a }, btree2.rangeIterator(
                null, null));
        assertSameIterator(new byte[][] { v3a, v7a }, view.rangeIterator(null,
                null));
        assertTrue(view.contains(k3));
        assertFalse(view.contains(k5));
        assertTrue(view.contains(k7));

        /*
         * Write on the view.
         * 
         * btree1 {k5=v5a;}
         * 
         * btree2 {k3=v3a; k7=v7a}
         */
        assertEquals(null,view.insert(k5, v5a));
        assertEquals(1, btree1.rangeCount(null, null));
        assertEquals(2, btree2.rangeCount(null, null));
        assertEquals(3, view.rangeCount(null, null));
        assertSameIterator(new byte[][] { v5a }, btree1.rangeIterator(null,
                null));
        assertSameIterator(new byte[][] { v3a, v7a }, btree2.rangeIterator(
                null, null));
        assertSameIterator(new byte[][] { v3a, v5a, v7a }, view.rangeIterator(
                null, null));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertTrue(view.contains(k7));
        
        /*
         * Write on the view.
         * 
         * btree1 {k5=v5a; k7=v7b}
         * 
         * btree2 {k3=v3a; k7=v7a}
         */
        assertEquals(v7a,view.insert(k7, v7b));
        assertEquals(2, btree1.rangeCount(null, null));
        assertEquals(2, btree2.rangeCount(null, null));
        assertEquals(4, view.rangeCount(null, null));
        assertSameIterator(new byte[][] { v5a, v7b }, btree1.rangeIterator(
                null, null));
        assertSameIterator(new byte[][] { v3a, v7a }, btree2.rangeIterator(
                null, null));
        assertSameIterator(new byte[][] { v3a, v5a, v7b }, view.rangeIterator(
                null, null));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertTrue(view.contains(k7));

        /*
         * Write on the view.
         * 
         * btree1 {k3:=deleted; k5=v5a; k7=v7b}
         * 
         * btree2 {k3=v3a; k7=v7a}
         */
        assertEquals(v3a, view.remove(k3));
        assertEquals(3, btree1.rangeCount(null, null));
        assertEquals(2, btree2.rangeCount(null, null));
        assertEquals(5, view.rangeCount(null, null));
        assertSameIterator(new byte[][] { v5a, v7b }, btree1.rangeIterator(
                null, null));
        assertSameIterator(new byte[][] { null, v5a, v7b }, btree1
                .rangeIterator(null, null, 0/* capacity */, IRangeQuery.DEFAULT
                        | IRangeQuery.DELETED, null/*filter*/));
        assertSameIterator(new byte[][] { v3a, v7a }, btree2.rangeIterator(
                null, null));
        assertSameIterator(new byte[][] { v5a, v7b }, view.rangeIterator(null,
                null));
        assertSameIterator(new byte[][] { null, v5a, v7b }, view.rangeIterator(
                null, null, 0/* capacity */, IRangeQuery.DEFAULT
                        | IRangeQuery.DELETED, null/* filter */));
        assertFalse(view.contains(k3));
        assertTrue(view.contains(k5));
        assertTrue(view.contains(k7));

        /*
         * Write on the view.
         * 
         * btree1 {k3:=deleted; k5=v5b; k7=v7b}
         * 
         * btree2 {k3=v3a; k7=v7a}
         */
        assertEquals(v5a, view.insert(k5,v5b));
        assertEquals(3, btree1.rangeCount(null, null));
        assertEquals(2, btree2.rangeCount(null, null));
        assertEquals(5, view.rangeCount(null, null));
        assertSameIterator(new byte[][] { v5b, v7b }, btree1.rangeIterator(
                null, null));
        assertSameIterator(new byte[][] { null, v5b, v7b }, btree1
                .rangeIterator(null, null, 0/* capacity */, IRangeQuery.DEFAULT
                        | IRangeQuery.DELETED, null/*filter*/));
        assertSameIterator(new byte[][] { v3a, v7a }, btree2.rangeIterator(
                null, null));
        assertSameIterator(new byte[][] { v5b, v7b }, view.rangeIterator(null,
                null));
        assertSameIterator(new byte[][] { null, v5b, v7b }, view.rangeIterator(
                null, null, 0/* capacity */, IRangeQuery.DEFAULT
                        | IRangeQuery.DELETED, null/* filter */));
        assertFalse(view.contains(k3));
        assertTrue(view.contains(k5));
        assertTrue(view.contains(k7));

        /*
         * Write on the view.
         * 
         * btree1 {k3:=v3b; k5=v5b; k7=v7b}
         * 
         * btree2 {k3=v3a; k7=v7a}
         */
        assertEquals(null, view.insert(k3,v3b)); // Note: return is [null] because k3 was deleted in btree1 !
        assertEquals(3, btree1.rangeCount(null, null));
        assertEquals(2, btree2.rangeCount(null, null));
        assertEquals(5, view.rangeCount(null, null));
        assertSameIterator(new byte[][] { v3b, v5b, v7b }, btree1.rangeIterator(
                null, null));
        assertSameIterator(new byte[][] { v3b, v5b, v7b }, btree1
                .rangeIterator(null, null, 0/* capacity */, IRangeQuery.DEFAULT
                        | IRangeQuery.DELETED, null/*filter*/));
        assertSameIterator(new byte[][] { v3a, v7a }, btree2.rangeIterator(
                null, null));
        assertSameIterator(new byte[][] { v3b, v5b, v7b }, view.rangeIterator(null,
                null));
        assertSameIterator(new byte[][] { v3b, v5b, v7b }, view.rangeIterator(
                null, null, 0/* capacity */, IRangeQuery.DEFAULT
                        | IRangeQuery.DELETED, null/* filter */));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertTrue(view.contains(k7));

    }

    /**
     * Unit test of the bloom filter.
     * 
     * @todo bloom filter is on the mutable index, but not on the 2nd index in
     *       the view.
     * 
     * @todo test bloom filter is not on the mutable index, but is on the 2nd
     *       index in the view.
     * 
     * @todo test bloom filter is on both and then the mutable index bloom
     *       filter gets disabled.
     * 
     * @todo test bloom filter is on both and then the 2nd index bloom filter
     *       gets disabled.
     */
    public void test_bloomFilter() {
        
        byte[] k3 = i2k(3);
        byte[] k5 = i2k(5);
        byte[] k7 = i2k(7);

        byte[] v3a = new byte[]{3};
        byte[] v5a = new byte[]{5};
        byte[] v7a = new byte[]{7};
        
        byte[] v3b = new byte[]{3,1};
        byte[] v5b = new byte[]{5,1};
        byte[] v7b = new byte[]{7,1};
        
        final IRawStore store = new SimpleMemoryRawStore();
        
        // two btrees with the same index UUID.
        BTree btree1, btree2;
        {
            
            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setBranchingFactor(3);
            
            md.setDeleteMarkers(true);

            md.setBloomFilterFactory(BloomFilterFactory.DEFAULT);

            md.setTupleSerializer(NOPTupleSerializer.INSTANCE);

            btree1 = BTree.create(store, md);
            
            btree2 = BTree.create(store, md.clone());
            
        }
        
        /*
         * Create an ordered view onto {btree1, btree2}. Keys found in btree1
         * will cause the search to halt. If the key is not in btree1 then
         * btree2 will also be searched. A miss is reported if the key is not
         * found in either btree.
         * 
         * Note: Since delete markers are enabled keys will be recognized when
         * the index entry has been marked as deleted.
         */
        FusedView view = new FusedView(new AbstractBTree[] { btree1, btree2 });

        assertNotNull(view.getBloomFilter());

        // Note: false positives are possible here, but very unlikely.
        assertFalse(btree2.getBloomFilter().contains(k3));
        assertFalse(btree2.getBloomFilter().contains(k5));
        assertFalse(btree2.getBloomFilter().contains(k7));
        assertFalse(btree1.getBloomFilter().contains(k3));
        assertFalse(btree1.getBloomFilter().contains(k5));
        assertFalse(btree1.getBloomFilter().contains(k7));
        assertFalse(view.getBloomFilter().contains(k3));
        assertFalse(view.getBloomFilter().contains(k5));
        assertFalse(view.getBloomFilter().contains(k7));

        btree2.insert(k3,v3a);
        btree2.insert(k5,v5a);
//        btree2.insert(k7,v7a);

//        btree1.insert(k3,v3b);
        btree1.insert(k5,v5b);
        btree1.insert(k7,v7b);

        assertTrue(btree2.getBloomFilter().contains(k3));
        assertTrue(btree2.getBloomFilter().contains(k5));
        // Note: false positive is possible here, but very unlikely.
        assertFalse(btree2.getBloomFilter().contains(k7));

        // Note: false positive is possible here, but very unlikely.
        assertFalse(btree1.getBloomFilter().contains(k3));
        assertTrue(btree1.getBloomFilter().contains(k5));
        assertTrue(btree1.getBloomFilter().contains(k7));

        assertTrue(view.getBloomFilter().contains(k3));
        assertTrue(view.getBloomFilter().contains(k5));
        assertTrue(view.getBloomFilter().contains(k7));

        /*
         * Checkpoint the indices so we can reload them from their current
         * state.
         */
        final Checkpoint btree2Checkpoint = btree2.writeCheckpoint2();
        final Checkpoint btree1Checkpoint = btree1.writeCheckpoint2();

        /*
         * Disable the bloom filter on btree1 and test.
         */
        {
            
            btree1.getBloomFilter().disable();

            assertNull(btree1.getBloomFilter());
            assertNotNull(btree2.getBloomFilter());
            assertNotNull(view.getBloomFilter());

            assertTrue(view.getBloomFilter().contains(k3));
            assertTrue(view.getBloomFilter().contains(k5));
            assertTrue(view.getBloomFilter().contains(k7));
            
        }

        // recover the view from the checkpoints.
        btree1 = BTree.load(store, btree1Checkpoint.getCheckpointAddr());
        btree2 = BTree.load(store, btree2Checkpoint.getCheckpointAddr());
        view = new FusedView(new AbstractBTree[] { btree1, btree2 });
        
        /*
         * Disable the bloom filter on btree2 and test.
         */
        {
            
            btree2.getBloomFilter().disable();

            assertNotNull(btree1.getBloomFilter());
            assertNull(btree2.getBloomFilter());
            assertNotNull(view.getBloomFilter());

            assertTrue(view.getBloomFilter().contains(k3));
            assertTrue(view.getBloomFilter().contains(k5));
            assertTrue(view.getBloomFilter().contains(k7));
            
        }

    }
    
    /**
     * Test verifies some of the basic principles of the fused view, including
     * that a deleted entry in the first source will mask an undeleted entry in
     * a secondary source.
     * 
     * @todo explore rangeIterator with N > 2 indices.
     * 
     * @todo explore where one of the indices are {@link IndexSegment}s.
     */
    public void test_rangeIterator() {
        
        final byte[] k3 = i2k(3);
        final byte[] k5 = i2k(5);
        final byte[] k7 = i2k(7);

        final byte[] v3a = new byte[]{3};
        final byte[] v5a = new byte[]{5};
//        byte[] v7a = new byte[]{7};
//        
//        byte[] v3b = new byte[]{3,1};
//        byte[] v5b = new byte[]{5,1};
//        byte[] v7b = new byte[]{7,1};
        
        final IRawStore store = new SimpleMemoryRawStore();
        
        // two btrees with the same index UUID.
        final BTree btree1, btree2;
        {
            
            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setBranchingFactor(3);
            
            md.setDeleteMarkers(true);

            md.setTupleSerializer(NOPTupleSerializer.INSTANCE);

            btree1 = BTree.create(store, md);
            
            btree2 = BTree.create(store, md.clone());
            
        }
        
        /*
         * Create an ordered view onto {btree1, btree2}. Keys found in btree1
         * will cause the search to halt. If the key is not in btree1 then
         * btree2 will also be searched. A miss is reported if the key is not
         * found in either btree.
         * 
         * Note: Since delete markers are enabled keys will be recognized when
         * the index entry has been marked as deleted.
         */
        final FusedView view = new FusedView(new AbstractBTree[] { btree1,
                btree2 });
        
        /*
         * Verify initial conditions for both source btrees and the view.
         */
        assertEquals(0, btree1.rangeCount(null, null));
        assertEquals(0, btree2.rangeCount(null, null));
        assertEquals(0, view.rangeCount(null, null));
        assertEquals(0, view.rangeCountExact(null, null));
        assertEquals(0, view.rangeCountExactWithDeleted(null, null));
        assertSameIterator(new byte[][] {}, btree1.rangeIterator(null, null));
        assertSameIterator(new byte[][] {}, btree2.rangeIterator(null, null));
        assertSameIterator(new byte[][] {}, view.rangeIterator(null, null));
        assertFalse(view.contains(k3));
        assertFalse(view.contains(k5));
        assertFalse(view.contains(k7));

        /*
         * Insert an entry into btree2.
         * 
         * btree2: {k3:=v3a}
         */
        btree2.insert(k3, v3a);
        assertEquals(0, btree1.rangeCount(null, null));
        assertEquals(1, btree2.rangeCount(null, null));
        assertEquals(1, view.rangeCount(null, null));
        assertEquals(1, view.rangeCountExact(null, null));
        assertEquals(1, view.rangeCountExactWithDeleted(null, null));
        assertSameIterator(new byte[][]{},btree1.rangeIterator(null,null));
        assertSameIterator(new byte[][]{v3a},btree2.rangeIterator(null,null));
        assertSameIterator(new byte[][]{v3a},view.rangeIterator(null, null));
        assertTrue(view.contains(k3));
        assertFalse(view.contains(k5));
        assertFalse(view.contains(k7));

        /*
         * Insert an entry into btree1.
         * 
         * btree1: {k5:=v5a}
         * 
         * btree2: {k3:=v3a}
         */
        btree1.insert(k5, v5a);
        assertEquals(1, btree1.rangeCount(null, null));
        assertEquals(1, btree2.rangeCount(null, null));
        assertEquals(2, view.rangeCount(null, null));
        assertEquals(2, view.rangeCountExact(null, null));
        assertEquals(2, view.rangeCountExactWithDeleted(null, null));
        assertSameIterator(new byte[][]{v5a},btree1.rangeIterator(null,null));
        assertSameIterator(new byte[][]{v3a},btree2.rangeIterator(null,null));
        assertSameIterator(new byte[][]{v3a,v5a},view.rangeIterator(null, null));
        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertFalse(view.contains(k7));

        /*
         * Delete the key for an entry found in btree2 from btree1. This will
         * insert a delete marker for that key into btree1. Btree1 will now
         * report one more entry and the entry will not be visible in the view
         * unless you use DELETED on the iterator.
         * 
         * btree1: {k3:=deleted; k5:=v5a}
         * 
         * btree2: {k3:=v3a}
         */
        btree1.remove(k3);
        assertEquals(2, btree1.rangeCount(null, null));
        assertEquals(1, btree2.rangeCount(null, null));
        assertEquals(3, view.rangeCount(null, null));
        assertEquals(1, view.rangeCountExact(null, null));
        assertEquals(2, view.rangeCountExactWithDeleted(null, null));
        assertSameIterator(new byte[][] { v5a }, btree1.rangeIterator(null,
                null));
        // verify the deleted entry in the iterator.
        assertSameIterator(new byte[][] { null, v5a }, btree1.rangeIterator(
                null, null, 0/* capacity */, IRangeQuery.DEFAULT
                        | IRangeQuery.DELETED, null/* filter */));
        assertSameIterator(new byte[][] { v3a }, btree2.rangeIterator(null,
                null));
        assertSameIterator(new byte[][] { v5a }, view.rangeIterator(null,
                null));
        assertFalse(view.contains(k3));
        assertTrue(view.contains(k5));
        assertFalse(view.contains(k7));

    }

    /**
     * Test of the bit manipulation required under java to turn off the
     * {@link IRangeQuery#REMOVEALL} flag.
     */
    public void test_bitMath() {

        final int flags1 = IRangeQuery.REMOVEALL;

        System.err.println("flags1="+Integer.toBinaryString(flags1));
        
        assertEquals(true, (flags1 & IRangeQuery.REMOVEALL) != 0);
        
        // turn off the bit.
        final int flags2 = flags1 & (~IRangeQuery.REMOVEALL);

        System.err.println("flags1="+Integer.toBinaryString(flags2));

        assertEquals(false, (flags2 & IRangeQuery.REMOVEALL) != 0);
        
    }
    
    /**
     * Test of {@link FusedTupleIterator#remove()}. Note that tuples are
     * removed by writing a delete marker into the first B+Tree in the ordered
     * sources.
     */
    public void test_remove() {

        final byte[] k3 = new byte[]{3};
        final byte[] k5 = new byte[]{5};
        final byte[] k7 = new byte[]{7};
        final byte[] k9 = new byte[]{9};

        final byte[] v3 = new byte[]{3};
        final byte[] v5 = new byte[]{5};
        final byte[] v7 = new byte[]{7};
        final byte[] v9 = new byte[]{9};

        final IRawStore store = new SimpleMemoryRawStore();

        // two btrees with the same index UUID.
        final BTree btree1, btree2;
        {

            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());

            md.setBranchingFactor(3);

            md.setDeleteMarkers(true);

            md.setTupleSerializer(NOPTupleSerializer.INSTANCE);
            
            btree1 = BTree.create(store, md);

            btree2 = BTree.create(store, md.clone());

        }

        /*
         * Create an ordered view onto {btree1, btree2}. Keys found in btree1
         * will cause the search to halt. If the key is not in btree1 then
         * btree2 will also be searched. A miss is reported if the key is not
         * found in either btree.
         * 
         * Note: Since delete markers are enabled keys will be recognized when
         * the index entry has been marked as deleted.
         */
        final FusedView view = new FusedView(new AbstractBTree[] { btree1,
                btree2 });

        /*
         * Setup the view.
         * 
         * Note: k5 is found in both source B+Trees but with a different value
         * stored under the key.
         */
        btree2.insert(k3, v3);
        btree1.insert(k5, v5);
        btree2.insert(k7, v7);
        btree1.insert(k9, v9);

        // Note: CURSOR specified so that remove() will be supported.
        final ITupleIterator itr = view.rangeIterator(null/* fromKey */,
                null/* toKey */, 0/* capacity */, IRangeQuery.DEFAULT
                        | IRangeQuery.CURSOR, null/* filter */);

        {
            ITuple tuple;

            // k3 : found in btree2, but write delete marker in btree1.
            assertTrue(itr.hasNext());
            tuple = itr.next();
            assertEquals(k3, tuple.getKey());
            assertEquals(v3, tuple.getValue());
            assertFalse(btree1.contains(k3)); // not found in btree1
            assertTrue(btree2.contains(k3)); // found in btree2
            // tuple not found in btree1.
            assertNull(btree1.lookup(k3, btree1.getLookupTuple()));
            itr.remove();
            assertFalse(btree1.contains(k3)); // not found in btree1
            assertTrue(btree2.contains(k3)); // found in btree2
            // deleted tuple now found in btree1.
            assertTrue(btree1.lookup(k3, btree1.getLookupTuple()).isDeletedVersion());

            // k5 : found in btree1, write delete marker in btree1.
            assertTrue(itr.hasNext());
            tuple = itr.next();
            assertEquals(k5, tuple.getKey());
            assertEquals(v5, tuple.getValue());
            assertTrue(btree1.contains(k5)); // found in btree1
            assertFalse(btree2.contains(k5)); // not found in btree2
            // undeleted tuple found in btree1.
            assertFalse(btree1.lookup(k5, btree1.getLookupTuple()).isDeletedVersion());
            itr.remove();
            assertFalse(btree1.contains(k5)); // not found in btree1
            assertFalse(btree2.contains(k5)); // not found in btree2
            // deleted tuple found in btree1.
            assertTrue(btree1.lookup(k5, btree1.getLookupTuple()).isDeletedVersion());
            
        }

    }

    /**
     * Test of {@link IRangeQuery#REMOVEALL}. Note that tuples are removed by
     * writing a delete marker into the first B+Tree in the ordered sources.
     */
    public void test_removeAll() {
        
        final byte[] k3 = new byte[]{3};
        final byte[] k5 = new byte[]{5};
        final byte[] k7 = new byte[]{7};
        final byte[] k9 = new byte[]{9};

        final byte[] v3 = new byte[]{3};
        final byte[] v5 = new byte[]{5};
        final byte[] v7 = new byte[]{7};
        final byte[] v9 = new byte[]{9};
        
        final IRawStore store = new SimpleMemoryRawStore();
        
        // two btrees with the same index UUID.
        final BTree btree1, btree2;
        {

            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setBranchingFactor(3);
            
            md.setDeleteMarkers(true);

            md.setTupleSerializer(NOPTupleSerializer.INSTANCE);
            
            btree1 = BTree.create(store, md);
            
            btree2 = BTree.create(store, md.clone());
            
        }
        
        /*
         * Create an ordered view onto {btree1, btree2}. Keys found in btree1
         * will cause the search to halt. If the key is not in btree1 then
         * btree2 will also be searched. A miss is reported if the key is not
         * found in either btree.
         * 
         * Note: Since delete markers are enabled keys will be recognized when
         * the index entry has been marked as deleted.
         */
        final FusedView view = new FusedView(new AbstractBTree[] { btree1,
                btree2 });
        
        /*
         * Setup the view.
         * 
         * Note: k5 is found in both source B+Trees but with a different value
         * stored under the key.
         */
        btree2.insert(k3, v3);
        btree1.insert(k5, v5);
        btree2.insert(k7, v7);
        btree1.insert(k9, v9);

        final ITupleIterator itr = view
                .rangeIterator(null/* fromKey */, null/* toKey */,
                        0/* capacity */, IRangeQuery.REMOVEALL, null/* filter */);

        // remove all tuples.
        while (itr.hasNext()) {

            itr.next();
            
        }

        // still found in btree2 since not overwritten there.
        assertTrue(btree2.contains(k3));
        assertTrue(btree2.contains(k7));
        
        // not found in btree1 since overwritten.
        assertFalse(btree1.contains(k5));
        assertFalse(btree1.contains(k9));
        
        /*
         * now verify that btree1 does in fact contain a deleted tuple for each
         * of the original keys regardles of which of the source indices that
         * tuple was in before it was deleted.
         */
        assertTrue(btree1.lookup(k3, btree1.getLookupTuple()).isDeletedVersion());
        assertTrue(btree1.lookup(k5, btree1.getLookupTuple()).isDeletedVersion());
        assertTrue(btree1.lookup(k7, btree1.getLookupTuple()).isDeletedVersion());
        assertTrue(btree1.lookup(k9, btree1.getLookupTuple()).isDeletedVersion());
        
    }

    /**
     * Test of {@link IRangeQuery#REMOVEALL} with a filter verifies that only
     * those tuples which satisify the filter are visited and removed.
     */
    public void test_removeAll_filter() {
        
        final byte[] k3 = new byte[]{3};
        final byte[] k5 = new byte[]{5};
        final byte[] k7 = new byte[]{7};
        final byte[] k9 = new byte[]{9};

        final byte[] v3 = new byte[]{3};
        final byte[] v5 = new byte[]{5};
        final byte[] v7 = new byte[]{7};
        final byte[] v9 = new byte[]{9};
        
        final IRawStore store = new SimpleMemoryRawStore();
        
        // two btrees with the same index UUID.
        final BTree btree1, btree2;
        {

            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setBranchingFactor(3);
            
            md.setDeleteMarkers(true);

            md.setTupleSerializer(NOPTupleSerializer.INSTANCE);
            
            btree1 = BTree.create(store, md);
            
            btree2 = BTree.create(store, md.clone());
            
        }
        
        /*
         * Create an ordered view onto {btree1, btree2}. Keys found in btree1
         * will cause the search to halt. If the key is not in btree1 then
         * btree2 will also be searched. A miss is reported if the key is not
         * found in either btree.
         * 
         * Note: Since delete markers are enabled keys will be recognized when
         * the index entry has been marked as deleted.
         */
        final FusedView view = new FusedView(new AbstractBTree[] { btree1,
                btree2 });
        
        /*
         * Setup the view.
         * 
         * Note: k5 is found in both source B+Trees but with a different value
         * stored under the key.
         */
        btree2.insert(k3, v3);
        btree1.insert(k5, v5);
        btree2.insert(k7, v7);
        btree1.insert(k9, v9);

        final TupleFilter filter = new TupleFilter(){
            private static final long serialVersionUID = 1L;
            @Override
            protected boolean isValid(ITuple tuple) {
                System.out.println("Considering tuple: " + tuple);
                if (BytesUtil.compareBytes(k5, tuple.getKey()) == 0) {
                    System.err.println("Skipping tuple: " + tuple);
                    return false;
                }
                return true;
            }};

        assertTrue(view.contains(k3));
        assertTrue(view.contains(k5));
        assertTrue(view.contains(k7));
        assertTrue(view.contains(k9));
        
        final ITupleIterator itr = view
                .rangeIterator(null/* fromKey */, null/* toKey */,
                        0/* capacity */, IRangeQuery.REMOVEALL, filter);

        // remove all tuples matching the filter.
        while (itr.hasNext()) {

            itr.next();
            
        }

        assertFalse(view.contains(k3));
        assertTrue(view.contains(k5)); // note: tuple was NOT deleted!
        assertFalse(view.contains(k7));
        assertFalse(view.contains(k9));
        
    }
    
    /**
     * This tests the ability to traverse the tuples in the {@link FusedView} in
     * reverse order. This ability is a requirement for several aspects of the
     * total architecture, including atomic append for the bigdata file system,
     * locating an index partition, and finding the last entry in a set or a
     * map.
     * 
     * @see FusedTupleIterator
     */
    public void test_reverseScan() {
        
        final byte[] k3 = i2k(3);
        final byte[] k5 = i2k(5);
//        final byte[] k7 = i2k(7);

        final byte[] v3a = new byte[]{3};
        final byte[] v5a = new byte[]{5};
//        byte[] v7a = new byte[]{7};
//        
//        byte[] v3b = new byte[]{3,1};
//        byte[] v5b = new byte[]{5,1};
//        byte[] v7b = new byte[]{7,1};
        
        final IRawStore store = new SimpleMemoryRawStore();
        
        // two btrees with the same index UUID.
        final BTree btree1, btree2;
        {

            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setBranchingFactor(3);
            
            md.setDeleteMarkers(true);

            md.setTupleSerializer(NOPTupleSerializer.INSTANCE);

            btree1 = BTree.create(store, md);
            
            btree2 = BTree.create(store, md.clone());
            
        }
        
        /*
         * Create an ordered view onto {btree1, btree2}. Keys found in btree1
         * will cause the search to halt. If the key is not in btree1 then
         * btree2 will also be searched. A miss is reported if the key is not
         * found in either btree.
         * 
         * Note: Since delete markers are enabled keys will be recognized when
         * the index entry has been marked as deleted.
         */
        final FusedView view = new FusedView(new AbstractBTree[] { btree1,
                btree2 });
        
        /*
         * btree1: {k5:=v5a}
         * 
         * btree2: {k3:=v3a}
         */
        btree1.insert(k5, v5a);
        btree2.insert(k3, v3a);

        // forward
        assertSameIterator(new byte[][] { v3a, v5a }, view.rangeIterator(null,
                null));

        // reverse
        assertSameIterator(new byte[][] { v5a, v3a }, view.rangeIterator(null,
                null, 0/* capacity */,
                IRangeQuery.DEFAULT | IRangeQuery.REVERSE, null/*filter*/));

        /*
         * Delete the key for an entry found in btree2 from btree1. This will
         * insert a delete marker for that key into btree1. Btree1 will now
         * report one more entry and the entry will not be visible in the view
         * unless you use DELETED on the iterator.
         * 
         * btree1: {k3:=deleted; k5:=v5a}
         * 
         * btree2: {k3:=v3a}
         */

        btree1.remove(k3);
        
        // forward
        assertSameIterator(new byte[][] { v5a }, view.rangeIterator(null,
                null));

        // reverse.
        assertSameIterator(new byte[][] { v5a }, view.rangeIterator(null, null,
                0/* capacity */, IRangeQuery.DEFAULT | IRangeQuery.REVERSE,
                null/* filter */));

    }

    /**
     * Unit test for correct layering of filters on top of the
     * {@link FusedTupleIterator}. Note that the filters must be layered on top
     * of the {@link FusedTupleIterator} rather than being passed into the
     * per-index source {@link ITupleIterator}s so that the filters will see a
     * fused iterator. This test is designed to verify that the filters are in
     * fact applied to the {@link FusedTupleIterator} rather than to the source
     * iterators.
     */
    public void test_filter() {
        
        final byte[] k3 = i2k(3);
        final byte[] k5 = i2k(5);
        final byte[] k7 = i2k(7);
        final byte[] k9 = i2k(9);

        final byte[] v3a = new byte[]{3};
        final byte[] v5a = new byte[]{5};
        final byte[] v5b = new byte[]{5,1};
        final byte[] v7a = new byte[]{7};
        final byte[] v9a = new byte[]{9};
        
        final IRawStore store = new SimpleMemoryRawStore();
        
        // two btrees with the same index UUID.
        final BTree btree1, btree2;
        {

            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setBranchingFactor(3);
            
            md.setDeleteMarkers(true);

            md.setTupleSerializer(NOPTupleSerializer.INSTANCE);

            btree1 = BTree.create(store, md);
            
            btree2 = BTree.create(store, md.clone());
            
        }
        
        /*
         * Create an ordered view onto {btree1, btree2}. Keys found in btree1
         * will cause the search to halt. If the key is not in btree1 then
         * btree2 will also be searched. A miss is reported if the key is not
         * found in either btree.
         * 
         * Note: Since delete markers are enabled keys will be recognized when
         * the index entry has been marked as deleted.
         */
        final FusedView view = new FusedView(new AbstractBTree[] { btree1,
                btree2 });
        
        /*
         * Setup the view.
         * 
         * Note: [k5] is found in both source B+Trees.
         */
        btree2.insert(k3, v3a);
        btree1.insert(k5, v5a);
        btree2.insert(k5, v5b);
        btree1.insert(k7, v7a);
        btree2.insert(k9, v9a);

        /*
         * Setup a filter that counts the #of tuples _examined_ by the iterator
         * (not just those that it visits). If this filter is applied to the
         * source iterators then it would examine (2) tuples for btree1 and (3)
         * tuples for btree2 for a total of (5). However, if it is applied to
         * the fused tuple iterator, then it will only examine (4) tuples since
         * there is a tuple with [k5] in both btree1 and btree2 and therefore
         * the [k5] tuple from btree2 is dropped from the fused tuple iterator.
         */
        final AtomicInteger count = new AtomicInteger(0);
        final IFilter filter = new TupleFilter(){
            private static final long serialVersionUID = 1L;
            @Override
            protected boolean isValid(ITuple tuple) {
                count.incrementAndGet();
                return true;
            }};
        
        // forward : counts four distinct tuples.
        count.set(0);
        assertSameIterator(new byte[][] { v3a, v5a, v7a, v9a }, view
                .rangeIterator(null, null,0/*capacity*/,IRangeQuery.DEFAULT,filter));
        assertEquals(4,count.get());

        // reverse : counts four distinct tuples.
        count.set(0);
        assertSameIterator(new byte[][] { v9a, v7a, v5a, v3a }, view.rangeIterator(null,
                null, 0/* capacity */,
                IRangeQuery.DEFAULT | IRangeQuery.REVERSE, filter));
        assertEquals(4,count.get());

    }
    
    /**
     * Unit test for the {@link ITupleCursor} API for a {@link FusedView}.
     * <p>
     * Note: The requirement for {@link FusedView} to implement
     * {@link ITupleCursor} arises in order to support an {@link Advancer} over
     * a {@link FusedView} (scale-out for the RDF DB) and the requirement to
     * efficiently choose splits points for an index partition, especially for
     * the {@link SparseRowStore} (it must not split a logical row).
     */
    @SuppressWarnings("unchecked")
    public void test_cursor() {
        
        final Integer k3 = 3;
        final Integer k5 = 5;
        final Integer k6 = 6;
        final Integer k7 = 7;
        final Integer k9 = 9;
        final Integer k11 = 11;

        final String v3a = "3";
        final String v5a = "5";
        final String v5b = "5b";
        final String v7a = "7";
        final String v9a = "9";
        final String v11a = "11";
        
        final IRawStore store = new SimpleMemoryRawStore();
        
        // two btrees with the same index UUID.
        final BTree btree1, btree2;
        {

            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setBranchingFactor(3);
            
            md.setDeleteMarkers(true);

            btree1 = BTree.create(store, md);
            
            btree2 = BTree.create(store, md.clone());
            
        }
        
        /*
         * Create an ordered view onto {btree1, btree2}. Keys found in btree1
         * will cause the search to halt. If the key is not in btree1 then
         * btree2 will also be searched. A miss is reported if the key is not
         * found in either btree.
         * 
         * Note: Since delete markers are enabled keys will be recognized when
         * the index entry has been marked as deleted.
         */
        final FusedView view = new FusedView(new AbstractBTree[] { btree1,
                btree2 });
        
        /*
         * Setup the view.
         * 
         * Note: k5 is found in both source B+Trees but with a different value
         * stored under the key.
         */
        btree2.insert(k3, v3a);
        btree1.insert(k5, v5a); // Note: k5 is in both source indices.
        btree2.insert(k5, v5b);
        btree2.insert(k7, v7a); // Note: k7 is deleted (overwritten in btree1).
        btree1.remove(k7);
        btree2.insert(k9, v9a);
        btree1.insert(k11, v11a);
        
        final int flags = IRangeQuery.DEFAULT | IRangeQuery.CURSOR;
        
        final ITupleCursor cursor = (ITupleCursor) view.rangeIterator(
                null/* fromKey */, null/* toKey */, 0/* capacity */,
                flags, null/* filter */);
        
        // verify expected index reference.
        assertTrue(view == cursor.getIndex());
        
        /*
         * First, do a forward scan tuple by tuple.
         */
        {

            ITuple actual;

            assertTrue(cursor.hasNext());
            assertEquals(//
                    new TestTuple(flags, k3, v3a), //
                    actual = cursor.next());
            assertEquals(1, actual.getSourceIndex());

            assertTrue(cursor.hasNext());
            assertEquals(//
                    new TestTuple(flags, k5, v5a), //
                    actual = cursor.next());
            assertEquals(0, actual.getSourceIndex());

            assertTrue(cursor.hasNext());
            assertEquals(//
                    new TestTuple(flags, k9, v9a), //
                    actual = cursor.next());
            assertEquals(1, actual.getSourceIndex());

            assertTrue(cursor.hasNext());
            assertEquals(//
                    new TestTuple(flags, k11, v11a), //
                    actual = cursor.next());
            assertEquals(0, actual.getSourceIndex());

            assertFalse(cursor.hasNext());

        }

        /*
         * Now, do a reverse scan tuple by tuple.
         */

        {

            ITuple actual;

            assertTrue(cursor.hasPrior());
            assertEquals(//
                    new TestTuple(flags, k9, v9a), //
                    actual = cursor.prior());
            assertEquals(1, actual.getSourceIndex());

            assertTrue(cursor.hasPrior());
            assertEquals(//
                    new TestTuple(flags, k5, v5a), //
                    actual = cursor.prior());
            assertEquals(0, actual.getSourceIndex());

            assertTrue(cursor.hasPrior());
            assertEquals(//
                    new TestTuple(flags, k3, v3a), //
                    actual = cursor.prior());
            assertEquals(1, actual.getSourceIndex());

            assertFalse(cursor.hasPrior());

        }

        /*
         * Now seek to each tuple in turn and verify the tuple state, including
         * the source index from which the tuple was obtained; seek() + prior() +
         * next(); and seek() + next() + prior().
         */
        {

            ITuple actual;

            // k3
            actual = cursor.seek(k3);
            assertNotNull(actual);
            assertEquals(new TestTuple(flags, k3, v3a), actual);
            assertEquals(1, actual.getSourceIndex());
            assertTrue(cursor.hasNext());
            assertFalse(cursor.hasPrior());
            try {
                cursor.prior();
                fail("Expecting: " + NoSuchElementException.class);
            } catch (NoSuchElementException ex) {
                log.info("Ignoring expected exception: " + ex);
            }
            actual = cursor.next();
            assertEquals(new TestTuple(flags, k5, v5a), actual);
            actual = cursor.prior();
            assertEquals(new TestTuple(flags, k3, v3a), actual);

            // k5
            actual = cursor.seek(k5);
            assertNotNull(actual);
            assertEquals(new TestTuple(flags, k5, v5a), actual);
            assertEquals(0, actual.getSourceIndex());
            assertTrue(cursor.hasNext());
            assertTrue(cursor.hasPrior());
            actual = cursor.prior();
            assertEquals(new TestTuple(flags, k3, v3a), actual);
            actual = cursor.next();
            assertEquals(new TestTuple(flags, k5, v5a), actual);
            actual = cursor.next();
            assertEquals(new TestTuple(flags, k9, v9a), actual);

            // k6 : key not found.
            assertNull(cursor.seek(k6));

            // k7 : key deleted.
            assertNull(cursor.seek(k7));
            {
                /*
                 * Test seek to deleted key when DELETED is specified.
                 */
                
                // another cursor with DELETED in the flags.
                final ITupleCursor tmp = (ITupleCursor) view.rangeIterator(
                        null/* fromKey */, null/* toKey */, 0/* capacity */,
                        flags|IRangeQuery.DELETED, null/* filter */);
                
                // seek to a deleted key.
                actual = tmp.seek(k7);
                
                // verify tuple.
                assertEquals(
                        new TestTuple(flags | IRangeQuery.DELETED, k7,
                                null/* val */, true/* deleted */, 0L/* timestamp */),
                        actual);
                
            }

            // k9
            actual = cursor.seek(k9);
            assertNotNull(actual);
            assertEquals(new TestTuple(flags, k9, v9a), actual);
            assertEquals(1, cursor.seek(k9).getSourceIndex());
            assertTrue(cursor.hasNext());
            assertTrue(cursor.hasPrior());
            actual = cursor.prior();
            assertEquals(new TestTuple(flags, k5, v5a), actual);
            actual = cursor.next();
            assertEquals(new TestTuple(flags, k9, v9a), actual);
            actual = cursor.next();
            assertEquals(new TestTuple(flags, k11, v11a), actual);

            // k11
            actual = cursor.seek(k11);
            assertNotNull(actual);
            assertEquals(new TestTuple(flags, k11, v11a), actual);
            assertEquals(0, cursor.seek(k11).getSourceIndex());
            assertFalse(cursor.hasNext());
            assertTrue(cursor.hasPrior());
            try {
                cursor.next();
                fail("Expecting: " + NoSuchElementException.class);
            } catch (NoSuchElementException ex) {
                log.info("Ignoring expected exception: " + ex);
            }
            actual = cursor.prior();
            assertEquals(new TestTuple(flags, k9, v9a), actual);
            actual = cursor.next();
            assertEquals(new TestTuple(flags, k11, v11a), actual);

        }

        /*
         * Now test remove() after seek. This should always remove the last
         * visited tuple.
         * 
         * Note: In all cases the tuple must be removed by writing a delete
         * marker into [btree1] regardless of which source B+Tree the tuple was
         * read from.
         */

        // pre-condition test.
        assertSameIterator(new Object[] { v3a, v5a, null/*v7a*/,v9a, v11a },
                new Striterator(view.rangeIterator(null, null, 0/* capacity */,
                                IRangeQuery.DEFAULT | IRangeQuery.DELETED,null/*filter*/))
                                .addFilter(new Resolver(){
                                    private static final long serialVersionUID = 1L;
                                    @Override
                                    protected Object resolve(Object e) {
                                        return ((ITuple)e).getObject();
                                    }
                                }));

        {
            
            ITuple actual = cursor.seek(k5);
            assertNotNull(actual);
            assertEquals(new TestTuple(flags, k5, v5a), actual);
            assertEquals(0, actual.getSourceIndex());
            assertTrue(view.contains(k5));
            cursor.remove();
            assertFalse(view.contains(k5));
            
            // post-condition test.
            assertSameIterator(new Object[] { v3a, null/*v5a*/, null/*v7a*/,v9a, v11a },
                    new Striterator(view.rangeIterator(null, null, 0/* capacity */,
                                    IRangeQuery.DEFAULT | IRangeQuery.DELETED,null/*filter*/))
                                    .addFilter(new Resolver(){
                                        private static final long serialVersionUID = 1L;
                                        @Override
                                        protected Object resolve(Object e) {
                                            return ((ITuple)e).getObject();
                                        }
                                    }));
            
        }
        
        {

            ITuple actual = cursor.seek(k11);
            assertNotNull(actual);
            assertEquals(new TestTuple(flags, k11, v11a), actual);
            assertEquals(0, actual.getSourceIndex());
            assertTrue(view.contains(k11));
            cursor.remove();
            assertFalse(view.contains(k11));
        
            // post-condition test.
            assertSameIterator(new Object[] { v3a, null/*v5a*/, null/*v7a*/,v9a, null/*v11a*/ },
                    new Striterator(view.rangeIterator(null, null, 0/* capacity */,
                                    IRangeQuery.DEFAULT | IRangeQuery.DELETED,null/*filter*/))
                                    .addFilter(new Resolver(){
                                        private static final long serialVersionUID = 1L;
                                        @Override
                                        protected Object resolve(Object e) {
                                            return ((ITuple)e).getObject();
                                        }
                                    }));

        }
        
        /* 
         * Done.
         */

    }

    /**
     * Compares {@link ITuple}s for equality in their data.
     * 
     * @param expected
     * @param actual
     */
    protected void assertEquals(ITuple expected, ITuple actual) {
        
        AbstractTupleCursorTestCase.assertEquals(expected,actual);
        
    }
    
}
