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

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.relation.accesspath.AccessPathFusedView;

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
     * Test verifies some of the basic principles of the fused view, including
     * that a deleted entry in the first source will mask an undeleted entry in
     * a secondary source.
     * 
     * @todo explore rangeIterator with N > 2 indices.
     */
    public void test_rangeIterator() {
        
        byte[] k3 = i2k(3);
        byte[] k5 = i2k(5);
        byte[] k7 = i2k(7);

        byte[] v3a = new byte[]{3};
        byte[] v5a = new byte[]{5};
//        byte[] v7a = new byte[]{7};
//        
//        byte[] v3b = new byte[]{3,1};
//        byte[] v5b = new byte[]{5,1};
//        byte[] v7b = new byte[]{7,1};
        
        IRawStore store = new SimpleMemoryRawStore();
        
        // two btrees with the same index UUID.
        final BTree btree1, btree2;
        {
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
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
         * Verify initial conditions for both source btrees and the view.
         */
        assertEquals(0, btree1.rangeCount(null, null));
        assertEquals(0, btree2.rangeCount(null, null));
        assertEquals(0, view.rangeCount(null, null));
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
        assertEquals(0,btree1.rangeCount(null, null));
        assertEquals(1,btree2.rangeCount(null, null));
        assertEquals(1,view.rangeCount(null, null));
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
        assertEquals(1,btree1.rangeCount(null, null));
        assertEquals(1,btree2.rangeCount(null, null));
        assertEquals(2,view.rangeCount(null, null));
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
     * This tests the ability to traverse the tuples in the {@link FusedView} in
     * reverse order. This ability is a requirement for several aspects of the
     * total architecture, including atomic append for the bigdata file system,
     * locating an index partition, and finding the last entry in a set or a
     * map.
     * 
     * @see FusedEntryIterator
     * 
     * @todo write test of the alternative ctor for {@link FusedEntryIterator}
     *       which is used by {@link AccessPathFusedView}.
     */
    public void test_reverseScan() {
        
        byte[] k3 = i2k(3);
        byte[] k5 = i2k(5);
        byte[] k7 = i2k(7);

        byte[] v3a = new byte[]{3};
        byte[] v5a = new byte[]{5};
//        byte[] v7a = new byte[]{7};
//        
//        byte[] v3b = new byte[]{3,1};
//        byte[] v5b = new byte[]{5,1};
//        byte[] v7b = new byte[]{7,1};
        
        IRawStore store = new SimpleMemoryRawStore();
        
        // two btrees with the same index UUID.
        final BTree btree1, btree2;
        {
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
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
    
//    /**
//     * Unit tests for the {@link ILinearList} API when the index is a view
//     * composed of more than one index resource.
//     */
//    public void test_linearList() {
//        
//        byte[] k3 = i2k(3);
//        byte[] k5 = i2k(5);
//        byte[] k7 = i2k(7);
//
//        byte[] v3a = new byte[]{3};
//        byte[] v5a = new byte[]{5};
//        byte[] v7a = new byte[]{7};
//        
//        byte[] v3b = new byte[]{3,1};
//        byte[] v5b = new byte[]{5,1};
//        byte[] v7b = new byte[]{7,1};
//        
//        IRawStore store = new SimpleMemoryRawStore();
//        
//        // two btrees with the same index UUID.
//        final BTree btree1, btree2;
//        {
//            IndexMetadata md = new IndexMetadata(UUID.randomUUID());
//            
//            md.setBranchingFactor(3);
//            
//            md.setDeleteMarkers(true);
//
//            btree1 = BTree.create(store, md);
//            
//            btree2 = BTree.create(store, md.clone());
//            
//        }
//        
//        /*
//         * Create an ordered view onto {btree1, btree2}. Keys found in btree1
//         * will cause the search to halt. If the key is not in btree1 then
//         * btree2 will also be searched. A miss is reported if the key is not
//         * found in either btree.
//         * 
//         * Note: Since delete markers are enabled keys will be recognized when
//         * the index entry has been marked as deleted.
//         */
//        final FusedView view = new FusedView(new AbstractBTree[] { btree1,
//                btree2 });
//        
//        /* 
//         * Some tests when the view is empty.
//         */
//
//        // probe with null is disallowed.
//        try {
//            view.indexOf(null);
//            fail("Expecting: " + IllegalArgumentException.class);
//        } catch (IllegalArgumentException ex) {
//            log.info("Ignoring expected exception: " + ex);
//        }
//        
//        // not found.
//        assertEquals(-1,view.indexOf(new byte[]{}));
//        assertEquals(-1,view.indexOf(k3));
//        assertEquals(-1,view.indexOf(new byte[]{9,9,9}));
//            
//        /*
//         * Write some data on btree2 and test the linear list API. This lets us
//         * examine the case where the first btree is empty and all data is in
//         * the 2nd part of the view.
//         */
//        btree2.insert(k3,v3a);
//        btree2.insert(k5,v5a);
//        btree2.insert(k7,v7a);
//        
//        // not found.
//        assertEquals(-1,view.indexOf(new byte[]{}));
//        assertEquals(-3,view.indexOf(new byte[]{9,9,9}));
//
//        // found
//        assertEquals(0,view.indexOf(k3));
//        assertEquals(1,view.indexOf(k5));
//        assertEquals(2,view.indexOf(k7));
//
//        btree1.insert(k3,v3b);
////      btree1.insert(k5,v5a);
////      btree1.insert(k7,v7b);
//      btree1.remove(k7);
//      
//
//    }
    
//        btree1.insert(k5, v5a);
//        assertEquals(2,btree1.rangeCount(null, null));
//        assertEquals(1,btree2.rangeCount(null, null));
//        assertEquals(3,view.rangeCount(null, null));
//        assertSameIterator(new byte[][]{v3a,v5a},btree1.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v5b},btree2.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v3a,v5a},view.rangeIterator(null, null));
//        assertTrue(view.contains(k3));
//        assertTrue(view.contains(k5));
//        assertFalse(view.contains(k7));
//
//        btree2.insert(k7, v7b);
//        assertEquals(2,btree1.rangeCount(null, null));
//        assertEquals(2,btree2.rangeCount(null, null));
//        assertEquals(4,view.rangeCount(null, null));
//        assertSameIterator(new byte[][]{v3a,v5a},btree1.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v5b,v7b},btree2.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v3a,v5a,v7b},view.rangeIterator(null, null));
//        assertTrue(view.contains(k3));
//        assertTrue(view.contains(k5));
//        assertTrue(view.contains(k7));
//
//        btree1.insert(k7, v7a);
//        assertEquals(3,btree1.rangeCount(null, null));
//        assertEquals(2,btree2.rangeCount(null, null));
//        assertEquals(5,view.rangeCount(null, null));
//        assertSameIterator(new byte[][]{v3a,v5a,v7a},btree1.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v5b,v7b},btree2.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v3a,v5a,v7a},view.rangeIterator(null, null));
//        assertTrue(view.contains(k3));
//        assertTrue(view.contains(k5));
//        assertTrue(view.contains(k7));
//
//        btree2.insert(k3, v3b);
//        assertEquals(3,btree1.rangeCount(null, null));
//        assertEquals(3,btree2.rangeCount(null, null));
//        assertEquals(6,view.rangeCount(null, null));
//        assertSameIterator(new byte[][]{v3a,v5a,v7a},btree1.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v3b,v5b,v7b},btree2.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v3a,v5a,v7a},view.rangeIterator(null, null));
//        assertTrue(view.contains(k3));
//        assertTrue(view.contains(k5));
//        assertTrue(view.contains(k7));
//
//        btree1.remove(k3);
//        assertEquals(2,btree1.rangeCount(null, null));
//        assertEquals(3,btree2.rangeCount(null, null));
//        assertEquals(5,view.rangeCount(null, null));
//        assertSameIterator(new byte[][]{v5a,v7a},btree1.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v3b,v5b,v7b},btree2.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v3b,v5a,v7a},view.rangeIterator(null, null));
//        assertTrue(view.contains(k3));
//        assertTrue(view.contains(k5));
//        assertTrue(view.contains(k7));
//
//        btree1.remove(k5);
//        assertEquals(1,btree1.rangeCount(null, null));
//        assertEquals(3,btree2.rangeCount(null, null));
//        assertEquals(4,view.rangeCount(null, null));
//        assertSameIterator(new byte[][]{v7a},btree1.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v3b,v5b,v7b},btree2.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v3b,v5b,v7a},view.rangeIterator(null, null));
//        assertTrue(view.contains(k3));
//        assertTrue(view.contains(k5));
//        assertTrue(view.contains(k7));
//
//        btree1.remove(k7);
//        assertEquals(0,btree1.rangeCount(null, null));
//        assertEquals(3,btree2.rangeCount(null, null));
//        assertEquals(3,view.rangeCount(null, null));
//        assertSameIterator(new byte[][]{},btree1.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v3b,v5b,v7b},btree2.rangeIterator(null,null));
//        assertSameIterator(new byte[][]{v3b,v5b,v7b},view.rangeIterator(null, null));
//        assertTrue(view.contains(k3));
//        assertTrue(view.contains(k5));
//        assertTrue(view.contains(k7));

}
