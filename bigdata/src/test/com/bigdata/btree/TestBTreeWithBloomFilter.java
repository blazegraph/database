/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Nov 4, 2008
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Unit tests for a {@link BTree} with its bloom filter enabled. This class is
 * mostly focused on the basic bloom filter mechanics. Also see
 * {@link TestIndexSegmentWithBloomFilter} which was originally written to test
 * the {@link IndexSegment} behavior with a bloom filter, but which also tests
 * the {@link BTree} to some extent now that the {@link BTree} also maintains a
 * bloom filter.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBTreeWithBloomFilter extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestBTreeWithBloomFilter() {
    }

    /**
     * @param name
     */
    public TestBTreeWithBloomFilter(String name) {
        super(name);
    }

    /**
     * Return a btree backed by a journal with the indicated branching factor.
     * The serializer requires that values in leaves are {@link SimpleEntry}
     * objects.
     * 
     * @param branchingFactor
     *            The branching factor.
     * 
     * @return The btree.
     */
    public BTree getBTree(int branchingFactor, boolean bloomFilter) {

        IRawStore store = new SimpleMemoryRawStore(); 

        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setBranchingFactor(branchingFactor);

        if (bloomFilter)
            metadata.setBloomFilterFactory(new BloomFilterFactory(10000/* n */));
        
        BTree btree = BTree.create(store, metadata);

        return btree;

    }

    public void test_create() {
        
        {
            
            BTree btree = getBTree(4/* branchingFactor */, false/* bloomFilter */);
            
            // ensure that the root leaf is defined and the bloom filter also (if used).
            btree.reopen();
            
            // no bloom filter
            assertNull(btree.getBloomFilter());
            
        }
        
        {
            
            BTree btree = getBTree(4/* branchingFactor */, true/* bloomFilter */);
            
            // ensure that the root leaf is defined and the bloom filter also (if used).
            btree.reopen();
            
            // bloom filter exists
            assertNotNull(btree.getBloomFilter());
            
        }
        
    }

    /**
     * Simple test to verify that the bloom filter does not break the semantics
     * of insert, lookup, contains, or remove.
     * 
     * @todo there should be another test that looks at the semantics when
     *       delete markers are also enabled.
     */
    public void test_add_contains() {

        final BTree btree = getBTree(4/* branchingFactor */, true/* bloomFilter */);
        
        /*
         * ensure that the root leaf is defined and the bloom filter also (if
         * used).
         */
        btree.reopen();
        
        // bloom filter exists
        assertNotNull(btree.getBloomFilter());

        // show the filter state.
        System.err.println(btree.getBloomFilter().toString());
        
        final byte[] k0 = new byte[]{0};
        final byte[] k1 = new byte[]{1};
        
        assertFalse(btree.contains(k1));
        assertFalse(btree.contains(k0));
        
        // add an index entry.
        assertNull(btree.insert(k0, k0));

        // verify at BTree API (contains)
        assertFalse(btree.contains(k1));
        assertTrue(btree.contains(k0));

        // verify at BTree API (lookup)
        assertEquals(null,btree.lookup(k1));
        assertEquals(k0,btree.lookup(k0));

        // verify at bloom filter API.
        assertFalse(btree.getBloomFilter().contains(k1));
        assertTrue(btree.getBloomFilter().contains(k0));

        // show the filter state.
        System.err.println(btree.getBloomFilter().toString());

        // remove the index entry.
        assertEquals(k0,btree.remove(k0));

        // show the filter state.
        System.err.println(btree.getBloomFilter().toString());

        // verify at BTree API (contains)
        assertFalse(btree.contains(k1));
        assertFalse(btree.contains(k0));

        // verify at BTree API (lookup)
        assertEquals(null,btree.lookup(k1));
        assertEquals(null,btree.lookup(k0));

        // verify at bloom filter API.
        assertFalse(btree.getBloomFilter().contains(k1));
        assertTrue(btree.getBloomFilter().contains(k0)); // Note: now a false positive!
        
    }

    /*
     * Note: this does not work out since it is not so easy to determine when
     * the iterator is a point test as toKey is the exclusive upper bound.
     */
//    /**
//     * Test verifies that the BTree will automatically apply the bloom filter to
//     * reject range iterator requests that correspond to a point test, but only
//     * when the iterator was not requested with any options that would permit
//     * concurrent modification of the index.
//     */
//    public void test_iterator() {
//    
//        final BTree btree = getBTree(4/* branchingFactor */, .0001d/* errorRate */);
//        
//        /*
//         * ensure that the root leaf is defined and the bloom filter also (if
//         * used).
//         */
//        btree.reopen();
//        
//        // bloom filter exists
//        assertTrue(btree.isBloomFilter());
//        assertNotNull(btree.getBloomFilter());
//
//        // show the filter state.
//        System.err.println(btree.getBloomFilter().toString());
//        
//        final byte[] k0 = new byte[]{0};
//        final byte[] k1 = new byte[]{1};
//        
//        assertFalse(btree.contains(k1));
//        assertFalse(btree.contains(k0));
//        
//        // add an index entry.
//        assertNull(btree.insert(k0, k0));
//
//        // verify at BTree API (contains)
//        assertFalse(btree.contains(k1));
//        assertTrue(btree.contains(k0));
//
//        // verify at BTree API (lookup)
//        assertEquals(null,btree.lookup(k1));
//        assertEquals(k0,btree.lookup(k0));
//
//        // verify at bloom filter API.
//        assertFalse(btree.getBloomFilter().contains(k1));
//        assertTrue(btree.getBloomFilter().contains(k0));
//
//        // show the filter state.
//        System.err.println(btree.getBloomFilter().toString());
//
//        /*
//         * verify that the normal iterator is used for [k0] since the filter
//         * will report that it is in the index.
//         */
//        {
//
//            assertTrue(btree.rangeIterator(k0, k0).hasNext());
//
//        }
//
//        /*
//         * verify that an EmptyTupleIterator is used for [k1] since the filter
//         * will report that it is not in the index.
//         */
//        {
//
//            assertFalse(btree.rangeIterator(k1, k1).hasNext());
//
//            assertTrue(btree.rangeIterator(k1, k1) instanceof EmptyTupleIterator);
//            
//        }
//        
//    }
    
    /**
     * Simple test that the bloom filter is persisted with the btree and
     * reloaded from the store.
     * 
     * @see BloomFilter
     * @see AbstractBTree#bloomFilter
     * @see AbstractBTree#usesBloomFilter
     */
    public void test_persistence() {

        final IRawStore store = new SimpleMemoryRawStore();
        
        final BTree btree;
        {

            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            // enable bloom filter.
            metadata.setBloomFilterFactory(new BloomFilterFactory(100000/*n*/));
            
            btree = BTree.create(store, metadata);
            
        }
        
        // force create of the root leaf.
        btree.getRoot();

        // show the filter state.
        System.err.println(btree.getBloomFilter().toString());

        final byte[] k0 = new byte[]{0};
        final byte[] k1 = new byte[]{1};
        
        // add an index entry.
        assertNull(btree.insert(k0, k0));

        // verify at BTree API (contains)
        assertFalse(btree.contains(k1));
        assertTrue(btree.contains(k0));

        // verify at BTree API (lookup)
        assertEquals(null,btree.lookup(k1));
        assertEquals(k0,btree.lookup(k0));

        // verify at bloom filter API.
        assertFalse(btree.getBloomFilter().contains(k1));
        assertTrue(btree.getBloomFilter().contains(k0));

        // show the filter state.
        System.err.println("before checkpoint: "+btree.getBloomFilter());

        // write a checkpoint (should force the bloom filter to the store).
        final long addrCheckpoint = btree.writeCheckpoint();
        
        /*
         * verify that the bloom filter addr is in the checkpoint record and
         * that we can read the bloom filter from the store using that address.
         */
        {
            
            // load the checkpoint record.
            final Checkpoint checkpoint = Checkpoint.load(store, addrCheckpoint);
            
            System.err.println(checkpoint.toString());
            
            // assert bloom filter address is defined.
            assertNotSame(0L, checkpoint.getBloomFilterAddr());
            
            // read the bloom filter from the store.
            final BloomFilter bloomFilter = (BloomFilter) SerializerUtil
                    .deserialize(store.read(checkpoint.getBloomFilterAddr()));
            
            System.err.println("as read from store: "+bloomFilter);

            /*
             * Verify that we read in a bloom filter instance that has the same
             * state by testing some keys against the filter.
             */

            assertFalse(bloomFilter.contains(k1));
            assertTrue(bloomFilter.contains(k0));
            
            // show the filter state.
            System.err.println(btree.getBloomFilter().toString());

        }
        
        /*
         * Close and then re-open the btree, verifying that the bloom filter was
         * discarded and then auto-magically re-appeared.
         */
        {
            
            btree.close();

            // reference was cleared.
            assertNull(btree.bloomFilter); 
            
            btree.reopen();

            // bloom filter is defined again.
            assertNotNull(btree.getBloomFilter());

            // show the filter state.
            System.err.println(btree.getBloomFilter().toString());

            /*
             * Verify that the auto-magical reappearance of the bloom filter
             * gave us a bloom filter instance that has the same state (vs an
             * empty bloom filter) by testing some points in the index.
             */

            // verify at BTree API (contains)
            assertFalse(btree.contains(k1));
            assertTrue(btree.contains(k0));

            // verify at BTree API (lookup)
            assertEquals(null, btree.lookup(k1));
            assertEquals(k0, btree.lookup(k0));

            // verify at bloom filter API.
            assertFalse(btree.getBloomFilter().contains(k1));
            assertTrue(btree.getBloomFilter().contains(k0));

        }
        
    }
    
    /**
     * Simple test that the bloom filter is discarded if the btree is closed
     * without writing a checkpoint.
     */
    public void test_persistence_bloomFilterDiscarded() {

        final IRawStore store = new SimpleMemoryRawStore();
        
        final BTree btree;
        {

            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            // enable bloom filter.
            metadata.setBloomFilterFactory(new BloomFilterFactory(100000/*n*/));
            
            btree = BTree.create(store, metadata);
            
        }
        
        // force create of the root leaf.
        btree.getRoot();
        
        final byte[] k0 = new byte[]{0};
        final byte[] k1 = new byte[]{1};
        
        // add an index entry.
        assertNull(btree.insert(k0, k0));

        // verify at BTree API (contains)
        assertFalse(btree.contains(k1));
        assertTrue(btree.contains(k0));

        // verify at BTree API (lookup)
        assertEquals(null,btree.lookup(k1));
        assertEquals(k0,btree.lookup(k0));

        // verify at bloom filter API.
        assertFalse(btree.getBloomFilter().contains(k1));
        assertTrue(btree.getBloomFilter().contains(k0));

        // Note: closes the BTree but DOES NOT write a checkpoint.
        btree.close();
        
        /*
         * Verify that the bloom filter was discarded by testing it to see
         * whether or not it accepts the key inserted before we discarded the
         * writes on the index using close().
         */

        assertFalse(btree.getBloomFilter().contains(k1));
        assertFalse(btree.getBloomFilter().contains(k0));
            
    }

    /**
     * Verifies that {@link BTree#removeAll()} resets the bloom filter.
     */
    public void test_removeAll() {
       
        final IRawStore store = new SimpleMemoryRawStore();
        
        final BTree btree;
        {

            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            // enable bloom filter.
            metadata.setBloomFilterFactory(new BloomFilterFactory(100000/*n*/));
            
            btree = BTree.create(store, metadata);
            
        }
        
        // force create of the root leaf.
        btree.getRoot();
        
        final byte[] k0 = new byte[]{0};
        final byte[] k1 = new byte[]{1};
        
        // add an index entry.
        assertNull(btree.insert(k0, k0));

        // verify at bloom filter API.
        assertFalse(btree.getBloomFilter().contains(k1));
        assertTrue(btree.getBloomFilter().contains(k0));

        final BloomFilter oldFilter = btree.getBloomFilter();
        
        btree.removeAll();
        
        // verify different bloom filter reference.
        assertFalse(oldFilter == btree.getBloomFilter());
        
        // verify NOT present at bloom filter API.
        assertFalse(btree.getBloomFilter().contains(k1));
        assertFalse(btree.getBloomFilter().contains(k0));
        
    }
    
    /**
     * Unit test disables the bloom filter and verifies that a checkpoint writes
     * a 0L as the address of the bloom filter and that on reload the btree does
     * not have a bloomfilter
     */
    public void test_disable() {

        final IRawStore store = new SimpleMemoryRawStore();
        
        final BTree btree;
        {

            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            // enable bloom filter.
            metadata.setBloomFilterFactory(new BloomFilterFactory(100000/*n*/));
            
            btree = BTree.create(store, metadata);
            
        }
        
        // force create of the root leaf.
        btree.getRoot();
        
        final byte[] k0 = new byte[]{0};
        final byte[] k1 = new byte[]{1};
        
        // add an index entry.
        assertNull(btree.insert(k0, k0));

        // verify at bloom filter API.
        assertFalse(btree.getBloomFilter().contains(k1));
        assertTrue(btree.getBloomFilter().contains(k0));

        // disable the bloom filter.
        btree.getBloomFilter().disable();
        
        // reference is still there
        assertNotNull(btree.bloomFilter);
        // bloom filter reports that it is disabled.
        assertFalse(btree.bloomFilter.isEnabled());
        // access method now reports null since it is disabled.
        assertNull(btree.getBloomFilter());

        // check point the btree.
        final long addrCheckpoint = btree.writeCheckpoint();

        // load the checkpoint record from the store.
        final Checkpoint checkpoint = Checkpoint.load(btree.getStore(),
                addrCheckpoint);
        
        // bloom filter address is 0L in the checkpoint record.
        assertEquals(0L, checkpoint.getBloomFilterAddr());

        // load the btree from the store using the new checkpoint.
        {
            
            final BTree btree2 = BTree.load(store, addrCheckpoint);
            
            // the bloom filter is gone.
            assertNull(btree2.getBloomFilter());
            
        }
        
    }

    /**
     * Unit test verifies that the bloom filter is automatically disabled once
     * the #of entries in the {@link BTree} exceeds the <code>maxN</code> (the
     * calculated #of index entries at which the bloom filter performance will
     * have degraded to below the desired maximum error rate).
     */
    public void test_autoDisable() {
        
        final IRawStore store = new SimpleMemoryRawStore();

        final int n = 10;
        final double p = 0.01;
        final double maxP = 0.12;
        final int maxN;// computed below.

        final BTree btree;
        {

            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            BloomFilterFactory factory = new BloomFilterFactory(n, p, maxP);
            
            // enable bloom filter.
            metadata.setBloomFilterFactory(factory);
            
            maxN = factory.maxN;
            
            System.err.println("factory="+factory);
            
            btree = BTree.create(store, metadata);
            
        }

        // make sure the root leaf is open.
        btree.reopen();
        
        // bloom filter is defined.
        assertNotNull(btree.bloomFilter);

        // bloom filter is not dirty.
        assertFalse(btree.bloomFilter.isDirty());

        // bloom filter is enabled.
        assertTrue(btree.bloomFilter.isEnabled());

        for (int i = 0; i < maxN; i++) {
         
            btree.insert(Integer.valueOf(i), null);

            assertTrue(btree.bloomFilter.isDirty());
            
            assertTrue(btree.bloomFilter.isEnabled());
            
        }

        // the straw the breaks the filters back.
        btree.insert(Integer.valueOf(maxN), null);
        
        // the filter was disabled.
        assertFalse(btree.bloomFilter.isEnabled());
        
    }

}
