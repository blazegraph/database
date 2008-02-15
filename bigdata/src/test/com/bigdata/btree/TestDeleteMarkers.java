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
 * Created on Feb 13, 2008
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test of basic btree operations when delete markers are maintained.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDeleteMarkers extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestDeleteMarkers() {
    }

    /**
     * @param name
     */
    public TestDeleteMarkers(String name) {
        super(name);
    }

    /**
     * Test of basic index operation semantics when delete markers are enabled.
     */
    public void test_crud() {
        
        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setDeleteMarkers(true);

        // Note: timestamp is 0L since version timestamps are not enabled.
        final long timestamp = 0L;

        // create index.
        BTree btree = BTree.create(new SimpleMemoryRawStore(),metadata);
        
        // verify delete markers are in use.
        assertTrue(btree.getIndexMetadata().getDeleteMarkers());
        
        final byte[] k1 = new byte[] { 1 };
        final byte[] v1 = new byte[] { 1 };
        
        /*
         * verify initial conditions.
         */
        assertFalse(btree.contains( k1 ));
        
        assertEquals(null,btree.lookup( k1 ));
        
        assertEquals(0,btree.getEntryCount());
        
        assertEquals(0,btree.rangeCount(null, null));
        
        assertSameIterator(new byte[][] {}, btree.rangeIterator(null, null,
                0/* capacity */,
                IRangeQuery.DEFAULT | IRangeQuery.DELETED, null/* filter */));
        
        /*
         * insert a null value under the key.
         */
        btree.insert(k1, null, false/*delete*/, timestamp, null/*tuple*/);

        assertTrue(btree.contains( k1 ));

        assertEquals(null,btree.lookup( k1 ));

        assertEquals(1,btree.getEntryCount());
        
        assertEquals(1,btree.rangeCount(null, null));
        
        assertSameIterator(new byte[][] { null }, btree.rangeIterator(null,
                null, 0/* capacity */, IRangeQuery.DEFAULT
                        | IRangeQuery.DELETED, null/* filter */));

        /*
         * insert a non-null value under that key.
         */
        btree.insert(k1, v1, false/*delete*/, timestamp, null/*tuple*/);
        
        assertTrue(btree.contains( k1 ));

        assertEquals(v1,btree.lookup( k1 ));

        assertEquals(1,btree.getEntryCount());
        
        assertEquals(1,btree.rangeCount(null, null));
        
        assertSameIterator(new byte[][] { v1 },
                btree
                        .rangeIterator(null, null, 0/* capacity */,
                                IRangeQuery.DEFAULT | IRangeQuery.DELETED,
                                null/* filter */));

        /*
         * insert a delete marker under that key.
         */
        btree.insert(k1, null, true/* delete */, timestamp, null/* tuple */);

        assertFalse(btree.contains(k1));

        assertEquals(null, btree.lookup(k1));

        // verify that the delete marker is present.
        assertTrue(btree.lookup(k1, btree.lookupTuple.get()).isDeletedVersion());

        assertEquals(1, btree.getEntryCount());

        assertEquals(1, btree.rangeCount(null, null));

        // the deleted entry is NOT visible when we do NOT specify DELETED.
        assertSameIterator(new byte[][] {}, btree.rangeIterator(null,
                null, 0/* capacity */, IRangeQuery.DEFAULT, null/* filter */));
        
        // the deleted entry is visible when we specify DELETED.
        assertSameIterator(new byte[][] { null }, btree.rangeIterator(null,
                null, 0/* capacity */, IRangeQuery.DEFAULT
                        | IRangeQuery.DELETED, null/* filter */));

    }

    /**
     * Test that {@link BTree#remove(byte[],ITuple)} is disabled when delete
     * markers are enabled.
     * 
     * @todo test the semantics of {@link BTree#remove(byte[])}, which should
     *       be equivilant to an insert of a delete marker.
     */
    public void test_removeNotAllowed() {
        
        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setDeleteMarkers(true);

        // create index.
        BTree btree = BTree.create(new SimpleMemoryRawStore(),metadata);
        
        // verify delete markers are in use.
        assertTrue(btree.getIndexMetadata().getDeleteMarkers());

        try {
            
            btree.remove(new byte[] { 1 }, null);
            
            fail("Expecting: " + UnsupportedOperationException.class);
            
        } catch (UnsupportedOperationException ex) {
            
            log.info("Ignoring expected exception: " + ex);
            
        }

    }

    
    /**
     * Test verifies that {@link BTree#removeAll()} causes deletion markers to
     * be written for each undeleted entry in the B+Tree.
     * <p>
     * Note: This test only tests removal with a root leaf and therefore relies
     * on the underlying iterator to correctly support removal or update of an
     * entry in the btree in any leaf regardless of the height of the btree.
     */
    public void test_removeAll_rootLeaf() {
        
        final byte[] k3 = new byte[]{3};
        final byte[] k5 = new byte[]{5};
        final byte[] k7 = new byte[]{7};
        
        final byte[] v3  = new byte[]{3};
        final byte[] v5  = new byte[]{5};
        final byte[] v7  = new byte[]{7};

        final IRawStore store = new SimpleMemoryRawStore();
        
        // create btrees
        final BTree btree;
        {
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setBranchingFactor(3);
            
            md.setDeleteMarkers(true);

            btree = BTree.create(store, md);
            
        }
        
        /*
         * fill the root leaf on the btree.
         */
        btree.insert(k3,v3);
        btree.insert(k5,v5);
        btree.insert(k7,v7);

        assertEquals(3,btree.getEntryCount());
        
        // verify view iterator.
        assertSameIterator(new byte[][]{v3,v5,v7},btree.rangeIterator(null,null));

        /*
         * remove all un-deleted entries by writing deletion markers.
         */
        btree.removeAll();

        assertEquals(3,btree.getEntryCount());

        assertFalse(btree.contains(k3));
        assertFalse(btree.contains(k5));
        assertFalse(btree.contains(k7));
        
        assertSameIterator(new byte[][]{},btree.entryIterator());

        /*
         * verify presence of deletion markers for the deleted index entries.
         */
        {

            Tuple tuple = btree.lookupTuple.get();
            
            tuple = btree.lookup(k3, tuple);
            
            assertNotNull(tuple);
            
            assertTrue(tuple.isDeletedVersion());
            
        }
        {

            Tuple tuple = btree.lookupTuple.get();
            
            tuple = btree.lookup(k5, tuple);
            
            assertNotNull(tuple);
            
            assertTrue(tuple.isDeletedVersion());
            
        }
        {

            Tuple tuple = btree.lookupTuple.get();
            
            tuple = btree.lookup(k7, tuple);
            
            assertNotNull(tuple);
            
            assertTrue(tuple.isDeletedVersion());
            
        }

    }

}
