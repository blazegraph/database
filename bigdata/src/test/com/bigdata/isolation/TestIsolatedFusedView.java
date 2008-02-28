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
 * Created on Feb 14, 2008
 */

package com.bigdata.isolation;

import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.TestFusedView;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;

/**
 * Test suite for {@link IsolatedFusedView}.
 * <p>
 * Note: the test suite for an isolated write set is similar to a
 * {@link FusedView} except that it must also handle timestamps properly.
 * timestamps do not really effect the iterator or various lookup methods - they
 * mainly interact with the logic for detecting write-write conflicts.
 * 
 * @see TestFusedView
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIsolatedFusedView extends AbstractBTreeTestCase {

    public TestIsolatedFusedView() {
    }

    public TestIsolatedFusedView(String name) {
        super(name);
    }

    /**
     * Unit test examines the propagation of timestamps from the ground state
     * into the isolated write set with both insert() and remove() operations.
     * The test does not explore validation of the write set or mergeDown onto
     * the unisolated index (aka the commit of the transaction).
     */
    public void test_writeSetIsolation() {
        
        byte[] k3 = i2k(3);
        byte[] k5 = i2k(5);
        byte[] k7 = i2k(7);

        byte[] v3a = new byte[]{3};
        byte[] v5a = new byte[]{5};
        byte[] v7a = new byte[]{7};
        
        byte[] v3b = new byte[]{3,1};
        byte[] v5b = new byte[]{5,1};
        byte[] v7b = new byte[]{7,1};
        
        Properties properties = new Properties();
        
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
        
        Journal journal = new Journal(properties);
        
        // two btrees with the same index UUID.
        final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
        {
            
            md.setBranchingFactor(3);
            
            md.setDeleteMarkers(true);

            md.setVersionTimestamps(true);

        }

        /*
         * Some distinct timestamps.
         */
        
        final long t1 = journal.nextTimestamp();

        final long t2 = journal.nextTimestamp();

        final long t3 = journal.nextTimestamp();
        
        final long startTime = journal.nextTimestamp();

        final long t4 = journal.nextTimestamp();
        
        /*
         * Create an index, write some data on that index, and checkpoint the
         * index to obtain the ground state against which the transaction will
         * run.
         */
        final BTree unisolatedIndex = BTree.create(journal, md);
        
        unisolatedIndex.insert(k3, v3a, false/* deleted */, t1, null/* tuple */);

        unisolatedIndex.insert(k7, null, true/* deleted */, t2, null/* tuple */);

        // checkpoint the unisolated index.
        final long addrCheckpoint1 = unisolatedIndex.checkpoint();

        // load the ground state from that checkpoint.
        final BTree groundState = BTree.load(journal, addrCheckpoint1);
        
        /*
         * Create the isolated write set. Keys found in the isolated write set
         * will cause the search to halt. If the key is not in the isolated
         * write set then the groundState will also be searched. A miss is
         * reported if the key is not found in the transaction's view of the
         * index.
         * 
         * Note: Since delete markers are enabled keys will be recognized when
         * the index entry has been marked as deleted.
         */
        
        // create the transaction's write set.
        final BTree writeSet = BTree.create(journal, md.clone());
        
        // create the transaction's isolated view.
        final IsolatedFusedView view = new IsolatedFusedView(startTime,
                new AbstractBTree[] { writeSet, groundState });

        assertTrue(view.contains(k3));
        assertFalse(view.contains(k7));
        assertSameIterator(new byte[][]{v3a}, view.rangeIterator(null, null));
        {
            /*
             * Verify all entries, included those that are marked as deleted.
             */
            final ITupleIterator itr = view
                    .rangeIterator(null, null, 0/* capacity */,
                            IRangeQuery.DEFAULT | IRangeQuery.DELETED, null/* filter */);
           
            // k3
            assertTrue(itr.hasNext());
            ITuple tuple;
            tuple = itr.next();
            assertEquals(k3,tuple.getKey());
            assertEquals(v3a,tuple.getValue());
            assertFalse(tuple.isDeletedVersion());
            assertEquals(t1,tuple.getVersionTimestamp());
            
            // k7 (deleted)
            assertTrue(itr.hasNext());
            tuple = itr.next();
            assertEquals(k7,tuple.getKey());
            assertTrue(tuple.isDeletedVersion());
            assertEquals(t2,tuple.getVersionTimestamp());
        }
        
        /*
         * Write on the isolated view under a new key (k5).
         */
        assertEquals(null,view.insert(k5, v5a));
        assertEquals(v5a,view.lookup(k5));// in view
        assertEquals(v5a,writeSet.lookup(k5)); // on write set.
        assertFalse(groundState.contains(k5)); // not on ground state.
        {
            /*
             * Verify the written entry.
             */
            ITuple tuple = view.rangeIterator(k5, BytesUtil.successor(k5)).next();
            assertEquals(k5,tuple.getKey());
            assertEquals(v5a,tuple.getValue());
            assertFalse(tuple.isDeletedVersion());
            assertEquals(startTime,tuple.getVersionTimestamp());
        }
        
        /*
         * Write on the isolated view under an existing key (k3).
         */
        assertEquals(v3a,view.insert(k3, v3b));
        assertEquals(v3b,view.lookup(k3));// in view
        assertEquals(v3b,writeSet.lookup(k3)); // on write set.
        assertEquals(v3a,groundState.lookup(k3)); // unchanged on groundState.
        {
            /*
             * Verify the written entry.
             */
            ITuple tuple = view.rangeIterator(k3, BytesUtil.successor(k3)).next();
            assertEquals(k3,tuple.getKey());
            assertEquals(v3b,tuple.getValue());
            assertFalse(tuple.isDeletedVersion());
            assertEquals(t1,tuple.getVersionTimestamp()); // note: timestamp is copied from groundState!
        }
        
        /*
         * Write on the isolated view under an existing key with a deleted entry (k7).
         */
        assertEquals(null,view.insert(k7, v7a));
        assertEquals(v7a,view.lookup(k7));// in view
        assertEquals(v7a,writeSet.lookup(k7)); // on write set.
        assertEquals(null,groundState.lookup(k7)); // unchanged on groundState.
        {
            /*
             * Verify the written entry.
             */
            ITuple tuple = view.rangeIterator(k7, BytesUtil.successor(k7)).next();
            assertEquals(k7,tuple.getKey());
            assertEquals(v7a,tuple.getValue());
            assertFalse(tuple.isDeletedVersion());
            assertEquals(t2,tuple.getVersionTimestamp()); // note: timestamp is copied from groundState!
        }
        
        /*
         * Verify re-write of k7 updates the value but not the timestamp.
         */
        assertEquals(v7a,view.insert(k7, v7b));
        assertEquals(v7b,view.lookup(k7));// in view
        assertEquals(v7b,writeSet.lookup(k7)); // on write set.
        assertEquals(null,groundState.lookup(k7)); // unchanged on groundState.
        {
            /*
             * Verify the written entry.
             */
            ITuple tuple = view.rangeIterator(k7, BytesUtil.successor(k7)).next();
            assertEquals(k7,tuple.getKey());
            assertEquals(v7b,tuple.getValue());
            assertFalse(tuple.isDeletedVersion());
            assertEquals(t2,tuple.getVersionTimestamp()); // note: timestamp unchanged from groundState!
        
        }
       
    }
    
    /**
     * Unit test for validating a write set against the current ground state.
     * ground state and are not detected by the isolated view. when those writes
     * on are the same keys as writes by the transaction a write-write conflict
     * will result. When they are on different keys there will be no conflict.
     * 
     * @todo write tests that correctly detect write-write conflicts. pay
     *       attention to write-delete, delete-delete, and delete-write cases as
     *       well.
     * 
     * @todo verify that writes on the unisolated index are not visible in the
     * 
     * @todo write on some keys that are not used by the tx to verify absence of
     *       write-write conflict.
     * 
     * @todo verify commit
     */
    public void test_validate() {

        fail("write test");

    }
    
    /**
     * Unit test for merging down a validated write set onto the current ground
     * state.
     */
    public void test_mergeDown() {
        
        fail("write test");

    }
    
}
