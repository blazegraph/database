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
 * Created on Dec 15, 2008
 */

package com.bigdata.btree;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.bigdata.btree.AbstractBTree.HardReference;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Unit tests for transient {@link BTree}s (no backing store).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTransientBTree extends AbstractBTreeTestCase {

    public TestTransientBTree() {
        super();
    }
    
    public TestTransientBTree(String name) {
        super(name);
    }

    /**
     * Test the ability to create a transient {@link BTree} (one not backed by a
     * persistence store).
     */
    public void test_createTransient() {
        
        final BTree btree = BTree.createTransient(new IndexMetadata(UUID
                .randomUUID()));

        assertNotNull(btree);

        assertNull(btree.getStore());

        assertEquals(0, btree.getEntryCount());
        
        assertNotNull(btree.getRoot());

        final String expected = "def";
        
        btree.insert("abc", expected);
        
        assertEquals(expected,(String)btree.lookup("abc"));
       
        assertTrue(btree.getRoot().self instanceof HardReference<?>);
        
    }

    /**
     * Verifies that closing a transient {@link BTree} is allowed and that
     * all data is discarded.
     */
    public void test_close() {
        
        final BTree btree = BTree.createTransient(new IndexMetadata(UUID
                .randomUUID()));

        assertEquals(0,btree.getEntryCount());
        
        btree.insert("abc", "def");
        
        assertEquals(1, btree.getEntryCount());

        btree.close();

        // force re-open.
        btree.reopen();

        assertEquals(0, btree.getEntryCount());
        
    }
    
    /**
     * Test inserts a bunch of data into a transient {@link BTree} and verifies
     * that eviction of dirty nodes and leaves does not result in errors arising
     * from an attempt to persist their state on the (non-existent) backing
     * store.
     */
    public void test_eviction() {

        final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
        
        final int branchingFactor = 3;

        md.setBranchingFactor(branchingFactor);
        
        final BTree btree = BTree.createTransient(md);

        final int writeRetentionQueueCapacity = btree.writeRetentionQueue
                .capacity();

        if (log.isInfoEnabled())
            log.info(btree.toString());

        /*
         * Until the write retention queue is full.
         */
        long key = 0L;
        while (btree.writeRetentionQueue.size() < writeRetentionQueueCapacity) {

            btree.insert(key, key * 2);

            key++;
            
        }
        
        if (log.isInfoEnabled())
            log.info(btree.toString());
        
        // insert several more leaves worth of data into the btree.
        for (int i = 0; i < branchingFactor * 10; i++) {

            btree.insert(key, key * 2);

            key++;

        }
        
        if (log.isInfoEnabled())
            log.info(btree.toString());
        
        /*
         * no errors!
         */
        
    }

    /**
     * Test verifies that the nodes and leaves become weakly reachable once they
     * have been deleted.
     * <p>
     * The test builds up a modest amount of data in the {@link BTree} using a
     * small branching factor to force a large #of nodes and leaves to be
     * created. A traversal is then performed of the nodes and leaves and all of
     * their references are placed into a weak value collection. It then removes
     * all entries in a key range, which should cause some leaves (and perhaps
     * some nodes) to become weakly reachable. Finally, it forces a large number
     * of object allocations in order to prompt a GC that will clear those weak
     * references. The weak reference collection is then scanned to verify that
     * its size has been decreased.
     * <p>
     * Note: This test is of necessity subject to the whims of the garbage
     * collector. If it fails, try increasing some of the constants in the test
     * and see if that will provoke a GC that will clear the references.
     */
    public void test_delete() {
    
        final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
        
        final int branchingFactor = 3;

        md.setBranchingFactor(branchingFactor);
        
        final BTree btree = BTree.createTransient(md);

        if (log.isInfoEnabled())
            log.info(btree.toString());

        /*
         * Until the write retention queue is full.
         */
        long key = 0L;
        while (key < 100000) {

            btree.insert(key, key * 2);

            key++;
            
        }
        
        if (log.isInfoEnabled())
            log.info(btree.toString());

        /*
         * Populate a weak value collection from the BTree's nodes and leaves.
         */
        final LinkedList<WeakReference<AbstractNode>> refs = new LinkedList<WeakReference<AbstractNode>>();
        {

            final Iterator<AbstractNode> itr = btree.getRoot().postOrderNodeIterator();
            
            while(itr.hasNext()) {
                
                final AbstractNode node = itr.next();
                
                refs.add( new WeakReference(node) );
                
            }

            if (log.isInfoEnabled())
                log.info("There are " + refs.size() + " nodes in the btree");

            if (log.isInfoEnabled())
                log.info("after inserting keys: " + btree.toString());

            assertEquals(btree.getNodeCount()+btree.getLeafCount(),refs.size());
            
        }

        /*
         * Now delete a key-range and verify that #of nodes in the btree has
         * been decreased.
         */  
        {
            
            final ITupleIterator itr = btree.rangeIterator(KeyBuilder
                    .asSortKey(10000L), KeyBuilder.asSortKey(20000L),
                    0/* capacity */, IRangeQuery.DEFAULT | IRangeQuery.CURSOR,
                    null/* filter */);
            
            while(itr.hasNext()) {
                
                itr.next();
                
                itr.remove();
                
            }
            
            if (log.isInfoEnabled())
                log.info("after deleting key range: " + btree.toString());
        
            assertTrue(btree.getNodeCount() + btree.getLeafCount() < refs
                    .size());        

        }

        /*
         * Loop until GC activity has caused references to be cleared.
         */
        final int limit = 100;
        for (int x = 0; x < limit; x++) {

            System.gc();

            final int n = countClearedRefs(refs);

            if (log.isInfoEnabled())
                log.info("pass " + x + "of " + limit
                        + ": #of cleared references=" + n);

            if (n <= refs.size()) {
             
                return;
                
            }
            
            final List<byte[]> stuff = new LinkedList<byte[]>();

            for (int y = 0; y < 1000; y++) {

                stuff.add(new byte[y * 1000 + 1]);

            }

        }
        
        fail("Did not clear references after "+limit+" passes");
        
    }

    /**
     * Return the #of entries in the collection whose references have been
     * cleared.
     * 
     * @param refs
     * 
     * @param <T>
     * 
     * @return
     */
    private <T> int countClearedRefs(List<WeakReference<T>> refs) {
        
        final Iterator<WeakReference<T>> itr = refs.iterator();
        
        int n = 0;
        
        while(itr.hasNext()) {
            
            final WeakReference<T> ref = itr.next();
            
            if(ref.get()==null) n++;
            
        }
    
        return n;
        
    }

    /**
     * Tests various methods that deal with persistence and makes sure that we
     * have reasonable error messages.
     */
    public void test_niceErrors() {

        final BTree btree = BTree.createTransient(new IndexMetadata(UUID
                .randomUUID()));

        try {
            btree.handleCommit(System.currentTimeMillis());
        } catch (UnsupportedOperationException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

        try {
            btree.flush();
        } catch (UnsupportedOperationException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

        try {
            btree.writeCheckpoint();
        } catch (UnsupportedOperationException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

    }

    /**
     * This is the same as {@link #test_delete()} but the {@link BTree} is
     * backed by an {@link IRawStore}.
     * 
     * @todo since the code is identical other than allocating the {@link BTree}
     *       , factor out a doDeleteTest(BTree) method.
     */
    public void test_deletePersistent() {
        
        final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
        
        final int branchingFactor = 3;

        md.setBranchingFactor(branchingFactor);
        
        final BTree btree = BTree.create(new SimpleMemoryRawStore(), md);

        if (log.isInfoEnabled())
            log.info(btree.toString());

        /*
         * Until the write retention queue is full.
         */
        long key = 0L;
        while (key < 100000) {

            btree.insert(key, key * 2);

            key++;
            
        }
        
        if (log.isInfoEnabled())
            log.info(btree.toString());

        /*
         * Populate a weak value collection from the BTree's nodes and leaves.
         */
        final LinkedList<WeakReference<AbstractNode>> refs = new LinkedList<WeakReference<AbstractNode>>();
        {

            final Iterator<AbstractNode> itr = btree.getRoot().postOrderNodeIterator();
            
            while(itr.hasNext()) {
                
                final AbstractNode node = itr.next();
                
                refs.add( new WeakReference(node) );
                
            }

            if (log.isInfoEnabled())
                log.info("There are " + refs.size() + " nodes in the btree");

            if (log.isInfoEnabled())
                log.info("after inserting keys: " + btree.toString());

            assertEquals(btree.getNodeCount()+btree.getLeafCount(),refs.size());
            
        }

        /*
         * Now delete a key-range and verify that #of nodes in the btree has
         * been decreased.
         */  
        {
            
            final ITupleIterator itr = btree.rangeIterator(KeyBuilder
                    .asSortKey(10000L), KeyBuilder.asSortKey(20000L),
                    0/* capacity */, IRangeQuery.DEFAULT | IRangeQuery.CURSOR,
                    null/* filter */);
            
            while(itr.hasNext()) {
                
                itr.next();
                
                itr.remove();
                
            }
            
            if (log.isInfoEnabled())
                log.info("after deleting key range: " + btree.toString());
        
            assertTrue(btree.getNodeCount() + btree.getLeafCount() < refs
                    .size());        

        }
        
        /*
         * Loop until GC activity has caused references to be cleared.
         */
        final int limit = 100;
        for (int x = 0; x < limit; x++) {

            System.gc();

            final int n = countClearedRefs(refs);

            if (log.isInfoEnabled())
                log.info("pass " + x + "of " + limit
                        + ": #of cleared references=" + n);

            if (n <= refs.size()) {
             
                return;
                
            }
            
            final List<byte[]> stuff = new LinkedList<byte[]>();

            for (int y = 0; y < 1000; y++) {

                stuff.add(new byte[y * 1000 + 1]);

            }

        }
        
        fail("Did not clear references after : " + limit + " passes");
        
    }

}
