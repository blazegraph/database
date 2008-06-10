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
 * Created on Jun 10, 2008
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.btree.AbstractBTreeTupleCursor.MutableBTreeTupleCursor;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.journal.TemporaryRawStore;

/**
 * Test ability to traverse tuples using an {@link ITupleCursor} while the SAME
 * THREAD is used to insert, update, or remove tuples from a mutable
 * {@link BTree}.
 * 
 * @todo test with multiple inserts such that the leaf becomes invalidated
 *       several times over in order to verify that the cursor position is
 *       re-establishing it's listener each time it re-locates the leaf spanning
 *       the current tuple. (do this for remove also).
 * 
 * @todo most tests should be run with and without delete markers.
 * 
 * @todo most tests should have variants for the {@link IsolatedFusedView}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMutableBTreeCursors extends AbstractBTreeCursorTestCase {

    /**
     * 
     */
    public TestMutableBTreeCursors() {
    }

    /**
     * @param name
     */
    public TestMutableBTreeCursors(String name) {
    
        super(name);
        
    }

    @Override
    protected boolean isReadOnly() {

        return false;
        
    }

    @Override
    protected ITupleCursor<String> newCursor(AbstractBTree btree, int flags,
            byte[] fromKey, byte[] toKey) {
        
        assert ! btree.isReadOnly();
        
        return new MutableBTreeTupleCursor<String>((BTree) btree,
                new Tuple<String>(btree, IRangeQuery.DEFAULT),
                null/* fromKey */, null/* toKey */);
        
    }

    /**
     * Test ability to remove tuples using {@link ITupleCursor#remove()}.
     */
    public void test_cursor_remove() {

        final BTree btree;
        {
       
            btree = BTree.create(new TemporaryRawStore(), new IndexMetadata(
                    UUID.randomUUID()));

            btree.insert(10, "Bryan");
            btree.insert(20, "Mike");
            btree.insert(30, "James");
            
        }
        
        /*
         * loop over the index removing entries as we go.
         * 
         * Note: this loop is unrolled in order to make it easier to track the
         * state changed as tuples are deleted.
         */
        {
            
            ITupleCursor<String> cursor = newCursor(btree);

            // visit (10,Bryan) and then delete that tuple.
            {

                assertTrue(cursor.hasNext());
                
                ITuple tuple = cursor.next();
                
                assertEquals(new TestTuple<String>(10,"Bryan"),tuple);
                
                assertTrue(btree.contains(tuple.getKey()));

                cursor.remove();

                assertFalse(btree.contains(tuple.getKey()));

            }

            // visit (20,Mike) and then delete that tuple.
            {

                assertTrue(cursor.hasNext());
                
                ITuple tuple = cursor.next();
                
                assertEquals(new TestTuple<String>(20,"Mike"),tuple);
                
                assertTrue(btree.contains(tuple.getKey()));

                cursor.remove();

                assertFalse(btree.contains(tuple.getKey()));

            }

            // visit (30,James) and then delete that tuple.
            {

                assertTrue(cursor.hasNext());
                
                ITuple tuple = cursor.next();
                
                assertEquals(new TestTuple<String>(30,"James"),tuple);
                
                assertTrue(btree.contains(tuple.getKey()));

                cursor.remove();

                assertFalse(btree.contains(tuple.getKey()));

            }
            
            // the iterator is exhausted.
            assertFalse(cursor.hasNext());
            
            // and the index is empty.
            assertFalse(cursor.hasPrior());

            /*
             * This is the rolled up version of the loop.
             */
            
//            while(cursor.hasNext()) {
//                
//                ITuple tuple = cursor.next();
//                
//                assertTrue(btree.contains(tuple.getKey()));
//
//                cursor.remove();
//
//                assertFalse(btree.contains(tuple.getKey()));
//
//            }
            
            assertEquals(0,btree.getEntryCount());
            
        }
        
    }
    
    /**
     * Test for update (the tuple state must be re-copied from the index).
     * <p>
     * Note that copy-on-write is handled differently even when the trigger is
     * an update (vs an insert or a remove).
     * 
     * @todo test delete of a tuple when delete markers are supported (this is a
     *       actually an update of the tuple in the index).
     */
    public void test_concurrent_modification_update() {

        final BTree btree;
        {
       
            btree = BTree.create(new TemporaryRawStore(), new IndexMetadata(
                    UUID.randomUUID()));

            btree.insert(10, "Bryan");
            btree.insert(20, "Mike");
            btree.insert(30, "James");
            
        }
        
        // loop over the index removing entries as we go.
        {
            
            ITupleCursor<String> cursor = newCursor(btree);

            assertNotNull(cursor.seek(20));
            
            assertEquals("Mike",cursor.tuple().getObject());

            // update the tuple.
            btree.insert(20, "Michael");

            // verify update is reflected by the tuple.
            assertEquals("Michael",cursor.tuple().getObject());

        }
        
    }
    
    /**
     * Unit test for concurrent modification resulting from an insert() of a
     * precedessor (the tuple may have been moved down in the leaf or the tree
     * structure may have been changed more broadly if the leaf overflows). The
     * cursor position's listener needs to notice the event and relocate itself
     * within the BTree.
     * 
     * @todo test when splitting the root leaf (at m=3 this example would split
     *       the root leaf).
     */
    public void test_concurrent_modification_insert() {

        fail("write test");
        
    }
   
    /**
     * Unit test for concurrent modification resulting from an remove() of a
     * predecessor (the tuple may have been moved down in the leaf or the tree
     * structure may have been changed more broadly if the leaf underflows). The
     * cursor position's listener needs to notice the event and relocate itself
     * within the BTree.
     * 
     * @todo test when a leaf underflows causing it to be joined with its
     *       neighbor (a) when a tuple is rotated from the neighbor; and (b)
     *       when the leaves are joined and the root is replaced by a root leaf.
     */
    public void test_concurrent_modification_remove() {

        fail("write test");
        
    }
    
    /**
     * Unit test for copy-on-write (the leaf is clean and then and update,
     * insert, or remove is requested which forces copy-on-write to clone the
     * leaf). The listener needs to notice the event and relocate itself within
     * the BTree.
     */
    public void test_concurrent_modification_copy_on_write() {

        fail("write test");

    }
    
}
