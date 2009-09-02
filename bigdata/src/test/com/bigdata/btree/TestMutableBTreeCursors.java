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

import java.util.NoSuchElementException;
import java.util.UUID;

import com.bigdata.btree.AbstractBTreeTupleCursor.MutableBTreeTupleCursor;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test ability to traverse tuples using an {@link ITupleCursor} while the SAME
 * THREAD is used to insert, update, or remove tuples from a mutable
 * {@link BTree}.
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
    protected ITupleCursor2<String> newCursor(AbstractBTree btree, int flags,
            byte[] fromKey, byte[] toKey) {
        
        assert ! btree.isReadOnly();
        
        return new MutableBTreeTupleCursor<String>((BTree) btree,
                new Tuple<String>(btree, flags), fromKey, toKey);
        
    }

    /**
     * Test ability to remove tuples using {@link ITupleCursor#remove()} during
     * forward traversal.
     */
    public void test_cursor_remove_during_forward_traversal() {

        final BTree btree;
        {
       
            btree = BTree.create(new SimpleMemoryRawStore(), new IndexMetadata(
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
            
            ITupleCursor2<String> cursor = newCursor(btree);

            // visit (10,Bryan) and then delete that tuple.
            {

                // visit the first tuple and verify its state.
                assertEquals(new TestTuple<String>(10,"Bryan"),cursor.next());
                
                // remove it.
                cursor.remove();

                // verify cursor reports tuple is null since it no longer exists in the index.
                assertNull(cursor.tuple());

            }

            // visit (20,Mike) and then delete that tuple.
            {

                // visit the next tuple and verify its state.
                assertEquals(new TestTuple<String>(20,"Mike"),cursor.next());
                
                // remove it.
                cursor.remove();

                // verify cursor reports tuple is null since it no longer exists in the index.
                assertNull(cursor.tuple());

            }

            // visit (30,James) and then delete that tuple.
            {

                // visit the next tuple and verify its state.
                assertEquals(new TestTuple<String>(30,"James"),cursor.next());
                
                // remove it.
                cursor.remove();

                // verify cursor reports tuple is null since it no longer exists in the index.
                assertNull(cursor.tuple());

            }
            
            // the iterator is exhausted.
            assertFalse(cursor.hasNext());

            // the index is empty.
            assertEquals(0,btree.getEntryCount());
            
        }
        
    }
    
    /**
     * Test ability to remove tuples using {@link ITupleCursor#remove()} during
     * reverse traversal.
     */
    public void test_cursor_remove_during_reverse_traversal() {

        final BTree btree;
        {
       
            btree = BTree.create(new SimpleMemoryRawStore(), new IndexMetadata(
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
            
            ITupleCursor2<String> cursor = newCursor(btree);

            // visit (30,James) and then delete that tuple.
            {

                // verify the state of the last tuple.
                assertEquals(new TestTuple<String>(30,"James"),cursor.prior());

                // remove it.
                cursor.remove();
                
                // verify tuple() reports null since it was deleted.
                assertNull(cursor.tuple());

            }
            
            // visit (20,Mike) and then delete that tuple.
            {

                // visit the prior tuple and verify its state.
                assertEquals(new TestTuple<String>(20,"Mike"),cursor.prior());

                // remove it.
                cursor.remove();

                // verify tuple() reports null since it was deleted.
                assertNull(cursor.tuple());

            }

            // visit (10,Bryan) and then delete that tuple.
            {

                // visit the prior tuple and verify its state.
                assertEquals(new TestTuple<String>(10,"Bryan"),cursor.prior());
                
                // remote it.
                cursor.remove();

                // verify tuple() reports null since it was deleted.
                assertNull(cursor.tuple());
                
            }

            // the iterator is exhausted.
            assertFalse(cursor.hasPrior());

            // the index is empty.
            assertEquals(0,btree.getEntryCount());
            
        }
        
    }
    
    /**
     * Test for update (the tuple state must be re-copied from the index).
     * <p>
     * Note that copy-on-write is handled differently even when the trigger is
     * an update (vs an insert or a remove).
     */
    public void test_concurrent_modification_update() {

        final BTree btree;
        {
       
            btree = BTree.create(new SimpleMemoryRawStore(), new IndexMetadata(
                    UUID.randomUUID()));

            btree.insert(10, "Bryan");
            btree.insert(20, "Mike");
            btree.insert(30, "James");
            
        }
        
        /*
         * seek to a tuple, update it using the btree api and then verify that
         * the tuple state is re-copied from the btree by tuple().
         */
        {

            ITupleCursor2<String> cursor = newCursor(btree);

            // seek to a tuple and verify its state.
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.seek(20));
            
            // verify state reported by tuple()
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.tuple());

            // update the tuple.
            btree.insert(20, "Michael");

            // verify update is reflected by the tuple.
            assertEquals(new TestTuple<String>(20, "Michael"), cursor.tuple());

        }
        
    }
    
    /**
     * Unit test for concurrent modification resulting from insert() and remove().
     */
    public void test_concurrent_modification_insert() {
        
        final BTree btree;
        {
       
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());

            // Explictly specify a large branching factor so that we do not split the root leaf.
            md.setBranchingFactor(20);
            
            btree = BTree.create(new SimpleMemoryRawStore(), md);

            btree.insert(10, "Bryan");
            btree.insert(20, "Mike");
            btree.insert(30, "James");
            
        }

        /*
         * seek to a tuple, insert another tuple using the btree api and then
         * verify that the current tuple / cursor position appears unchanged
         * from the perspective of the cursor.
         */
        {

            ITupleCursor2<String> cursor = newCursor(btree);

            // seek to a tuple and verify its state.
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.seek(20));

            // verify same state reported by tuple().
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.tuple());

            /*
             * insert a tuple before the current tuple (moves the current tuple
             * down by one).
             */
            btree.insert(15, "Paul");

            // verify the cursor position is unchanged.
            assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());

            // verify the current tuple state is unchanged.
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.tuple());

            // visit the prior tuple (the one we just inserted).
            assertEquals(new TestTuple<String>(15, "Paul"), cursor.prior());
            
            // verify the tuple state using tuple().
            assertEquals(new TestTuple<String>(15, "Paul"), cursor.tuple());

            /*
             * remove the current tuple (moves the successors of this tuple in
             * the leaf down by one).
             */
            btree.remove(15);

            // verify the tuple state - it should be null since the tuple for that key was just deleted.
            assertEquals(null, cursor.tuple());

            // verify the cursor position is unchanged.
            assertEquals(KeyBuilder.asSortKey(15),cursor.currentKey());

            // visit the next tuple.
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.next());

            // delete that tuple.
            btree.remove(20);
            
            // verify the tuple state - it should be null since the tuple for that key was just deleted.
            assertEquals(null, cursor.tuple());
            
            // verify the cursor position is unchanged.
            assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());
            
            // insert another tuple that is a successor of the deleted tuple.
            btree.insert(25, "Allen");

            // verify the cursor position is unchanged.
            assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());

            // verify the tuple state - still null since we have not repositioned the cursor.
            assertEquals(null, cursor.tuple());
            
            // advance the cursor and verify the tuple state
            assertEquals(new TestTuple<String>(25,"Allen"), cursor.next());

            // verify the tuple state using tuple().
            assertEquals(new TestTuple<String>(25,"Allen"), cursor.tuple());

        }

    }
     
    /**
     * Unit test for concurrent modification resulting from insert() and
     * remove() including (a) where the root leaf is split by the insert() and
     * (b) where remove() causes an underflow that triggers a join of the leaf
     * with its sibling forcing the underflow of the parent such that the leaf
     * then becomes the new root leaf.
     * <p>
     * Note: This test does not covert overflow where a tuple is rotated to the
     * sibling or underflow where a tuple is rotated from the sibling. Those
     * cases (and all cases involving deeper trees) are covered by the various
     * stress tests where a BTree is perturbed randomly and checked against
     * ground truth.
     */
    public void test_concurrent_modification_insert_split_root_leaf() {

        final BTree btree;
        {
       
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());

            // Note: at m=3 this example splits the root leaf).
            md.setBranchingFactor(3);
            
            btree = BTree.create(new SimpleMemoryRawStore(), md);

            btree.insert(10, "Bryan");
            btree.insert(20, "Mike");
            btree.insert(30, "James");
            
        }

        /*
         * seek to a tuple, insert another tuple using the btree api and then
         * verify that the current tuple / cursor position appears unchanged
         * from the perspective of the cursor.
         */
        {

            ITupleCursor2<String> cursor = newCursor(btree);

            // seek to a tuple and verify its state.
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.seek(20));

            // verify same state reported by tuple().
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.tuple());

            // insert a tuple before the current tuple (forces the leaf to be split).
            btree.insert(15, "Paul");

            // verify the cursor position is unchanged.
            assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());

            // verify the current tuple state is unchanged.
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.tuple());

            // visit the prior tuple (the one we just inserted).
            assertEquals(new TestTuple<String>(15, "Paul"), cursor.prior());
            
            // verify the tuple state using tuple().
            assertEquals(new TestTuple<String>(15, "Paul"), cursor.tuple());

            /*
             * remove the current tuple. This causes the leaf to underflow and
             * since the parent would then be deficient the leaf becomes the new
             * root leaf and its right sibling is discarded.
             */
            btree.remove(15);

            // verify the cursor position is unchanged.
            assertEquals(KeyBuilder.asSortKey(15),cursor.currentKey());

            // verify the tuple state - it should be null since the tuple for that key was just deleted.
            assertEquals(null, cursor.tuple());
            
            // visit the next tuple.
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.next());

            // delete that tuple.
            btree.remove(20);
            
            // verify the cursor position is unchanged.
            assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());

            // verify the tuple state - it should be null since the tuple for that key was just deleted.
            assertEquals(null, cursor.tuple());
            
            // insert another tuple that is a successor of the deleted tuple.
            btree.insert(25, "Allen");

            // verify the cursor position is unchanged.
            assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());

            // verify the tuple state - still null since we have not repositioned the cursor.
            assertEquals(null, cursor.tuple());
            
            // advance the cursor and verify the tuple state
            assertEquals(new TestTuple<String>(25,"Allen"), cursor.next());

            // verify the tuple state using tuple().
            assertEquals(new TestTuple<String>(25,"Allen"), cursor.tuple());

        }
        
    }

    /**
     * Unit test for copy-on-write (the leaf is clean and then an update,
     * insert, or remove is requested which forces copy-on-write to clone the
     * leaf). The listener needs to notice the event and relocate itself within
     * the BTree.
     */
    public void test_concurrent_modification_copy_on_write() {

        final BTree btree;
        {
       
            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());

            // Note: at m=3 this example splits the root leaf if anything is inserted.
            md.setBranchingFactor(3);
            
            btree = BTree.create(new SimpleMemoryRawStore(), md);

            btree.insert(10, "Bryan");
            btree.insert(20, "Mike");
            btree.insert(30, "James");
            
        }

        {

            final ITupleCursor2<String> cursor = newCursor(btree);

            /*
             * flush the btree to the store making all nodes clean. any mutation
             * will trigger copy on write.
             */
            assertTrue(btree.flush());

            assertTrue(cursor.hasNext());

            // visit the first tuple.
            assertEquals(new TestTuple<String>(10,"Bryan"),cursor.next());
            
            // remove that tuple from the index (triggers copy-on-write).
            btree.remove(10);
            
            // verify the cursor position is unchanged.
            assertEquals(KeyBuilder.asSortKey(10),cursor.currentKey());

            // verify the tuple state - still null since we have not repositioned the cursor.
            assertEquals(null, cursor.tuple());
            
            // visit the next tuple.
            assertEquals(new TestTuple<String>(20,"Mike"),cursor.next());

            // verify the cursor position.
            assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());

            /*
             * flush the btree again making all nodes clean.
             */
            assertTrue(btree.flush());

            // insert a tuple (triggers copy-on-write).
            btree.insert(10, "Bryan");
            
            // verify the cursor position is unchanged.
            assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());

            // verify the current tuple state.
            assertEquals(new TestTuple<String>(20,"Mike"),cursor.tuple());

            // visit the prior tuple (the one that we just inserted).
            assertEquals(new TestTuple<String>(10,"Bryan"),cursor.prior());
            
        }
        
    }

    /**
     * Test examines the behavior of the cursor when delete markers are enabled
     * and the cursor is willing to visited deleted tuples. Since delete markers
     * are enabled remove() will actually be an update that sets the delete
     * marker on the tuple and clears the value associated with that tuple.
     * Since deleted tuples are being visited by the cursor, the caller will see
     * the deleted tuples.
     */
    public void test_delete_markers_visitDeleted() {
        
        final BTree btree;
        {
       
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());

            // enable delete markers.
            md.setDeleteMarkers(true);
            
            btree = BTree.create(new SimpleMemoryRawStore(), md);

            btree.insert(10, "Bryan");
            btree.insert(20, "Mike");
            btree.insert(30, "James");
            
        }

        final int flags = IRangeQuery.DEFAULT | IRangeQuery.DELETED;
        
        /*
         * remove()
         */
        {

            ITupleCursor2<String> cursor = newCursor(btree, flags,
                    null/* fromKey */, null/* toKey */);

            assertEquals(new TestTuple<String>(flags, 10, "Bryan",
                    false/* deleted */, 0L/* timestamp */), cursor.seek(10));

            cursor.remove();

            assertEquals(new TestTuple<String>(flags, 10, null,
                    true/* deleted */, 0L/* timestamp */), cursor.tuple());

            assertTrue(cursor.hasNext());

            assertEquals(new TestTuple<String>(flags, 20, "Mike",
                    false/* deleted */, 0L/* timestamp */), cursor.next());

            assertTrue(cursor.hasPrior());

            assertEquals(new TestTuple<String>(flags, 10, null,
                    true/* deleted */, 0L/* timestamp */), cursor.prior());

        }
        
    }
    
    /**
     * Test examines the behavior of the cursor when delete markers are enabled
     * and the cursor is NOT willing to visited deleted tuples. Since delete
     * markers are enabled remove() will actually be an update that sets the
     * delete marker on the tuple and clears the value associated with that
     * tuple. Since deleted tuples are NOT being visited by the cursor, the
     * caller deleted tuples will appear to "disappear" just like they do when a
     * tuple is remove()'d from an index that does not support delete markers.
     */
    public void test_delete_markers_doNotVisitDeleted() {
        
        final BTree btree;
        {
       
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());

            // enable delete markers.
            md.setDeleteMarkers(true);

            btree = BTree.create(new SimpleMemoryRawStore(), md);

            btree.insert(10, "Bryan");
            btree.insert(20, "Mike");
            btree.insert(30, "James");
            
        }
        
        // Note: Cursor will NOT visit the deleted tuples.
        final int flags = IRangeQuery.DEFAULT;
        
        /*
         * remove()
         */
        {

            ITupleCursor2<String> cursor = newCursor(btree, flags,
                    null/* fromKey */, null/* toKey */);

            assertEquals(new TestTuple<String>(flags, 10, "Bryan",
                    false/* deleted */, 0L/* timestamp */), cursor.seek(10));

            cursor.remove();

            // tuple is no longer visitable since the delete flag was set.
            assertEquals(null, cursor.tuple());

            assertTrue(cursor.hasNext());

            assertEquals(new TestTuple<String>(flags, 20, "Mike",
                    false/* deleted */, 0L/* timestamp */), cursor.next());

            // the prior tuple is the one that we deleted so it is not visitable.
            assertFalse(cursor.hasPrior());

            try {
                cursor.prior();
                fail("Expecting " + NoSuchElementException.class);
            } catch (NoSuchElementException ex) {
                log.info("Ignoring expected exception: " + ex);
            }

        }
        
    }
    
    /**
     * Test verifies that an iterator which is scanning in forward order can be
     * exhausted when it reaches the end of the visitable tuples but that a
     * visitable tuple inserted after the current cursor position will be
     * noticed by {@link ITupleCursor#hasNext()} and visited.
     */
    public void test_hasNext_continues_after_insert() {
        
        final BTree btree;
        {
       
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());

            btree = BTree.create(new SimpleMemoryRawStore(), md);

            btree.insert(10, "Bryan");
            
        }

        {
            
            ITupleCursor2<String> cursor = newCursor(btree);
            
            assertTrue(cursor.hasNext());
            
            assertEquals(new TestTuple<String>(10,"Bryan"),cursor.next());
            
            assertFalse(cursor.hasNext());
            
            // insert after
            btree.insert(20, "Mike");
            
            assertEquals(new TestTuple<String>(10,"Bryan"),cursor.tuple());
            
            assertTrue(cursor.hasNext());
            
            assertEquals(new TestTuple<String>(20,"Mike"),cursor.next());
            
        }
        
    }
    
    /**
     * Test verifies that an iterator which is scanning in reverse order can be
     * exhausted when it reaches the end of the visitable tuples but that a
     * visitable tuple inserted after the current cursor position will be
     * noticed by {@link ITupleCursor#hasPrior()} and visited.
     */
    public void test_hasPrior_continues_after_insert() {
        
        final BTree btree;
        {
       
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());

            btree = BTree.create(new SimpleMemoryRawStore(), md);

            btree.insert(20, "Mike");
            
        }

        {
            
            ITupleCursor2<String> cursor = newCursor(btree);
            
            assertTrue(cursor.hasPrior());
            
            assertEquals(new TestTuple<String>(20,"Mike"),cursor.prior());
            
            assertFalse(cursor.hasPrior());
            
            // insert before.
            btree.insert(10, "Bryan");
            
            assertEquals(new TestTuple<String>(20,"Mike"),cursor.tuple());
            
            assertTrue(cursor.hasPrior());
            
            assertEquals(new TestTuple<String>(10,"Bryan"),cursor.prior());
            
        }
        
    }
    
}
