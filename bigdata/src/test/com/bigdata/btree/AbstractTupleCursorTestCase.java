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

import junit.framework.TestCase2;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Abstract base class for {@link ITupleCursor} test suites.
 * 
 * @todo also run tests against the FusedView and the scale-out federation
 *       variant (progressive forward or reverse scan against a partitioned
 *       index).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTupleCursorTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractTupleCursorTestCase() {
    }

    /**
     * @param arg0
     */
    public AbstractTupleCursorTestCase(String arg0) {
        super(arg0);
    }

    /**
     * Create an appropriate cursor instance for the given B+Tree.
     * 
     * @param btree
     * @param flags
     * @param fromKey
     * @param toKey
     * 
     * @return An {@link ITupleCursor} for that B+Tree.
     */
    abstract protected ITupleCursor2<String> newCursor(AbstractBTree btree, int flags,
            byte[] fromKey, byte[] toKey);
    
    /**
     * Create an appropriate cursor instance for the given B+Tree.
     * 
     * @param btree
     * 
     * @return
     */
    protected ITupleCursor2<String> newCursor(AbstractBTree btree) {

        return newCursor(btree, IRangeQuery.DEFAULT, null/* fromKey */, null/* toKey */);
        
    }

    /**
     * Return a B+Tree populated with data for
     * {@link #doBaseCaseTest(AbstractBTree)}.
     */
    protected BTree getBaseCaseBTree() {

        final BTree btree = BTree.create(new SimpleMemoryRawStore(),
                new IndexMetadata(UUID.randomUUID()));

        btree.insert(10, "Bryan");
        btree.insert(20, "Mike");
        btree.insert(30, "James");

        return btree;

    }
    
    /**
     * Test helper tests first(), last(), next(), prior(), and seek() given a
     * B+Tree that has been pre-populated with some known tuples.
     * 
     * @param btree
     *            The B+Tree.
     * 
     * @see #getBaseCaseBTree()
     */
    protected void doBaseCaseTest(final AbstractBTree btree) {

        // test first()
        {

            ITupleCursor2<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.first());

        }

        // test last()
        {

            ITupleCursor2<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            assertEquals(new TestTuple<String>(30, "James"), cursor.last());

        }

        // test tuple()
        {

            ITupleCursor2<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            // current tuple is initially not defined.
            assertNull(cursor.tuple());

            // defines the current tuple.
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.first());

            // same tuple.
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.tuple());

        }

        // test next()
        {

            ITupleCursor2<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            assertTrue(cursor.hasNext());

            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.next());

            assertTrue(cursor.hasNext());

            assertEquals(new TestTuple<String>(20, "Mike"), cursor.next());

            assertTrue(cursor.hasNext());

            assertEquals(new TestTuple<String>(30, "James"), cursor.next());

            // itr is exhausted.
            assertFalse(cursor.hasNext());

            // the cursor position is still defined.
            assertTrue(cursor.isCursorPositionDefined());
            
            // itr is exhausted.
            try {
                cursor.next();
                fail("Expecting " + NoSuchElementException.class);
            } catch (NoSuchElementException ex) {
                log.info("Ignoring expected exception: " + ex);
            }

            // make sure that the iterator will not restart.
            assertFalse(cursor.hasNext());
            
            // make sure that the iterator will not restart.
            try {
                cursor.next();
                fail("Expecting " + NoSuchElementException.class);
            } catch (NoSuchElementException ex) {
                log.info("Ignoring expected exception: " + ex);
            }
            
        }

        // test prior()
        {

            ITupleCursor2<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            assertTrue(cursor.hasPrior());

            assertEquals(new TestTuple<String>(30, "James"), cursor.prior());

            assertTrue(cursor.hasPrior());

            assertEquals(new TestTuple<String>(20, "Mike"), cursor.prior());

            assertTrue(cursor.hasPrior());

            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.prior());

            // itr is exhausted.
            assertFalse(cursor.hasPrior());

            // the cursor position is still defined.
            assertTrue(cursor.isCursorPositionDefined());

            // itr is exhausted.
            try {
                cursor.prior();
                fail("Expecting " + NoSuchElementException.class);
            } catch (NoSuchElementException ex) {
                log.info("Ignoring expected exception: " + ex);
            }

            // make sure that the iterator will not restart.
            assertFalse(cursor.hasPrior());

            // make sure that the iterator will not restart.
            try {
                cursor.prior();
                fail("Expecting " + NoSuchElementException.class);
            } catch (NoSuchElementException ex) {
                log.info("Ignoring expected exception: " + ex);
            }

        }

        /*
         * test seek(), including prior() and next() after a seek()
         */
        {

            final ITupleCursor<String> cursor = newCursor(btree,
                    IRangeQuery.DEFAULT, null/* fromKey */, null/* toKey */);

            // probe(30)
            assertEquals(new TestTuple<String>(30, "James"), cursor.seek(30));
            assertFalse(cursor.hasNext());
            assertTrue(cursor.hasPrior());
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.prior());

            // probe(10)
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.seek(10));
            assertFalse(cursor.hasPrior());
            assertTrue(cursor.hasNext());
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.next());

            // probe(20)
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.seek(20));
            assertTrue(cursor.hasNext());
            assertEquals(new TestTuple<String>(30, "James"), cursor.next());

            assertEquals(new TestTuple<String>(20, "Mike"), cursor.seek(20));
            assertTrue(cursor.hasPrior());
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.prior());

        }

        /*
         * test seek() when the probe key is not found / visitable.
         * 
         * this also tests prior() and next() after the seek to a probe key that
         * does not exist in the index.
         */
        {

            final ITupleCursor2<String> cursor = newCursor(btree);

            // seek to a probe key that does not exist.
            assertEquals(null, cursor.seek(29));
            assertEquals(null, cursor.tuple());
            assertEquals(KeyBuilder.asSortKey(29),cursor.currentKey());
            assertTrue(cursor.hasNext());
            assertEquals(new TestTuple<String>(30, "James"), cursor.next());
            assertEquals(new TestTuple<String>(30, "James"), cursor.tuple());
            assertEquals(KeyBuilder.asSortKey(30),cursor.currentKey());
            assertFalse(cursor.hasNext());
            assertTrue(cursor.hasPrior());
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.prior());
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.tuple());
            assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());

            // seek to a probe key that does not exist.
            assertEquals(null, cursor.seek(9));
            assertEquals(null, cursor.tuple());
            assertEquals(KeyBuilder.asSortKey(9),cursor.currentKey());
            assertTrue(cursor.hasNext());
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.next());
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.tuple());
            assertEquals(KeyBuilder.asSortKey(10),cursor.currentKey());
            assertFalse(cursor.hasPrior());
            assertTrue(cursor.hasNext());
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.next());
            assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());

            // seek to a probe key that does not exist and scan forward.
            assertEquals(null, cursor.seek(19));
            assertEquals(null, cursor.tuple());
            assertEquals(KeyBuilder.asSortKey(19),cursor.currentKey());
            assertTrue(cursor.hasNext());
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.next());
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.tuple());
            assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());
            assertTrue(cursor.hasNext());
            assertEquals(new TestTuple<String>(30, "James"), cursor.next());
            assertEquals(new TestTuple<String>(30, "James"), cursor.tuple());
            assertEquals(KeyBuilder.asSortKey(30),cursor.currentKey());

            // seek to a probe key that does not exist and scan backward.
            assertEquals(null, cursor.seek(19));
            assertEquals(null, cursor.tuple());
            assertEquals(KeyBuilder.asSortKey(19),cursor.currentKey());
            assertTrue(cursor.hasPrior());
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.prior());
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.tuple());
            assertEquals(KeyBuilder.asSortKey(10),cursor.currentKey());
            assertFalse(cursor.hasPrior());

            // seek to a probe key that does not exist (after all valid tuples).
            assertEquals(null, cursor.seek(31));
            assertEquals(null, cursor.tuple());
            assertTrue(cursor.isCursorPositionDefined());
            assertEquals(KeyBuilder.asSortKey(31),cursor.currentKey());
            assertFalse(cursor.hasNext());

            // seek to a probe key that does not exist (after all valid tuples).
            assertEquals(null, cursor.seek(31));
            assertEquals(null, cursor.tuple());
            assertTrue(cursor.isCursorPositionDefined());
            assertEquals(KeyBuilder.asSortKey(31),cursor.currentKey());
            assertTrue(cursor.hasPrior());
            assertEquals(new TestTuple<String>(30, "James"), cursor.prior());

        }

        /*
         * Test to verify that optional range constraints are correctly imposed,
         * including when the inclusive lower bound and the exclusive upper
         * bound correspond to tuples actually present in the B+Tree.
         */
        {

            /*
             * The inclusive lower bound (fromKey) is on a tuple that exists in
             * the B+Tree (the first tuple).
             * 
             * The exclusive upper bound (toKey) is on a tuple that exists and
             * which is the successor of the first tuple.
             * 
             * The cursor should only visit the first tuple.
             */
            {
             
                final byte[] fromKey = KeyBuilder.asSortKey(10);
                
                final byte[] toKey = KeyBuilder.asSortKey(20);

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertTrue(cursor.hasNext());
                
                assertEquals(new TestTuple<String>(10,"Bryan"),cursor.next());
                
                assertFalse(cursor.hasNext());

                // now seek to the last tuple.
                assertEquals(new TestTuple<String>(10,"Bryan"),cursor.last());

                assertFalse(cursor.hasNext());
                assertFalse(cursor.hasPrior());

                // accessible via seek()
                assertEquals(new TestTuple<String>(10,"Bryan"),cursor.seek(10));

                // not accessible via seek().
                try {
                    cursor.seek(20);
                    fail("Expecting: "+IllegalArgumentException.class);
                } catch(IllegalArgumentException ex) {
                    log.info("Ignoring expected exception: "+ex);
                }
                
            }
            
            /*
             * The inclusive lower bound (fromKey) is on a tuple that exists in
             * the B+Tree (the second tuple).
             * 
             * The exclusive upper bound (toKey) is on a tuple that exists in
             * the B+Tree (the third and last tuple).
             * 
             * The cursor should only visit the 2nd tuple.
             */
            {
             
                final byte[] fromKey = KeyBuilder.asSortKey(20);
                
                final byte[] toKey = KeyBuilder.asSortKey(30);

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertTrue(cursor.hasNext());
                
                assertEquals(new TestTuple<String>(20,"Mike"),cursor.next());
                
                assertFalse(cursor.hasNext());
                assertFalse(cursor.hasPrior());

                // now seek to the last tuple.
                assertEquals(new TestTuple<String>(20,"Mike"),cursor.last());

                assertFalse(cursor.hasNext());
                assertFalse(cursor.hasPrior());

                // accessible via seek()
                assertEquals(new TestTuple<String>(20,"Mike"),cursor.seek(20));

                // not accessible via seek().
                try {
                    cursor.seek(10);
                    fail("Expecting: "+IllegalArgumentException.class);
                } catch(IllegalArgumentException ex) {
                    log.info("Ignoring expected exception: "+ex);
                }
                
                // not accessible via seek().
                try {
                    cursor.seek(30);
                    fail("Expecting: "+IllegalArgumentException.class);
                } catch(IllegalArgumentException ex) {
                    log.info("Ignoring expected exception: "+ex);
                }
                
            }

            /*
             * Test when the toKey does not exist for reverse traversal.
             */
            {

                final byte[] fromKey = KeyBuilder.asSortKey(10);
                
                final byte[] toKey = KeyBuilder.asSortKey(19);

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                // seek to last and scan backward.
//                assertEquals(null, cursor.last());
//                assertEquals(null, cursor.tuple());
//                assertEquals(KeyBuilder.asSortKey(19),cursor.currentKey());
                assertTrue(cursor.hasPrior());
                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.prior());
                assertEquals(KeyBuilder.asSortKey(10),cursor.currentKey());
                assertFalse(cursor.hasPrior());

            }
            
            /*
             * Test when the toKey does not exist for reverse traversal.
             */
            {

                final byte[] fromKey = KeyBuilder.asSortKey(10);
                
                final byte[] toKey = KeyBuilder.asSortKey(29);

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertTrue(cursor.hasPrior());
                assertEquals(new TestTuple<String>(20, "Mike"), cursor.prior());
                assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());
                assertTrue(cursor.hasPrior());
                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.prior());
                assertEquals(KeyBuilder.asSortKey(10),cursor.currentKey());
                assertFalse(cursor.hasPrior());

            }
            
            /*
             * Test when the toKey does not exist for reverse traversal.
             */
            {

                final byte[] fromKey = KeyBuilder.asSortKey(10);
                
                final byte[] toKey = KeyBuilder.asSortKey(11);

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertTrue(cursor.hasPrior());
                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.prior());
                assertEquals(KeyBuilder.asSortKey(10),cursor.currentKey());
                assertFalse(cursor.hasPrior());

            }
            
        } // end test optional range constraints
        
    }

    /**
     * Return a B+Tree populated with data for
     * {@link #doReverseTraversalTest(AbstractBTree)}.
     * <p>
     * Note: this unit test is setup to create a B+Tree with 2 leaves and a root
     * node. This allows us to test the edge case where reverse traversal begins
     * with a key that is LTE the first key in the 2nd leaf.
     * <p>
     * Note: This unit test does not work for the {@link IndexSegment} because
     * the {@link IndexSegmentBuilder} will fill up each leaf in turn, so the
     * first leaf winds up with 3 tuples and the second with only 2 rather than
     * it being the other way around.
     */
    protected BTree getReverseTraversalBTree() {

        final IndexMetadata md = new IndexMetadata(UUID.randomUUID());

        md.setBranchingFactor(3);

        md.setIndexSegmentBranchingFactor(3);

        final BTree btree = BTree.create(new SimpleMemoryRawStore(), md);

        // leaf one.
        btree.insert(10, "Bryan");
        btree.insert(20, "Mike");
        // leaf two.
        btree.insert(30, "James");
        btree.insert(40, "Amy");
        btree.insert(50, "Mary");
//        btree.insert(60, "Karen");

        assertReverseTraversalData(btree);

        return btree;

    }

    /**
     * Verify the data expectations.
     * 
     * @todo Is this possible when the {@link IndexSegment} is generated since
     *       the plan can assign 3 tuples to the first leaf and two to the 2nd
     *       leaf.
     */
    private void assertReverseTraversalData(final AbstractBTree btree) {

        assertEquals("height", 1, btree.getHeight());
        assertEquals("nnodes", 1, btree.getNodeCount());
        assertEquals("nleaves", 2, btree.getLeafCount());
        assertEquals("ntuples", 5, btree.getEntryCount());

        // The separator key is (30).
        assertEquals(KeyBuilder.asSortKey(30), ((Node) btree.getRoot())
                .getKeys().get(0));
        
        // Verify the expected keys in the 1st leaf.
        AbstractBTreeTestCase.assertKeys(
                //
                new ReadOnlyKeysRaba(new byte[][] {//
                        KeyBuilder.asSortKey(10), //
                        KeyBuilder.asSortKey(20), //
                        }),//
                ((Node) btree.getRoot()).getChild(0/* 1st leaf */).getKeys());

        // Verify the expected keys in the 2nd leaf.
        AbstractBTreeTestCase.assertKeys(
                //
                new ReadOnlyKeysRaba(new byte[][] {//
                        KeyBuilder.asSortKey(30), //
                        KeyBuilder.asSortKey(40), //
                        KeyBuilder.asSortKey(50),//
                        }),//
                ((Node) btree.getRoot()).getChild(1/* 2nd leaf */).getKeys());

    }
    
    /**
     * Unit test for reverse traversal under a variety of edge cases. The data
     * is a B+Tree with two leaves
     * 
     * <pre>
     * (10,Bryan)
     * (20,Mike)
     * 
     * and
     * 
     * (30,James)
     * (40,Amy)
     * (50,Mary)
     * </pre>
     * 
     * This allows us to test the edge case where reverse traversal begins with
     * a key that is LTE the first key in the 2nd leaf.
     * 
     * @param btree
     */
    protected void doReverseTraversalTest(final AbstractBTree btree) {

        assertReverseTraversalData(btree);

        /*
         * Test when the toKey exists and is at index zero on the 2nd leaf. 
         */
        {

            final byte[] fromKey = KeyBuilder.asSortKey(10);
            
            final byte[] toKey = KeyBuilder.asSortKey(30);

            final ITupleCursor2<String> cursor = newCursor(btree,
                    IRangeQuery.DEFAULT, fromKey, toKey);

            assertTrue(cursor.hasPrior());
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.prior());
            assertEquals(KeyBuilder.asSortKey(20),cursor.currentKey());

        }

        /*
         * Test when the toKey does not exist for reverse traversal and the
         * insertion point returned by the search on the leaf is -2 for the
         * FIRST leaf.
         */
        {

            final byte[] fromKey = KeyBuilder.asSortKey(0);
            
            final byte[] toKey = KeyBuilder.asSortKey(19);

            final ITupleCursor2<String> cursor = newCursor(btree,
                    IRangeQuery.DEFAULT, fromKey, toKey);

            assertTrue(cursor.hasPrior());
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.prior());
            assertEquals(KeyBuilder.asSortKey(10),cursor.currentKey());
            assertFalse(cursor.hasPrior());

        }
        
        /*
         * Test when the toKey does not exist for reverse traversal and the
         * insertion point returned by the search on the leaf is -1 for the
         * FIRST leaf.
         */
        {

            final byte[] fromKey = KeyBuilder.asSortKey(0);
            
            final byte[] toKey = KeyBuilder.asSortKey(9);

            final ITupleCursor2<String> cursor = newCursor(btree,
                    IRangeQuery.DEFAULT, fromKey, toKey);

            assertFalse(cursor.hasPrior());

        }

        /*
         * Test when the toKey does not exist for reverse traversal and the
         * insertion point returned by search on the 2nd leaf is -1, which
         * corresponds to index 0 when it is converted to an index position in
         * the leaf. In this case the cursor should be adjusted to the last
         * tuple in the prior leaf.
         * 
         * Note: This condition can not arise if the separator key in the parent
         * is the first tuple in the rightSibling. This is normally the case.
         * However, if delete markers are not enabled then we can delete the
         * first tuple in the 2nd leaf, at which point the separatorKey no
         * longer corresponds to the first tuple in that leaf.
         */
        if (!btree.isReadOnly() && !btree.getIndexMetadata().getDeleteMarkers()) {

            /*
             * Verify that the separatorKey in the parent is the first tuple we
             * expect to find in the 2nd leaf.
             */
            assertEquals(KeyBuilder.asSortKey(30), ((Node) btree.getRoot())
                    .getKeys().get(0));

            /*
             * Modify the B+Tree such that (30) is still the separatorKey for
             * the two leaves, so anything GTE (30) is directed to the 2nd leaf.
             * However, the key (30) is no longer found in the B+Tree (not even
             * as a deleted tuple).
             */
            
            // Remove the first tuple in the 2nd leaf.
            btree.remove(30);
            // The separator key has not been changed.
            assertEquals(((Node) btree.getRoot()).getKeys().get(0), KeyBuilder
                    .asSortKey(30));
            // The #of leaves has not been changed.
            assertEquals(2, btree.getLeafCount());
            // Verify the expected keys in the 2nd leaf.
            AbstractBTreeTestCase.assertKeys(//
                    new ReadOnlyKeysRaba(new byte[][]{//
                            KeyBuilder.asSortKey(40),//
                            KeyBuilder.asSortKey(50),//
                            }),//
                    ((Node) btree.getRoot()).getChild(1/*2nd leaf*/).getKeys());
            
            final byte[] fromKey = KeyBuilder.asSortKey(10);

            // search for the tuple we just deleted from the 2nd leaf.
            final byte[] toKey = KeyBuilder.asSortKey(30);

            final ITupleCursor2<String> cursor = newCursor(btree,
                    IRangeQuery.DEFAULT, fromKey, toKey);

            assertTrue(cursor.hasPrior());
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.prior());
            assertEquals(KeyBuilder.asSortKey(20), cursor.currentKey());
            assertTrue(cursor.hasPrior());

        }

    }
    
    /**
     * Test helper tests for fence posts when the index is empty
     * <p>
     * Note: this test can not be written for an {@link IndexSegment} since you
     * can't have an empty {@link IndexSegment}.
     * 
     * @param btree
     *            An empty B+Tree.
     */
    protected void doEmptyIndexTest(final AbstractBTree btree) {

        /*
         * Test with no range limits.
         */
        {

            // first()
            {

                final ITupleCursor2<String> cursor = newCursor(btree);

                assertNull(cursor.first());

                // since there was nothing visitable the cursor position NOT
                // defined
                assertFalse(cursor.isCursorPositionDefined());

                // no current key.
                assertEquals(null, cursor.currentKey());

                assertNull(cursor.tuple());

                assertFalse(cursor.hasNext());

                assertFalse(cursor.hasPrior());

            }

            // last()
            {

                final ITupleCursor2<String> cursor = newCursor(btree);

                assertNull(cursor.last());

                // since there was nothing visitable the cursor position NOT
                // defined
                assertFalse(cursor.isCursorPositionDefined());

                // no current key.
                assertEquals(null, cursor.currentKey());

                assertNull(cursor.tuple());

                assertFalse(cursor.hasNext());

                assertFalse(cursor.hasPrior());

            }

            // tuple()
            {

                final ITupleCursor2<String> cursor = newCursor(btree);

                assertNull(cursor.tuple());

            }

            // hasNext(), next().
            {

                final ITupleCursor<String> cursor = newCursor(btree);

                assertFalse(cursor.hasNext());

                try {
                    cursor.next();
                    fail("Expecting " + NoSuchElementException.class);
                } catch (NoSuchElementException ex) {
                    log.info("Ignoring expected exception: " + ex);
                }

            }

            // hasPrior(), prior().
            {

                final ITupleCursor<String> cursor = newCursor(btree);

                assertFalse(cursor.hasPrior());

                try {
                    cursor.prior();
                    fail("Expecting " + NoSuchElementException.class);
                } catch (NoSuchElementException ex) {
                    log.info("Ignoring expected exception: " + ex);
                }

            }

            // seek()
            {

                final ITupleCursor<String> cursor = newCursor(btree);

                assertNull(cursor.seek(1));

                assertFalse(cursor.hasPrior());

                assertFalse(cursor.hasNext());

            }

        }

        /*
         * Test with range limit. Since there is no data in the index the actual
         * range limits imposed matter very little.
         */
        {

            final byte[] fromKey = KeyBuilder.asSortKey(2);
            
            final byte[] toKey = KeyBuilder.asSortKey(7);
            
            // first()
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertNull(cursor.first());

                // since there was nothing visitable the cursor position NOT
                // defined
                assertFalse(cursor.isCursorPositionDefined());

                // no current key.
                assertEquals(null, cursor.currentKey());

                assertNull(cursor.tuple());

                assertFalse(cursor.hasNext());

                assertFalse(cursor.hasPrior());

            }

            // last()
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertNull(cursor.last());

                // since there was nothing visitable the cursor position NOT
                // defined
                assertFalse(cursor.isCursorPositionDefined());

                // no current key.
                assertEquals(null, cursor.currentKey());

                assertNull(cursor.tuple());

                assertFalse(cursor.hasNext());

                assertFalse(cursor.hasPrior());

            }

            // tuple()
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertNull(cursor.tuple());

            }

            // hasNext(), next().
            {

                final ITupleCursor<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertFalse(cursor.hasNext());

                try {
                    cursor.next();
                    fail("Expecting " + NoSuchElementException.class);
                } catch (NoSuchElementException ex) {
                    log.info("Ignoring expected exception: " + ex);
                }

            }

            // hasPrior(), prior().
            {

                final ITupleCursor<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertFalse(cursor.hasPrior());

                try {
                    cursor.prior();
                    fail("Expecting " + NoSuchElementException.class);
                } catch (NoSuchElementException ex) {
                    log.info("Ignoring expected exception: " + ex);
                }

            }

            // seek()
            {

                final ITupleCursor<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertNull(cursor.seek(4));

                assertFalse(cursor.hasPrior());

                assertFalse(cursor.hasNext());

            }

        }

    }

    /**
     * Creates, populates and returns a {@link BTree} for
     * {@link #doOneTupleTest(AbstractBTree)}
     */
    protected BTree getOneTupleBTree() {

        final BTree btree = BTree.create(new SimpleMemoryRawStore(),
                new IndexMetadata(UUID.randomUUID()));

        btree.insert(10, "Bryan");

        return btree;

    }
    
    /**
     * Test helper for fence posts when there is only a single tuple. including
     * when attempting to visit tuples in a key range that does not overlap with
     * the tuple that is actually in the index.
     * 
     * @param btree
     */
    protected void doOneTupleTest(AbstractBTree btree) {

        /*
         * Test with no range limits.
         */
        {
            
            // first()
            {

                final ITupleCursor2<String> cursor = newCursor(btree);

                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.first());

                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.tuple());

                assertFalse(cursor.hasNext());

                assertFalse(cursor.hasPrior());

            }

            // last()
            {

                final ITupleCursor2<String> cursor = newCursor(btree);

                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.last());

                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.tuple());

                assertFalse(cursor.hasNext());

                assertFalse(cursor.hasPrior());

            }

            // tuple()
            {

                final ITupleCursor2<String> cursor = newCursor(btree);

                assertNull(cursor.tuple());

            }

            // hasNext(), next().
            {

                final ITupleCursor2<String> cursor = newCursor(btree);

                assertTrue(cursor.hasNext());

                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.next());

                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.tuple());

                assertFalse(cursor.hasNext());

                try {
                    cursor.next();
                    fail("Expecting " + NoSuchElementException.class);
                } catch (NoSuchElementException ex) {
                    log.info("Ignoring expected exception: " + ex);
                }

            }

            // hasPrior(), prior().
            {

                final ITupleCursor2<String> cursor = newCursor(btree);

                assertTrue(cursor.hasPrior());

                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.prior());

                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.tuple());

                assertFalse(cursor.hasPrior());

                try {
                    cursor.prior();
                    fail("Expecting " + NoSuchElementException.class);
                } catch (NoSuchElementException ex) {
                    log.info("Ignoring expected exception: " + ex);
                }

            }

            // seek() (found)
            {

                final ITupleCursor2<String> cursor = newCursor(btree);

                assertEquals(new TestTuple<String>(10, "Bryan"), cursor
                        .seek(10));

                assertFalse(cursor.hasPrior());

                assertFalse(cursor.hasNext());

            }

            // seek() (not found before a valid tuple)
            {

                final ITupleCursor2<String> cursor = newCursor(btree);

                assertNull(cursor.seek(1));

                assertEquals(KeyBuilder.asSortKey(1), cursor.currentKey());

                assertFalse(cursor.hasPrior());

                assertTrue(cursor.hasNext());

                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.next());

            }

            // seek() (not found after a valid tuple)
            {

                final ITupleCursor2<String> cursor = newCursor(btree);

                assertNull(cursor.seek(11));

                assertTrue(cursor.hasPrior());

                assertFalse(cursor.hasNext());

                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.prior());

            }

        }

        /*
         * Now use a cursor whose key-range constraint does not overlap the
         * tuple (the cursor is constrained to only visit tuples that are
         * ordered BEFORE the sole tuple actually present in the index).
         */
        {
            
            final byte[] fromKey = KeyBuilder.asSortKey(5);

            final byte[] toKey = KeyBuilder.asSortKey(9);
            
            // first()
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertEquals(null, cursor.first());

                // since there was nothing visitable the cursor position NOT defined 
                assertFalse(cursor.isCursorPositionDefined());
                
                // no current key.
                assertEquals(null,cursor.currentKey());
                
                assertFalse(cursor.hasNext());

                assertFalse(cursor.hasPrior());

            }

            // last()
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertEquals(null, cursor.last());

                // since there was nothing visitable the cursor position NOT defined 
                assertFalse(cursor.isCursorPositionDefined());
                
                // no current key.
                assertEquals(null,cursor.currentKey());
                
                assertFalse(cursor.hasNext());

                assertFalse(cursor.hasPrior());

            }

            // tuple()
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertNull(cursor.tuple());

            }

            // hasNext(), next().
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertFalse(cursor.hasNext());

                try {
                    cursor.next();
                    fail("Expecting " + NoSuchElementException.class);
                } catch (NoSuchElementException ex) {
                    log.info("Ignoring expected exception: " + ex);
                }

            }

            // hasPrior(), prior().
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertFalse(cursor.hasPrior());

                try {
                    cursor.prior();
                    fail("Expecting " + NoSuchElementException.class);
                } catch (NoSuchElementException ex) {
                    log.info("Ignoring expected exception: " + ex);
                }

            }

            // seek() (not found)
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertNull(cursor.seek(7));

                assertEquals(KeyBuilder.asSortKey(7), cursor.currentKey());

                assertFalse(cursor.hasPrior());

                assertFalse(cursor.hasNext());

            }

        }
        
        /*
         * Now use a cursor whose key-range constraint does not overlap the
         * tuple (the cursor is constrained to only visit tuples that are
         * ordered AFTER the sole tuple actually present in the index).
         */
        {
            
            final byte[] fromKey = KeyBuilder.asSortKey(15);

            final byte[] toKey = KeyBuilder.asSortKey(19);
            
            // first()
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertEquals(null, cursor.first());

                assertFalse(cursor.hasNext());

                assertFalse(cursor.hasPrior());

            }

            // last()
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertEquals(null, cursor.last());

                assertFalse(cursor.hasNext());

                assertFalse(cursor.hasPrior());

            }

            // tuple()
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertNull(cursor.tuple());

            }

            // hasNext(), next().
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertFalse(cursor.hasNext());

                try {
                    cursor.next();
                    fail("Expecting " + NoSuchElementException.class);
                } catch (NoSuchElementException ex) {
                    log.info("Ignoring expected exception: " + ex);
                }

            }

            // hasPrior(), prior().
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertFalse(cursor.hasPrior());

                try {
                    cursor.prior();
                    fail("Expecting " + NoSuchElementException.class);
                } catch (NoSuchElementException ex) {
                    log.info("Ignoring expected exception: " + ex);
                }

            }

            // seek() (not found)
            {

                final ITupleCursor2<String> cursor = newCursor(btree,
                        IRangeQuery.DEFAULT, fromKey, toKey);

                assertNull(cursor.seek(17));

                assertEquals(KeyBuilder.asSortKey(17), cursor.currentKey());

                assertFalse(cursor.hasPrior());

                assertFalse(cursor.hasNext());

            }

        }
        
    }

    /**
     * Compares two tuples for equality based on their data (flags, keys,
     * values, deleted marker, and version timestamp).
     * <p>
     * Note: This will fail if you apply it to tuples reported by
     * {@link ITupleIterator}s whose DELETE flag was different since it verifies
     * the DELETE flag state and that is a property of the iterator NOT the
     * tuple. Whether or not a tuple is deleted is detected using
     * {@link ITuple#isDeletedVersion()}.
     * 
     * @param expected
     * @param actual
     */
    public static void assertEquals(ITuple expected, ITuple actual) {

        if (expected == null) {

            assertNull("Expecting a null tuple", actual);

            return;

        } else {

            assertNotNull("Not expecting a null tuple", actual);

        }

        assertEquals("flags.KEYS",
                ((expected.flags() & IRangeQuery.KEYS) != 0),
                ((actual.flags() & IRangeQuery.KEYS) != 0));

        assertEquals("flags.VALS",
                ((expected.flags() & IRangeQuery.VALS) != 0),
                ((actual.flags() & IRangeQuery.VALS) != 0));

        assertEquals("flags.DELETED",//
                ((expected.flags() & IRangeQuery.DELETED) != 0),//
                ((actual.flags() & IRangeQuery.DELETED) != 0));

        assertEquals("flags.REMOVEALL",//
                ((expected.flags() & IRangeQuery.REMOVEALL) != 0),//
                ((actual.flags() & IRangeQuery.REMOVEALL) != 0));

        assertEquals("flags.CURSOR",//
                ((expected.flags() & IRangeQuery.CURSOR) != 0),//
                ((actual.flags() & IRangeQuery.CURSOR) != 0));
        
        assertEquals("flags.REVERSE",//
                ((expected.flags() & IRangeQuery.REVERSE) != 0),//
                ((actual.flags() & IRangeQuery.REVERSE) != 0));
        
        assertEquals("flags.READONLY",//
                ((expected.flags() & IRangeQuery.READONLY) != 0),//
                ((actual.flags() & IRangeQuery.READONLY) != 0));
        
        assertEquals("flags.PARALLEL",//
                ((expected.flags() & IRangeQuery.PARALLEL) != 0),//
                ((actual.flags() & IRangeQuery.PARALLEL) != 0));

        assertEquals("flags", expected.flags(), actual.flags());

        assertEquals("key", expected.getKey(), actual.getKey());

        assertEquals("deleted", expected.isDeletedVersion(), actual
                .isDeletedVersion());

        if (!expected.isDeletedVersion()) {

            assertEquals("val", expected.getValue(), actual.getValue());

        }

        assertEquals("isNull", expected.isNull(), actual.isNull());

        assertEquals("timestamp", expected.getVersionTimestamp(), actual
                .getVersionTimestamp());

    }

}
