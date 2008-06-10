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

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.rawstore.IBlock;

/**
 * Abstract base class for {@link ITupleCursor} test suites.
 * 
 * @todo run tests against read-only BTree as well as against mutable BTree and
 *       mutable FusedView and the scale-out federation variant (progressive
 *       forward or reverse scan against a partitioned index).
 * 
 * @todo write test of remove() for mutable variants.
 * 
 * @todo write test of concurrent removal for mutable variants.
 * 
 * @todo unit test that verifies that the tuple exposed by the cursor will
 *       appear to be deleted if the corresponding tuple is deleted from the
 *       index (mutable BTree and FusedView only).
 * 
 * @todo unit tests to verify that the optional constraints on the key-range for
 *       the cursor are correctly imposed. E.g., #first() or #next() must not
 *       visit a tuple that lies outside of the allowable key range for the
 *       cursor.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractCursorTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractCursorTestCase() {
    }

    /**
     * @param arg0
     */
    public AbstractCursorTestCase(String arg0) {
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
    abstract protected ITupleCursor<String> newCursor(AbstractBTree btree, int flags,
            byte[] fromKey, byte[] toKey);
    
    /**
     * Create an appropriate cursor instance for the given B+Tree.
     * 
     * @param btree
     * 
     * @return
     */
    protected ITupleCursor<String> newCursor(AbstractBTree btree) {

        return newCursor(btree, IRangeQuery.DEFAULT, null/* fromKey */, null/* toKey */);
        
    }
    
    /**
     * Return a B+Tree populated with data for
     * {@link #doBaseCaseTest(IndexSegment)}
     */
    protected BTree getBaseCaseBTree() {

        BTree btree = BTree.create(new TemporaryRawStore(), new IndexMetadata(
                UUID.randomUUID()));

        btree.insert(10, "Bryan");
        btree.insert(20, "Mike");
        btree.insert(30, "James");

        return btree;

    }

    /**
     * Test helper tests first(), last(), next(), prior(), and seek() given a
     * B+Tree that has been pre-popluated with some known tuples.
     * 
     * @param btree
     *            The B+Tree.
     * 
     * @see #getBaseCaseBTree()
     * 
     * @todo test variant with a key-range constraint on the index partition.
     * 
     * @todo test variant using delete markers.
     */
    protected void doBaseCaseTest(AbstractBTree btree) {

        // test first()
        {

            ITupleCursor<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.first());

        }

        // test last()
        {

            ITupleCursor<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            assertEquals(new TestTuple<String>(30, "James"), cursor.last());

        }

        // test tuple()
        {

            ITupleCursor<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            // current tuple is initially not defined.
            assertNull(cursor.tuple());

            // defines the current tuple.
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.first());

            // same tuple.
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.tuple());

        }

        // test next()
        if (true) {

            ITupleCursor<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            assertTrue(cursor.hasNext());

            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.next());

            assertTrue(cursor.hasNext());

            assertEquals(new TestTuple<String>(20, "Mike"), cursor.next());

            assertTrue(cursor.hasNext());

            assertEquals(new TestTuple<String>(30, "James"), cursor.next());

            assertFalse(cursor.hasNext());

            try {
                cursor.next();
                fail("Expecting " + NoSuchElementException.class);
            } catch (NoSuchElementException ex) {
                log.info("Ignoring expected exception: " + ex);
            }

        }

        // test prior()
        if (true) {

            ITupleCursor<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            assertTrue(cursor.hasPrior());

            assertEquals(new TestTuple<String>(30, "James"), cursor.prior());

            assertTrue(cursor.hasPrior());

            assertEquals(new TestTuple<String>(20, "Mike"), cursor.prior());

            assertTrue(cursor.hasPrior());

            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.prior());

            assertFalse(cursor.hasPrior());

            try {
                cursor.prior();
                fail("Expecting " + NoSuchElementException.class);
            } catch (NoSuchElementException ex) {
                log.info("Ignoring expected exception: " + ex);
            }

        }

        // test reverse iterator, including linked state with cursor.
        {

            ITupleCursor<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            ITupleIterator<String> itr = cursor.asReverseIterator();

            assertEquals(null, cursor.tuple());

            assertTrue(itr.hasNext());

            assertEquals(new TestTuple<String>(30, "James"), itr.next());
            assertEquals(new TestTuple<String>(30, "James"), cursor.tuple());

            assertTrue(itr.hasNext());

            assertEquals(new TestTuple<String>(20, "Mike"), itr.next());
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.tuple());

            assertTrue(itr.hasNext());

            assertEquals(new TestTuple<String>(10, "Bryan"), itr.next());
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.tuple());

            assertFalse(itr.hasNext());

            try {
                itr.next();
                fail("Expecting " + NoSuchElementException.class);
            } catch (NoSuchElementException ex) {
                log.info("Ignoring expected exception: " + ex);
            }

        }

        /*
         * test seek(), including prior() and next() after a seek()
         */
        {

            ITupleCursor<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

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
         * test seek(), prior(), and next() when the probe key is not found /
         * visitable and when there is no successor of a probe key not found in
         * the index.
         */
        {

            ITupleCursor<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            // seek finds the visitable successor of the probe key.
            assertEquals(null, cursor.seek(29));
            assertEquals(new TestTuple<String>(30, "James"), cursor.tuple());
            assertFalse(cursor.hasNext());
            assertTrue(cursor.hasPrior());
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.prior());

            // seek finds the visitable successor of the probe key.
            assertEquals(null, cursor.seek(9));
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.tuple());
            assertFalse(cursor.hasPrior());
            assertTrue(cursor.hasNext());
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.next());

            // seek finds the visitable successor of the probe key.
            assertEquals(null, cursor.seek(19));
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.tuple());
            assertTrue(cursor.hasNext());
            assertEquals(new TestTuple<String>(30, "James"), cursor.next());

            assertEquals(null, cursor.seek(19));
            assertEquals(new TestTuple<String>(20, "Mike"), cursor.tuple());
            assertTrue(cursor.hasPrior());
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.prior());

            // seek finds no successor for the probe key.
            assertEquals(null, cursor.seek(31));
            assertEquals(null, cursor.tuple());
            assertFalse(cursor.isCursorPositionDefined());
            // Note: iterator starts from the start of the key range since
            // cursor position is not defined!
            assertTrue(cursor.hasNext());
            assertEquals(new TestTuple<String>(10, "Bryan"), cursor.next());

            // seek finds no successor for the probe key.
            assertEquals(null, cursor.seek(31));
            assertEquals(null, cursor.tuple());
            assertFalse(cursor.isCursorPositionDefined());
            // Note: starts from the end of the key range since cursor position
            // is not defined!
            assertTrue(cursor.hasPrior());
            assertEquals(new TestTuple<String>(30, "James"), cursor.prior());

        }

    }

    /**
     * Test helper tests for fence posts when the index is empty
     * <p>
     * Note: this test can not be written for an {@link IndexSegment} since you
     * can't have an empty {@link IndexSegment}.
     * 
     * @param btree
     * 
     * @todo also test with key-range limits when the index is empty.
     * 
     * @todo also test with a key-range that does not overlap the data actually
     *       present in a non-empty index.
     */
    protected void doEmptyIndexTest(AbstractBTree btree) {
        
        // first()
        {
         
            final ITupleCursor<String> cursor = newCursor(btree);
            
            assertNull(cursor.first());
            
            assertFalse(cursor.isCursorPositionDefined());

            assertNull(cursor.tuple());
            
            assertFalse(cursor.hasNext());

            assertFalse(cursor.hasPrior());
            
        }
        
        // last()
        {
            
            final ITupleCursor<String> cursor = newCursor(btree);
            
            assertNull(cursor.last());
            
            assertFalse(cursor.isCursorPositionDefined());

            assertNull(cursor.tuple());
            
            assertFalse(cursor.hasNext());

            assertFalse(cursor.hasPrior());

        }

        // tuple()
        {

            final ITupleCursor<String> cursor = newCursor(btree);

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

    /**
     * @todo write test for fence posts when there is only a single tuple
     *       including when attempting to visit tuples in a key range that does
     *       not overlap with the tuple that is actually in the index.
     */
    public void test_oneTuple() {

        fail("write test");

    }

    /**
     * Compares two tuples for equality based on their data (flags, keys,
     * values, deleted marker, and version timestamp).
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

        assertEquals("flags.DELETED",
                ((expected.flags() & IRangeQuery.DELETED) != 0), ((actual
                        .flags() & IRangeQuery.DELETED) != 0));

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

    /**
     * Test helper for a tuple with static data.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    static class TestTuple<E> implements ITuple<E> {

        private final int flags;

        private final byte[] key;

        private final byte[] val;

        private final boolean deleted;

        final private long timestamp;

        public TestTuple(Object key, E value) {

            this(IRangeQuery.DEFAULT, key, value);

        }

        public TestTuple(int flags, Object key, E value) {

            this(flags, key, value, false/* deleted */, 0L/* timestamp */);

        }

        public TestTuple(int flags, Object key, E val, boolean deleted,
                long timestamp) {

            this.flags = flags;

            this.key = KeyBuilder.asSortKey(key);

            this.val = SerializerUtil.serialize(val);

            this.deleted = deleted;

            this.timestamp = timestamp;

        }

        public int flags() {

            return flags;

        }

        public byte[] getKey() {

            return key;

        }

        public ByteArrayBuffer getKeyBuffer() {

            return new ByteArrayBuffer(0, key.length, key);

        }

        public DataInputBuffer getKeyStream() {

            return new DataInputBuffer(key);

        }

        public boolean getKeysRequested() {

            return ((flags & IRangeQuery.KEYS) != 0);

        }

        @SuppressWarnings("unchecked")
        public E getObject() {

            return (E) SerializerUtil.deserialize(val);

        }

        public int getSourceIndex() {
            // TODO Auto-generated method stub
            return 0;
        }

        public byte[] getValue() {
            return val;
        }

        public ByteArrayBuffer getValueBuffer() {

            if (val == null)
                throw new UnsupportedOperationException();

            return new ByteArrayBuffer(0, val.length, val);

        }

        public DataInputBuffer getValueStream() {

            return new DataInputBuffer(val);

        }

        public boolean getValuesRequested() {

            return ((flags & IRangeQuery.VALS) != 0);

        }

        public long getVersionTimestamp() {
            return timestamp;
        }

        public long getVisitCount() {
            // TODO Auto-generated method stub
            return 0;
        }

        public boolean isDeletedVersion() {
            return deleted;
        }

        public boolean isNull() {
            return val == null;
        }

        public IBlock readBlock(long addr) {
            // TODO Auto-generated method stub
            return null;
        }

    }

}
