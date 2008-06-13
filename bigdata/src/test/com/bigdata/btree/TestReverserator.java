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
 * Created on Jun 11, 2008
 */

package com.bigdata.btree;

import java.util.NoSuchElementException;
import java.util.UUID;

import com.bigdata.btree.AbstractBTreeTupleCursor.ReadOnlyBTreeTupleCursor;
import com.bigdata.btree.AbstractBTreeTupleCursor.Reverserator;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for the {@link Reverserator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReverserator extends AbstractCursorTestCase {

    /**
     * 
     */
    public TestReverserator() {
    }

    /**
     * @param arg0
     */
    public TestReverserator(String arg0) {
        super(arg0);
    }

    protected ITupleCursor<String> newCursor(AbstractBTree btree, int flags,
            byte[] fromKey, byte[] toKey) {
        
        return new ReadOnlyBTreeTupleCursor<String>((BTree) btree,
                new Tuple<String>(btree, IRangeQuery.DEFAULT),
                fromKey, toKey);
        
    }

    public void test_reverse_traversal() {

        BTree btree = BTree.create(new SimpleMemoryRawStore(), new IndexMetadata(
                UUID.randomUUID()));

        btree.insert(10, "Bryan");
        btree.insert(20, "Mike");
        btree.insert(30, "James");

        // test reverse iterator, including linked state with cursor.
        {

            ITupleCursor<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            ITupleIterator<String> itr = new Reverserator(cursor);

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

            // exhausted.
            assertFalse(itr.hasNext());

            // exhausted.
            try {
                itr.next();
                fail("Expecting " + NoSuchElementException.class);
            } catch (NoSuchElementException ex) {
                log.info("Ignoring expected exception: " + ex);
            }

            // make sure itr will not restart.
            assertFalse(itr.hasNext());

            // make sure itr will not restart.
            try {
                itr.next();
                fail("Expecting " + NoSuchElementException.class);
            } catch (NoSuchElementException ex) {
                log.info("Ignoring expected exception: " + ex);
            }

        }

    }

}
