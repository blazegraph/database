/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

package com.bigdata.btree.filter;

import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.AbstractBTreeTupleCursor.ReadOnlyBTreeTupleCursor;
import com.bigdata.btree.AbstractTupleCursorTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleCursor2;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.TestTuple;
import com.bigdata.btree.Tuple;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for the {@link Reverserator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestReverserator extends AbstractTupleCursorTestCase {

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

    @Override
    protected ITupleCursor2<String> newCursor(AbstractBTree btree, int flags,
            byte[] fromKey, byte[] toKey) {
        
        return new ReadOnlyBTreeTupleCursor<String>((BTree) btree,
                new Tuple<String>(btree, IRangeQuery.DEFAULT),
                fromKey, toKey);
        
    }

    /**
     * @todo add tests for the rest of the {@link ITupleCursor} API since the
     *       {@link Reverserator} implements the full {@link ITupleCursor} API.
     */
    public void test_reverse_traversal() {

        BTree btree = BTree.create(new SimpleMemoryRawStore(), new IndexMetadata(
                UUID.randomUUID()));

        btree.insert(10, "Bryan");
        btree.insert(20, "Mike");
        btree.insert(30, "James");

        // test reverse iterator, including linked state with cursor.
        {

            final ITupleCursor2<String> cursor = newCursor(btree, IRangeQuery.DEFAULT,
                    null/* fromKey */, null/* toKey */);

            final ITupleIterator<String> itr = new Reverserator<String>(cursor);

//            assertEquals(null, cursor.tuple());

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

    /**
	 * Stress test written in an attempt to find a spin lock in the reverse
	 * cursor traversal.
	 * <p>
	 * Note: This test was in the jetty branch. I have picked it up and copied
	 * it into the master. The version in the master should be preserved.
	 * 
	 * @see <a href="http://trac.blazegraph.com/ticket/1078"> Possible tight loop
	 *      in cursor.prior() </a>
	 */
    public void test_reverse_with_branching_factor() {

		final Random r = new Random();

		for (int bf = 32; bf < 1024; bf += r.nextInt(32)) {
			
			final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
			
			metadata.setBranchingFactor(bf);

			final BTree btree = BTree.create(new SimpleMemoryRawStore(), metadata);

			long ntuples = 0L;
			
			for (int i = 1; i < 2000; i++) {

				final String key = "key" + r.nextInt();
				if (btree.contains(key))
					continue;

				btree.insert(key, "value" + r.nextInt());
				ntuples++;
				
				final ITupleCursor2<String> cursor = newCursor(btree,
						IRangeQuery.DEFAULT, null/* fromKey */, null/* toKey */);

				final ITupleIterator<String> itr = new Reverserator<String>(
						cursor);

				long count = 0;

				while (itr.hasNext()) {
					itr.next();
					count++;
				}

				assertEquals(count, ntuples);
			}
		}

    }

}
