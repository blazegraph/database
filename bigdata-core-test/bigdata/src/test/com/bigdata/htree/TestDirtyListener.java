/**

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
 * Created on Oct 8, 2007
 */

package com.bigdata.htree;

import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.IDirtyListener;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for the {@link IDirtyListener} protocol.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDirtyListener extends AbstractHTreeTestCase {

	/**
     * 
     */
	public TestDirtyListener() {
		super();
	}

	/**
	 * @param name
	 */
	public TestDirtyListener(String name) {
		super(name);
	}

	/**
	 * Test exercises the various ways in which {@link HTree#fireDirtyEvent()}
	 * should and should not be triggered.
	 * <p>
	 * Note: One of the things that this is testing is that we do not fire dirty
	 * events when the tree is already dirty. This is unlikely to be harmful
	 * since the {@link IDirtyListener} is primarily used to build up a commit
	 * list but it does not add any information since we just want to know when
	 * a htree becomes dirty so that we can make sure that it will participate
	 * in a commit.
	 */
	public void test_dirtyListener1() {

		// some keys
		final byte[] k1 = new byte[]{1};
		final byte[] k2 = new byte[]{2};
		final byte[] k3 = new byte[]{3};
		final byte[] k4 = new byte[]{4};
		final byte[] k5 = new byte[]{5};
		final byte[] k6 = new byte[]{6};
		
		// a value.
		final byte[] val = new byte[]{};

		/*
		 * Create a new btree.
		 */
		final long addr1;
		final long addr2;
		final IRawStore store = new SimpleMemoryRawStore();
		try {
			{

				// helper class listens for dirty events.
				final MyDirtyListener listener = new MyDirtyListener();

				// Create a new htree.
				final HTree btree = getHTree(store, 2/* addressBits */);

				// verify new btree is dirty.
				assertTrue("dirty", btree.root.isDirty());

				// set the dirty event listener.
				btree.setDirtyListener(listener);

				// verify counter is zero on the listener.
				listener.assertCounter(0);

				// flush the btree onto the store.
				addr1 = btree.writeCheckpoint();

				// verify btree is no longer dirty.
				assertFalse("dirty", btree.root.isDirty());

				// verify event was not generated.
				listener.assertCounter(0);
				
				/*
				 * Write on the btree and verify that the listener is notified.
				 */
				btree.insert(k1,val);
				if(log.isInfoEnabled())
					log.info("after insert of 1 key:\n"+ btree.PP());

				assertTrue("dirty", btree.root.isDirty());

				listener.assertCounter(1);

				/*
				 * Write again -- there should be no notice at the listener
				 * since the tree is still dirty. We write enough entries to
				 * split the initial bucket page.
				 */
				btree.insert(k2,val);
				btree.insert(k3,val);
				btree.insert(k4,val);
				btree.insert(k5,val);
				if(log.isInfoEnabled())
					log.info("after insert of 3 more keys:\n"+ btree.PP());
				assertTrue("nleaves", btree.nleaves > 1);

				listener.assertCounter(1);

				/*
				 * Flush to the store. This makes the tree clean again.
				 */
				assertTrue("dirty", btree.root.isDirty());
				addr2 = btree.writeCheckpoint();
				assertFalse("dirty", btree.root.isDirty());

				/*
				 * Insert another record. The tree still becomes dirty and the
				 * listener gets notified.
				 */
				listener.setExpected(true);
				btree.insert(k6, val);
				assertTrue("dirty", btree.root.isDirty());
				listener.assertCounter(2);

			}

			if (true) {
				/*
				 * FIXME The rest of this test relies on the ability to remove
				 * tuples. It was originally written for the B+Tree. It needs to
				 * be ported to the HTree once we support removal of key/val
				 * pairs.
				 */
				log.warn("Test is only partly complete. Finish when we add support for removal");
				return;
			}
			/*
			 * Re-load from addr2 and test removeAll().
			 */
			{

				// helper class listens for dirty events.
				final MyDirtyListener listener = new MyDirtyListener();

				// reload the btree from the store.
				final HTree btree = HTree.load(store, addr2, false/* readOnly */);

				// a newly loaded tree is always clean.
				assertFalse("dirty", btree.root.isDirty());

				// set our listener.
				btree.setDirtyListener(listener);

				listener.setExpected(true);

				btree.removeAll();

				listener.assertCounter(1);

			}

			/*
			 * Re-load from addr2 and test removal of entries that forces the
			 * root node to be replaced by a root leaf.
			 */
			final long addr3;
			{

				// helper class listens for dirty events.
				final MyDirtyListener listener = new MyDirtyListener();

				// reload the btree from the store.
				final HTree btree = HTree.load(store, addr2, false/* readOnly */);

				// a newly loaded tree is always clean.
				assertFalse("dirty", btree.root.isDirty());

				// set our listener.
				btree.setDirtyListener(listener);

				listener.setExpected(true);

				assertEquals("nnodes", 1, btree.nnodes);
				assertEquals("nleaves", 2, btree.nleaves);
				assertEquals("nentries", 4, btree.nentries);
				btree.remove(new byte[] { 2, 2, 3 });
				assertTrue("dirty", btree.root.isDirty());
				assertEquals("nnodes", 0, btree.nnodes);
				assertEquals("nleaves", 1, btree.nleaves);
				assertEquals("nentries", 3, btree.nentries);

				listener.assertCounter(1);

				/*
				 * Flush to the store making the tree clean again.
				 */
				addr3 = btree.writeCheckpoint();

			}

			/*
			 * Re-load and remove the remaining entries until the tree is empty.
			 */
			{

				// helper class listens for dirty events.
				final MyDirtyListener listener = new MyDirtyListener();

				// reload the btree from the store.
				final HTree btree = HTree.load(store, addr3, false/* readOnly */);

				// a newly loaded tree is always clean.
				assertFalse("dirty", btree.root.isDirty());

				// set our listener.
				btree.setDirtyListener(listener);

				listener.setExpected(true);

				assertEquals("nnodes", 0, btree.nnodes);
				assertEquals("nleaves", 1, btree.nleaves);
				assertEquals("nentries", 3, btree.nentries);
				btree.remove(new byte[] { 1, 2, 3 });
				listener.assertCounter(1);
				btree.remove(new byte[] { 3, 2, 3 });
				btree.remove(new byte[] { 4, 2, 3 });
				assertTrue("dirty", btree.root.isDirty());
				assertEquals("nnodes", 0, btree.nnodes);
				assertEquals("nleaves", 1, btree.nleaves);
				assertEquals("nentries", 0, btree.nentries);
				listener.assertCounter(1); // unchanged.

			}
		} finally {
			store.destroy();
		}

	}

	/**
	 * Used to detect correct and incorrect
	 * {@link IDirtyListener#dirtyEvent(ICheckpointProtocol)} events.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * @version $Id$
	 */
	private static class MyDirtyListener implements IDirtyListener {

        /**
         * Note: The flag is initially <code>false</code>. Even through a new
         * BTree will always generate this event, the listener is not registered
         * until after the index has been created so we never see that event.
         */
		private boolean expected = true;

		/**
		 * #of events received by this listener.
		 */
		private int counter = 0;

		/**
		 * Asserts the #of times the dirty event was sent to this listener.
		 * 
		 * @param expected
		 */
		public void assertCounter(int expected) {

			assertEquals("counter", expected, counter);

		}

		/**
		 * Set the {@link #expected} flag.
		 * 
		 * @param newValue
		 */
		public void setExpected(boolean newValue) {

			this.expected = newValue;

		}

		/**
		 * Throws an exception unless the event is expected.
		 * <p>
		 * Note: The {@link #expected} flag is cleared after each invocation.
		 */
		public void dirtyEvent(final ICheckpointProtocol htree) {

			if(log.isInfoEnabled())
				log.info("event: htree="+htree);
			
			assertTrue("expected", expected);

			expected = false;

			counter++;

		}

	}

}
