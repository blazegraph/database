/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 8, 2007
 */

package com.bigdata.btree;

import com.bigdata.rawstore.IRawStore;

/**
 * Test suite for the {@link IDirtyListener} protocol.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDirtyListener extends AbstractBTreeTestCase {

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
     * Test exercises the various ways in which {@link BTree#fireDirtyEvent()}
     * should and should not be triggered.
     * <p>
     * Note: One of the things that this is testing is that we do not fire dirty
     * events when the tree is already dirty. This is unlikely to be harmful
     * since the {@link IDirtyListener} is primarily used to build up a commit
     * list but it does not add any information since we just want to know when
     * a btree becomes dirty so that we can make sure that it will partitipate
     * in a commit.
     */
    public void test_dirtyListener1() {

        /*
         * Create a new btree.
         */
        final long addr1;
        final long addr2;
        final IRawStore store;
        {

            // helper class listens for dirty events.
            MyDirtyListener listener = new MyDirtyListener();

            // Create a new btree.
            BTree btree = getBTree(3);

            // save reference to the backing store.
            store = btree.getStore();

            // verify new btree is dirty.
            assertTrue("dirty", btree.root.isDirty());

            // set the dirty event listener.
            btree.setDirtyListener(listener);

            // verify counter is zero on the listener.
            listener.assertCounter(0);

            // flush the btree onto the store.
            addr1 = btree.write();

            // verify btree is no longer dirty.
            assertFalse("dirty", btree.root.isDirty());

            // verify event was not generated.
            listener.assertCounter(0);

            /*
             * Write on the btree and verify that the listener is notified.
             */
            btree.insert(new byte[]{1,2,3},new SimpleEntry());

            assertTrue("dirty",btree.root.dirty);

            listener.assertCounter(1);
            
            /*
             * Write again -- there should be no notice. We write enough entries
             * to split the root leaf into a node and two leaves and still there
             * should be no notice since the tree is still dirty.
             */
            btree.insert(new byte[]{2,2,3},new SimpleEntry());
            btree.insert(new byte[]{3,2,3},new SimpleEntry());
            btree.insert(new byte[]{4,2,3},new SimpleEntry());
            assertEquals("nnodes", 1,btree.nnodes);
            assertEquals("nleaves", 2,btree.nleaves);
            assertEquals("nentries", 4,btree.nentries);

            listener.assertCounter(1);

            /*
             * Flush to the store.  This makes the tree clean again.
             */
            assertTrue("dirty",btree.root.dirty);
            addr2 = btree.write();
            assertFalse("dirty",btree.root.dirty);
            
            /*
             * Insert another record - this overwrites an existing entry, but
             * the tree still becomes dirty and the listener gets notified.
             */
            listener.setExpected(true);
            btree.insert(new byte[]{2,2,3},new SimpleEntry());
            assertTrue("dirty",btree.root.dirty);
            listener.assertCounter(2);
            
        }

        /*
         * Re-load from addr2 and test removeAll(). 
         */
        {
            
            // helper class listens for dirty events.
            MyDirtyListener listener = new MyDirtyListener();

            // reload the btree from the store.
            BTree btree = BTree.load(store, addr2);

            // a newly loaded tree is always clean.
            assertFalse("dirty",btree.root.dirty);
            
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
            MyDirtyListener listener = new MyDirtyListener();

            // reload the btree from the store.
            BTree btree = BTree.load(store, addr2);

            // a newly loaded tree is always clean.
            assertFalse("dirty",btree.root.dirty);
            
            // set our listener.
            btree.setDirtyListener(listener);
            
            listener.setExpected(true);
            
            assertEquals("nnodes", 1,btree.nnodes);
            assertEquals("nleaves", 2,btree.nleaves);
            assertEquals("nentries", 4,btree.nentries);
            btree.remove(new byte[]{2,2,3});
            assertTrue("dirty",btree.root.dirty);
            assertEquals("nnodes", 0,btree.nnodes);
            assertEquals("nleaves", 1,btree.nleaves);
            assertEquals("nentries", 3,btree.nentries);
            
            listener.assertCounter(1);

            /*
             * Flush to the store making the tree clean again.
             */
            addr3 = btree.write();
            
        }

        /*
         * Re-load and remove the remaining entries until the tree is empty.
         */
        {

            // helper class listens for dirty events.
            MyDirtyListener listener = new MyDirtyListener();

            // reload the btree from the store.
            BTree btree = BTree.load(store, addr3);

            // a newly loaded tree is always clean.
            assertFalse("dirty",btree.root.dirty);
            
            // set our listener.
            btree.setDirtyListener(listener);
            
            listener.setExpected(true);
            
            assertEquals("nnodes", 0,btree.nnodes);
            assertEquals("nleaves", 1,btree.nleaves);
            assertEquals("nentries", 3,btree.nentries);
            btree.remove(new byte[]{1,2,3});
            listener.assertCounter(1);
            btree.remove(new byte[]{3,2,3});
            btree.remove(new byte[]{4,2,3});
            assertTrue("dirty",btree.root.dirty);
            assertEquals("nnodes", 0,btree.nnodes);
            assertEquals("nleaves", 1,btree.nleaves);
            assertEquals("nentries", 0,btree.nentries);
            listener.assertCounter(1); // unchanged.
            
        }
        
    }

    /**
     * Used to detect correct and incorrect
     * {@link IDirtyListener#dirtyEvent(BTree)} events.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class MyDirtyListener implements IDirtyListener {

        /**
         * Note: The flag is initially <code>false</code>. Even through a new
         * BTree will always generate this event, the listener is not registered
         * until after the {@link BTree} has been created so we never see that
         * event.
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
            
            assertEquals("counter",expected,counter);
            
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
        public void dirtyEvent(BTree btree) {
            
            assertTrue("expected",expected);
            
            expected = false;
            
            counter++;
            
        }
        
    }

}
