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
package com.bigdata.btree;

import java.util.UUID;

import org.apache.log4j.Level;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Unit tests for the close/checkpoint/reopen protocol designed to manage the
 * resource burden of indices without invalidating the index objects (indices
 * opens can be reopened as long as their backing store remains available).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReopen extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestReopen() {
    }

    /**
     * @param name
     */
    public TestReopen(String name) {
        super(name);
    }

    /**
     * Test close on a new tree - should force the root to the store since a new
     * root is dirty (if empty). reopen should then reload the empty root and on
     * life goes.
     */
    public void test_reopen01() {

        final IRawStore store = new SimpleMemoryRawStore();

        /*
         * The btree under test.
         */
        final BTree btree;
        {
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());

            md.setBranchingFactor(3);
            
            btree = BTree.create(store, md);
            
        }

        assertTrue(btree.isOpen());

        btree.close();

        assertFalse(btree.isOpen());

        try {
            btree.close();
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        assertNotNull(btree.getRoot());

        assertTrue(btree.isOpen());

    }

    /**
     * Test with a btree containing both branch nodes and leaves.
     */
    public void test_reopen02() {
     
        final IRawStore store = new SimpleMemoryRawStore();

        /*
         * The btree under test.
         */
        final BTree btree;
        {
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setBranchingFactor(3);
            
            btree = BTree.create(store, md);
            
        }

        btree.insert(KeyBuilder.asSortKey(1), new SimpleEntry(1));
        btree.insert(KeyBuilder.asSortKey(2), new SimpleEntry(2));
        btree.insert(KeyBuilder.asSortKey(3), new SimpleEntry(3));
        btree.insert(KeyBuilder.asSortKey(4), new SimpleEntry(4));
        
        // verify that the root was split.
        assertEquals("height",1,btree.getHeight());
        assertEquals("#entries",4,btree.getEntryCount());
        assertEquals("#nodes",1,btree.getNodeCount());
        assertEquals("#leaves",2,btree.getLeafCount());
        
        // dump after inserts.
        System.out.println("Dump of final btree:");
        btree.dump(Level.DEBUG,System.out);
        
        // checkpoint the index.
        btree.writeCheckpoint();
        
        // force close.
        btree.close();
        
        // force reopen.
        assertNotNull(btree.getRoot());
        assertTrue(btree.isOpen());
        
        // force materialization of the leaves.
        btree.lookup(KeyBuilder.asSortKey(1));
        btree.lookup(KeyBuilder.asSortKey(2));
        btree.lookup(KeyBuilder.asSortKey(3));
        btree.lookup(KeyBuilder.asSortKey(4));

        // dump after re-open.
        System.out.println("Dump after reopen:");
        btree.dump(Level.DEBUG,System.out);

        // reload the tree from the store.
        final BTree btree2 = BTree.load(store, btree.getCheckpoint()
                .getCheckpointAddr());

        // verify same data.
        assertSameBTree(btree, btree2);
        
    }
    
    /**
     * Stress test comparison with ground truth btree when {@link BTree#close()}
     * is randomly invoked during mutation operations.
     */
    public void test_reopen03() {

        final IRawStore store = new SimpleMemoryRawStore();

        final UUID indexUUID = UUID.randomUUID();
        
        /*
         * The btree under test.
         * 
         * Note: the fixture factory is NOT used since this node evictions will
         * be forced when this tree is closed (node evictions are not permitted
         * by the default fixture factory).
         */
//        final BTree btree = new BTree(store, 3, indexUUID);
        final BTree btree;
        {
            IndexMetadata md = new IndexMetadata(indexUUID);
            md.setBranchingFactor(3);
            
            btree = BTree.create(store, md);
        }

        /*
         * The btree used to maintain ground truth.
         * 
         * Note: the fixture factory is NOT used here since the stress test will
         * eventually overflow the hard reference queue and begin evicting nodes
         * and leaves onto the store.
         */
//        final BTree groundTruth = new BTree(store, 3, indexUUID);
        final BTree groundTruth;
        {
            IndexMetadata md = new IndexMetadata(indexUUID);
            md.setBranchingFactor(3);
            
            groundTruth = BTree.create(store, md);
        }

        final int limit = 10000;
        final int keylen = 6;

        for (int i = 0; i < limit; i++) {

            int n = r.nextInt(100);

            if (n < 5) {
                /* periodically force a checkpoint + close of the btree. */
                if(btree.isOpen()) {
                    btree.writeCheckpoint();
                    btree.close();
                }
            } else if (n < 20) {
                // remove an entry.
                byte[] key = new byte[keylen];
                r.nextBytes(key);
                btree.remove(key);
                groundTruth.remove(key);
            } else {
                // add an entry.
                byte[] key = new byte[keylen];
                r.nextBytes(key);
                SimpleEntry value = new SimpleEntry(i);
                btree.insert(key, value);
                groundTruth.insert(key, value);
            }

        }

        assertSameBTree(groundTruth, btree);

    }

}
