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
 * Created on Nov 17, 2006
 */

package com.bigdata.btree;

import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Unit tests for commit functionality that do not trigger copy-on-write.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCommit extends TestCase2 {

    /**
     * 
     */
    public TestCommit() {
    }

    /**
     * @param name
     */
    public TestCommit(String name) {
        super(name);
    }

    /*
     * Tests of commit processing (without triggering copy-on-write).
     */
    
    /**
     * Test commit of a new tree (the root is a leaf node).
     */
    public void test_commit01() {

        IRawStore store = new SimpleMemoryRawStore();

        final int branchingFactor = 4;
        
        final long addrMetadata;
        final long rootId;
        {

            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            metadata.setBranchingFactor(branchingFactor);
            
            BTree btree = BTree.create(store, metadata);
            
//            BTree btree = new BTree(store, branchingFactor, UUID.randomUUID());
            
            assertTrue(btree.root.isDirty());

            // Commit of tree with dirty root.
            addrMetadata = btree.checkpoint();

            assertFalse(btree.root.isDirty());

            rootId = btree.root.getIdentity();
            
            assertEquals("height", 0, btree.height);
            assertEquals("#nodes", 0, btree.nnodes);
            assertEquals("#leaves", 1, btree.nleaves);
            assertEquals("#entries", 0, btree.nentries);

        }
        
        final long metadata2;
        {

            // Load the tree.
            BTree btree = BTree.load(store, addrMetadata);
            
            // verify addrRoot.
            assertEquals(rootId,btree.root.getIdentity());
            assertFalse(btree.root.isDirty());

            assertEquals("height", 0, btree.height);
            assertEquals("#nodes", 0, btree.nnodes);
            assertEquals("#leaves", 1, btree.nleaves);
            assertEquals("#entries", 0, btree.nentries);

            /*
             * Commit of tree with clean root writes a new metadata record but
             * does not change the addrRoot.
             */
            metadata2 = btree.checkpoint();
            assertNotSame( addrMetadata, metadata2 );

        }

        {   // re-verify.

            // Load the tree.
            BTree btree = BTree.load(store, addrMetadata);

            // verify addrRoot.
            assertEquals(rootId,btree.root.getIdentity());

            assertEquals("height", 0, btree.height);
            assertEquals("#nodes", 0, btree.nnodes);
            assertEquals("#leaves", 1, btree.nleaves);
            assertEquals("#entries", 0, btree.nentries);

        }
        
    }
    
}
