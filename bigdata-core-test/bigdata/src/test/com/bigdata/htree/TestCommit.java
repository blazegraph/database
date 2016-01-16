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
 * Created on Nov 17, 2006
 */

package com.bigdata.htree;

import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Unit tests for commit functionality that do not trigger copy-on-write.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCommit extends AbstractHTreeTestCase {

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

		final IRawStore store = new SimpleMemoryRawStore();

		try {

			final long addrMetadata;
			final long rootId;
			{

				final HTree btree = getHTree(store, 2/* addressBits */,
						false/* rawRecords */, true/* persistent */);

				assertTrue(btree.root.isDirty());

				// Commit of tree with dirty root.
				addrMetadata = btree.writeCheckpoint();

				assertFalse(btree.root.isDirty());

				rootId = btree.root.getIdentity();

//				assertEquals("height", 0, btree.height);
				assertEquals("#nodes", 1, btree.nnodes);
				assertEquals("#leaves", 1, btree.nleaves);
				assertEquals("#entries", 0, btree.nentries);

			}

			final long metadata2;
			{

				// Load the tree.
				final HTree htree = HTree
						.load(store, addrMetadata, false/* readOnly */);

				// verify addrRoot.
				assertEquals(rootId, htree.root.getIdentity());
				assertFalse(htree.root.isDirty());

//				assertEquals("height", 0, btree.height);
				assertEquals("#nodes", 1, htree.nnodes);
				assertEquals("#leaves", 1, htree.nleaves);
				assertEquals("#entries", 0, htree.nentries);

				/*
				 * Commit of tree with clean root writes a new metadata record
				 * but does not change the addrRoot.
				 */
				metadata2 = htree.writeCheckpoint();
				assertNotSame(addrMetadata, metadata2);

			}

			{ // re-verify.

				// Load the tree.
				final HTree htree = HTree
						.load(store, addrMetadata, false/* readOnly */);

				// verify addrRoot.
				assertEquals(rootId, htree.root.getIdentity());

//				assertEquals("height", 0, btree.height);
				assertEquals("#nodes", 1, htree.nnodes);
				assertEquals("#leaves", 1, htree.nleaves);
				assertEquals("#entries", 0, htree.nentries);

			}

		} finally {

			store.destroy();

        }
        
    }
    
}
