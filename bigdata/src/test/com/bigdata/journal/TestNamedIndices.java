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
 * Created on Feb 7, 2007
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.htree.HTree;

/**
 * Test suite for api supporting registration, lookup, use, and atomic commit of
 * named indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNamedIndices extends ProxyTestCase<Journal> {

    public TestNamedIndices() {
    }

    public TestNamedIndices(String name) {
        super(name);
    }

    /**
     * Test the ability to register and use named index, including whether the
     * named index is restart safe.
     */
    public void test_registerAndUseBTree() {

        Journal journal = new Journal(getProperties());

        try {

            final String name = "abc";

            final UUID indexUUID = UUID.randomUUID();

            BTree btree;
            {

                final IndexMetadata metadata = new IndexMetadata(indexUUID);

                metadata.setBranchingFactor(3);

                btree = BTree.create(journal, metadata);

            }

            assertNull(journal.getIndex(name));

            journal.registerIndex(name, btree);

            assertTrue(btree == journal.getIndex(name));

			final byte[] k0 = new byte[] { 0 };
			final byte[] v0 = new byte[] { 1, 2, 3 };

            btree.insert(k0, v0);

            /*
             * commit and close the journal
             */
            journal.commit();

            if (journal.isStable()) {

                /*
                 * re-open the journal and test restart safety.
                 */
                journal = reopenStore(journal);

                btree = (BTree) journal.getIndex(name);

                assertNotNull("btree", btree);
                assertEquals("indexUUID", indexUUID, btree.getIndexMetadata()
                        .getIndexUUID());
                assertEquals("entryCount", 1, btree.getEntryCount());
                assertEquals(v0, btree.lookup(k0));

            }

        } finally {

            journal.destroy();

        }

    }

	public void test_registerAndUseHTree() {

		Journal journal = new Journal(getProperties());

		try {

			final String name = "abc";

			final UUID indexUUID = UUID.randomUUID();

            assertNull(journal.getHTree(name));

            final HTreeIndexMetadata metadata = new HTreeIndexMetadata(
                        indexUUID);
            
            final HTree htree0 = HTree.create(journal, metadata);

            journal.registerIndex(name, htree0);

            HTree htree1 = journal.getHTree(name);
            
            // same reference.
            assertTrue(htree0 == htree1);

			final byte[] k0 = new byte[] { 0 };
			final byte[] v0 = new byte[] { 1, 2, 3 };

			htree1.insert(k0, v0);

			/*
			 * commit and close the journal
			 */
			journal.commit();

			if (journal.isStable()) {

				/*
				 * re-open the journal and test restart safety.
				 */
				journal = reopenStore(journal);

				htree1 = (HTree) journal.getHTree(name);

				assertNotNull("btree", htree1);
				assertEquals("indexUUID", indexUUID, htree1.getIndexMetadata()
						.getIndexUUID());
				assertEquals("entryCount", 1, htree1.getEntryCount());
				assertEquals(v0, htree1.lookupFirst(k0));

			}

		} finally {

			journal.destroy();

		}

    }
    
}
