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
 * Created on Feb 22, 2008
 */

package com.bigdata.journal;

import java.io.IOException;
import java.util.UUID;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.Bytes;

/**
 * Basic tests for
 * {@link ResourceManager#overflow(boolean, WriteExecutorService)}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestOverflow extends AbstractResourceManagerTestCase {

    /**
     * 
     */
    public TestOverflow() {
        super();
    }

    /**
     * @param arg0
     */
    public TestOverflow(String arg0) {
        super(arg0);
    }

    /**
     * A test for overflow of the {@link ResourceManager}. We begin with a
     * blank slate, so the {@link ResourceManager} creates an initial
     * {@link Journal} for us and put its into play. The test then registers an
     * initial partition of scale-out index on that journal and some data is
     * written on that index. An overflow operation is executed, which causes a
     * new {@link Journal} to be created and brought into play. The index is
     * re-defined on the new journal such that its view includes the data on the
     * old journal as well.
     * 
     * @throws IOException 
     */
    public void test_overflow() throws IOException {

        /*
         * Define, register, and populate the initial partition of a named
         * scale-out index.
         */
        final String indexName = "testIndex";
        final int nentries = 100;
        {

            AbstractJournal journal = resourceManager.getLiveJournal();

            IndexMetadata indexMetadata = new IndexMetadata(indexName, UUID
                    .randomUUID());

            // required for scale-out indices.
            indexMetadata.setDeleteMarkers(true);

            indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(//
                    0, // partitionId
                    new byte[]{}, // leftSeparator.
                    null, // rightSeparator.
                    new IResourceMetadata[]{
                            journal.getResourceMetadata()
                    }));
            
            // create index and register on the journal.
            IIndex ndx = journal.registerIndex(indexName, BTree.create(journal,
                    indexMetadata));

            DataOutputBuffer buf = new DataOutputBuffer(Bytes.SIZEOF_INT);

            // populate with some data.
            for (int j = 0; j < nentries; j++) {

                // format the value.
                buf.reset().putInt(j);

                // insert values.
                ndx.insert(KeyBuilder.asSortKey(j), buf.toByteArray());

                // bump the counter. 
                ndx.getCounter().incrementAndGet();
                
            }

            // commit data on the journal
            journal.commit();

        }

        /*
         * Do overflow operation. This should create a new journal and migrate
         * the index definition to the new journal while re-defining the view to
         * include the data on the old journal.
         */
        {

            IJournal oldJ = resourceManager.getLiveJournal();
            
            assertEquals(1, resourceManager.getJournalCount());

            // do overflow.
            resourceManager.doOverflow();

            assertEquals(2, resourceManager.getJournalCount());

            // verify live journal is a different instance.
            assertTrue(oldJ != resourceManager.getLiveJournal());

        }

        /*
         * Verify new view on the index partition.
         */
        {
            
            AbstractBTree[] sources = resourceManager
                    .getIndexSources(indexName, 0L/* timestamp */); 
            
            assertNotNull("sources",sources);
            
            assertEquals("#sources",2,sources.length);

            assertTrue(sources[0] != sources[1]);
            
            // entries are still on the old index.
            assertEquals(nentries,sources[1].getEntryCount());

            // verify counter on the old index is unchanged.
            assertEquals(nentries,sources[1].getCounter().get());

            // verify no entries yet on the new index.
            assertEquals(0,sources[0].getEntryCount());

            // verify counter was carried forward to the new index(!)
            assertEquals(nentries,sources[0].getCounter().get());
            
        }
        
    }

}
