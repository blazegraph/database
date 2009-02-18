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
 * Created on Nov 25, 2008
 */

package com.bigdata.resources;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;

/**
 * Unit test of the {@link StoreManager} when adding and deleting journals and
 * index segments.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAddDeleteResource extends AbstractResourceManagerTestCase {

    /**
     * 
     */
    public TestAddDeleteResource() {
    }

    /**
     * @param arg0
     */
    public TestAddDeleteResource(String arg0) {
        super(arg0);
    }

    /**
     * Test verifies add and delete of a journal.
     * 
     * @throws IOException
     */
    public void test_addDeleteJournal() throws IOException {
        
        final Journal j1;
        {

            Properties p = new Properties();
            
            p.setProperty(Journal.Options.FILE, File.createTempFile("journal_1",
                    Journal.Options.JNL, resourceManager.dataDir).toString());
            
            j1 = new Journal(p);
            
        }
        
        try {

            assertEquals(1, resourceManager.getManagedJournalCount());

            // add
            resourceManager.addResource(j1.getResourceMetadata(), j1.getFile());

            assertEquals(2, resourceManager.getManagedJournalCount());

            // store must be closed to be deleted.
            j1.close();
            
            // delete
            resourceManager.deleteResource(j1.getResourceMetadata().getUUID(),
                    true/* isJournal */);

            assertEquals(1, resourceManager.getManagedJournalCount());
            
        } finally {

            if (j1.isOpen()) {
                j1.destroy();
            } else {
                j1.getFile().delete(); 
            }

        }

    }

    /**
     * Test verifies add and delete of an index segment.
     * 
     * @throws Exception 
     */
    public void test_addDeleteSegment() throws Exception {

        assertEquals(0, resourceManager.getManagedSegmentCount());

        final BuildResult buildResult;
        {

//            Properties p = new Properties();
            
//            p.setProperty(IndexSegmentStore.Options.SEGMENT_FILE, File
//                    .createTempFile("segment_1", Journal.Options.SEG,
//                            resourceManager.dataDir).toString());
            
            final String INDEX_NAME = "test index";
            
            final BTree btree = BTree.create(new SimpleMemoryRawStore(),
                    new IndexMetadata(INDEX_NAME, UUID.randomUUID()));
            
            // insert a tuple.
            btree.insert("abc", "def");
            
            final long createTime = resourceManager.nextTimestamp();

            // build an index segment from that btree.
            buildResult = resourceManager.buildIndexSegment(INDEX_NAME, btree,
                    true/* compactingMerge */, createTime, null/* fromKey */,
                    null /* toKey */, new Event(resourceManager
                            .getFederation(), new EventResource(INDEX_NAME),
                            "test", ""/* details */));

        }

        try {

            // Note: the build already added the index segment for us.
            assertEquals(1, resourceManager.getManagedSegmentCount());

            // delete
            resourceManager.deleteResource(buildResult.segmentMetadata
                    .getUUID(), false/* isJournal */);

            assertEquals(0, resourceManager.getManagedSegmentCount());

        } finally {

            if (buildResult != null) {

                buildResult.builder.outFile.delete();

            }

        }

    }

}
