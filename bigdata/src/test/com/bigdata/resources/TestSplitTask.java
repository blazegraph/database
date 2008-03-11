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
 * Created on Feb 21, 2008
 */

package com.bigdata.resources;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.BatchInsert.BatchInsertConstructor;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.service.TestOverflow;

/**
 * Basic tests of splitting an index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSplitTask extends AbstractResourceManagerTestCase {

    /**
     * 
     */
    public TestSplitTask() {
        super();
    }

    /**
     * @param arg0
     */
    public TestSplitTask(String arg0) {
        super(arg0);
    }

    /**
     * Test generation of N index splits based on an index partition that has
     * been pre-populated with index entries and a specified target #of index
     * entries per index partition.
     * <p>
     * Note: This test does NOT compare the resulting index partitions against
     * ground truth. However, that comparison is performed by a similar test as
     * part of the com.bigdata.services package. See {@link TestOverflow}.
     * 
     * @throws Exception
     */
    public void test_splitTask() throws Exception {
        
        /*
         * Register the index.
         */
        final String name = "testIndex";
        final UUID indexUUID = UUID.randomUUID();
        final IndexMetadata indexMetadata = new IndexMetadata(name,indexUUID);
        {

            // The minimum #of index entries per partition before they will be joined.
            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setMinimumEntryCount(100);
            
            // The target #of index entries per partition.
            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setEntryCountPerSplit(400);
            
            // must support delete markers
            indexMetadata.setDeleteMarkers(true);
            
            // must be an index partition.
            indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(
                    0, // partitionId (arbitrary since no metadata index).
                    new byte[]{}, //leftSeparator
                    null, // rightSeparator
                    new IResourceMetadata[]{resourceManager.getLiveJournal().getResourceMetadata()},
                    "" // history
                    ));
            
            // submit task to register the index and wait for it to complete.
            concurrencyManager.submit(new RegisterIndexTask(concurrencyManager,name,indexMetadata)).get();
        
        }
        
        /*
         * Populate the index with some data.
         */
        final BTree groundTruth = BTree.create(new SimpleMemoryRawStore(),new IndexMetadata(indexUUID));
        {
            
            final int nentries = 1000;
            
            final byte[][] keys = new byte[nentries][];
            final byte[][] vals = new byte[nentries][];

            Random r = new Random();

            for (int i = 0; i < nentries; i++) {

                keys[i] = KeyBuilder.asSortKey(i);

                vals[i] = new byte[4];

                r.nextBytes(vals[i]);

                groundTruth.insert(keys[i],vals[i]);
                                
            }

            IIndexProcedure proc = BatchInsertConstructor.RETURN_NO_VALUES
                    .newInstance(indexMetadata, 0/* fromIndex */,
                            nentries/*toIndex*/, keys, vals);

            // submit the task and wait for it to complete.
            concurrencyManager.submit(
                    new IndexProcedureTask(concurrencyManager, ITx.UNISOLATED,
                            name, proc)).get();
            
        }

        /*
         * Overflow the journal.
         */
        
        // createTime of the old journal.
        final long createTime0 = resourceManager.getLiveJournal().getRootBlockView().getCreateTime();
        // uuid of the old journal.
        final UUID uuid0 = resourceManager.getLiveJournal().getRootBlockView().getUUID();
        
        // force overflow onto a new journal.
        Future future = resourceManager.overflow();

        // await completion of the task.
        future.get();
        
        // lookup the old journal again using its createTime.
        final AbstractJournal oldJournal = resourceManager.getJournal(createTime0);
        assertEquals("uuid",uuid0,oldJournal.getRootBlockView().getUUID());
        assertNotSame("closeTime",0L,oldJournal.getRootBlockView().getCloseTime());
        
//        /*
//         * Run task that will post-process the old journal.
//         */
//
//        // lastCreateTime on the old journal.
//        final long lastCommitTime = oldJournal.getRootBlockView().getLastCommitTime();
//
//        // run post-processing task.
//
//        assertTrue(resourceManager.overflowAllowed.compareAndSet(
//                true/* expect */, false/* set */));
//
//        new PostProcessOldJournalTask(resourceManager, lastCommitTime).call();

        assertTrue(resourceManager.overflowAllowed.get());
        
        // verify that the old index partition is no longer registered.
        try {
            resourceManager.getIndex(name, ITx.UNISOLATED);
        } catch(StaleLocatorException ex) {
            assertEquals("split",ex.getReason());
        }

        /*
         * Note: If you suspect a problem here you really need to examine the
         * new index partitions as registered on the new journal after the
         * post-processing step.
         * 
         * FIXME we could force a join task to rejoin the index partitions
         * and then verify the result against ground truth.
         */ 
        
    }
    
}
