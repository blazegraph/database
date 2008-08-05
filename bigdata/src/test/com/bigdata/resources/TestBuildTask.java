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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.resources.BuildIndexSegmentTask.AtomicUpdateBuildIndexSegmentTask;
import com.bigdata.resources.BuildIndexSegmentTask.BuildResult;

/**
 * Basic test of building an index segment from an index partition on overflow.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBuildTask extends AbstractResourceManagerTestCase {

    /**
     * 
     */
    public TestBuildTask() {
        super();

    }

    /**
     * @param arg0
     */
    public TestBuildTask(String arg0) {
        super(arg0);
    }

    /**
     * Test generates an {@link IndexSegment} from a (typically historical)
     * fused view of an index partition. The resulting {@link IndexSegment} is a
     * complete replacement for the historical view but does not possess any
     * deleted index entries. Typically the {@link IndexSegment} will be used to
     * replace the current index partition definition such that the resources
     * that were the inputs to the view from which the {@link IndexSegment} was
     * built are no longer required to read on that view. This change needs to
     * be recorded in the {@link MetadataIndex} before clients will being
     * reading from the new view using the new {@link IndexSegment}.
     * 
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_buildIndexSegment() throws IOException,
            InterruptedException, ExecutionException {

        /*
         * Register the index.
         */
        final String name = "testIndex";
        final UUID indexUUID = UUID.randomUUID();
        final IndexMetadata indexMetadata = new IndexMetadata(name, indexUUID);
        {

            // must support delete markers
            indexMetadata.setDeleteMarkers(true);

            // must be an index partition.
            indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(
                    // partitionId (arbitrary since no metadata index).
                    0,//
                    new byte[] {}, // leftSeparator
                    null, // rightSeparator
                    new IResourceMetadata[] {//
                            resourceManager.getLiveJournal().getResourceMetadata(), //
                    }, //
                    "" // history
                    ));

            // submit task to register the index and wait for it to complete.
            concurrencyManager.submit(
                    new RegisterIndexTask(concurrencyManager, name,
                            indexMetadata)).get();

        }

        /*
         * Populate the index with some data.
         */
        final BTree groundTruth = BTree.create(new SimpleMemoryRawStore(),
                new IndexMetadata(indexUUID));
        {

            final int nentries = 10;

            final byte[][] keys = new byte[nentries][];
            final byte[][] vals = new byte[nentries][];

            Random r = new Random();

            for (int i = 0; i < nentries; i++) {

                keys[i] = KeyBuilder.asSortKey(i);

                vals[i] = new byte[4];

                r.nextBytes(vals[i]);

                groundTruth.insert(keys[i], vals[i]);

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
         * Force overflow causing an empty btree to be created for that index on
         * a new journal and the view definition in the new btree to be updated.
         */

        // createTime of the old journal.
        final long createTime0 = resourceManager.getLiveJournal()
                .getRootBlockView().getCreateTime();
        // uuid of the old journal.
        final UUID uuid0 = resourceManager.getLiveJournal().getRootBlockView()
                .getUUID();

        final Set<String> copied = new HashSet<String>();
        
        // force overflow onto a new journal.
        resourceManager.doSynchronousOverflow( copied );
        
        // nothing should have been copied to the new journal.
        assertEquals(0,copied.size());

        // lookup the old journal again using its createTime.
        final AbstractJournal oldJournal = resourceManager
                .getJournal(createTime0);
        assertEquals("uuid", uuid0, oldJournal.getRootBlockView().getUUID());
        assertNotSame("closeTime", 0L, oldJournal.getRootBlockView()
                .getCloseTime());

        // run merge task.
        final BuildResult result;
        {

            /*
             * Note: The task start time is a historical read on the final
             * committed state of the old journal. This means that the generated
             * index segment will have a createTime EQ to the lastCommitTime on
             * the old journal. This also means that it will have been generated
             * from a fused view of all data as of the final commit state of the
             * old journal.
             */
            final long lastCommitTime = oldJournal.getRootBlockView()
                    .getLastCommitTime();

            final File outFile = resourceManager
                    .getIndexSegmentFile(indexMetadata);

            // task to run.
            final AbstractTask task = new BuildIndexSegmentTask(
                    resourceManager, lastCommitTime, name, outFile);

            try {

                // overflow must be disallowed as a task pre-condition.
                resourceManager.overflowAllowed.compareAndSet(true, false);

                /*
                 * Submit task and await result (metadata describing the new
                 * index segment).
                 */
                result = (BuildResult) concurrencyManager.submit(
                    task).get();
                
            } finally {

                // re-enable overflow processing.
                resourceManager.overflowAllowed.set(true);
                
            }

            final IResourceMetadata segmentMetadata = result.segmentMetadata;

            System.err.println(segmentMetadata.toString());

            // verify index segment can be opened.
            resourceManager.openStore(segmentMetadata.getUUID());
            
            // Note: this assertion only works if we store the file path vs its basename.
//            assertTrue(new File(segmentMetadata.getFile()).exists());

            // verify createTime == lastCommitTime on the old journal.
            assertEquals("createTime", oldJournal.getRootBlockView()
                    .getLastCommitTime(), segmentMetadata.getCreateTime());

            // verify segment has all data in the groundTruth btree.
            {

                IndexSegmentStore segStore = (IndexSegmentStore) resourceManager
                        .openStore(segmentMetadata.getUUID());

                IndexSegment seg = segStore.loadIndexSegment();

                AbstractBTreeTestCase.assertSameBTree(groundTruth, seg);

            }

        }

        // run task that re-defines the index partition view.
        {

            // fake out the task so that it will run (it can only run during asyn overflow).
            resourceManager.overflowAllowed.set(false);
            
            AbstractTask task = new AtomicUpdateBuildIndexSegmentTask(resourceManager,
                    concurrencyManager, name, result);

            // run task, await completion.
            concurrencyManager.submit(task).get();

        }

        /*
         * 
         * @todo verify the updated definition of the live view.
         */

        /*
         * verify same data from ground truth and the new view (using btree
         * helper classes for this).
         */
        {

            final IIndex actual = resourceManager
                    .getIndex(name, ITx.UNISOLATED);

            AbstractBTreeTestCase.assertSameBTree(groundTruth, actual);

        }

    }

}
