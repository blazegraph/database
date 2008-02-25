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

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.BatchInsert.BatchInsertConstructor;
import com.bigdata.journal.ResourceManager.BuildResult;
import com.bigdata.journal.ResourceManager.DefaultSplitHandler;
import com.bigdata.journal.ResourceManager.PostProcessOldJournalTask;
import com.bigdata.journal.ResourceManager.SplitIndexPartitionTask;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionMetadataWithSeparatorKeys;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * FIXME Work through build, compacting merge, merge+split at the
 * {@link ResourceManager}, get the {@link MetadataIndex} to update and
 * clients to notice those updates, and then try out on the RDF database. I can
 * set the split initially in terms of the #of index entries in an index
 * partition, e.g., 10 or 100M. I can also simplify by only implementing the
 * compacting merges and splits, leaving joins for later. I will need some means
 * to choose the host onto which to put an index partition, but that can be
 * (partitionId MOD nhosts) at first. Likewise I can defer failover support. I
 * will need to move index partitions around, at least when new partitions are
 * first created, or they will just jamb up on the first machine(s). The initial
 * indices can be distributed onto one host each, so that is 5-12 hosts
 * depending on whether justifications, full text search, or a quad store is
 * being used.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMergeTasks extends AbstractResourceManagerTestCase {

    /**
     * 
     */
    public TestMergeTasks() {
        super();

    }

    /**
     * @param arg0
     */
    public TestMergeTasks(String arg0) {
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
     * 
     * @todo test variant supporting split where a constained key-range and the
     *       target partition identifier are specified. in this use case we
     *       split the source view into two output views from which we will then
     *       assemble the new index partitions and finally register the split
     *       with the metadata index.  Call this variant a MergeSplitTask since
     *       it needs different inputs to the ctor       
     */
    public void test_compactingMerge() throws IOException, InterruptedException, ExecutionException {
        
        /*
         * Register the index.
         */
        final String name = "testIndex";
        final UUID indexUUID = UUID.randomUUID();
        {
        
            IndexMetadata indexMetadata = new IndexMetadata(name,indexUUID);
            
            // must support delete markers
            indexMetadata.setDeleteMarkers(true);
            
            // must be an index partition.
            indexMetadata.setPartitionMetadata(new PartitionMetadataWithSeparatorKeys(
                    0, // partitionId (arbitrary since no metadata index).
                    new UUID[]{UUID.randomUUID()},// data services (arbitrary since not scale-out)
                    new IResourceMetadata[]{resourceManager.getLiveJournal().getResourceMetadata()},
                    new byte[]{}, //leftSeparator
                    null // rightSeparator
                    ));
            
            // submit task to register the index and wait for it to complete.
            concurrencyManager.submit(new RegisterIndexTask(concurrencyManager,name,indexMetadata)).get();
        
        }
        
        /*
         * Populate the index with some data.
         */
        final BTree groundTruth = BTree.create(new SimpleMemoryRawStore(),new IndexMetadata(indexUUID));
        {
            
            final int nentries = 10;
            
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
                    .newInstance(nentries, 0/* offset */, keys, vals);

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
        final long createTime0 = resourceManager.getLiveJournal().getRootBlockView().getCreateTime();
        // uuid of the old journal.
        final UUID uuid0 = resourceManager.getLiveJournal().getRootBlockView().getUUID();
        
        // force overflow onto a new journal.
        resourceManager.doOverflow();
        
        // lookup the old journal again using its createTime.
        final AbstractJournal oldJournal = resourceManager.getJournal(createTime0);
        assertEquals("uuid",uuid0,oldJournal.getRootBlockView().getUUID());
        assertNotSame("closeTime",0L,oldJournal.getRootBlockView().getCloseTime());
        
        // run merge task.
        final SegmentMetadata segmentMetadata;
        {
            
            /*
             * Note: The task start time is a historical read on the final
             * committed state of the old journal. This means that the generated
             * index segment will have a createTime EQ to the lastCommitTime on
             * the old journal. This also means that it will have been generated
             * from a fused view of all data as of the final commit state of the
             * old journal.
             */ 
            final long startTime = -oldJournal.getRootBlockView().getLastCommitTime();
        
            // task to run.
            final AbstractTask task = resourceManager.new BuildIndexSegmentTask(
                    concurrencyManager, startTime, name);
            
            // submit task and await result (metadata describing the new index segment).
            final BuildResult result = (BuildResult) concurrencyManager.submit(task).get();
            
            segmentMetadata = result.segmentMetadata; 
            
            System.err.println(segmentMetadata.toString());

            // verify file exists.
            assertTrue(new File(segmentMetadata.getFile()).exists());
            
            // verify createTime == lastCommitTime on the old journal. 
            assertEquals("createTime", oldJournal.getRootBlockView()
                    .getLastCommitTime(), segmentMetadata.getCreateTime());
            
        }
        
        // verify segment has all data in the groundTruth btree.
        {
            
            IndexSegmentFileStore segStore = new IndexSegmentFileStore(new File(segmentMetadata.getFile()));
            
            IndexSegment seg = segStore.load();
            
            AbstractBTreeTestCase.assertSameBTree(groundTruth, seg);
            
        }

        // run task that re-defines the index partition view.
        {

            AbstractTask task = resourceManager.new UpdateIndexPartition(
                    concurrencyManager, name, segmentMetadata);

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

        /* @todo verify that can delete the old journal and still read on the
         * new view against ground truth.
         * 
         * @todo verify a repeat of the operation where we again do an overflow,
         * merge task, and update the view - this checks the path where the view
         * already includes an index segment.
         * 
         * @todo verify that we do not generate a new index segment if there have
         * been no writes on a named index on a given journal.  I need to figure
         * out how to test for that.  This case applies to the resource manager's
         * thread that runs these tasks automatically.
         */

        fail("write tests");

    }
    
    /**
     * Test generation of N index splits based on an index partition that has
     * been pre-populated with index entries and a specified target #of index
     * entries per index partition.
     * 
     * @throws Exception 
     */
    public void test_splitTask() throws Exception {
        
        /*
         * Register the index.
         */
        final String name = "testIndex";
        final UUID indexUUID = UUID.randomUUID();
        {
        
            final IndexMetadata indexMetadata = new IndexMetadata(name,indexUUID);

            // The target #of index entries per partition.
            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setEntryCountPerSplit(400);
            
            // must support delete markers
            indexMetadata.setDeleteMarkers(true);
            
            // must be an index partition.
            indexMetadata.setPartitionMetadata(new PartitionMetadataWithSeparatorKeys(
                    0, // partitionId (arbitrary since no metadata index).
                    new UUID[]{UUID.randomUUID()},// data services (arbitrary since not scale-out)
                    new IResourceMetadata[]{resourceManager.getLiveJournal().getResourceMetadata()},
                    new byte[]{}, //leftSeparator
                    null // rightSeparator
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
                    .newInstance(nentries, 0/* offset */, keys, vals);

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
        resourceManager.doOverflow();
        
        // lookup the old journal again using its createTime.
        final AbstractJournal oldJournal = resourceManager.getJournal(createTime0);
        assertEquals("uuid",uuid0,oldJournal.getRootBlockView().getUUID());
        assertNotSame("closeTime",0L,oldJournal.getRootBlockView().getCloseTime());
        
        /*
         * Run task that will post-process the old journal.
         */

        // lastCreateTime on the old journal.
        final long lastCommitTime = oldJournal.getRootBlockView().getLastCommitTime();

        // run post-processing task.
        resourceManager.new PostProcessOldJournalTask(lastCommitTime).call();
        
        // verify that the old index partition is no longer registered.
        assertNull(resourceManager
        .getIndex(name, ITx.UNISOLATED));
        
        /*
         * Compare the index against ground truth after the post-processing is
         * done.
         * 
         * @todo verify the new index partitions are registered. 
         * 
         * FIXME This comparison can only be carried out piece meal or using a
         * metadata index so that we have a view of the now partitioned index.
         */
//        AbstractBTreeTestCase.assertSameBTree(groundTruth, actual);
        
        fail("write test");
        
    }
    
}
