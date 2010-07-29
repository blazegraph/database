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

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.ILocalBTreeView;
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
import com.bigdata.mdi.IndexPartitionCause;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.resources.ResourceManager.Options;

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

    public Properties getProperties() {
        
        final Properties properties = new Properties( super.getProperties() );
        
        // Disable index copy - overflow will always cause an index segment build.
        properties.setProperty(Options.COPY_INDEX_THRESHOLD,"0");

        return properties;
        
    }

    /**
     * Test generates an {@link IndexSegment} from an ordered subset of the
     * sources specified for the fused view of an index partition. The resulting
     * {@link IndexSegment} is a partial replacement for the historical view and
     * must retain the most recent tuple or delete marker written for any key in
     * the accepted sources in the generated {@link IndexSegment}.
     * <p>
     * When the {@link IndexSegment} is incorporated back into the view, it will
     * be placed after the {@link BTree} on the live journal that is used to
     * absorb buffered writes and before any sources which were not incorporated
     * into the view. This change needs to be recorded in the
     * {@link MetadataIndex} before clients will being reading from the new view
     * using the new {@link IndexSegment}.
     */
    public void test_buildWithOverflow() throws IOException,
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
                    0, // partitionId.
                    -1, // not a move.
                    new byte[] {}, // leftSeparator
                    null, // rightSeparator
                    new IResourceMetadata[] {//
                            resourceManager.getLiveJournal().getResourceMetadata(), //
                    }, //
                    IndexPartitionCause.register(resourceManager)
//                    "" // history
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

            final Random r = new Random();

            for (int i = 0; i < nentries; i++) {

                keys[i] = KeyBuilder.asSortKey(i);

                vals[i] = new byte[4];

                r.nextBytes(vals[i]);

                groundTruth.insert(keys[i], vals[i]);

            }

            final IIndexProcedure proc = BatchInsertConstructor.RETURN_NO_VALUES
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

        // force overflow onto a new journal.
        final OverflowMetadata overflowMetadata = resourceManager
                .doSynchronousOverflow();
        
        // nothing should have been copied to the new journal.
        assertEquals(0, overflowMetadata
                .getActionCount(OverflowActionEnum.Copy));

        // lookup the old journal again using its createTime.
        final AbstractJournal oldJournal = resourceManager
                .getJournal(createTime0);
        assertEquals("uuid", uuid0, oldJournal.getRootBlockView().getUUID());
        assertNotSame("closeTime", 0L, oldJournal.getRootBlockView()
                .getCloseTime());

        // run build task.
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
//            final OverflowMetadata omd = new OverflowMetadata(resourceManager);
            
            final ViewMetadata vmd = overflowMetadata.getViewMetadata(name);
            
            // task to run.
            final IncrementalBuildTask task = new IncrementalBuildTask(vmd);

            try {

                // overflow must be disallowed as a task pre-condition.
                resourceManager.overflowAllowed.compareAndSet(true, false);

                /*
                 * Submit task and await result (metadata describing the new
                 * index segment).
                 */
                result = concurrencyManager.submit(task).get();

            } finally {

                // re-enable overflow processing.
                resourceManager.overflowAllowed.set(true);
                
            }

            /*
             * since all sources were incorporated into the build, this was
             * actually a compacting merge.
             */
            assertTrue(result.compactingMerge);
            
            final IResourceMetadata segmentMetadata = result.segmentMetadata;

            if (log.isInfoEnabled())
                log.info(segmentMetadata.toString());

            // verify index segment can be opened.
            resourceManager.openStore(segmentMetadata.getUUID());

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

        /*
         * verify same data from ground truth and the new view (using btree
         * helper classes for this).
         */
        {

            final ILocalBTreeView actual = resourceManager.getIndex(name,
                    ITx.UNISOLATED);

            AbstractBTreeTestCase.assertSameBTree(groundTruth, actual);

        }

    }

    /**
     * Unit test of a build conducted against a historical snapshot of a view
     * created by {@link BTree#createViewCheckpoint()}.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_buildWithoutOverflow() throws InterruptedException,
            ExecutionException {

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
                    0, // partitionId.
                    -1, // not a move.
                    new byte[] {}, // leftSeparator
                    null, // rightSeparator
                    new IResourceMetadata[] {//
                            resourceManager.getLiveJournal().getResourceMetadata(), //
                    }, //
                    IndexPartitionCause.register(resourceManager)
//                    ,"" // history
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

            final Random r = new Random();

            for (int i = 0; i < nentries; i++) {

                keys[i] = KeyBuilder.asSortKey(i);

                vals[i] = new byte[4];

                r.nextBytes(vals[i]);

                groundTruth.insert(keys[i], vals[i]);

            }

            final IIndexProcedure proc = BatchInsertConstructor.RETURN_NO_VALUES
                    .newInstance(indexMetadata, 0/* fromIndex */,
                            nentries/*toIndex*/, keys, vals);

            // submit the task and wait for it to complete.
            concurrencyManager.submit(
                    new IndexProcedureTask(concurrencyManager, ITx.UNISOLATED,
                            name, proc)).get();

        }

        /*
         * Rather than forcing overflow, we run a task which replaces the root
         * of the BTree with an empty leaf and updates view definition to
         * include the previous BTree plus the new BTree. This prepares us to
         * run a build without overflow. We need to know the timestamp of the
         * previous view, which is available from the 2nd source in the new
         * view.
         */
        final long priorCommitTime1 = resourceManager.getLiveJournal().getLastCommitTime();
        {

            final AbstractTask<Long> redefineView = new AbstractTask<Long>(
                    concurrencyManager, ITx.UNISOLATED, new String[] { name }) {

                @Override
                protected Long doTask() throws Exception {

                    final ILocalBTreeView view = getIndex(getOnlyResource());

                    final long priorCommitTime = view.getMutableBTree().createViewCheckpoint();
                    
                    /*
                     * Done. The new view will be seen by any task executing
                     * after this one within the commit group and by any task
                     * starting after the group commit iff the commit is
                     * successful.
                     */
                    
                    return priorCommitTime;

                }
                
            };

            // run that task and verify that it executed Ok.
            final long priorCommitTime = concurrencyManager
                    .submit(redefineView).get();

            // true unless intervening commit.
            assertEquals(priorCommitTime, priorCommitTime1);
            
        }

        /*
         * Verify the new view was applied and also verify that the view agrees
         * with the ground truth.
         */
        {

            final ILocalBTreeView actual = resourceManager.getIndex(name,
                    ITx.UNISOLATED);

            final LocalPartitionMetadata pmd = actual.getIndexMetadata()
                    .getPartitionMetadata();

            final IResourceMetadata[] resources = pmd.getResources();

            assertEquals(2, resources.length);

            assertEquals(0, resources[0].getCommitTime());

            assertEquals(priorCommitTime1, resources[1].getCommitTime());

            assertEquals(resourceManager.getLiveJournal(),
                    actual.getSources()[0].getStore());

            assertEquals(resourceManager.getLiveJournal(),
                    actual.getSources()[1].getStore());

            // the mutable btree should be empty.
            assertEquals(0, actual.getMutableBTree().getEntryCount());

            // the other btree should not be empty.
            assertNotSame(0, actual.getSources()[1].getEntryCount());

            AbstractBTreeTestCase.assertSameBTree(groundTruth, actual);

        }

        // buffer some more writes on the index before doing the build.
        {

                final int nentries = 10;

                final byte[][] keys = new byte[nentries][];
                final byte[][] vals = new byte[nentries][];

                final Random r = new Random();

                for (int i = 0; i < nentries; i++) {

                    keys[i] = KeyBuilder.asSortKey(i+100);

                    vals[i] = new byte[4];

                    r.nextBytes(vals[i]);

                    groundTruth.insert(keys[i], vals[i]);

                }

                final IIndexProcedure proc = BatchInsertConstructor.RETURN_NO_VALUES
                        .newInstance(indexMetadata, 0/* fromIndex */,
                                nentries/*toIndex*/, keys, vals);

                // submit the task and wait for it to complete.
                concurrencyManager.submit(
                        new IndexProcedureTask(concurrencyManager, ITx.UNISOLATED,
                                name, proc)).get();

        }

        // Verify the new writes showed up in the unisolated view.
        {

            final ILocalBTreeView actual = resourceManager.getIndex(name,
                    ITx.UNISOLATED);

            AbstractBTreeTestCase.assertSameBTree(groundTruth, actual);

        }

        /*
         * Run the build task.
         * 
         * Note: The task start time is the timestamp of the BTree before we
         * created the view checkpoint. This means that the generated index
         * segment will have a createTime EQ to that commitTime and will have
         * been generated from a fused view of all data as of the corresponding
         * commit point.
         */
        final BuildResult result;
        {

            // grab the btree performance counters.
            final BTreeCounters btreeCounters = resourceManager
                    .getIndexCounters(name).clone();

            final ViewMetadata vmd = new ViewMetadata(resourceManager,
                    priorCommitTime1, name, btreeCounters);

            // task to run.
            final IncrementalBuildTask task = new IncrementalBuildTask(vmd);

            try {

                /*
                 * Overflow must be disallowed as a task pre-condition.
                 */
                resourceManager.overflowAllowed.compareAndSet(true, false);

                /*
                 * Submit task and await result (metadata describing the new
                 * index segment).
                 */
                result = concurrencyManager.submit(task).get();

            } finally {

                // re-enable overflow processing.
                resourceManager.overflowAllowed.set(true);
                
            }

            /*
             * since all sources were incorporated into the build, this was
             * actually a compacting merge.
             */
            assertTrue(result.compactingMerge);
            
            final IResourceMetadata segmentMetadata = result.segmentMetadata;

            if (log.isInfoEnabled())
                log.info(segmentMetadata.toString());

            // verify index segment can be opened.
            resourceManager.openStore(segmentMetadata.getUUID());

            // verify createTime == commitTime for view checkpoint.
            assertEquals("createTime", priorCommitTime1, segmentMetadata
                    .getCreateTime());

        }

        /*
         * Reverify the post-build view against the ground truth.
         */
        {

            final ILocalBTreeView actual = resourceManager.getIndex(name,
                    ITx.UNISOLATED);

            AbstractBTreeTestCase.assertSameBTree(groundTruth, actual);

        }

//        /*
//         * @todo do a compacting merge on the current state of the view and
//         * verify the resulting view is compact and agrees with the ground truth
//         * btree.
//         */
//
//        // verify segment has all data in the groundTruth btree.
//        {
//
//            final IndexSegmentStore segStore = (IndexSegmentStore) resourceManager
//                    .openStore(segmentMetadata.getUUID());
//
//            final IndexSegment seg = segStore.loadIndexSegment();
//
//            AbstractBTreeTestCase.assertSameBTree(groundTruth, seg);
//
//        }

    }

}
