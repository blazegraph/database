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

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.btree.proc.BatchRemove.BatchRemoveConstructor;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.resources.ResourceManager.Options;
import com.bigdata.resources.StoreManager.ManagedJournal;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;

/**
 * Basic test of building an index segment from an index partition on overflow.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBuildTask2 extends AbstractResourceManagerTestCase {

    /**
     * 
     */
    public TestBuildTask2() {
        super();

    }

    /**
     * @param arg0
     */
    public TestBuildTask2(String arg0) {
        super(arg0);
    }

    /*
     * Note: The parameters here determine how stressful this test will be.
     */

    
    final private long maxSumSegBytes = (Bytes.kilobyte * 4);

    /**
     * The maximum #of tuples to insert or remove in each update.
     */
    final int maxtuples = 100;

    /**
     * When <code>true</code>, uses randomly generated but strictly
     * increasing keys within each insert pass. Otherwise uses strictly
     * increasing keys generated using a fixed pattern.
     */
    final boolean randomKeys = true;

    /**
     * Used iff {@link #randomKeys} is <code>true</code>.
     */
    final int maxBaseKey = 1000;

    /**
     * Used iff {@link #randomKeys} is <code>true</code>.
     */
    final int maxKeyInc = 100;

    /**
     * Percentage of updates that delete the tuple under a key. When zero (0d),
     * only inserts will be performed. Otherwise a random population of keys
     * will be selected for deletion from the ground truth. The size of the
     * population is a random number in [1:nentries-1] times the value of this
     * field.
     * <p>
     * Note: There needs to be a preponderance of inserts so the test will
     * eventually complete. If there are a lot of deletes and a compacting merge
     * is always choosen then the resulting index segment always will be small
     * and will fail to trigger an incremental build so the test will not
     * terminate.
     */
    final double percentRemove = 0.2d;

    /**
     * This sets a very low threshold for the #of index segment bytes which can
     * be incorporated into the accepted view for an incremental build. The unit
     * test below will run until it has exceeded this threshold so that it can
     * verify use cases where all sources are accepted and the build is actually
     * a compacting merge as well as cases where only an ordered subset of the
     * sources are accepted, which is the true incremental build.
     * 
     * @see OverflowManager.Options#MAXIMUM_BUILD_SEGMENT_BYTES
     */
    public Properties getProperties() {
        
        final Properties properties = new Properties( super.getProperties() );
        
        properties.setProperty(
                OverflowManager.Options.MAXIMUM_BUILD_SEGMENT_BYTES, ""
                        + maxSumSegBytes);
        
        // Disable index copy - overflow will always cause an index segment build.
        properties.setProperty(Options.COPY_INDEX_THRESHOLD,"0");

        return properties;
        
    }

    /**
     * Test maintains ground truth and writes on an index partition and peforms
     * a controlled overflow operation in which we do an index partition build.
     * After each overflow we re-verify the index partition against ground
     * truth. This continues until there is enough data in the view that an
     * incremental build is performed rather than a compacting merge. Once
     * again, we verify the outcomes and then quit.
     * 
     * @todo run this test through two or more cycles such that #of index
     *       segments in the view builds up to more than just one.  
     */
    public void test_builds() throws InterruptedException, ExecutionException {

        final Random r = new Random();

        /*
         * Register the index.
         */
        final String name = "testIndex";
        final UUID indexUUID = UUID.randomUUID();
        final IndexMetadata indexMetadata = new IndexMetadata(name, indexUUID);
        final BTree groundTruth = BTree.create(new SimpleMemoryRawStore(),
                new IndexMetadata(indexUUID));
        {

            // must support delete markers
            indexMetadata.setDeleteMarkers(true);

            // must be an index partition.
            indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(0, // partitionId.
                    -1, // not a move.
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
         * Now loop until we exceed the threshold so that an incremental build
         * will be performed.
         */
        int npasses = 0;
        while (npasses++ < 50) {

            final Event e = new Event(resourceManager.getFederation(),
                    new EventResource(name), "test", "pass=" + npasses);
            
            /*
             * Write more data on the index, updating ground truth as we go.
             */
            {

                /*
                 * Add some tuples.
                 */
                
                final int nentries = r.nextInt(maxtuples) + 1;
                final int base = r.nextInt(maxBaseKey);

                final byte[][] keys = new byte[nentries][];
                final byte[][] vals = new byte[nentries][];

                for (int i = 0; i < nentries; i++) {

                    if (randomKeys) {
                        // strictly increasing but random ordered keys.
                        keys[i] = KeyBuilder.asSortKey(base
                                + r.nextInt(maxKeyInc) + 1);
                    } else {
                        // strictly increasing non-random ordered keys.
                        keys[i] = KeyBuilder.asSortKey(i + npasses * nentries);
                    }

                    vals[i] = new byte[4];

                    r.nextBytes(vals[i]);

                    groundTruth.insert(keys[i], vals[i]);

                }

                final IIndexProcedure proc = BatchInsertConstructor.RETURN_NO_VALUES
                        .newInstance(indexMetadata, 0/* fromIndex */,
                                nentries/* toIndex */, keys, vals);

                // submit the task and wait for it to complete.
                concurrencyManager.submit(
                        new IndexProcedureTask(concurrencyManager,
                                ITx.UNISOLATED, name, proc)).get();

                if (log.isInfoEnabled())
                    log.info("groundTruth: entryCount now "
                            + groundTruth.getEntryCount());
                
            }
            
            if (percentRemove > 0) {
            
                /*
                 * Delete some randomly selected tuples.
                 */
                
                final int nentries = (int) ((r.nextInt(maxtuples) + 1)
                        * percentRemove + 1);

                final byte[][] keys = new byte[nentries][];

                for (int i = 0; i < nentries; i++) {

                    final int entryCount = groundTruth.getEntryCount();

                    // any existing key.
                    final int j = r.nextInt(entryCount);

                    keys[i] = groundTruth.keyAt(j); 
                    
                    groundTruth.remove(keys[i]);

                }

                final IIndexProcedure proc = BatchRemoveConstructor.RETURN_NO_VALUES
                        .newInstance(indexMetadata, 0/* fromIndex */,
                                nentries/* toIndex */, keys, null/* vals */);

                // submit the task and wait for it to complete.
                concurrencyManager.submit(
                        new IndexProcedureTask(concurrencyManager,
                                ITx.UNISOLATED, name, proc)).get();

                if (log.isInfoEnabled())
                    log.info("groundTruth: entryCount now "
                            + groundTruth.getEntryCount());
                
            }

            /*
             * Force overflow causing an empty btree to be created for that index on
             * a new journal and the view definition in the new btree to be updated.
             */
            final OverflowMetadata overflowMetadata;
            final ManagedJournal oldJournal;
            {

                final long priorOverflowCount = resourceManager
                        .getSynchronousOverflowCount();

                // createTime of the old journal.
                final long createTime0 = resourceManager.getLiveJournal()
                        .getRootBlockView().getCreateTime();

                // uuid of the old journal.
                final UUID uuid0 = resourceManager.getLiveJournal()
                        .getRootBlockView().getUUID();

                // force overflow onto a new journal.
                overflowMetadata = resourceManager.doSynchronousOverflow();

                // make sure that the overflow counter was incremented.
                assertEquals("synchronousOverflowCount", priorOverflowCount + 1,
                        resourceManager.getSynchronousOverflowCount());
                
                // nothing should have been copied to the new journal.
                assertEquals(0, overflowMetadata
                        .getActionCount(OverflowActionEnum.Copy));

                // lookup the old journal again using its createTime.
                oldJournal = (ManagedJournal) resourceManager
                        .getJournal(createTime0);
                
                assertEquals("uuid", uuid0, oldJournal.getRootBlockView()
                        .getUUID());
                
                assertNotSame("closeTime", 0L, oldJournal.getRootBlockView()
                        .getCloseTime());
                
            }

            /*
             * Tally up the view as of the lastCommitTime on the oldJournal.
             */
            final BuildViewMetadata acceptedView;
            {
               
                final ILocalBTreeView actual = resourceManager.getIndex(name,
                        oldJournal.getLastCommitTime());
                
                acceptedView = new BuildViewMetadata(actual,
                        maxSumSegBytes, e);

                if (log.isInfoEnabled()) {

                    log.info(AbstractResourceManagerTask
                            .toString("actualViewResources", actual
                                    .getResourceMetadata()));
                    
                    log.info(AbstractResourceManagerTask.toString(
                            "actualViewSources  ", actual.getSources()));
                    
                    log.info("\npass=" + npasses + " : acceptedView="
                            + acceptedView);
                    
                }

                assertEquals(actual.getSourceCount(), acceptedView.nsources);
                
            }

            /*
             * Run build task.
             * 
             * Note: The task start time is a historical read on the final
             * committed state of the old journal. This means that the generated
             * index segment will have a createTime EQ to the lastCommitTime on
             * the old journal.
             */
            final BuildResult buildResult;
            {

                /*
                 * Metadata about the index partition generated during sync
                 * overflow.
                 * 
                 * Note: This reflects the state of the index partition before
                 * overflow. For example, the new live journal is NOT part of
                 * the view.
                 */
                final ViewMetadata vmd = overflowMetadata.getViewMetadata(name);

                if (log.isInfoEnabled())
                    log.info("pre-condition view: " + vmd);

                assertTrue(vmd.getView().getSources()[0].getStore() == oldJournal);

                try {

                    // overflow must be disallowed as a task pre-condition.
                    resourceManager.overflowAllowed.compareAndSet(true, false);

                    /*
                     * Submit task and await result (metadata describing the new
                     * index segment).
                     */
                    buildResult = concurrencyManager.submit(
                            new IncrementalBuildTask(vmd)).get();

                } finally {

                    // re-enable overflow processing.
                    resourceManager.overflowAllowed.set(true);
                    
                }

                /*
                 * Verify that the BuildResult reports that the anticipated #of
                 * sources were incorporated into the index segment.
                 */
                assertEquals(acceptedView.naccepted, buildResult.sourceCount);
                
                /*
                 * Verify that the ordered sources in the BuildResult are the
                 * first N ordered sources from the view of the index partition
                 * as of the lastCommitTime on the old journal that were
                 * accepted into the build's view.
                 */
                {

                    final IResourceMetadata[] expected = resourceManager
                            .getIndex(name, oldJournal.getLastCommitTime())
                            .getResourceMetadata();

                    final IResourceMetadata[] actual = buildResult.sources;
                    
                    assertEquals(acceptedView.naccepted, actual.length);
                    
                    for (int i = 0; i < acceptedView.naccepted; i++) {

                        assertEquals(expected[i], actual[i]);

                    }

                }

                /*
                 * Spot check access to the new index segment and its
                 * createTime.
                 */

                final IResourceMetadata segmentMetadata = buildResult.segmentMetadata;

                if (log.isInfoEnabled())
                    log.info(segmentMetadata.toString());

                // verify index segment can be opened.
                resourceManager.openStore(segmentMetadata.getUUID());

                // verify createTime == lastCommitTime on the old journal.
                assertEquals("createTime", oldJournal.getRootBlockView()
                        .getLastCommitTime(), segmentMetadata.getCreateTime());

            }

            // verify unisolated index view against groundTruth.
            {
             
                /*
                 * Note: The groundTruth index reflects the total write set to
                 * date. The index segment after a compacting merge reflects
                 * only those historical writes before the last overflow.
                 * Therefore this will fail if you write on the groundTruth
                 * index after the overflow and before you verify against
                 * groundTruth.
                 */
                final ILocalBTreeView actual = resourceManager.getIndex(name,
                        ITx.UNISOLATED);

                /*
                 * There should be no writes on the mutable btree on the live
                 * journal.
                 */
                assertEquals("entryCount", 0, actual.getMutableBTree()
                        .getEntryCount());

                /*
                 * Verify same data from ground truth and the new view (using
                 * btree helper classes for this).
                 */
                AbstractBTreeTestCase.assertSameBTree(groundTruth, actual);
                
            }
            
            if (acceptedView.compactingMerge) {

                /*
                 * Verify segment has all data in the groundTruth btree.
                 */

                final IndexSegmentStore segStore = (IndexSegmentStore) resourceManager
                        .openStore(buildResult.segmentMetadata.getUUID());

                final IndexSegment seg = segStore.loadIndexSegment();

                AbstractBTreeTestCase.assertSameBTree(groundTruth, seg);

                continue;
                
            } else {
                
                // Success.
                return;

            }

        }

        fail("Terminated after npasses=" + npasses
                + " without doing incremental build.");
        
    }

}
