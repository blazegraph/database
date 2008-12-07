package com.bigdata.resources;

import java.io.File;
import java.util.Arrays;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.SegmentMetadata;

/**
 * Task builds an {@link IndexSegment} from the fused view of an index partition
 * as of some historical timestamp and then atomically updates the view (aka a
 * compacting merge).
 * <p>
 * Note: This task may be used after
 * {@link IResourceManager#overflow(boolean, boolean)} in order to produce a
 * compact view of the index as of the <i>lastCommitTime</i> on the old
 * journal.
 * <p>
 * Note: As its last action, this task submits a
 * {@link AtomicReplaceHistoryTask} which replaces the view with one
 * defined by the current {@link BTree} on the journal and the newly built
 * {@link IndexSegment}.
 * <p>
 * Note: If the task fails, then the output {@link IndexSegment} will be
 * deleted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CompactingMergeTask extends
        AbstractResourceManagerTask<BuildResult> {

    final protected long lastCommitTime;

    final protected File outFile;

    /**
     * 
     * @param concurrencyManager
     * @param lastCommitTime
     *            The lastCommitTime of the journal whose view of the index
     *            you wish to capture in the generated {@link IndexSegment}.
     * @param name
     *            The name of the index.
     * @param outFile
     *            The file on which the {@link IndexSegment} will be
     *            written.
     */
    public CompactingMergeTask(final ResourceManager resourceManager,
            final long lastCommitTime, final String name, final File outFile) {

        super(resourceManager, TimestampUtility
                .asHistoricalRead(lastCommitTime), name);

        this.lastCommitTime = lastCommitTime;

        if (outFile == null)
            throw new IllegalArgumentException();

        this.outFile = outFile;

    }

    /**
     * Build an {@link IndexSegment} from the compacting merge of an index
     * partition.
     * 
     * @return The {@link BuildResult}.
     */
    public BuildResult doTask() throws Exception {

        if (resourceManager.isOverflowAllowed())
            throw new IllegalStateException();
        
        // the name under which the index partition is registered.
        final String name = getOnlyResource();

        // The source view.
        final ILocalBTreeView src = (ILocalBTreeView)getIndex(name);

        // The UUID associated with the scale-out index.
        final UUID indexUUID = src.getIndexMetadata().getIndexUUID();
        
        if (INFO) {

            // note: the mutable btree - accessed here for debugging only.
            final BTree btree = src.getMutableBTree();

            log.info("src=" + name + ",counter=" + src.getCounter().get()
                    + ",checkpoint=" + btree.getCheckpoint());

        }
        
        // Build the index segment.
        final BuildResult result = resourceManager.buildIndexSegment(name, src,
                outFile, true/* compactingMerge */, lastCommitTime,
                null/* fromKey */, null/* toKey */);

        /*
         * @todo error handling should be inside of the atomic update task since
         * it has more visibility into the state changes and when we can no
         * longer delete the new index segment.
         */
        try {
            
            // task will update the index partition view definition.
            final AbstractTask<Void> task = new AtomicReplaceHistoryTask(
                    resourceManager, concurrencyManager, name, indexUUID,
                    result);

            if (INFO)
                log.info("src=" + name + ", will run atomic update task");

            // submit task and wait for it to complete @todo config timeout?
            concurrencyManager.submit(task).get();

            /*
             * Verify that the view was updated. If the atomic update task runs
             * correctly then it will replace the IndexMetadata object on the
             * mutable BTree with a new view containing only the live journal
             * and the new index segment (for a compacting merge). We verify
             * that right now to make sure that the state change to the BTree
             * was noticed and resulted in a commit before returning control to
             * us here.
             * 
             * @todo comment this out or replicate for the index build task also?
             */
            concurrencyManager.submit(
                    new VerifyAtomicUpdateTask(resourceManager,
                            concurrencyManager, name, indexUUID, result)).get();
            
        } catch (Throwable t) {

            // delete the generated index segment.
            resourceManager.deleteResource(result.segmentMetadata.getUUID(),
                    false/* isJournal */);

            // re-throw the exception
            throw new Exception(t);

        }

        return result;

    }

    /**
     * A paranoia test that verifies that the definition of the view was in fact
     * updated.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private class VerifyAtomicUpdateTask extends
            AbstractResourceManagerTask<Void> {

        final protected BuildResult buildResult;

        /**
         * @param resourceManager
         * @param concurrencyManager
         * @param resource
         * @param buildResult
         */
        public VerifyAtomicUpdateTask(ResourceManager resourceManager,
                IConcurrencyManager concurrencyManager, String resource,
                UUID indexUUID, BuildResult buildResult) {

            super(resourceManager, ITx.UNISOLATED, resource);

            if (buildResult == null)
                throw new IllegalArgumentException();

            this.buildResult = buildResult;

            assert resource.equals(buildResult.name);

        }

        /**
         * Verify that the update was correctly registered on the mutable
         * {@link BTree}.
         * 
         * @return <code>null</code>
         */
        @Override
        protected Void doTask() throws Exception {

            if (resourceManager.isOverflowAllowed())
                throw new IllegalStateException();

            final SegmentMetadata segmentMetadata = buildResult.segmentMetadata;

            // the correct view definition.
            final IResourceMetadata[] expected = new IResourceMetadata[] {
                    // the live journal.
                    getJournal().getResourceMetadata(),
                    // the newly built index segment.
                    segmentMetadata
                    };

            /*
             * Open the unisolated B+Tree on the live journal that is absorbing
             * writes and verify the definition of the view.
             */
            final ILocalBTreeView view = (ILocalBTreeView) getIndex(getOnlyResource());

            // The live B+Tree.
            final BTree btree = view.getMutableBTree();

            final LocalPartitionMetadata pmd = btree.getIndexMetadata().getPartitionMetadata();
            
            final IResourceMetadata[] actual = pmd.getResources();
            
            if (expected.length != actual.length) {
                
                throw new RuntimeException("expected=" + expected
                        + ", but actual=" + actual);
                
            }
            
            for(int i=0; i<expected.length; i++) {
                
                if(!expected[i].equals(actual[i])) {

                    throw new RuntimeException("Differs at index=" + i
                            + ", expected=" + expected + ", but actual="
                            + actual);

                }
                
            }
            
            return null;
            
        }

    }
    
    
    /**
     * <p>
     * The source view is pre-overflow (the last writes are on the old journal)
     * while the current view is post-overflow (reflects writes made since
     * overflow). What we are doing is replacing the pre-overflow history with
     * an {@link IndexSegment}.
     * </p>
     * 
     * <pre>
     * journal A
     * view={A}
     * ---- sync overflow begins ----
     * create journal B
     * view={B,A}
     * Begin build segment from view={A} (identified by the lastCommitTime)
     * ---- sync overflow ends ----
     * ... build continues ...
     * ... writes against view={B,A}
     * ... index segment S0 complete (based on view={A}).
     * ... 
     * atomic build update task runs: view={B,S0}
     * ... writes continue.
     * </pre>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class AtomicReplaceHistoryTask extends
            AbstractAtomicUpdateTask<Void> {

        final protected BuildResult buildResult;
        
        /**
         * @param resourceManager 
         * @param concurrencyManager
         * @param resource
         * @param buildResult
         */
        public AtomicReplaceHistoryTask(ResourceManager resourceManager,
                IConcurrencyManager concurrencyManager, String resource,
                UUID indexUUID, BuildResult buildResult) {

            super(resourceManager, ITx.UNISOLATED, resource, indexUUID);

            if(buildResult == null)
                throw new IllegalArgumentException();

            this.buildResult = buildResult;

            assert resource.equals( buildResult.name );
            
        }

        /**
         * <p>
         * Atomic update.
         * </p>
         * 
         * @return <code>null</code>
         */
        @Override
        protected Void doTask() throws Exception {

            if (resourceManager.isOverflowAllowed())
                throw new IllegalStateException();

            final SegmentMetadata segmentMetadata = buildResult.segmentMetadata;

            if (INFO)
                log.info("Begin: name=" + getOnlyResource() + ", newSegment="
                        + segmentMetadata);

            /*
             * Open the unisolated B+Tree on the live journal that is absorbing
             * writes. We are going to update its index metadata.
             * 
             * Note: I am using AbstractTask#getIndex(String name) so that the
             * concurrency control logic will notice the changes to the BTree
             * and cause it to be checkpointed if this task succeeds normally.
             */
            final ILocalBTreeView view = (ILocalBTreeView) getIndex(getOnlyResource());

            // make sure that this is the same scale-out index.
            assertSameIndex(view.getMutableBTree());

            if(view instanceof BTree) {
                
                /*
                 * Note: there is an expectation that this is not a simple BTree
                 * because this the build task is supposed to be invoked after
                 * an overflow event, and that event should have re-defined the
                 * view to include the BTree on the new journal plus the
                 * historical view.
                 * 
                 * One explanation for finding a simple view here is that the
                 * view was a simple BTree on the old journal and the data was
                 * copied from the old journal into the new journal and then
                 * someone decided to do a build even through a copy had already
                 * been done. However, this is not a very good explanation since
                 * we try to avoid doing a build if we have already done a copy!
                 */
                
                throw new RuntimeException("View is only a B+Tree: name="
                        + buildResult.name + ", pmd="
                        + view.getIndexMetadata().getPartitionMetadata());
                
            }
            
            // The live B+Tree.
            final BTree btree = view.getMutableBTree();
            
            if (INFO)
                log.info("src=" + getOnlyResource() + ",counter="
                        + view.getCounter().get() + ",checkpoint="
                        + btree.getCheckpoint());

            assert btree != null : "Expecting index: "+getOnlyResource();
            
            // clone the current metadata record for the live index.
            final IndexMetadata indexMetadata = btree.getIndexMetadata().clone();

            /*
             * This is the index partition definition on the live index - the
             * one that will be replaced with a new view as the result of this
             * atomic update.
             */
            final LocalPartitionMetadata currentpmd = indexMetadata.getPartitionMetadata();

            // Check pre-conditions.
            final IResourceMetadata[] currentResources = currentpmd.getResources();
            {

                if (currentpmd == null) {
                 
                    throw new IllegalStateException("Not an index partition: "
                            + getOnlyResource());
                    
                }

                if (!currentResources[0].getUUID().equals(
                        getJournal().getRootBlockView().getUUID())) {
                 
                    throw new IllegalStateException(
                            "Expecting live journal to be the first resource: "
                                    + currentResources);
                    
                }
                
                /*
                 * Note: I have commented out a bunch of pre-condition tests that are not 
                 * valid for histories such as:
                 * 
                 * history=create() register(0) split(0) copy(entryCount=314)
                 * 
                 * This case arises when there are not enough index entries written on the
                 * journal after a split to warrant a build so the buffered writes are just
                 * copied to the new journal. The resources in the view are:
                 * 
                 * 1. journal
                 * 2. segment
                 * 
                 * And this update will replace the segment. 
                 */
                        
//                // the old journal's resource metadata.
//                final IResourceMetadata oldJournalMetadata = oldResources[1];
//                assert oldJournalMetadata != null;
//                assert oldJournalMetadata instanceof JournalMetadata : "name="
//                        + getOnlyResource() + ", old pmd=" + oldpmd
//                        + ", segmentMetadata=" + buildResult.segmentMetadata;
    //
//                // live journal must be newer.
//                assert journal.getRootBlockView().getCreateTime() > oldJournalMetadata
//                        .getCreateTime();

                // new index segment build from a view that did not include data from the live journal.
                assert segmentMetadata.getCreateTime() < getJournal().getRootBlockView()
                        .getFirstCommitTime();

//                if (oldResources.length == 3) {
    //
//                    // the old index segment's resource metadata.
//                    final IResourceMetadata oldSegmentMetadata = oldResources[2];
//                    assert oldSegmentMetadata != null;
//                    assert oldSegmentMetadata instanceof SegmentMetadata;
    //
//                    assert oldSegmentMetadata.getCreateTime() <= oldJournalMetadata
//                            .getCreateTime();
    //
//                }

            }

            // new view definition.
            final IResourceMetadata[] newResources = new IResourceMetadata[] {
                    // the live journal.
                    getJournal().getResourceMetadata(),
                    // the newly built index segment.
                    segmentMetadata
                    };

            // describe the index partition.
            indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(//
                    currentpmd.getPartitionId(),//
                    currentpmd.getSourcePartitionId(),//
                    currentpmd.getLeftSeparatorKey(),//
                    currentpmd.getRightSeparatorKey(),//
                    newResources, //
                    currentpmd.getHistory()+
                    "compactingMerge"//
                    +"(lastCommitTime="+ segmentMetadata.getCreateTime()//
                    +",segment="+ segmentMetadata.getUUID()//
                    +",counter="+btree.getCounter().get()//
                    +",oldResources="+Arrays.toString(currentResources)
                    +") "
                    ));

            // update the metadata associated with the btree
            btree.setIndexMetadata(indexMetadata);

            if (INFO)
                log.info("Updated view: name=" + getOnlyResource() + ", pmd="
                        + indexMetadata.getPartitionMetadata());
            
            /*
             * Verify that the btree recognizes that it needs to be
             * checkpointed.
             * 
             * Note: The atomic commit point is when this task commits.
             */
            assert btree.needsCheckpoint();
//            btree.writeCheckpoint();
//            {
//                final long id0 = btree.getCounter().get();
//                final long pid = id0 >> 32;
//                final long mask = 0xffffffffL;
//                final int ctr = (int) (id0 & mask);
//                log.warn("name="+getOnlyResource()+", counter="+id0+", pid="+pid+", ctr="+ctr);
//            }

            // notify successful index partition build.
            resourceManager.compactingMergeCounter.incrementAndGet();
            
            return null;

        }

    }

}
