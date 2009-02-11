package com.bigdata.resources;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.service.Event;

/**
 * Task builds an {@link IndexSegment} from the mutable {@link BTree} and zero
 * or more additional sources in the index partition view and then atomically
 * updates the view (aka an incremental build).
 * <p>
 * Build uses mutable {@link BTree} of the lastCommitTime for the old journal
 * PLUS ZERO OR MORE additional source(s) taken in view order up to but not
 * including the source in the view with significant content. This let's us keep
 * the #of {@link IndexSegment}s in the view down without incurring the cost of
 * a compacting merge. (The cost of the compacting merge itself comes from
 * having a large index segment in the view, generally in the last position of
 * the view.) In turn, this keeps the cost of overflow down and can be a
 * significant win if there are a number of large index partitions that receive
 * a few writes in each overflow.
 * <p>
 * For example, assuming a large index segment exists from a previous compacting
 * merge, then once the #of writes exceeds the "copy" threshold there will be an
 * index build. The view will then have [live, smallSeg1, largeSeg1]. The next
 * time the copy threshold is exceeded we would get [live, smallSeg2, smallSeg1,
 * largeSeg1]. However if we include smallSeg1 in the build, then we get [live,
 * smallSeg2, largeSeg1]. This can continue until we have enough data to warrant
 * a split or until we have another "large" segment but not yet enough data to
 * split, at which point we get [live, largeSeg2, largeSeg1] and then [live,
 * smallSeg3, largeSeg2, largeSeg1].
 * <p>
 * Note: As its last action, this task submits a
 * {@link AtomicUpdateIncrementalBuildTask} which replaces the view with one
 * defined by the current {@link BTree} on the journal and the newly built
 * {@link IndexSegment}.
 * <p>
 * Note: If the task fails, then the output {@link IndexSegment} will be
 * deleted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IncrementalBuildTask extends AbstractPrepareTask<BuildResult> {

    final protected ViewMetadata vmd;

    /**
     * The file on which the {@link IndexSegment} will be written.
     */
    final protected File outFile;

    /**
     * The event corresponding to the build action.
     */
    final private Event e;
    
    /**
     * The source view.
     */
    BTree src;

    /**
     * @param vmd
     *            Metadata about the index partition view.
     */
    public IncrementalBuildTask(final ViewMetadata vmd) {

        super(vmd.resourceManager, TimestampUtility
                .asHistoricalRead(vmd.commitTime), vmd.name);

        this.vmd = vmd;

        // the file to be generated.
        this.outFile = resourceManager.getIndexSegmentFile(vmd.indexMetadata);

        this.e = new Event(resourceManager.getFederation(), vmd.name,
                OverflowActionEnum.Build, OverflowActionEnum.Build + "("
                        + vmd.name + ") : " + vmd);

        /*
         * Put a hard reference hold on the btree.
         * 
         * Note: This could be too aggressive if the data service is memory
         * starved, but it will help us to finish the index segment builds as
         * quickly as possible.
         */
        this.src = vmd.getBTree();
        
        /*
         * Release soft references to the full view and the mutable btree on the
         * ViewMetadata object (we are relying on the reference that we made
         * above).
         */  
        vmd.clearRef();
        
    }

    @Override
    protected void clearRefs() {

        // release soft references.
        vmd.clearRef();

        // release hard reference.
        src = null;
        
    }

//    final int BUILD_MAX_JOURNAL_COUNT = 3;
//
//    final long BUILD_MAX_SUM_ENTRY_COUNT = Bytes.megabyte * 10;

    /**
     * Build an {@link IndexSegment} from the compacting merge of an index
     * partition.
     * 
     * @return The {@link BuildResult}.
     */
    protected BuildResult doTask() throws Exception {

        e.start();

        try {

            final BuildResult result;
            try {

                if (resourceManager.isOverflowAllowed())
                    throw new IllegalStateException();

                // index partition name.
                final String name = vmd.name;

                /*
                 * The source view.
                 */
                if (INFO) {

                    log.info("src=" + name + ", counter="
                            + src.getCounter().get() + ", checkpoint="
                            + src.getCheckpoint());

                }

                /*
                 * Figure out which sources we want to include. We MUST always
                 * include the 1st source in the view since that is the mutable
                 * BTree on the old journal. We continue to include sources in
                 * the view until we find the first "large" source. We stop
                 * there because the goal is to keep down the #of components in
                 * the view without doing the heavy work of processing a large
                 * source (winds up copying a lot of tuples). We only do that
                 * additional work on a compacting merge.
                 */
                final AbstractBTree[] sources = src.getSources();

                final List<AbstractBTree> accepted = new ArrayList<AbstractBTree>(
                        sources.length);
                
                // the mutable BTree on the old journal.
                accepted.add( sources[ 0 ] );

                int journalCount = 1;
                long sumEntryCount = sources[0].getEntryCount();
                long sumSegBytes = 0L;
                for (int i = 1; i < sources.length; i++) {

                    final AbstractBTree s = sources[i];

                    sumEntryCount += s.getEntryCount();

                    if (s instanceof IndexSegment) {

                        final IndexSegment seg = (IndexSegment) s;

                        sumSegBytes += seg.getStore().size();

                    } else {
                        
                        journalCount++;
                        
                    }

//                    if (journalCount > BUILD_MAX_JOURNAL_COUNT)
//                        break;
//
//                    if (sumEntryCount > BUILD_MAX_SUM_ENTRY_COUNT)
//                        break;

                    if (sumSegBytes > resourceManager.maximumBuildSegmentBytes)
                        break;

                    // accept another source into the view for the build.
                    accepted.add(s);
                    
                }
                
                /*
                 * Note: If ALL sources are accepted, then we are actually doing
                 * a compacting merge and we set the flag appropriately!
                 */
                final boolean compactingMerge = accepted.size() == sources.length;

                // Build the index segment.
                result = resourceManager.buildIndexSegment(name, src, outFile,
                        compactingMerge, vmd.commitTime, null/* fromKey */,
                        null/* toKey */, e);

            } finally {

                /*
                 * Release our hold on the source index partition view. We only
                 * needed it during the the index partition build.
                 */

                clearRefs();

            }

            /*
             * @todo error handling should be inside of the atomic update task
             * since it has more visibility into the state changes and when we
             * can no longer delete the new index segment.
             */
            try {

                // task will update the index partition view definition.
                final AbstractTask<Void> task = new AtomicUpdateIncrementalBuildTask(
                        resourceManager, concurrencyManager, vmd.name,
                        vmd.indexMetadata.getIndexUUID(), result);

                final Event updateEvent = e.newSubEvent(
                        OverflowSubtaskEnum.AtomicUpdate,
                        (result.compactingMerge ? OverflowActionEnum.Merge
                                : OverflowActionEnum.Build)
                                + "(" + vmd.name + ") : " + vmd).start();
                
                try {

                    // submit task and wait for it to complete
                    concurrencyManager.submit(task).get();

                } finally {

                    updateEvent.end();
                    
                }

            } catch (Throwable t) {

                // delete the generated index segment.
                resourceManager
                        .deleteResource(result.segmentMetadata.getUUID(), false/* isJournal */);

                // re-throw the exception
                throw new Exception(t);

            }

            return result;

        } finally {

            e.end();

        }

    }

    /**
     * <p>
     * The source is an {@link IndexSegment} that was built from the mutable
     * {@link BTree} associated with the lastCommitTime on old journal of some
     * index partition. What we are doing is replacing the role of that
     * {@link BTree} on the closed out journal with the {@link IndexSegment}.
     * Note that the {@link IndexSegment} contains the same data as the
     * {@link BTree} as of the lastCommitTime. The new view (as defined by this
     * task) will be selected when the desired view is GTE the lastCommitTime.
     * The old view will be used whenever the desired view is LT the
     * lastCommitTime.
     * </p>
     * 
     * <pre>
     * journal A
     * view={A,...}
     * ---- sync overflow begins ----
     * create journal B
     * view={B,A,...}
     * Begin incremental build of segment from A (just the BTree state as identified by the lastCommitTime)
     * ---- sync overflow ends ----
     * ... build continues ...
     * ... writes against view={B,A,...} are written on B.
     * ... index segment S0 complete (based on A).
     * ... 
     * atomic update task runs: view={B,S0,...}
     * ... writes continue.
     * </pre>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class AtomicUpdateIncrementalBuildTask extends
            AbstractAtomicUpdateTask<Void> {

        /**
         * The expected UUID of the scale-out index.
         */
        final protected UUID indexUUID;
        
        final protected BuildResult buildResult;
        
        /**
         * @param resourceManager 
         * @param concurrencyManager
         * @param resource
         * @param buildResult
         */
        public AtomicUpdateIncrementalBuildTask(ResourceManager resourceManager,
                IConcurrencyManager concurrencyManager, String resource,
                UUID indexUUID, BuildResult buildResult) {

            super(resourceManager, ITx.UNISOLATED, resource);

            if(indexUUID == null)
                throw new IllegalArgumentException();
            
            if(buildResult == null)
                throw new IllegalArgumentException();

            this.indexUUID = indexUUID;
            
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
            final ILocalBTreeView view = (ILocalBTreeView)getIndex(getOnlyResource());

            // The live B+Tree.
            final BTree btree = view.getMutableBTree();
            
            // make sure that we are working with the same index.
            assertSameIndex(indexUUID, btree);
            
            if(view instanceof BTree) {
                
                /*
                 * Note: there is an expectation that this is not a simple BTree
                 * because this the build task is supposed to be invoked after
                 * an overflow event, and that event should have re-defined the
                 * view to include the BTree on the new journal plus the
                 * historical view.
                 * 
                 * One explanation for finding a simple view here is that the
                 * old index was deleted and a new one created in its place. We
                 * check that above.
                 */
                
                throw new RuntimeException("View is only a B+Tree: name="
                        + buildResult.name + ", pmd="
                        + view.getIndexMetadata().getPartitionMetadata());
                
            }
            
            if (INFO)
                log.info("src=" + getOnlyResource() + ", counter="
                        + view.getCounter().get() + ", checkpoint="
                        + btree.getCheckpoint());
            
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

                /*
                 * verify that there are at least two resources in the current
                 * view:
                 * 
                 * 1. currentResources[0] is the mutable BTree on the live
                 * journal
                 * 
                 * 2. currentResources[1] is the mutable BTree on the old
                 * journal (since closed out for writes so it is no longer
                 * mutable).
                 */
                
                if (currentResources.length < 2) {
                    
                    throw new IllegalStateException(
                            "Expecting at least 2 resources in the view: "
                                    + Arrays.toString(currentResources));
                    
                }
                
                if (!currentResources[0].getUUID().equals(
                        getJournal().getRootBlockView().getUUID())) {
                 
                    throw new IllegalStateException(
                            "Expecting live journal to be the first resource: "
                                    + currentResources);
                    
                }
                
                /*
                 * verify that the 2nd resource in the view is the BTree on the
                 * old journal [this only verifies that the 2nd resource in the
                 * view is also on a journal].
                 */
                if (!currentResources[1].isJournal()) {
                 
                    throw new IllegalStateException(
                            "Expecting live journal to be the first resource: "
                                    + currentResources);
                    
                }

                /*
                 * verify that the new index segment was built from a view that
                 * did not include data from the live journal.
                 */
                assert segmentMetadata.getCreateTime() < getJournal().getRootBlockView()
                        .getFirstCommitTime();
                
            }

            // new view definition.
            final IResourceMetadata[] newResources;
            {
                
                final List<IResourceMetadata> newView = new LinkedList<IResourceMetadata>();

                // the live journal.
                newView.add(getJournal().getResourceMetadata());

                /*
                 * The newly built index segment. This was built from at least
                 * one source, but it MAY have been built from more than one
                 * source.
                 */
                newView.add(segmentMetadata);

                /*
                 * The rest of the components of the old view.
                 * 
                 * Note: We start copying resources into the view AFTER the last
                 * source which was included in the view used to generate the
                 * index segment.
                 * 
                 * For example, if the index segment was built from a single
                 * journal (the old journal), then [startIndex := 1 + 1 == 2].
                 * So we retain resources in the current view start at
                 * currentResources[2].
                 * 
                 * If there are 3 sources in the current view (new journal, old
                 * journal, and an index segment) and the sourceCount was 2 then
                 * then build was actually a compacting merge and [startIndex :=
                 * 1 + 2 == 3]. Since 3 EQ currentResources.length we will not
                 * include ANY sources from the old view. This is the semantics
                 * of a compacting merge. All data in the view is captured by
                 * the data on the live journal and the newly built index
                 * segment [live, newSeg].
                 */
                
                final int startIndex = 1 + buildResult.sourceCount;
                
                for (int i = startIndex; i < currentResources.length; i++) {

                    newView.add(currentResources[i]);

                }

                newResources = (IResourceMetadata[]) newView
                        .toArray(new IResourceMetadata[] {});

            }

            // describe the index partition.
            indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(//
                    currentpmd.getPartitionId(),//
                    currentpmd.getSourcePartitionId(),//
                    currentpmd.getLeftSeparatorKey(),//
                    currentpmd.getRightSeparatorKey(),//
                    newResources, //
                    currentpmd.getHistory()+
                    OverflowActionEnum.Build//
                    +"(lastCommitTime="+ segmentMetadata.getCreateTime()//
                    +",segment="+ segmentMetadata.getUUID()//
                    +",#buildSources="+buildResult.sourceCount//
                    +",merge="+buildResult.compactingMerge//
                    +",counter="+btree.getCounter().get()//
                    +",oldResources="+Arrays.toString(currentResources)
                    +") "
                    ));
            
            // update the metadata associated with the btree
            btree.setIndexMetadata(indexMetadata);

            if (INFO)
                log.info("Updated view: name=" + getOnlyResource() + ", pmd="
                        + indexMetadata.getPartitionMetadata()
                        + "\noldResources=" + Arrays.toString(currentResources)
                        + "\nnewResources=" + Arrays.toString(newResources));
            
            /*
             * Verify that the btree recognizes that it needs to be
             * checkpointed.
             * 
             * Note: The atomic commit point is when this task commits.
             */
            assert btree.needsCheckpoint();

            // notify successful index partition build.
            // @todo could count as a compacting merge if we used all sources.
            resourceManager.indexPartitionBuildCounter.incrementAndGet();
            
            return null;

        }

    }

}
