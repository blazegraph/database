package com.bigdata.resources;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;

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

    final private ViewMetadata vmd;

    /**
     * @param vmd
     *            Metadata about the index partition view.
     */
    public IncrementalBuildTask(final ViewMetadata vmd) {

        super(vmd.resourceManager, TimestampUtility
                .asHistoricalRead(vmd.commitTime), vmd.name);

        this.vmd = vmd;
        
    }

    @Override
    protected void clearRefs() {

        // release soft references.
        vmd.clearRef();
        
    }

    /**
     * Build an {@link IndexSegment} from one or more sources for an index
     * partition view. The sources are chosen in view order. New sources are
     * incorporated until too much work would be performed for the lightweight
     * semantics of "build". If all sources are incorporated by the build, then
     * the result is identical a compacting merge.
     * 
     * @return The {@link BuildResult}.
     */
    protected BuildResult doTask() throws Exception {

        final Event e = new Event(resourceManager.getFederation(),
                new EventResource(vmd.indexMetadata), OverflowActionEnum.Build,
                vmd.getParams()).start();

        BuildResult buildResult = null;
        try {

            if (resourceManager.isOverflowAllowed())
                throw new IllegalStateException();

            try {

                /*
                 * Figure out which sources will be used in the build operation.
                 * The sources are chosen in order. The first source is always
                 * a BTree on a journal and is always in the accepted view.
                 * 
                 * Note: The order of the sources MUST be maintained. This
                 * ensures that the generated index segment will preserve only
                 * the most recently written tuple (or delete marker) for each
                 * tuple in the accepted view. We are only permitted to purge
                 * deleted tuples when all sources are accepted in the build
                 * view since that is the only time we have a guarantee that
                 * there is not a delete version of that tuple further back in
                 * history which would reemerge if we dropped the delete marker.
                 */
                final BuildViewMetadata buildViewMetadata = new BuildViewMetadata(
                        vmd.getView(),
                        resourceManager.maximumBuildSegmentBytes, e);

                e.addDetails(buildViewMetadata.getParams());
                
                if(INFO)
                    log.info("acceptedView: " + buildViewMetadata);
                
                /*
                 * Build the index segment from a view comprised of just the
                 * accepted sources.
                 */
                buildResult = resourceManager.buildIndexSegment(vmd.name,
                        buildViewMetadata.acceptedView,
                        buildViewMetadata.compactingMerge, vmd.commitTime,
                        null/* fromKey */, null/* toKey */, e);

                e.addDetails(buildResult.getParams());
                
                if (buildResult.sourceCount != buildViewMetadata.naccepted) {

                    throw new AssertionError("Build result has "
                            + buildResult.sourceCount + ", but expected "
                            + buildViewMetadata.naccepted + " : acceptedView="
                            + buildViewMetadata + ", buildResult=" + buildResult);
                    
                }

                if (INFO)
                    log.info("buildResult=" + buildResult);

                {

                    /*
                     * Verify that the resource manager can open the new index
                     * segment. This provides verification both that the index
                     * segment is registered with the store manager and that the
                     * index segment can be read. However, we do not actually
                     * read the leaves of the index segment here so there still
                     * could be errors on the disk.
                     */

                    final IndexSegmentStore segStore = (IndexSegmentStore) resourceManager
                            .openStore(buildResult.segmentMetadata.getUUID());

                    assert segStore != null;

                    if (INFO)
                        log.info("indexSegmentStore="
                                + segStore.loadIndexSegment());

                }
                
            } finally {

                /*
                 * Release our hold on the source index partition view. We only
                 * needed it during the the index partition build.
                 */

                clearRefs();

            }

            if (buildResult.compactingMerge
                    && buildResult.builder.getCheckpoint().length >= resourceManager.nominalShardSize) {

                /*
                 * If a compacting merge was performed and sumSegBytes exceeds the
                 * threshold, then do a split here just as if CompactingMerge was
                 * run instead.
                 * 
                 * Note: This is unlikely since build does not accept sources if
                 * they would cause a lot of work. The most likely reasons why this
                 * would happen would be a single index partition on the journal
                 * which receives all writes or the journal size is a healthy
                 * multiple of the target shard size.
                 */

                // FIXME reconcile return type and enable post-merge split.
//                return new SplitCompactViewTask(vmd.name, buildResult);

            }            
            
            try {

                /*
                 * Submit task that will update the definition of the index
                 * partition view and wait for it to complete.
                 */
                concurrencyManager.submit(
                        new AtomicUpdateIncrementalBuildTask(resourceManager,
                                concurrencyManager, vmd.name, vmd.indexMetadata
                                        .getIndexUUID(), buildResult, e)).get();

            } catch (Throwable t) {

                // make it releasable.
                resourceManager.retentionSetRemove(buildResult.segmentMetadata
                        .getUUID());

                // delete the generated index segment.
                resourceManager
                        .deleteResource(buildResult.segmentMetadata.getUUID(), false/* isJournal */);

                // re-throw the exception
                throw new Exception(t);

            }

            return buildResult;

        } finally {

            if (buildResult != null) {

                /*
                 * At this point the index segment was either incorporated into
                 * the new view in a restart safe manner or there was an error.
                 * Either way, we now remove the index segment store's UUID from
                 * the retentionSet so it will be subject to the release policy
                 * of the StoreManager.
                 */
                resourceManager.retentionSetRemove(buildResult.segmentMetadata
                        .getUUID());

            }

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
            AbstractAtomicUpdateTask<IResourceMetadata[]> {

        /**
         * The expected UUID of the scale-out index.
         */
        final protected UUID indexUUID;
        
        final protected BuildResult buildResult;
        
        final private Event parentEvent;
        
        /**
         * @param resourceManager 
         * @param concurrencyManager
         * @param resource
         * @param buildResult
         */
        public AtomicUpdateIncrementalBuildTask(ResourceManager resourceManager,
                IConcurrencyManager concurrencyManager, String resource,
                UUID indexUUID, BuildResult buildResult, Event parentEvent) {

            super(resourceManager, ITx.UNISOLATED, resource);

            if(indexUUID == null)
                throw new IllegalArgumentException();
            
            if(buildResult == null)
                throw new IllegalArgumentException();

            if (!resource.equals(buildResult.name))
                throw new IllegalArgumentException();

            if (parentEvent == null)
                throw new IllegalArgumentException();

            this.indexUUID = indexUUID;
            
            this.buildResult = buildResult;
            
            this.parentEvent = parentEvent;
            
        }

        /**
         * <p>
         * Atomic update.
         * </p>
         * 
         * @return The ordered array of resources that define the post-condition
         *         view.
         */
        @Override
        protected IResourceMetadata[] doTask() throws Exception {

            // populated with the description of the ordered sources of the new view.
            final List<IResourceMetadata> newView = new LinkedList<IResourceMetadata>();

            /*
             * Note: The event is labeled a "build" even if all sources
             * participate in the build. This makes it easier to identify the
             * compacting merges in the events log. The compacting merges are of
             * interest since they are only triggered when the #of sources in
             * the view grows too large and they require more effort. By
             * contrast, some "builds" will in fact be compacting merges, but
             * they were selected as builds and they are compacting merges by
             * virtue of having so little work to do that it is cheaper to use
             * all sources in the view and thereby postpone a more intensive
             * compacting merge somewhat longer.
             */
            final Map<String, Object> v = buildResult.getParams();
            v.put("summary", OverflowActionEnum.Build + "(" + buildResult.name
                    + ")");
            final Event updateEvent = parentEvent.newSubEvent(
                    OverflowSubtaskEnum.AtomicUpdate).start();

            try {

                if (resourceManager.isOverflowAllowed())
                    throw new IllegalStateException();

                final SegmentMetadata segmentMetadata = buildResult.segmentMetadata;

                if(INFO)
                    log.info(buildResult.toString());

                /*
                 * Open the unisolated B+Tree on the live journal that is
                 * absorbing writes. We are going to update its index metadata.
                 * 
                 * Note: I am using AbstractTask#getIndex(String name) so that
                 * the concurrency control logic will notice the changes to the
                 * BTree and cause it to be checkpointed if this task succeeds
                 * normally.
                 */
                final ILocalBTreeView view = getIndex(getOnlyResource());

                // The live B+Tree.
                final BTree btree = view.getMutableBTree();

                // make sure that we are working with the same index.
                assertSameIndex(indexUUID, btree);

                if (view instanceof BTree) {

                    /*
                     * Note: there is an expectation that this is not a simple
                     * BTree because this the build task is supposed to be
                     * invoked after an overflow event (or a view checkpoint),
                     * and that event should have re-defined the view to include
                     * the BTree on the new journal plus the historical view.
                     * 
                     * One explanation for finding a simple view here is that
                     * the old index was deleted and a new one created in its
                     * place. We check that above.
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
                final IndexMetadata indexMetadata = btree.getIndexMetadata()
                        .clone();

                /*
                 * This is the index partition definition on the live index -
                 * the one that will be replaced with a new view as the result
                 * of this atomic update.
                 */
                final LocalPartitionMetadata currentpmd = indexMetadata
                        .getPartitionMetadata();

                if (currentpmd == null) {

                    throw new IllegalStateException(
                            "Not an index partition: " + getOnlyResource());

                }

                // Check pre-conditions.
                final IResourceMetadata[] currentResources = currentpmd
                        .getResources();
                {

                    /*
                     * verify that there are at least two resources in the
                     * current view:
                     * 
                     * 1. currentResources[0] is the mutable BTree on the live
                     * journal
                     * 
                     * 2. currentResources[1] is either the BTree on the old
                     * journal (since closed out for writes so it is no longer
                     * mutable) or a previous snapshot of the mutable BTree
                     * decoupled from the mutable BTree by a view checkpoint
                     * operation.
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
                                        + Arrays.toString(currentResources));

                    }

                    /*
                     * verify that the 2nd resource in the view is also a BTree
                     * on a journal.
                     */
                    if (!currentResources[1].isJournal()) {

                        throw new IllegalStateException(
                                "Expecting live journal to be the first resource: "
                                        + Arrays.toString(currentResources));

                    }

// Note: This constraint does not apply when a view checkpoint was used.
//                    /*
//                     * Verify that the new index segment was built from a view
//                     * that did not include data from the live journal.
//                     */
//                    if (segmentMetadata.getCreateTime() >= getJournal()
//                            .getRootBlockView().getFirstCommitTime()) {
//
//                        throw new AssertionError(
//                                "IndexSegment includes data from the live journal?");
//
//                    }

                }

                // new view definition.
                final IResourceMetadata[] newResources;
                {

                    // the live journal.
                    newView.add(getJournal().getResourceMetadata());

                    /*
                     * The newly built index segment. This was built from at
                     * least one source, but it MAY have been built from more
                     * than one source.
                     */
                    newView.add(segmentMetadata);

                    /*
                     * The rest of the components of the old view.
                     * 
                     * Note: We start copying resources into the view AFTER the
                     * last source which was included in the view used to
                     * generate the index segment.
                     * 
                     * For example, if the index segment was built from a single
                     * journal (the old journal), then [startIndex := 1 + 1 ==
                     * 2]. So we retain resources in the current view start at
                     * currentResources[2].
                     * 
                     * If there are 3 sources in the current view (new journal,
                     * old journal, and an index segment) and the sourceCount
                     * was 2 then then build was actually a compacting merge and
                     * [startIndex := 1 + 2 == 3]. Since 3 EQ
                     * currentResources.length we will not include ANY sources
                     * from the old view. This is the semantics of a compacting
                     * merge. All data in the view is captured by the data on
                     * the live journal and the newly built index segment [live,
                     * newSeg].
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
                        currentpmd.getIndexPartitionCause(),
                        currentpmd.getHistory()
                                + OverflowActionEnum.Build//
                                + "(lastCommitTime="
                                + segmentMetadata.getCreateTime()//
                                + ",segment="
                                + segmentMetadata.getUUID()//
                                + ",#buildSources="
                                + buildResult.sourceCount//
                                + ",merge="
                                + buildResult.compactingMerge//
                                + ",counter="
                                + btree.getCounter().get()//
                                + ",oldResources="
                                + Arrays.toString(currentResources) + ") "));

                // update the metadata associated with the btree
                btree.setIndexMetadata(indexMetadata);

                if (INFO)
                    log.info("Updated view: name=" + getOnlyResource()
                            + ", pmd=" + indexMetadata.getPartitionMetadata()
                            + toString("oldResources", currentResources)
                            + toString("newResources", newResources));

                /*
                 * Verify that the btree recognizes that it needs to be
                 * checkpointed.
                 * 
                 * Note: The atomic commit point is when this task commits.
                 */
                assert btree.needsCheckpoint();

                /*
                 * Update counter to reflect successful index partition build.
                 * 
                 * Note: All build tasks are reported as builds so that we can
                 * readily distinguish the tasks which were selected as
                 * compacting merges from those which were selected as builds.
                 * If you want to see how many tasks were "effective" compacting
                 * merges (because all sources were used) then you need to look
                 * at the events log for the indexSegmentBuild operation.
                 */
                resourceManager.indexPartitionBuildCounter.incrementAndGet();

                updateEvent.addDetail("newView", newView.toString());
                
                return newResources;

            } finally {

                updateEvent.end();

            }

        } // doTask()

    } // AtomicUpdate

}
