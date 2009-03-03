package com.bigdata.resources;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.DataService;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;
import com.bigdata.service.Split;
import com.bigdata.sparse.SparseRowStore;

/**
 * Task splits an index partition and should be invoked when there is strong
 * evidence that an index partition has grown large enough to be split into 2 or
 * more index partitions, each of which should be 50-75% full.
 * <p>
 * The task reads from the lastCommitTime of the old journal after an overflow.
 * It uses a key range scan to sample the index partition, building an ordered
 * set of {key,offset} tuples. Based on the actual #of index entries and the
 * target #of index entries per index partition, it chooses the #of output index
 * partitions, N, and selects N-1 {key,offset} tuples to split the index
 * partition. If the index defines a constraint on the split rule, then that
 * constraint will be applied to refine the actual split points, e.g., so as to
 * avoid splitting a logical row of a {@link SparseRowStore}.
 * <p>
 * Once the N-1 split points have been selected, N index segments are built -
 * one from each of the N key ranges which those N-1 split points define. Once
 * the index segment for each split has been built, an
 * {@link AtomicUpdateSplitIndexPartitionTask} will atomically re-define the
 * source index partition as N new index partition. During the atomic update the
 * original index partition becomes un-defined and new index partitions are
 * defined in its place which span the same total key range and have the same
 * data.
 * 
 * @see AtomicUpdateSplitIndexPartitionTask, which MUST be invoked in order to
 *      update the index partition definitions on the live journal and the
 *      {@link MetadataIndex} as an atomic operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SplitIndexPartitionTask extends
        AbstractPrepareTask<AbstractResult> {

    protected final ViewMetadata vmd;

    protected final UUID[] moveTargets;
    
    /**
     * The split handler to be applied. Note that this MAY have been overriden
     * in order to promote a split so you MUST use this instance and NOT the one
     * in the {@link IndexMetadata} object.
     */
    private final ISplitHandler splitHandler;
    
    /**
     * @param vmd
     * @param moveTarget
     *            When non-<code>null</code> the new right-sibling (the tail)
     *            will be moved to the specified data service after the split.
     */
    protected SplitIndexPartitionTask(final ViewMetadata vmd,
            final UUID moveTarget) {

        this(vmd, (moveTarget == null ? null : new UUID[] { moveTarget }));
        
    }

    /**
     * 
     * @param vmd
     * @param moveTargets
     *            When non-<code>null</code> the index partitions generated
     *            by the split will be moved to the identified data services. If
     *            this data service is included in the array, then an index
     *            partition will be left on this data service. If the array
     *            contains a single element, then only the rightSibling of the
     *            split will be moved.
     */
    protected SplitIndexPartitionTask(final ViewMetadata vmd,
            final UUID[] moveTargets) {

        super(vmd.resourceManager, TimestampUtility
                .asHistoricalRead(vmd.commitTime), vmd.name);

        if (vmd == null)
            throw new IllegalArgumentException(); 
        
        this.vmd = vmd;

        if (vmd.pmd == null) {

            throw new IllegalStateException("Not an index partition.");

        }

        if (vmd.pmd.getSourcePartitionId() != -1) {

            throw new IllegalStateException(
                    "Split not allowed during move: sourcePartitionId="
                            + vmd.pmd.getSourcePartitionId());

        }

        if (moveTargets != null) {

            if (moveTargets.length == 0)
                throw new IllegalArgumentException();

            if (moveTargets.length == 1
                    && moveTargets[0] == resourceManager.getDataServiceUUID()) {
                // can't specify this data service as the sole target for a move.
                throw new IllegalArgumentException();
            }
            
            for(UUID t : moveTargets) {
                
                if(t == null)
                    throw new IllegalArgumentException();
                
            }
            
        }
        
        this.moveTargets = moveTargets;
        
        this.splitHandler = vmd.getAdjustedSplitHandler();

        if (splitHandler == null) {
            
            // This was checked as a pre-condition and should not be null.
            
            throw new AssertionError();
            
        }

    }

    @Override
    protected void clearRefs() {
        
        vmd.clearRef();
        
    }
    
    /**
     * Decides how many index partitions should be generated (N) and builds N
     * {@link IndexSegment}s from the source index partition. If N will be ONE
     * (1) if a detailed inspection of the source index partition reveals that
     * it SHOULD NOT be split.
     * 
     * @return A {@link SplitResult} if the index partition was split into 2 or
     *         more index partitions -or- a {@link BuildResult} iff the index
     *         partition was not split.
     */
    @Override
    protected AbstractResult doTask() throws Exception {

        final Event e = new Event(resourceManager.getFederation(),
                new EventResource(vmd.indexMetadata), OverflowActionEnum.Split,
                OverflowActionEnum.Split
                        + (moveTargets != null ? "+" + OverflowActionEnum.Move
                                : "") + "(" + vmd.name + ") : " + vmd
                        + ", moveTargets=" + Arrays.toString(moveTargets))
                .start();

        try {

            final SplitResult result;
            try {

                if (resourceManager.isOverflowAllowed())
                    throw new IllegalStateException();

                final String name = vmd.name;

                // Note: fused view for the source index partition.
                final ILocalBTreeView src = vmd.getView();
                // final ILocalBTreeView src = (ILocalBTreeView)getIndex(name);

                if (INFO) {

                    // note: the mutable btree - accessed here for debugging
                    // only.
                    final BTree btree = src.getMutableBTree();

                    log.info("src=" + name + ",counter="
                            + src.getCounter().get() + ",checkpoint="
                            + btree.getCheckpoint());

                }

                /*
                 * Get the split points for the index. Each split point
                 * describes a new index partition. Together the split points
                 * MUST exactly span the source index partitions key range.
                 * There MUST NOT be any overlap in the key ranges for the
                 * splits.
                 */
                
                Split[] splits;
                try {

                    splits = splitHandler.getSplits(resourceManager, src);

                } catch (Throwable t) {

                    if (PostProcessOldJournalTask.isNormalShutdown(
                            resourceManager, t)) {

                        /*
                         * This looks like an exception arising from the normal
                         * shutdown of the data service so we return
                         * immediately. As of this point we have not had any
                         * side effects on the data service.
                         */

                        log.warn("Normal shutdown? : " + t);

                        return null;

                    }

                    /*
                     * Note: this makes the asynchronous overflow more robust to
                     * a failure in the split handler. However, if the split
                     * handler never succeeds then the index will never get
                     * split and it will eventually dominate the data service on
                     * which it resides.
                     */

                    log.error(
                            "Split handler failure - will do build instead: name="
                                    + name + " : " + t, t);

                    splits = null;

                }

                if (splits == null) {

                    /*
                     * No splits were choosen so the index will not be split at
                     * this time.
                     */

                    if (moveTargets != null && moveTargets.length >= 1) {

                        // There is a move target, so move the index partition.

                        log.warn("No splits identified: will move: " + vmd);

                        return concurrencyManager.submit(
                                new MoveIndexPartitionTask(vmd, moveTargets[0]))
                                .get();

                    } else if (vmd.manditoryMerge) {

                        // Manditory compacting merge.

                        log.warn("No splits identified: will merge: " + vmd);

                        return concurrencyManager.submit(
                                new CompactingMergeTask(vmd)).get();

                    } else {

                        // Incremental build.

                        log.warn("No splits identified: will build: " + vmd);

                        return concurrencyManager.submit(
                                new IncrementalBuildTask(vmd)).get();

                    }

                }

                // The #of splits.
                final int nsplits = splits.length;

                if (INFO)
                    log.info("Will build index segments for " + nsplits
                            + " splits for " + name + " : "
                            + Arrays.toString(splits));

                // validate the splits before processing them.
                SplitUtility.validateSplits(src, splits);

                result = SplitUtility.buildSplits(vmd, splits, e);

            } finally {

                /*
                 * We are done building index segments from the source index
                 * partition view so we clear our references for that view.
                 */

                clearRefs();

            }

            /*
             * Do the atomic update
             */
            doSplitAtomicUpdate(resourceManager, vmd, result,
                    OverflowActionEnum.Split,
                    resourceManager.indexPartitionSplitCounter, e);

            if (moveTargets != null) {
                
                /*
                 * Note: Unlike a normal move where there are writes on the old
                 * journal, all the historical data for the each of the index
                 * partitions is in an index segment that we just built (new
                 * writes MAY be buffered on the live journal, so we still have
                 * to deal with that). Therefore we use a different entry point
                 * into the MOVE operation, one which does not copy over the
                 * data from the old journal but will still copy over any
                 * buffered writes.
                 */

                if (moveTargets.length == 1) {

                    /*
                     * This handles the case where only one move target was
                     * specified.  In this case it is NOT permitted for the
                     * move target to be this data service (this condition
                     * is checked by the ctor).
                     */
                    
                    /*
                     * Find the split whose newly built index partition has the
                     * smallest size.
                     */
                    final int bestMoveIndex;
                    {
                        int indexOfMinLength = -1;
                        long minLength = Long.MAX_VALUE;
                        for (int i = 0; i < result.buildResults.length; i++) {
                            final BuildResult r = result.buildResults[i];
                            // #of bytes in that index segment.
                            final long length = r.builder.getCheckpoint().length;
                            if (length < minLength) {
                                indexOfMinLength = i;
                                minLength = length;
                            }
                        }
                        assert indexOfMinLength != -1 : result.toString();
                        bestMoveIndex = indexOfMinLength;
                        if (INFO)
                            log.info("Best split to move: "
                                    + result.splits[bestMoveIndex]);
                    }

                    /*
                     * The name of the post-split index partition that is the
                     * source for the move operation.
                     */
                    final String nameOfPartitionToMove = DataService
                            .getIndexPartitionName(vmd.indexMetadata.getName(),
                                    result.splits[bestMoveIndex].pmd
                                            .getPartitionId());

                    /*
                     * Obtain a new partition identifier for the partition that
                     * will be created when we move the index partition to the
                     * target data service.
                     */
                    final int newPartitionId = resourceManager
                            .nextPartitionId(vmd.indexMetadata.getName());

                    // register the new index partition.
                    final MoveResult moveResult = MoveIndexPartitionTask
                            .registerNewPartitionOnTargetDataService(
                                    resourceManager, moveTargets[0],
                                    nameOfPartitionToMove, newPartitionId, e);

                    /*
                     * Move the buffered writes since the split and go live with
                     * the new index partition.
                     */
                    MoveIndexPartitionTask.moveBufferedWritesAndGoLive(
                            resourceManager, moveResult, e);

                } else {

                    /*
                     * This handles the case where multiple move targets were
                     * specified. For this case, it is allowable for one of the
                     * move targets to be this data service, in which case we
                     * simply leaf the corresponding index partition in place.
                     */

                    final int nsplits = result.buildResults.length;

                    for (int i = 0; i < nsplits; i++) {

                        final UUID moveTarget = moveTargets[i
                                % moveTargets.length];
                        
                        if (moveTarget == resourceManager.getDataServiceUUID()) {

                            // ignore move to self.
                            if(INFO)
                                log.info("Ignoring move to self.");
                            continue;
                            
                        }
                        
                        /*
                         * The name of the post-split index partition that is
                         * the source for the move operation.
                         */
                        final String nameOfPartitionToMove = DataService
                                .getIndexPartitionName(vmd.indexMetadata
                                        .getName(), result.splits[i].pmd
                                        .getPartitionId());

                        /*
                         * Obtain a new partition identifier for the partition
                         * that will be created when we move the index partition
                         * to the target data service.
                         */
                        final int newPartitionId = resourceManager
                                .nextPartitionId(vmd.indexMetadata.getName());

                        // register the new index partition.
                        final MoveResult moveResult = MoveIndexPartitionTask
                                .registerNewPartitionOnTargetDataService(
                                        resourceManager, moveTarget,
                                        nameOfPartitionToMove, newPartitionId,
                                        e);

                        /*
                         * Move the buffered writes since the split and go live with
                         * the new index partition.
                         */
                        MoveIndexPartitionTask.moveBufferedWritesAndGoLive(
                                resourceManager, moveResult, e);

                    }

                }
                
            }

            // Done.
            return result;
            
        } finally {

            e.end();

        }

    }

    /**
     * 
     * @param resourceManager
     * @param vmd
     * @param splits
     * @param result
     * @param action
     * @param counter
     * @param parentEvent
     */
    static protected void doSplitAtomicUpdate(
            final ResourceManager resourceManager, final ViewMetadata vmd,
            final SplitResult result,
            final OverflowActionEnum action,
            final AtomicLong counter,
            final Event parentEvent) {

        try {

            /*
             * Form up the set of resources on which the atomic update task
             * must have an exclusive lock before it can run. This includes
             * both the source index partition and the name of each new
             * index partition which will be generated by this split.
             * 
             * Note: We MUST declare the resource locks for the indices that
             * we are going to create in order to prevent tasks from
             * accessing those indices until the atomic update task has
             * committed. Note that the metadata index will be updated
             * before the atomic update task commits, so it is possible (and
             * does in fact happen) for clients to submit tasks that wind up
             * directed to one of the new index partitions before the atomic
             * update task commits.
             */
            final Split[] splits = result.splits;
            final String[] resources = new String[splits.length + 1];
            {

                resources[0] = result.name;

                int i = 0;

                for (final Split split : splits) {

                    final int partitionId = split.pmd.getPartitionId();

                    resources[i + 1] = DataService.getIndexPartitionName(
                            vmd.indexMetadata.getName(), partitionId);

                    i++;

                }

            }

            /*
             * Create task that will perform atomic update, converting the
             * source index partition into N new index partitions.
             */
            final AbstractTask<Void> task = new AtomicUpdateSplitIndexPartitionTask(
                    resourceManager, resources, action, vmd.indexMetadata
                            .getIndexUUID(), result, parentEvent.newSubEvent(
                            OverflowSubtaskEnum.AtomicUpdate, action + "("
                                    + vmd.name + "->"
                                    + Arrays.toString(resources) + ") : src="
                                    + vmd));

            // submit atomic update task and wait for it to complete
            resourceManager.getConcurrencyManager().submit(task).get();
            
            // update the counter.
            counter.incrementAndGet();

            return;

        } catch (Throwable t) {

            /*
             * Error handling - remove all generated files.
             * 
             * @todo error handling should be in the atomic update task since it
             * has greater visibility into when the resources are incorporated
             * into a view and hence accessible to concurrent processes.
             */

            for (BuildResult r : result.buildResults) {

                if (r == null)
                    continue;

                resourceManager.deleteResource(r.segmentMetadata.getUUID(),
                        false/* isJournal */);

            }

            throw new RuntimeException(t);

        }

    }

    /**
     * An {@link ITx#UNISOLATED} operation that splits the live index using the
     * same {@link Split} points, generating new index partitions with new
     * partition identifiers. The old index partition is deleted as a
     * post-condition. The new index partitions are registered as a
     * post-condition. Any data that was accumulated in the live index on the
     * live journal is copied into the appropriate new {@link BTree} for the new
     * index partition on the live journal.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class AtomicUpdateSplitIndexPartitionTask extends
            AbstractAtomicUpdateTask<Void> {


        /**
         * The expected UUID of the scale-out index.
         */
        final protected UUID indexUUID;
        
        /**
         * Either a normal split or a tail split.
         */
        protected final OverflowActionEnum action;
        
        protected final SplitResult splitResult;

        private final Event updateEvent;
        
        /**
         * 
         * @param resourceManager
         * @param resource
         * @param action
         * @param indexUUID The UUID of the scale-out index.
         * @param splitResult
         */
        public AtomicUpdateSplitIndexPartitionTask(
                final ResourceManager resourceManager, final String[] resource,
                final OverflowActionEnum action, final UUID indexUUID,
                final SplitResult splitResult, final Event updateEvent) {

            super(resourceManager, ITx.UNISOLATED, resource);

            if (action == null)
                throw new IllegalArgumentException();

            if (indexUUID == null)
                throw new IllegalArgumentException();

            if (splitResult == null)
                throw new IllegalArgumentException();

            if (updateEvent == null)
                throw new IllegalArgumentException();

            this.action = action;

            this.indexUUID = indexUUID;
            
            this.splitResult = splitResult;

            this.updateEvent = updateEvent;
            
        }

        /**
         * Atomic update.
         * 
         * @return <code>null</code>.
         */
        @Override
        protected Void doTask() throws Exception {

            updateEvent.start();

            try {

                if (resourceManager.isOverflowAllowed())
                    throw new IllegalStateException();

                // The name of the scale-out index.
                final String scaleOutIndexName = splitResult.indexMetadata
                        .getName();

                // the name of the source index.
                final String name = splitResult.name;

                /*
                 * Note: the source index is the BTree on the live journal that
                 * has been absorbing writes since the last overflow (while the
                 * split was running asynchronously).
                 * 
                 * This is NOT a fused view. All we are doing is re-distributing
                 * the buffered writes onto the B+Trees buffering writes for the
                 * new index partitions created by the split.
                 */
                final BTree src = ((ILocalBTreeView) getIndex(name))
                        .getMutableBTree();

                assertSameIndex(indexUUID, src);

                if (INFO) {

                    log.info("src=" + name + ", counter="
                            + src.getCounter().get() + ", checkpoint="
                            + src.getCheckpoint());

                    log.info("src=" + name + ", splitResult=" + splitResult);

                }

                // the value of the counter on the source BTree.
                final long oldCounter = src.getCounter().get();

                /*
                 * Locators for the new index partitions.
                 */

                final LocalPartitionMetadata oldpmd = (LocalPartitionMetadata) src
                        .getIndexMetadata().getPartitionMetadata();

                if (oldpmd.getSourcePartitionId() != -1) {

                    throw new IllegalStateException(
                            "Split not allowed during move: sourcePartitionId="
                                    + oldpmd.getSourcePartitionId());

                }

                final Split[] splits = splitResult.splits;

                final PartitionLocator[] locators = new PartitionLocator[splits.length];

                for (int i = 0; i < splits.length; i++) {

                    // new metadata record (cloned).
                    final IndexMetadata md = src.getIndexMetadata().clone();

                    final LocalPartitionMetadata pmd = (LocalPartitionMetadata) splits[i].pmd;

                    assert pmd.getResources() == null : "Not expecting resources for index segment: "
                            + pmd;

                    // the new partition identifier.
                    final int partitionId = pmd.getPartitionId();

                    // name of the new index partition.
                    final String name2 = DataService.getIndexPartitionName(
                            scaleOutIndexName, partitionId);

                    /*
                     * form locator for the new index partition for this split..
                     */
                    final PartitionLocator locator = new PartitionLocator(pmd
                            .getPartitionId(),//
                            /*
                             * The (logical) data service.
                             * 
                             * @todo The index partition data will be replicated
                             * at the byte image level for the live journal.
                             * 
                             * @todo New index segment resources must be
                             * replicated as well.
                             * 
                             * @todo Once the index partition data is fully
                             * replicated we update the metadata index.
                             */
                            resourceManager.getDataServiceUUID(),//
                            pmd.getLeftSeparatorKey(),//
                            pmd.getRightSeparatorKey()//
                    );

                    locators[i] = locator;

                    final String summary = action + "(" + name + "->" + name2
                            + ")";

                    /*
                     * Update the view definition.
                     */
                    md
                            .setPartitionMetadata(new LocalPartitionMetadata(
                                    pmd.getPartitionId(),//
                                    -1, // Note: Split not allowed during move.
                                    pmd.getLeftSeparatorKey(),//
                                    pmd.getRightSeparatorKey(),//
                                    new IResourceMetadata[] {//
                                            /*
                                             * Resources are (a) the new btree;
                                             * and (b) the new index segment.
                                             */
                                            resourceManager.getLiveJournal()
                                                    .getResourceMetadata(),
                                            splitResult.buildResults[i].segmentMetadata },
                                    /*
                                     * Note: history is record of the split.
                                     */
                                    pmd.getHistory() + summary + " ")//
                            );

                    /*
                     * create new btree.
                     * 
                     * Note: the lower 32-bits of the counter will be zero. The
                     * high 32-bits will be the partition identifier assigned to
                     * the new index partition.
                     */
                    final BTree btree = BTree.create(resourceManager
                            .getLiveJournal(), md);

                    // make sure the partition identifier was asserted.
                    assert partitionId == btree.getIndexMetadata()
                            .getPartitionMetadata().getPartitionId();

                    final long newCounter = btree.getCounter().get();

                    /*
                     * Note: this is true because partition identifiers always
                     * increase and the partition identifier is placed into the
                     * high word of the counter value for an index partition.
                     */

                    assert newCounter > oldCounter : "newCounter=" + newCounter
                            + " not GT oldCounter=" + oldCounter;

                    // lower bound (inclusive) for copy.
                    final byte[] fromKey = pmd.getLeftSeparatorKey();

                    // upper bound (exclusive) for copy.
                    final byte[] toKey = pmd.getRightSeparatorKey();

                    if (INFO)
                        log.info("Copying data to new btree: index="
                                + scaleOutIndexName + ", pmd=" + pmd);

                    /*
                     * Copy all data in this split from the source index.
                     * 
                     * Note: [overflow := false] since the btrees are on the
                     * same backing store.
                     */
                    final long ncopied = btree.rangeCopy(src, fromKey, toKey,
                            false/* overflow */);

                    if (INFO)
                        log.info("Copied " + ncopied
                                + " index entries from the live index " + name
                                + " onto " + name2);

                    // register it on the live journal

                    if (INFO)
                        log.info("Registering index: " + name2);

                    getJournal().registerIndex(name2, btree);

                }

                // drop the source index (the old index partition)

                if (INFO)
                    log.info("Dropping source index: " + name);

                getJournal().dropIndex(name);

                /*
                 * Notify the metadata service that the index partition has been
                 * split.
                 */
                resourceManager.getFederation().getMetadataService()
                        .splitIndexPartition(src.getIndexMetadata().getName(),//
                                new PartitionLocator(//
                                        oldpmd.getPartitionId(), //
                                        resourceManager.getDataServiceUUID(), //
                                        oldpmd.getLeftSeparatorKey(),//
                                        oldpmd.getRightSeparatorKey()//
                                ), locators);

                if (INFO)
                    log.info("Notified metadata service: name=" + name
                            + " was split into " + Arrays.toString(locators));

                // will notify tasks that index partition was split.
                resourceManager.setIndexPartitionGone(name,
                        StaleLocatorReason.Split);

                return null;

            } finally {

                updateEvent.end();

            }
            
        } // doTask()
        
    } // class AtomicUpdate
    
}
