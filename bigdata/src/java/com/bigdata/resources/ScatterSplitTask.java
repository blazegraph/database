package com.bigdata.resources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.ISimpleSplitHandler;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.resources.SplitIndexPartitionTask.AtomicUpdateSplitIndexPartitionTask;
import com.bigdata.service.DataService;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;
import com.bigdata.service.Split;
import com.bigdata.sparse.SparseRowStore;

/**
 * Task splits an index partition into N equal sized index partitions and
 * scatters those index partitions across data services in the federation.
 * Unlike a normal split, this MAY result in index partitions which are under
 * the nominal minimum size requirements. The purpose of a scatter split is to
 * rapidly redistribute an index partition across the federation in order to
 * increase both the potential concurrency of operations on that index partition
 * and to permit more resources to be brought to bear on the index partition.
 * The "equal" splits are achieved by an "adjustment" to the split handler.
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
 * @see DefaultSplitHandler#getAdjustedSplitHandlerForEqualSplits(int, long)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ScatterSplitTask extends
        AbstractPrepareTask<AbstractResult> {

    protected final ViewMetadata vmd;

    /**
     * The #of index partitions that will be generated when we split the source
     * index partition.
     */
    protected final int nsplits;
    
    /**
     * An array of move targets for the new index partitions. The index
     * partitions will be assigned to the move targets using a round robin
     * process. If one of the move targets is this data service, then the
     * corresponding index partition will not be moved.
     */
    protected final UUID[] moveTargets;

    /**
     * The target size of a shard for the scatter split. This is computed by
     * dividing the size of the compact segment on the disk by the #of desired
     * splits.
     */
    protected final long adjustedNominalShardSize;
    
    /**
     * 
     * @param vmd
     *            The metadata for the index partition to be split.
     * @param nsplits
     *            The index will be split into this many index partitions
     *            without regard to the #of tuples in each split.
     * @param moveTargets
     *            An array of move targets for the new index partitions. The
     *            index partitions will be assigned to the move targets using a
     *            round robin process. If one of the move targets is this data
     *            service, then the corresponding index partition will not be
     *            moved.
     */
    protected ScatterSplitTask(final ViewMetadata vmd, final int nsplits,
            final UUID[] moveTargets) {

        super(vmd.resourceManager, TimestampUtility
                .asHistoricalRead(vmd.commitTime), vmd.name);

        if (vmd == null)
            throw new IllegalArgumentException(); 
        
        this.vmd = vmd;

        if (vmd.pmd == null) {

            throw new IllegalStateException("Not an index partition.");

        }
        
        if(!vmd.compactView) {
            
            throw new IllegalStateException("Not a compact view.");
            
        }

        if (vmd.pmd.getSourcePartitionId() != -1) {

            throw new IllegalStateException(
                    "Split not allowed during move: sourcePartitionId="
                            + vmd.pmd.getSourcePartitionId());

        }

        if (nsplits <= 1)
            throw new IllegalArgumentException();

        if (moveTargets != null) {

            if (moveTargets.length == 0)
                throw new IllegalArgumentException();

            for(UUID t : moveTargets) {
                
                if(t == null)
                    throw new IllegalArgumentException();
                
            }
            
        }
        
        this.nsplits = nsplits;
        
        this.moveTargets = moveTargets;

        this.adjustedNominalShardSize = vmd.sumSegBytes / (nsplits / 2);

    }

    @Override
    protected void clearRefs() {
        
        vmd.clearRef();
        
    }
    
    /**
     * Breaks the index partition into N splits, where N was specified to the
     * ctor, and redistributes those splits onto the move targets using a round
     * robin.
     * 
     * @return A {@link SplitResult} if the index partition was split into 2 or
     *         more index partitions -or- a {@link BuildResult} iff the index
     *         partition was not split.
     */
    @Override
    protected AbstractResult doTask() throws Exception {

        final Event e = new Event(resourceManager.getFederation(),
                new EventResource(vmd.indexMetadata), OverflowActionEnum.ScatterSplit,
                vmd.getParams()).addDetail(
                "summary",
                OverflowActionEnum.ScatterSplit + "+" + OverflowActionEnum.Move + "("
                        + vmd.name + ", nsplits=" + nsplits + ")").addDetail(
                "moveTargets", Arrays.toString(moveTargets)).start();

        SplitResult splitResult = null;
        try {

            if (resourceManager.isOverflowAllowed())
                throw new IllegalStateException();

            try {

                final String name = vmd.name;

                // Note: fused view for the source index partition.
                final ILocalBTreeView src = vmd.getView();

                /*
                 * Get the split points for the index. Each split point
                 * describes a new index partition. Together the split points
                 * MUST exactly span the source index partitions key range.
                 * There MUST NOT be any overlap in the key ranges for the
                 * splits.
                 */

                // The application split handler (if any).
                final ISimpleSplitHandler splitHandler = vmd.indexMetadata
                        .getSplitHandler();

                final Split[] splits = SplitUtility.getSplits(resourceManager,
                        vmd.pmd, (IndexSegment) src.getSources()[1],
                        adjustedNominalShardSize, splitHandler);

                if (splits == null) {

                    final double overextension = ((double) vmd.sumSegBytes)
                            / resourceManager.nominalShardSize;

                    if (overextension > resourceManager.shardOverextensionLimit
                            && !resourceManager.isDisabledWrites(vmd.name)) {

                        /*
                         * The shard is overextended (it is at least two times
                         * its nominal maximum size) and is refusing a split.
                         * Continuing to do incremental builds here will mask
                         * the problem and cause the cost of a merge on the
                         * shard to increase over time and will drag down
                         * performance for this DS. In order to prevent this we
                         * MUST disallow further writes on the shard. The shard
                         * can be re-enabled for writes by an administrative
                         * action once the problem has been fixed.
                         * 
                         * Note: The default split behavior should always find a
                         * separator key to split the shard. The mostly likely
                         * cause for a problem is an application defined split
                         * handler. Rather than allowing a poorly written split
                         * handler to foul up the works, we disallow further
                         * writes onto this shard until the application has
                         * fixed their split handler.
                         */

                        log.error("Shard will not split - writes are disabled"
                                + ": name="
                                + vmd.name
                                + ", size="
                                + vmd.sumSegBytes
                                + ", overextended="
                                + (int) overextension
                                + "x"
                                + ", splitHandler="
                                + (splitHandler == null ? "N/A" : splitHandler
                                        .getClass().getName()));

                        // Disable writes on the index partition.
                        resourceManager.disableWrites(vmd.name);
                        
                    }
                    
                    /*
                     * Do an incremental build.
                     */

                    log.warn("No splits identified: will build: " + vmd);

                    // Incremental build.
                    return concurrencyManager.submit(
                            new IncrementalBuildTask(vmd)).get();

                }

                // The #of splits.
                final int nsplits = splits.length;

                if (INFO)
                    log.info("Will build index segments for " + nsplits
                            + " splits for " + name + " : "
                            + Arrays.toString(splits));

                // validate the splits before processing them.
                SplitUtility.validateSplits(src, splits);

                splitResult = SplitUtility.buildSplits(vmd, splits, e);

            } finally {

                /*
                 * We are done building index segments from the source index
                 * partition view so we clear our references for that view.
                 */

                clearRefs();

            }

            /*
             * Do the atomic update.
             */
            SplitIndexPartitionTask
                    .doSplitAtomicUpdate(
                            resourceManager,
                            vmd,
                            splitResult,
                            OverflowActionEnum.ScatterSplit,
                            resourceManager.overflowCounters.indexPartitionSplitCounter,
                            e);

            /*
             * Note: Unlike a normal move where there are writes on the old
             * journal, all the historical data for the each of the index
             * partitions is in an index segment that we just built (new
             * writes MAY be buffered on the live journal, so we still have
             * to deal with that). Therefore we use a different entry point
             * into the MOVE operation.
             * 
             * Note: It is allowable for one of the move targets to be this
             * data service, in which case we simply leave the corresponding
             * index partition in place.
             */

            final int nsplits = splitResult.buildResults.length;

            final List<MoveTask.AtomicUpdate> moveTasks = new ArrayList<MoveTask.AtomicUpdate>(
                    nsplits);

            // create the move tasks.
            {

                for (int i = 0; i < nsplits; i++) {

                    // choose the move target using a round robin.
                    final UUID moveTarget = moveTargets[i % moveTargets.length];

                    if (resourceManager.getDataServiceUUID().equals(moveTarget)) {

                        // ignore move to self.
                        if (INFO)
                            log.info("Ignoring move to self.");
                        continue;

                    }

                    /*
                     * Obtain a new partition identifier for the partition that
                     * will be created when we move the index partition to the
                     * target data service.
                     */
                    final int newPartitionId = resourceManager
                            .nextPartitionId(vmd.indexMetadata.getName());

                    /*
                     * The name of the post-split index partition that is the
                     * source for the move operation.
                     */
                    final String nameOfPartitionToMove = DataService
                            .getIndexPartitionName(vmd.indexMetadata.getName(),
                                    splitResult.splits[i].pmd.getPartitionId());

                    /*
                     * Create a move task.
                     * 
                     * Note: We do not explicitly delete the source index
                     * segment for the source index partition after the move. It
                     * will be required for historical views of the that index
                     * partition in case any client gained access to the index
                     * partition after the split and before the move. It will
                     * eventually be released once the view of the source index
                     * partition becomes sufficiently aged that it falls off the
                     * head of the database history.
                     */
                    moveTasks.add(new MoveTask.AtomicUpdate(resourceManager,
                            nameOfPartitionToMove, splitResult.buildResults[i],
                            moveTarget, newPartitionId, e));

                }

            }

            /*
             * Submit the move tasks to executed in parallel and await their
             * outcomes.
             */
            final List<Future<MoveResult>> futures = resourceManager
                    .getConcurrencyManager().invokeAll(moveTasks);
            
            /*
             * Log error if any move task failed (other than being canceled).
             */
            for (Future<?> f : futures) {

                if (!f.isCancelled()) {

                    try {

                        f.get();

                    } catch (ExecutionException ex) {

                        // log and continue.
                        log.error(ex, ex);

                    }

                }
                
            }
            
            // Done.
            return splitResult;

        } finally {

            if (splitResult != null) {

                for (BuildResult buildResult : splitResult.buildResults) {

                    if (buildResult != null) {

                        /*
                         * At this point the index segment was either incorporated into
                         * the new view in a restart safe manner or there was an error.
                         * Either way, we now remove the index segment store's UUID from
                         * the retentionSet so it will be subject to the release policy
                         * of the StoreManager.
                         */
                        resourceManager
                                .retentionSetRemove(buildResult.segmentMetadata
                                        .getUUID());

                    }

                }

            }

            e.end();

        }

    }

}
