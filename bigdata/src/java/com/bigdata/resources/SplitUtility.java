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
 * Created on Feb 6, 2009
 */

package com.bigdata.resources;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.ISimpleSplitHandler;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.Leaf;
import com.bigdata.btree.Node;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.service.Event;
import com.bigdata.service.Split;
import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * Utility methods for {@link ISimpleSplitHandler}s and friends.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SplitUtility {

    protected static final Logger log = Logger.getLogger(SplitUtility.class);
    
    /**
     * Validate splits, including: that the separator keys are strictly
     * ascending, that the separator keys perfectly cover the source key range
     * without overlap, that the rightSeparator for each split is the
     * leftSeparator for the prior split, that the fromIndex offsets are
     * strictly ascending, etc.
     * 
     * @param src
     *            The source index.
     * @param splits
     *            The recommended split points.
     * 
     * @throws IllegalArgumentException
     *             if either argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the source index is not an index partition (if
     *             {@link IndexMetadata#getPartitionMetadata() returns <code>null</code>).
     */
    static public void validateSplits(final IIndex src, final Split[] splits) {

        if (src == null)
            throw new IllegalArgumentException();

        if (splits == null)
            throw new IllegalArgumentException();

        final LocalPartitionMetadata pmd = src.getIndexMetadata()
                .getPartitionMetadata();

        if (pmd == null)
            throw new IllegalArgumentException();
        
        validateSplits(pmd, splits, true/* checkFromToIndex */);
        
    }

    /**
     * Validate splits, including: that the separator keys are strictly
     * ascending, that the separator keys perfectly cover the source key range
     * without overlap, that the rightSeparator for each split is the
     * leftSeparator for the prior split, that the fromIndex offsets are
     * strictly ascending, etc.
     * 
     * @param originalPartitionMetadata
     *            The description of the key range of the index partition.
     * @param splits
     *            The recommended split points.
     * @param checkFromToIndex
     *            If the {@link Split#fromIndex}, {@link Split#toIndex} and
     *            {@link Split#ntuples} fields should be validated.
     *            
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     */
    static public void validateSplits(
            final LocalPartitionMetadata originalPartitionMetadata,
            final Split[] splits,
            final boolean checkFromToIndex) {
    
        if (originalPartitionMetadata == null)
            throw new IllegalArgumentException();
    
        if (splits == null)
            throw new IllegalArgumentException("splits[] is null.");
    
        final int nsplits = splits.length;
    
        if (nsplits <= 1)
            throw new AssertionError(
                    "Expecting at least two splits, but found " + nsplits);
    
        // verify splits obey index order constraints.
        int lastToIndex = -1;
    
        // Note: the first leftSeparator must be this value.
        byte[] fromKey = originalPartitionMetadata.getLeftSeparatorKey();
    
        for (int i = 0; i < nsplits; i++) {
    
            final Split split = splits[i];
    
            if (split == null)
                throw new AssertionError();
    
            if(split.pmd == null)
                throw new AssertionError();
    
            if(!(split.pmd instanceof LocalPartitionMetadata))
                throw new AssertionError();
    
            final LocalPartitionMetadata pmd = (LocalPartitionMetadata) split.pmd;
    
            // check the leftSeparator key.
            if(pmd.getLeftSeparatorKey() == null)
                throw new AssertionError();
            if(!BytesUtil.bytesEqual(fromKey, pmd.getLeftSeparatorKey()))
                throw new AssertionError();
    
            // verify rightSeparator is ordered after the left
            // separator.
            if(pmd.getRightSeparatorKey() != null) {
                if(BytesUtil.compareBytes(fromKey, pmd
                            .getRightSeparatorKey()) >= 0)
                    throw new AssertionError();
            }
    
            // next expected leftSeparatorKey.
            fromKey = pmd.getRightSeparatorKey();
    
            if (checkFromToIndex) {
            
                if (i == 0) {
    
                    if (split.fromIndex != 0)
                        throw new AssertionError();
    
                    if (split.toIndex <= split.fromIndex)
                        throw new AssertionError();
    
                } else {
    
                    if (split.fromIndex != lastToIndex)
                        throw new AssertionError();
    
                }
    
                if (i + 1 == nsplits && split.toIndex == 0) {
    
                    /*
                     * Note: This is allowed in case the index partition has
                     * more than int32 entries in which case the toIndex of the
                     * last split can not be defined and will be zero.
                     */
    
                    if (split.ntuples != 0)
                        throw new AssertionError();
    
                    log.warn("Last split has no definate tuple count");
    
                } else {
    
                    if (split.toIndex - split.fromIndex != split.ntuples)
                        throw new AssertionError();
    
                }
    
            }
            
            lastToIndex = split.toIndex;
    
        }
    
        /*
         * verify left separator key for 1st partition is equal to the left
         * separator key of the source (this condition is also checked
         * above).
         */
        if (!BytesUtil.bytesEqual(originalPartitionMetadata
                .getLeftSeparatorKey(), splits[0].pmd.getLeftSeparatorKey())) {
    
            throw new AssertionError("leftSeparator[0]"
                    + //
                    ": expected="
                    + BytesUtil.toString(originalPartitionMetadata
                            .getLeftSeparatorKey())
                    + //
                    ", actual="
                    + BytesUtil.toString(splits[0].pmd.getLeftSeparatorKey()));
            
        }
    
        /*
         * verify right separator key for last partition is equal to the
         * right separator key of the source.
         */
        {
            
            // right separator for the last split.
            final byte[] rightSeparator = ((LocalPartitionMetadata) splits[splits.length - 1].pmd)
                    .getRightSeparatorKey();
            
            if(rightSeparator == null ) {
                
                // if null then the source right separator must have been null.
                if (originalPartitionMetadata.getRightSeparatorKey() != null)
                    throw new AssertionError(
                            "rightSeparator for lastSplit: expected="
                                    + BytesUtil
                                            .toString(originalPartitionMetadata
                                                    .getRightSeparatorKey())
                                    + ", actual=null");
                
            } else {

                // otherwise must compare as equals byte-by-byte.
                if (!rightSeparator.equals(originalPartitionMetadata
                        .getRightSeparatorKey()))
                    throw new AssertionError(
                            "rightSeparator for lastSplit: expected="
                                    + BytesUtil
                                            .toString(originalPartitionMetadata
                                                    .getRightSeparatorKey())
                                    + ", actual="
                                    + BytesUtil.toString(rightSeparator));
                
            }
            
        }
    
    }

    /**
     * Identifies the splits for an index with heavy write append behavior.
     * <p>
     * The split point is chosen by locating the right-most non-leaf node. The
     * key range which would enter that node is placed within the new
     * right-sibling index partition (the tail). The rest of the key range is
     * placed within the new left-sibling index partition (the head).
     * <p>
     * 
     * @param btree
     *            The {@link BTree}.
     * 
     * @return The splits (split[0] is the head split, split[1] is the tail
     *         split).
     */
    public static Split[] tailSplit(final ResourceManager resourceManager,
            final BTree btree) {

        if (resourceManager == null)
            throw new IllegalArgumentException();

        if (btree == null)
            throw new IllegalArgumentException();

        if (btree.getHeight() == 0) {
            
            throw new IllegalArgumentException("B+Tree is only a root leaf.");
            
        }

        // The name of the scale-out index.
        final String name = btree.getIndexMetadata().getName();
        
        // The metadata for the index partition that is being split. 
        final LocalPartitionMetadata oldpmd = btree.getIndexMetadata().getPartitionMetadata();
        
        if (oldpmd == null) {
            
            throw new RuntimeException("Not an index partition?");
            
        }
        
        /*
         * We need to choose a key that will separate the head and the tail and
         * also identify the index of the last key that will enter into the
         * head. We do this using the right-most node (not a leaf) in the
         * mutable BTree loaded from the last commit time on the old journal.
         * 
         * First we choose a Leaf which is a child of that node. The one in the
         * middle is choosen as a decent guess at where we might split the index
         * in order to leave some activity on the head but most activity on the
         * tail.
         * 
         * Then we choose a specific key in the leaf which will be the first key
         * NOT copied into the head. For simplicity, we choose the first key in
         * this leaf since it is always defined.
         */

        final Node node = (Node) btree.getRightMostNode(true/* nodesOnly */);
        
        // choose the leaf that is in the center of the nodes children.
//        final int childIndex = (node.getChildCount() + 1) / 2;
        
        // choose the first leaf that is a child of this node.
        final int childIndex = 0;
        
        // leaf from the middle of the leaves of the node.
        final Leaf leaf = (Leaf) node.getChild(childIndex);
        
        // separator key is the first key in the leaf.
        final byte[] separatorKey = leaf == null ? null : leaf.getKeys()
                .get(0/* index */);

        if (leaf == null || separatorKey == null) {

            /*
             * Note: I have never seen a problem here.
             */
            
            throw new RuntimeException("Could not locate separator key? Node="
                    + node
                    + ", nchildren="
                    + node.getChildCount()
                    + ", childIndex="
                    + childIndex
                    + ", leaf="
                    + leaf
                    + (leaf == null ? "" : ("nkeys=" + leaf.getKeyCount()
                            + ", keys=" + leaf.getKeys())));
            
        }
        
        // The index within the btree of the tuple associated with that key.
        final int separatorIndex = btree.indexOf(separatorKey);

        /*
         * The key must exist since we just discovered it. even if the tuple is
         * deleted, indexOf should return its index and not its insertion point.
         */
        assert separatorIndex >= 0;

        /*
         * Ready to define the splits.
         */
        final Split[] splits = new Split[2];
        {

            /*
             * Head split.
             */
            
            // New partition identifier.
            final int partitionId = resourceManager.nextPartitionId(name);

            // Note: always assign the leftSeparator to the head split.
            final byte[] fromKey = oldpmd.getLeftSeparatorKey();

            final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                    partitionId, //
                    -1, // Note: split not allowed during move.
                    fromKey,//
                    separatorKey,//
                    /*
                     * Note: no resources for an index segment
                     */
                    null,//
                    /*
                     * Note: cause will be set by the atomic update task.
                     */
                    null//
//                    , oldpmd.getHistory()
//                            + "chooseTailSplitPoint(oldPartitionId="
//                            + oldpmd.getPartitionId() + ",nsplits=" + 2
//                            + ",newPartitionId=" + partitionId + ") "
                            );

            final int fromIndex = 0;

            splits[0] = new Split(pmd, fromIndex, separatorIndex);

        }

        {

            /*
             * Tail split.
             */
            
            // New partition identifier.
            final int partitionId = resourceManager.nextPartitionId(name);

            // Note: always assign the rightSeparator to the tail split.
            final byte[] toKey = oldpmd.getRightSeparatorKey();

            final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                    partitionId,//
                    -1, // Note: split not allowed during move.
                    separatorKey,//
                    toKey,//
                    /*
                     * Note: no resources for an index segment
                     */
                    null,//
                    /*
                     * Note: Cause will be set by the atomic update for the
                     * split task.
                     */
                    null//
//                    , oldpmd.getHistory()
//                            + "chooseTailSplitPoint(oldPartitionId="
//                            + oldpmd.getPartitionId() + ",nsplits=" + 2
//                            + ",newPartitionId=" + partitionId + ") "
                    );

            /*
             * Note: The index of the last tuple in the btree will be the
             * entryCount of the B+Tree. We want one beyond that last tuple
             * since this is the index of the first tuple NOT to be included in
             * the tail split.
             */
            
            splits[1] = new Split(pmd, separatorIndex, btree.getEntryCount());

        }

        return splits;

    }

    /**
     * Build N index segments based on those split points.
     * <p>
     * Note: This is done in parallel to minimize latency.
     * <p>
     * Note: The generated {@link IndexSegment}s are on the retentionSet and
     * MUST be removed from that set once it has been incorporated in a restart
     * safe manner into an index partition view or once the task fails.
     * 
     * @see StoreManager#retentionSetAdd(java.util.UUID)
     * 
     * @throws InterruptedException
     * @throws ExecutionExceptions
     * 
     * @todo The operation could be serialized (or run with limited parallelism)
     *       in order to minimize the RAM burden for buffers during index
     *       segment creation. You can also limit the parallelism to some upper
     *       bound. During normal operations, the #of splits generated should be
     *       fairly small, e.g., N >= 2 and N ~ 2. This requires a thread pool
     *       (or delegate for a thread pool) that can impose a limit on the
     *       actual parallelism.
     */
    public static SplitResult buildSplits(final ViewMetadata vmd,
            final Split[] splits, final Event parentEvent)
            throws InterruptedException, ExecutionExceptions {

        if (vmd == null)
            throw new IllegalArgumentException();

        if (splits == null)
            throw new IllegalArgumentException();
        
        final int nsplits = splits.length;
        
        final List<BuildIndexSegmentSplitTask> tasks = new ArrayList<BuildIndexSegmentSplitTask>(
                nsplits);

        for (int i = 0; i < splits.length; i++) {

            final Split split = splits[i];

            /*
             * Create task to build an index segment from the key-range
             * for the split.
             */
            final BuildIndexSegmentSplitTask task = new BuildIndexSegmentSplitTask(
                    vmd, split, parentEvent);

            // add to set of tasks to be run.
            tasks.add(task);

        }

        // submit and await completion.
        final List<Future<BuildResult>> futures = vmd.resourceManager
                .getConcurrencyManager().invokeAll(tasks);

        // copy the individual build results into an array.
        final BuildResult[] buildResults = new BuildResult[nsplits];
        final List<Throwable> causes = new LinkedList<Throwable>();
        {

            int i = 0;
            for (Future<BuildResult> f : futures) {

                try {

                    buildResults[i] = f.get();

                } catch (Throwable t) {

                    causes.add(t);

                    log.error(t.getLocalizedMessage());

                }

                // increment regardless of the task outcome.
                i++;

            }

        }

        if (!causes.isEmpty()) {

            /*
             * Error handling - remove all generated files.
             */

            for (BuildResult result : buildResults) {

                if (result == null)
                    continue;

                // make it releasable.
                vmd.resourceManager.retentionSetRemove(result.segmentMetadata
                        .getUUID());

                // delete it.
                vmd.resourceManager.deleteResource(result.segmentMetadata
                        .getUUID(), false/* isJournal */);

            }

            // throw wrapped set of exceptions.
            throw new ExecutionExceptions(causes);

        }

        if (log.isInfoEnabled())
            log.info("Generated " + splits.length
                    + " index segments: name=" + vmd.name);

        // form the split result.
        final SplitResult result = new SplitResult(vmd.name,
                vmd.indexMetadata, splits, buildResults);

        return result;
        
    }
    
    /**
     * Task used to build an {@link IndexSegment} from a restricted key-range of
     * an index during a {@link SplitIndexPartitionTask}. This is a compacting
     * merge since we want as much of the data for the index as possible in a
     * single {@link IndexSegment}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class BuildIndexSegmentSplitTask extends
            AbstractResourceManagerTask<BuildResult> {

        private final ViewMetadata vmd;
        private final Split split;
        private final Event parentEvent;

        /**
         * Builds an {@link IndexSegment} from the lastCommitTime of the old
         * journal.
         * 
         * @param vmd
         * @param split
         */
        public BuildIndexSegmentSplitTask(final ViewMetadata vmd,
                final Split split, final Event parentEvent) {

            super(vmd.resourceManager, TimestampUtility
                    .asHistoricalRead(vmd.commitTime), vmd.name);
            
            if (split == null)
                throw new IllegalArgumentException();

            this.vmd = vmd;
            
            this.split = split;
            
            this.parentEvent = parentEvent;
            
        }

        /**
         * Note: The generated {@link IndexSegment} is on the retentionSet and
         * MUST be removed from that set once it has been incorporated in a
         * restart safe manner into an index partition view or once the task
         * fails.
         * 
         * @see StoreManager#retentionSetAdd(java.util.UUID)
         */
        @Override
        protected BuildResult doTask() throws Exception {

            if (resourceManager.isOverflowAllowed())
                throw new IllegalStateException();

            final String name = getOnlyResource();
            
            final ILocalBTreeView src = (ILocalBTreeView)getIndex(name);

            if (log.isInfoEnabled()) {
                
                // note: the mutable btree - accessed here for debugging only.
                final BTree btree = src.getMutableBTree();

                log.info("src=" + name + ",counter=" + src.getCounter().get()
                        + ",checkpoint=" + btree.getCheckpoint());
            
            }

            final LocalPartitionMetadata pmd = (LocalPartitionMetadata) split.pmd;

            final byte[] fromKey = pmd.getLeftSeparatorKey();
            
            final byte[] toKey = pmd.getRightSeparatorKey();

            if (fromKey == null && toKey == null) {

                /*
                 * Note: This is not legal because it implies that we are
                 * building the index segment from the entire source key range -
                 * hence not a split at all!
                 */
                
                throw new RuntimeException("Not a key-range?");
                
            }
            
            /*
             * Build the index segment from the key range.
             * 
             * Note: The generated index segment is on the retentionSet and MUST
             * be removed from that set once it has been incorporated in a
             * restart safe manner into an index partition view or once the task
             * fails.
             */
            final BuildResult result = resourceManager.buildIndexSegment(name,
                    src, true/* compactingMerge */, vmd.commitTime,
                    fromKey, toKey, parentEvent);

            return result;
            
        }
        
    }

    /**
     * Choose a set of splits which may be reasonably expected to divide the
     * {@link IndexSegment} into extents each of which is approximately 50%
     * full. The first split MUST use the leftSeparator of the index view as its
     * leftSeparator. The last split MUST use the rightSeparator of the index
     * view as its rightSeparator. The #of splits SHOULD be chosen such that the
     * resulting index partitions are approximately 50% full.
     * 
     * @param keyRange
     *            The left and right separator keys for the view.
     * @param seg
     *            The {@link IndexSegment} containing most of the data for the
     *            view.
     * @param nominalShardSize
     *            The nominal size of an index partition (typically 200MB).
     * @param splitHandler
     *            Applies an application constraint to the choice of the
     *            separator key (optional).
     * 
     * @return A {@link Split}[] array contains everything that we need to
     *         define the new index partitions -or- <code>null</code> if a more
     *         detailed examination reveals that the index SHOULD NOT be split
     *         at this time. The returned array MUST containing at least two
     *         elements. If the {@link IndexSegment} CAN NOT be split, this MUST
     *         return <code>null</code> rather than array with a single element.
     * 
     * @see src/architecture/SplitMath.xls
     */
    public static Split[] getSplits(
            final IPartitionIdFactory partitionIdFactory,
            final LocalPartitionMetadata oldpmd, final IndexSegment seg,
            final long nominalShardSize,
            final ISimpleSplitHandler splitHandler) {

        if (partitionIdFactory == null)
            throw new IllegalArgumentException();

        if (oldpmd == null)
            throw new IllegalArgumentException();

        if (seg == null)
            throw new IllegalArgumentException();

        if (nominalShardSize <= 0)
            throw new IllegalArgumentException();

        if(!seg.getStore().getCheckpoint().compactingMerge) {
            /*
             * Note: You can only do this after a compacting merge since that is
             * the only time we have perfect information about the size on disk
             * of the shard's segments and a guarantee that there are no deleted
             * tuples in the index segment. Both of those assumptions greatly
             * simplify the logic here and the logic surrounding dynamic
             * sharding in general.
             */
            throw new IllegalArgumentException();
            
        }

        /*
         * Compute the target #of splits based on the size on disk without
         * regard to the #of tuples present in the index segment.
         * 
         * @see src/architecture/SplitMath.xls for this formula.
         */
        final int N1 = (int)(seg.getStore().size() / (nominalShardSize / 2.));
        
        if (N1 < 2) {

            /*
             * There is not enough data on the disk to split this index segment.
             */

            return null;
            
        }

        /*
         * This is the actual number of tuples in the index segment.
         * 
         * Note: These will all be non-deleted tuples since we verified that
         * this is a compact segment above.
         */
        final int entryCount = seg.getEntryCount();

        /*
         * This adjusts the #of splits if there are not enough tuples to
         * generate that many splits.
         * 
         * Note: This should only happen if you really, really dial down the
         * [nominalShardSize]. That case is exercised by the test suite.
         */
        final int N = (entryCount < N1) ? entryCount : N1;

        final String scaleOutIndexName = seg.getIndexMetadata().getName();
        
        if (log.isInfoEnabled()) {

            log.info("segSize="
                    + seg.getStore().size()
                    + ", nominalShardSize="
                    + nominalShardSize
                    + ", N1="
                    + N1
                    + ", entryCount="
                    + entryCount
                    + ", N="
                    + N
                    + (N != N1 ? " [#splits adjusted down to the entryCount]."
                            : ""));

        }

        // The splits (may be fewer than N).
        final List<Split> splits = new ArrayList<Split>(N);

        // the index of the inclusive lower bound.
        int low = 0;
        // the index of the _inclusive_ upper bound.
        final int high = entryCount - 1;
        // the next key to use as the left separator key.
        byte[] lastSeparatorKey = oldpmd.getLeftSeparatorKey();
        // false until we use the rightSeparatorKey in the last split.
        boolean didLastSplit = false;

        /*
         * do until done.
         * 
         * @see src/architecture/SplitMath.xls for the math in this loop.
         */
        while (low < high) {

            // The #of tuples in [low:high].
            final int rangeCount = high - low + 1;

            // The #of splits already decided.
            final int splitCount = splits.size();

            // The #of splits that we will still create.
            final int remainingSplits = N - splitCount;

            // inclusive lower bound of the split.
            final int fromIndex = low;
            final byte[] fromKey = lastSeparatorKey;
            
            // exclusive upper bound of the split.
            final int toIndex;
            final byte[] toKey;

            /*
             * Figure out the last tuple to copy into this split and the
             * separatorKey which is the exclusive upper bound for this split.
             * Note that the separatorKey does not need to correspond to the
             * last tuple copied into the split. It can be a successor of that
             * tuple in the unsigned byte[] ordering which is less than the next
             * tuple actually present in the index.
             */
            if (remainingSplits == 1) {

                /*
                 * This is the last split: always use the rightSeparator.
                 */

                toKey = oldpmd.getRightSeparatorKey();
                
                toIndex = high;
                
                didLastSplit = true;
                
            } else {
                
                // The index of recommended separatorKey.
                final int splitAt = (rangeCount / remainingSplits) + low;

                assert splitAt > low && splitAt <= high : "low=" + low
                        + ", high=" + high + ", splitAt=" + splitAt;
                ;

                if (splitHandler != null) {
                
                    /*
                     * Allow override of the recommended separator.
                     * 
                     * Note: we receive a separatorKey from the override so we
                     * can create a separation not represented by any key
                     * actually present in the index. To handle this case, we
                     * lookup the desired separatorKey in the index. If it is
                     * not found then we convert the insertion point to the
                     * index of the key where it would be inserted and use that
                     * as the toIndex.
                     */
                    
                    // Allow override of the separatorKey.
                    final byte[] chosenKey = splitHandler.getSeparatorKey(seg,
                            fromIndex, (high + 1)/* toIndex */, splitAt);

                    if (chosenKey == null) {

                        /*
                         * No separator key could be identified. The rest of the
                         * data in the segment will all go into this split.
                         */

                        final double overextension = ((double) seg.getStore()
                                .size())
                                / nominalShardSize;

                        if (splits.isEmpty() && overextension > 2d) {

                            /*
                             * Log, but keep going.
                             * 
                             * Note: An application with poorly written override
                             * logic could cause an index segment to fail to
                             * split. This has an extreme negative impact on
                             * performance once the segment grows to 1G or more.
                             * The DataService SHOULD refuse writes for shards
                             * which refuse splits, which pushes the problem
                             * back to the application where it belongs.
                             */
                            log.error("Segment overextended: "
                                            + overextension
                                            + "x : application refuses to split shard: "
                                            + scaleOutIndexName + "#"
                                            + oldpmd.getPartitionId());

                        }
                        
                        if(splits.isEmpty()) {
                            
                            /*
                             * Since no splits have been chosen we can not
                             * return everything in a single split. Instead we
                             * return null and the source WILL NOT be split.
                             */
 
                            return null;

                        }

                        /*
                         * Everything remaining goes into this split.
                         */
                        
                        toKey = oldpmd.getRightSeparatorKey();
                        
                        toIndex = high;

                        didLastSplit = true;

                    } else {

                        // Lookup key in the index.
                        final int pos = seg.indexOf(chosenKey);

                        // If key not found, convert pos to the insertion point.
                        toIndex = pos < 0 ? -(pos + 1) : pos;

                        if (toIndex < low || toIndex > high) {

                            /*
                             * The override did not return a separator key
                             * within the key range which we instructed it to
                             * use. This is a bug in the application's override
                             * code.
                             */

                            throw new RuntimeException(
                                    "bad split override: name="
                                            + scaleOutIndexName
                                            + ", fromIndex=" + fromIndex
                                            + ", toIndex=" + (high + 1)
                                            + ", recommendedSplitAt=" + splitAt
                                            + ", but split choose at "
                                            + toIndex);

                        }

                        if (toIndex == high) {

                            /*
                             * If they choose the last allowed index then all
                             * tuples have been consumed and we are done. In
                             * this case we use the rightSeparator rather than
                             * the chosen key in order to enforce the constraint
                             * that the last split has the same rightSeparator
                             * as the source index segment.
                             */
                            
                            toKey = oldpmd.getRightSeparatorKey();

                            didLastSplit = true;
                            
                        } else {
                            
                            // We will use the chosen key.
                            toKey = chosenKey;

                        }

                    }

                } else {

                    if (splitAt == high) {

                        /*
                         * Enforce the constraint that the last split has the
                         * same rightSeparator as the source index segment.
                         */
                        toKey = oldpmd.getRightSeparatorKey();
                        
                        didLastSplit = true;
                        
                    } else {

                        /*
                         * Take whatever key is at the recommended split index and
                         * use that as the separator key for this split.
                         */
                        toKey = seg.keyAt(splitAt);
                        
                    }
                    
                    toIndex = splitAt;

                }
                
            }

            if (log.isDebugEnabled()) {
                log.debug("splitCount=" + splitCount + ", remainingSplits="
                        + remainingSplits + ", low=" + low + ", high=" + high
                        + ", rangeCount=" + rangeCount + ", fromIndex="
                        + fromIndex + ", toIndex=" + toIndex + ", ntuples="
                        + (toIndex - fromIndex) + ", separatorKey="
                        + BytesUtil.toString(toKey));
            }
            
            // Create Split description.
            {
                
                /*
                 * Get the next partition identifier for the named scale-out
                 * index.
                 * 
                 * Note: This is a RMI.
                 */
                final int partitionId = partitionIdFactory
                        .nextPartitionId(scaleOutIndexName);

                /*
                 * Describe the partition metadata for the new split.
                 */
                final LocalPartitionMetadata newpmd = new LocalPartitionMetadata(
                        partitionId,//
                        -1, // Note: split not allowed during move.
                        fromKey, // leftSeparatorKey
                        toKey, // rightSeparatorKey
                        /*
                         * Note: no resources for an index segment.
                         */
                        null,//
                        /*
                         * Note: cause will be set by the atomic update task.
                         */
                        null //
//                        , oldpmd.getHistory()
//                                + "chooseSplitPoint(oldPartitionId="
//                                + oldpmd.getPartitionId() + ",nsplits=" + N
//                                + ",newPartitionId=" + partitionId + ") "
                        );

                final Split split = new Split(newpmd, fromIndex, toIndex);

                // add to list of splits.
                splits.add(split);
                
            }

            // prepare for the next round.
            low = toIndex;
            lastSeparatorKey = toKey;

        }

        if(!didLastSplit) {
            
            /*
             * This catches logic errors where we have failed to assign the
             * rightSeparatorKey explicitly to the last split.
             */

            throw new AssertionError();
            
        }
        
        return splits.toArray(new Split[splits.size()]);

    }

}
