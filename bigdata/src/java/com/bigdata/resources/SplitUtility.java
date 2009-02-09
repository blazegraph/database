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

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.Leaf;
import com.bigdata.btree.Node;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.service.Split;
import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * Utility methods for {@link ISplitHandler}s and friends.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SplitUtility {

    protected static final Logger log = Logger.getLogger(SplitUtility.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
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
     */
    static public void validateSplits(final IIndex src, final Split[] splits) {

        final IndexMetadata indexMetadata = src.getIndexMetadata();

        final int nsplits = splits.length;

        assert nsplits > 1 : "Expecting at least two splits, but found "
                + nsplits;

        // verify splits obey index order constraints.
        int lastToIndex = -1;

        // Note: the first leftSeparator must be this value.
        byte[] fromKey = indexMetadata.getPartitionMetadata()
                .getLeftSeparatorKey();

        for (int i = 0; i < nsplits; i++) {

            final Split split = splits[i];

            assert split != null;

            assert split.pmd != null;

            assert split.pmd instanceof LocalPartitionMetadata;

            final LocalPartitionMetadata pmd = (LocalPartitionMetadata) split.pmd;

            // check the leftSeparator key.
            assert pmd.getLeftSeparatorKey() != null;
            assert BytesUtil.bytesEqual(fromKey, pmd.getLeftSeparatorKey());

            // verify rightSeparator is ordered after the left
            // separator.
            assert pmd.getRightSeparatorKey() == null
                    || BytesUtil.compareBytes(fromKey, pmd
                            .getRightSeparatorKey()) < 0;

            // next expected leftSeparatorKey.
            fromKey = pmd.getRightSeparatorKey();

            if (i == 0) {

                assert split.fromIndex == 0;

                assert split.toIndex > split.fromIndex;

            } else {

                assert split.fromIndex == lastToIndex;

            }

            if (i + 1 == nsplits && split.toIndex == 0) {

                /*
                 * Note: This is allowed in case the index partition has
                 * more than int32 entries in which case the toIndex of the
                 * last split can not be defined and will be zero.
                 */

                assert split.ntuples == 0;

                log.warn("Last split has no definate tuple count");

            } else {

                assert split.toIndex - split.fromIndex == split.ntuples;

            }

            lastToIndex = split.toIndex;

        }

        /*
         * verify left separator key for 1st partition is equal to the left
         * separator key of the source (this condition is also checked
         * above).
         */
        assert ((LocalPartitionMetadata) splits[0].pmd)
                .getLeftSeparatorKey().equals(
                        indexMetadata.getPartitionMetadata()
                                .getLeftSeparatorKey());

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
                assert indexMetadata.getPartitionMetadata()
                        .getRightSeparatorKey() == null;
                
            } else {
                
                // otherwise must compare as equals byte-by-byte.
                assert rightSeparator.equals(
                        indexMetadata.getPartitionMetadata()
                                .getRightSeparatorKey());
                
            }
            
        }

    }

    /**
     * Identifies the splits for an index with heavy write append behavior.
     * <p>
     * The split point is choosen by locating the right-most non-leaf node. The
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
        
        final int childIndex = (node.getChildCount() + 1) / 2;
        
        // leaf from the middle of the leaves of the node.
        final Leaf leaf = (Leaf) node.getChild(childIndex);
        
        // separator key is the first key in the leaf.
        final byte[] separatorKey = leaf == null ? null : leaf.getKeys()
                .getKey(0/* index */);

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
                    oldpmd.getHistory()
                            + "chooseTailSplitPoint(oldPartitionId="
                            + oldpmd.getPartitionId() + ",nsplits=" + 2
                            + ",newPartitionId=" + partitionId + ") ");

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
                    oldpmd.getHistory()
                            + "chooseTailSplitPoint(oldPartitionId="
                            + oldpmd.getPartitionId() + ",nsplits=" + 2
                            + ",newPartitionId=" + partitionId + ") ");

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
            final Split[] splits) throws InterruptedException,
            ExecutionExceptions {

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
                    vmd, split);

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

                vmd.resourceManager.deleteResource(result.segmentMetadata
                        .getUUID(), false/* isJournal */);

            }

            // throw wrapped set of exceptions.
            throw new ExecutionExceptions(causes);

        }

        if (INFO)
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

        /**
         * 
         * @param vmd
         * @param split
         */
        public BuildIndexSegmentSplitTask(final ViewMetadata vmd,
                final Split split) {

            super(vmd.resourceManager, TimestampUtility
                    .asHistoricalRead(vmd.commitTime), vmd.name);
            
            if (split == null)
                throw new IllegalArgumentException();

            this.vmd = vmd;
            
            this.split = split;
            
        }

        @Override
        protected BuildResult doTask() throws Exception {

            // The file on which the index segment is being written.
            final File outFile = vmd.resourceManager
                    .getIndexSegmentFile(vmd.indexMetadata);

            if (resourceManager.isOverflowAllowed())
                throw new IllegalStateException();

            final String name = getOnlyResource();
            
            final IIndex src = getIndex(name);

            if (INFO) {
                
                // note: the mutable btree - accessed here for debugging only.
                final BTree btree = ((ILocalBTreeView) src).getMutableBTree();

                log.info("src=" + name + ",counter=" + src.getCounter().get()
                        + ",checkpoint=" + btree.getCheckpoint());
            
            }

            final LocalPartitionMetadata pmd = (LocalPartitionMetadata)split.pmd;

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
            
            if (INFO)
                log.info("begin: name=" + name + ", outFile=" + outFile
                        + ", pmd=" + pmd);
            
            // build the index segment from the key range.
            final BuildResult result = resourceManager.buildIndexSegment(name,
                    src, outFile, true/* compactingMerge */, vmd.commitTime,
                    fromKey, toKey);

            if (INFO)
                log.info("done: name=" + name + ", outFile=" + outFile
                        + ", pmd=" + pmd);
            
            return result;
            
        }
        
    }

}
