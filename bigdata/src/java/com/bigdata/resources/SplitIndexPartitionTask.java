package com.bigdata.resources;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.BuildIndexSegmentTask.BuildResult;
import com.bigdata.service.DataService;
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
 * one from each of the N key ranges which those N-1 split points define. After
 * each index segment is built, it is added to a queue of index segments that
 * will be used to update the live index partition definitions.
 * <p>
 * Unlike a {@link BuildIndexSegmentTask}, the {@link SplitIndexPartitionTask}
 * will eventually result in the original index partition becoming un-defined
 * and new index partitions being defined in its place which span the same total
 * key range.
 * 
 * @see UpdateSplitIndexPartition, which MUST be invoked in order to update the
 *      index partition definitions on the live journal and the
 *      {@link MetadataIndex} as an atomic operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SplitIndexPartitionTask extends AbstractResourceManagerTask {

    private final long lastCommitTime;
    
    /**
     * @param resourceManager 
     * @param concurrencyManager
     * @param lastCommitTime
     * @param resource
     */
    protected SplitIndexPartitionTask(ResourceManager resourceManager,
            long lastCommitTime,
            String resource) {

        super(resourceManager, -lastCommitTime, resource);

        this.lastCommitTime = lastCommitTime;

    }

    /**
     * Validate splits, including: that the separator keys are strictly
     * ascending, that the separator keys perfectly cover the source key
     * range without overlap, that the rightSeparator for each split is the
     * leftSeparator for the prior split, that the fromIndex offsets are
     * strictly ascending, etc.
     * 
     * @param src
     *            The source index.
     * @param splits
     *            The recommended split points.
     */
    protected void validateSplits(IIndex src, Split[] splits) {

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

            LocalPartitionMetadata pmd = (LocalPartitionMetadata) split.pmd;

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
     * 
     * @return A {@link SplitResult } if the index partition was split into
     *         2 or more index partitions -or- a {@link BuildResult} iff the
     *         index partition was not split.
     */
    @Override
    protected Object doTask() throws Exception {

        if (resourceManager.isOverflowAllowed())
            throw new IllegalStateException();

        final String name = getOnlyResource();
        
        final IIndex src = getIndex(name);
        
//        final long createTime = Math.abs(startTime);
        
        final IndexMetadata indexMetadata = src.getIndexMetadata();
        
        final ISplitHandler splitHandler = indexMetadata.getSplitHandler();
        
        if (splitHandler == null) {
            
            // This was checked as a pre-condition and should not be null.
            
            throw new AssertionError();
            
        }

        /*
         * Get the split points for the index. Each split point describes a
         * new index partition. Together the split points MUST exactly span
         * the source index partitions key range. There MUST NOT be any
         * overlap in the key ranges for the splits.
         */
        final Split[] splits = splitHandler.getSplits(resourceManager,src);
        
        if (splits == null) {
            
            /*
             * No splits were choosen so the index will not be split at this
             * time.  Instead we do a normal index segment build task.
             */
                            
            // the file to be generated.
            final File outFile = resourceManager.getIndexSegmentFile(indexMetadata);

            return resourceManager.buildIndexSegment(name, src, outFile,
                    lastCommitTime, null/* fromKey */, null/* toKey */);
            
        }
        
        final int nsplits = splits.length;
        
        log.info("Will build index segments for " + nsplits
                + " splits for " + name);
        
        // validate the splits before processing them.
        validateSplits(src, splits);

        /*
         * Build N index segments based on those split points.
         * 
         * Note: This is done in parallel to minimize latency.
         */
        
        final List<AbstractTask> tasks = new ArrayList<AbstractTask>(nsplits);
        
        for (int i=0; i<splits.length; i++) {
            
            final Split split = splits[i];

            final LocalPartitionMetadata pmd = (LocalPartitionMetadata)split.pmd;
            
            final File outFile = resourceManager.getIndexSegmentFile(indexMetadata);
            
            final AbstractTask task = new BuildIndexSegmentTask(resourceManager,
                    lastCommitTime,
                    name, //
                    outFile,//
                    pmd.getLeftSeparatorKey(), //
                    pmd.getRightSeparatorKey());

            tasks.add(task);
            
        }
        
        // submit and await completion. @todo timeout config?
        final List<Future<Object>> futures = resourceManager.getConcurrencyManager().invokeAll(tasks);
        
        final BuildResult[] buildResults = new BuildResult[nsplits];

        int i = 0;
        for( Future<Object> f : futures ) {

            // @todo error handling?
            buildResults[i++] = (BuildResult) f.get();
                
        }

        log.info("Generated "+splits.length+" index segments: name="+name);
        
        final SplitResult result = new SplitResult(name,indexMetadata,splits,buildResults);
        
        final AbstractTask task = new UpdateSplitIndexPartition(
                resourceManager, result.name, result);

        // submit atomic update task and wait for it to complete @todo config
        // timeout?
        concurrencyManager.submit(task).get();
        
        return result;
        
    }

    /**
     * The result of a {@link SplitIndexPartitionTask} including enough metadata
     * to identify the index partitions to be created and the index partition to
     * be deleted.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class SplitResult extends AbstractResult {

        /**
         * The array of {@link Split}s that describes the new key range for
         * each new index partition created by splitting the old index
         * partition.
         */
        public final Split[] splits;
        
        /**
         * An array of the {@link BuildResult}s for each output split.
         */
        public final BuildResult[] buildResults;

        /**
         * @param name
         *            The name under which the processed index partition was
         *            registered (this is typically different from the name of
         *            the scale-out index).
         * @param indexMetadata
         *            The index metadata object for the processed index as of
         *            the timestamp of the view from which the
         *            {@link IndexSegment} was generated.
         * @param splits
         *            Note: At this point we have the history as of the
         *            lastCommitTime in N index segments. Also, since we
         *            constain the resource manager to refuse another overflow
         *            until we have handle the old journal, all new writes are
         *            on the live index.
         * @param buildResults
         *            A {@link BuildResult} for each output split.
         */
        public SplitResult(String name, IndexMetadata indexMetadata,
                Split[] splits, BuildResult[] buildResults) {

            super( name, indexMetadata);

            assert splits != null;
            
            assert buildResults != null;
            
            assert splits.length == buildResults.length;
            
            for(int i=0; i<splits.length; i++) {
                
                assert splits[i] != null;

                assert splits[i].pmd != null;

                assert splits[i].pmd instanceof LocalPartitionMetadata;

                assert buildResults[i] != null;
                
            }
            
            this.splits = splits;
            
            this.buildResults = buildResults;

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
    static public class UpdateSplitIndexPartition extends AbstractResourceManagerTask {
        
        protected final SplitResult splitResult;
        
        public UpdateSplitIndexPartition(
                ResourceManager resourceManager, String resource,
                SplitResult splitResult) {

            super(resourceManager, ITx.UNISOLATED, resource);

            if (splitResult == null)
                throw new IllegalArgumentException();
            
            this.splitResult = splitResult;
            
        }

        @Override
        protected Object doTask() throws Exception {

            // The name of the scale-out index.
            final String scaleOutIndexName = splitResult.indexMetadata.getName();
            
            // the name of the source index.
            final String name = getOnlyResource();
            
            // this is the live journal since this task is unisolated.
            final AbstractJournal journal = getJournal();
            
            /*
             * Note: the source index is the BTree on the live journal that has
             * been absorbing writes since the last overflow.  This is NOT a fused
             * view.  All we are doing is re-distributing the writes onto the new
             * splits of the index partition.
             */
            final BTree src = (BTree) resourceManager.getIndexOnStore(name,
                    ITx.UNISOLATED, journal); 
            
            /*
             * Locators for the new index partitions.
             */

            final LocalPartitionMetadata oldpmd = (LocalPartitionMetadata) src
                    .getIndexMetadata().getPartitionMetadata();

            final Split[] splits = splitResult.splits;
            
            final PartitionLocator[] locators = new PartitionLocator[splits.length];

            for (int i = 0; i < splits.length; i++) {

                // new metadata record (cloned).
                final IndexMetadata md = src.getIndexMetadata().clone();

                final LocalPartitionMetadata pmd = (LocalPartitionMetadata) splits[i].pmd;

                assert pmd.getResources() == null : "Not expecting resources for index segment: "
                        + pmd;
                
                /*
                 * form locator for the new index partition for this split..
                 */
                final PartitionLocator locator = new PartitionLocator(
                        pmd.getPartitionId(),//
                        /*
                         * This is the set of failover services for this index
                         * partition. The first element of the array is always
                         * the data service on which this resource manager is
                         * running. The remainder of elements in the array are
                         * the failover services.
                         * 
                         * @todo The index partition data will be replicated at
                         * the byte image level for the live journal.
                         * 
                         * @todo New index segment resources will be replicated
                         * as well.
                         * 
                         * @todo Once the index partition data is fully
                         * replicated we update the metadata index.
                         */
                        resourceManager.getDataServiceUUIDs(),//
                        pmd.getLeftSeparatorKey(),//
                        pmd.getRightSeparatorKey()//
                        );
                
                locators[i] = locator;
                
                /*
                 * Update the view definition.
                 */
                md.setPartitionMetadata(new LocalPartitionMetadata(
                        pmd.getPartitionId(),//
                        pmd.getLeftSeparatorKey(),//
                        pmd.getRightSeparatorKey(),//
                        new IResourceMetadata[] {//
                            /*
                             * Resources are (a) the new btree; and (b) the new
                             * index segment.
                             */
                            journal.getResourceMetadata(),
                            splitResult.buildResults[i].segmentMetadata
                        },
                        /* 
                         * Note: history is record of the split.
                         */
                        pmd.getHistory()
                        ));
                
                // create new btree.
                final BTree btree = BTree.create(journal, md);

                // the new partition identifier.
                final int partitionId = pmd.getPartitionId();
                
                // name of the new index partition.
                final String name2 = DataService.getIndexPartitionName(scaleOutIndexName, partitionId);
                
                // register it on the live journal.
                journal.registerIndex(name2, btree);
                
                // lower bound (inclusive) for copy.
                final byte[] fromKey = pmd.getLeftSeparatorKey();
                
                // upper bound (exclusive) for copy.
                final byte[] toKey = pmd.getRightSeparatorKey();
                
                /*
                 * Copy all data in this split from the source index.
                 * 
                 * Note: [overflow := false] since the btrees are on the same
                 * backing store.
                 */
                final long ncopied = btree.rangeCopy(src, fromKey, toKey, false/*overflow*/);
                
                log.info("Copied " + ncopied
                        + " index entries from the live index " + name
                        + " onto " + name2);
                
            }

            // drop the source index (the old index partition).
            journal.dropIndex(name);
            
            // will notify tasks that index partition was split.
            resourceManager.setIndexPartitionGone(name, "split");
           
            /*
             * Notify the metadata service that the index partition has been
             * split.
             * 
             * @todo Is it possible for the dataServiceUUIDs[] that we have
             * locally to be out of sync with the one in the locator on the
             * metadata service? If so, then the metadata service needs to
             * ignore that field when validating the oldLocator that we form
             * below.
             */
            resourceManager.getMetadataService().splitIndexPartition(
                    src.getIndexMetadata().getName(),//
                    new PartitionLocator(//
                            oldpmd.getPartitionId(), //
                            resourceManager.getDataServiceUUIDs(), //
                            oldpmd.getLeftSeparatorKey(),//
                            oldpmd.getRightSeparatorKey()//
                            ),
                    locators);
            
            return null;
            
        }
        
    }
    
}
