package com.bigdata.resources;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
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
public class SplitIndexPartitionTask extends AbstractTask {

    /**
     * 
     */
    private final ResourceManager resourceManager;

    /**
     * @param resourceManager 
     * @param concurrencyManager
     * @param lastCommitTime
     * @param resource
     */
    protected SplitIndexPartitionTask(ResourceManager resourceManager,
            IConcurrencyManager concurrencyManager, long lastCommitTime,
            String resource) {

        super(concurrencyManager, -lastCommitTime, resource);

        if (resourceManager == null)
            throw new IllegalArgumentException();

        this.resourceManager = resourceManager;

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

        final String name = getOnlyResource();
        
        final IIndex src = getIndex(name);
        
        final long createTime = Math.abs(startTime);
        
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

            return resourceManager.buildIndexSegment(name, src, outFile, createTime,
                    null/* fromKey */, null/*toKey*/);
            
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
            
            AbstractTask task = new BuildIndexSegmentTask(
                    resourceManager, resourceManager.getConcurrencyManager(), -createTime, name, //
                    outFile,//
                    pmd.getLeftSeparatorKey(), //
                    pmd.getRightSeparatorKey());

            tasks.add(task);
            
        }
        
        // submit and await completion.
        final List<Future<Object>> futures = resourceManager.getConcurrencyManager().invokeAll(tasks);
        
        final BuildResult[] buildResults = new BuildResult[nsplits];

        int i = 0;
        for( Future<Object> f : futures ) {

            // @todo error handling?
            buildResults[i++] = (BuildResult) f.get();
                
        }

        log.info("Generated "+splits.length+" index segments: name="+name);
        
        return new SplitResult(name,indexMetadata,splits,buildResults);
        
    }

}