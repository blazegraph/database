package com.bigdata.resources;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.mdi.ISeparatorKeys;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.resources.DefaultSplitHandler.Sample;
import com.bigdata.service.Split;

/**
 * Helper class iteratively generates the {@link Split}s for an index
 * partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class Splitter {

    protected static final Logger log = Logger.getLogger(Splitter.class); 
    
    /**
     * The factory for partition identifiers for the generated {@link Split}s.
     */
    final IPartitionIdFactory partitionIdFactory;

    final DefaultSplitHandler splitHandler;
    
    /**
     * The target #of splits to generate.
     */
    final int targetSplitCount;

    /** The target #of index entries per split. */
    final int targetSplitCapacity;

    /**
     * The actual #of tuples in the source index.
     */
    final int actualTupleCount;

    /**
     * The samples drawn from the source index.
     */
    final Sample[] samples;

    /**
     * The average #of tuples spanned by each {@link Sample}.
     */
    final int tuplesPerSample;
    
    /**
     * The name of the scale-out index.
     */
    final String scaleOutIndexName;

    /** The metadata for the index partition that is being split. */
    final LocalPartitionMetadata oldpmd;

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getName());

        // immutable.
        sb.append("{ targetSplitCount=" + targetSplitCount);
        sb.append(", targetSplitCapacity=" + targetSplitCapacity);
        sb.append(", actualTupleCount=" + actualTupleCount);
        sb.append(", nsamples=" + samples.length);
        sb.append(", tuplesPerSamples=" + tuplesPerSample);
        sb.append(", scaleOutIndexName=" + scaleOutIndexName);
        sb.append(", fromIndex=" + fromIndex);
        sb.append(", oldpmd=" + oldpmd);

        // mutable
        sb.append(", splitCount=" + splitIndex);
        sb.append(", sampleIndex=" + sampleIndex);
        sb.append(", usedTupleCount=" + usedTupleCount);
        sb.append(", fromIndex=" + fromIndex);
        sb.append(", fromKey=" + BytesUtil.toString(fromKey));
        sb.append("}");

        return sb.toString();

    }

    /*
     * non-final state.
     */

    /** #of splits generated so far. */
    private int splitIndex = 0;

    /** index into the {@link #samples}[]. */
    private int sampleIndex = 0;

    /** #of index entries assigned into splits so far. */
    private int usedTupleCount = 0;

    /**
     * The index into the tuples of the first tuple that will go into the
     * next split. This is initially zero.
     */
    private int fromIndex = 0;

    /**
     * The first key to enter the next split. This is initially the
     * {@link ISeparatorKeys#getLeftSeparatorKey() left separator key}.
     */
    private byte[] fromKey;

    Splitter(final IPartitionIdFactory partitionIdFactory, final IIndex ndx,
            final int targetSplitCount, final Sample[] samples,
            final DefaultSplitHandler splitHandler, final long nvisited) {

        if (partitionIdFactory == null)
            throw new IllegalArgumentException();
        
        if (ndx == null)
            throw new IllegalArgumentException();
        
        if (targetSplitCount < 2)
            throw new IllegalArgumentException();
        
        if (samples == null)
            throw new IllegalArgumentException();
        
        if (splitHandler == null)
            throw new IllegalArgumentException();
        
        if (nvisited < 0)
            throw new IllegalArgumentException();

        this.partitionIdFactory = partitionIdFactory;

        this.splitHandler = splitHandler;
        
        this.targetSplitCount = targetSplitCount;

        /*
         * The nominal tuple count for a split. This uses the under capacity
         * multipler to compute an estimate of the #of tuples at the low end of
         * an index partition capacity. A split SHOULD have at least this many
         * tuples. However, it MAY have more (or fewer) tuples. For example, if
         * the separator key for the split would violate an application
         * constraint, then the separator key could be moved in either direction
         * so as to satisify that constraint. The other case where more tuples
         * may are often present is the last split where more tuples are
         * incorporated into the split in order to prevent the creation of
         * another split which would be "too small".
         */
        targetSplitCapacity = splitHandler.getTargetEntryCountPerSplit();

        this.actualTupleCount = (int) nvisited;

        this.samples = samples;
        
        this.tuplesPerSample = actualTupleCount / samples.length;

        this.scaleOutIndexName = ndx.getIndexMetadata().getName();

        this.oldpmd = ndx.getIndexMetadata().getPartitionMetadata();

        if (oldpmd == null) {

            throw new IllegalStateException("Not an index partition: " + ndx);

        }

        if (oldpmd.getSourcePartitionId() != -1) {

            throw new IllegalStateException(
                    "Split not allowed during move: sourcePartitionId="
                            + oldpmd.getSourcePartitionId());

        }

        fromKey = oldpmd.getLeftSeparatorKey();

    }

    /**
     * Return the next {@link Split} -or- <code>null</code> if there are
     * no more splits. This has a side-effect on {@link #fromKey} and
     * {@link #fromIndex} if it is able to generate a {@link Split}.
     * 
     * @return The next split -or- <code>null</code> if there are no more
     *         splits.
     */
    public Split nextSplit() {

        if (fromIndex == actualTupleCount) {

            if (log.isInfoEnabled())
                log.info("No more splits: " + this);

            return null;

        }

        if (fromKey == null)
            throw new AssertionError("fromKey is null: " + this);

        /*
         * Consider the remaining samples and find the last sample that we
         * will accept into this split.
         */
        for (; sampleIndex < samples.length; sampleIndex++) {

            final Sample t = samples[sampleIndex];

            assert t != null;

            final int count = t.offset - usedTupleCount;

            // #of tuples that would be remaining after this split.
            final int remaining = actualTupleCount - usedTupleCount - count;

            /*
             * True iff there are not enough tuples remaining to create two
             * splits @ .8 of their target capacity.
             */
            final boolean lastSplit = (count + remaining) / 2 < targetSplitCapacity * .8;

            if (lastSplit) {

                /*
                 * Everything remaining will go into this split. It may be a bit
                 * over the target capacity.
                 * 
                 * Note: always assign the rightSeparator to the last split.
                 */

                if (log.isInfoEnabled())
                    log.info("Remaining into the last split: count=" + count
                            + ", remaining=" + remaining);
                    
                usedTupleCount = actualTupleCount;

                return newSplit(actualTupleCount/* toIndex */, oldpmd
                        .getRightSeparatorKey());

            }

            /*
             * Note: Because we only sample the tuples, we are forced to
             * consider only every nvisited/nsamples tuples. This means that we
             * can easily run beyond the ideal split point. That is why the
             * sample size is considered and we will emit a split early if
             * accepting more tuples would rob the next split (or the last
             * split).
             */
            final boolean enough = (count + tuplesPerSample / 2) >= targetSplitCapacity;
//            final boolean enoughForLast2 = ((count + remaining) / 2) >= .9 * targetSplitCapacity;
//            if (count >= targetSplitCapacity) {
            if(enough) {
                /*
                 * Handles case where the split is filled with tuples before
                 * we run out of tuples.
                 */

                if (log.isInfoEnabled())
                    log.info("Filled split[" + splitIndex + "] with " + count
                            + " entries: targetCapacity=" + targetSplitCapacity
                            + ", samples[j]=" + t);

                /*
                 * Note: We have consumed this sample so we increment the
                 * counter before we jump out of the loop.
                 */
                sampleIndex++; // was consumed.

                usedTupleCount += count;

                return newSplit(t.offset, t.key);

            }

        }

        /*
         * We are out of samples so we assign the rightSeparatorKey to this
         * split.
         * 
         * Note: always assign the rightSeparator to the last split.
         */

        usedTupleCount = actualTupleCount;

        return newSplit(actualTupleCount/* toIndex */, oldpmd
                .getRightSeparatorKey());

    }

    /**
     * Create the {@link Split} corresponding to the current state of this
     * object, using the specified <i>toKey</i> as its left-separator key.
     * and having the specified <i>toIndex</i> into the source tuples. The
     * {@link #fromKey} and {@link #fromIndex} are updated as a side-effect.
     * 
     * @param toIndex
     * @param toKey
     * @return
     */
    private Split newSplit(final int toIndex, final byte[] toKey) {

        if (toIndex <= fromIndex)
            throw new IllegalArgumentException("toIndex=" + toIndex
                    + " LTE fromIndex=" + fromIndex);

        if (toKey == null && toIndex != actualTupleCount)
            throw new IllegalArgumentException("toKey is null, but toIndex("
                    + toIndex + ") NE actualTupleCount(" + actualTupleCount
                    + ")");

        /*
         * Get the next partition identifier for the named scale-out index.
         * 
         * Note: This is a RMI.
         */
        final int partitionId = partitionIdFactory
                .nextPartitionId(scaleOutIndexName);

        /*
         * Describe the partition metadata for the new split.
         */
        final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                partitionId,//
                -1, // Note: split not allowed during move.
                fromKey, // leftSeparatorKey
                toKey, // rightSeparatorKey
                /*
                 * Note: no resources for an index segment
                 */
                null,//
                /*
                 * Note: cause will be set by the atomic update task.
                 */
                null,//
                oldpmd.getHistory() + "chooseSplitPoint(oldPartitionId="
                        + oldpmd.getPartitionId() + ",nsplits="
                        + targetSplitCount + ",newPartitionId=" + partitionId
                        + ") ");

        try {

            return new Split(pmd, fromIndex, toIndex);

        } finally {

            // update the fromKey/fromIndex.

            fromKey = toKey;

            fromIndex = toIndex;

        }

    }

}
