package com.bigdata.resources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexMetadata.Options;
import com.bigdata.journal.IResourceManager;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.resources.SplitIndexPartitionTask.AtomicUpdateSplitIndexPartitionTask;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.Split;

/**
 * A configurable default policy for deciding when and where to split an
 * index partition into 2 or more index partitions.
 * <p>
 * Note: There is probably no single value for
 * {@link #getEntryCountPerSplit()} that is going to be "right" across
 * applications. The space requirements for keys is very difficult to
 * estimate since leading key compression will often provide a good win.
 * Likewise, indices are free to use compression on their values as well so
 * the size of the byte[] values is not a good estimate of their size in the
 * index.
 * <p>
 * Note: The #of index entries is a good proxy for the space requirements of
 * most indices. The {@link BigdataFileSystem} is one case where the space
 * requirements could be quite different since 64M blocks may be stored
 * along with the index entries, however in that case you can also test for
 * the size of the index segment that is part of the view and decide that
 * it's time to split the view.
 * 
 * @todo Perhaps I could do something to estimate the size of the nodes and
 *       the leaves in the index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultSplitHandler implements ISplitHandler {

    /**
     * 
     */
    private static final long serialVersionUID = 1675517991163473445L;

    /**
     * Logger.
     */
    protected static final Logger log = Logger
            .getLogger(DefaultSplitHandler.class);

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    private int minimumEntryCount;
    
    private int entryCountPerSplit;

    private int sampleRate;

    private double overCapacityMultiplier;

    private double underCapacityMultiplier;

    /**
     * De-serialization ctor.
     */
    public DefaultSplitHandler() {

    }

    /**
     * Setup a split handler.
     * 
     * @param minimumEntryCount
     *            An index partition which has no more than this many tuples
     *            should be joined with its rightSibling (if any).
     * @param entryCountPerSplit
     *            The target #of tuples for an index partition.
     * @param overCapacityMultiplier
     *            The index partition will be split when its actual entry count
     *            is GTE to
     *            <code>overCapacityMultiplier * entryCountPerSplit</code>
     * @param underCapacityMultiplier
     *            When an index partition will be split, the #of new index
     *            partitions will be choosen such that each index partition is
     *            approximately <i>underCapacityMultiple</i> full.
     * @param sampleRate
     *            #of samples to take per estimated split.
     * 
     * @throws IllegalArgumentException
     *             if any argument, or combination or arguments, is out of
     *             range.
     */
    public DefaultSplitHandler(int minimumEntryCount, int entryCountPerSplit,
            double overCapacityMultiplier, double underCapacityMultiplier,
            int sampleRate) {

        /*
         * Bootstap parameter settings. 
         * 
         * First, verify combination of parameters is legal.
         */
        assertSplitJoinStable(minimumEntryCount, entryCountPerSplit,
                underCapacityMultiplier);

        /*
         * Now that we know the combination is legal, set individual parameters
         * that have dependencies in their legal range. This will let us set the
         * individual parameters with their settor methods below.
         */
        this.minimumEntryCount = minimumEntryCount;
        this.entryCountPerSplit = entryCountPerSplit;
        this.underCapacityMultiplier = underCapacityMultiplier;
        
        /*
         * Use individual set methods to validate each parameter by itself.
         */

        setMinimumEntryCount(minimumEntryCount);

        setEntryCountPerSplit(entryCountPerSplit);

        setOverCapacityMultiplier(overCapacityMultiplier);

        setUnderCapacityMultiplier(underCapacityMultiplier);

        setSampleRate(sampleRate);
        
    }

    /**
     * Return <code>true</code> iff the range count of the index is less than
     * the {@link #getMinimumEntryCount()}.
     * <p>
     * Note: This relies on the fast range count, which is the upper bound on
     * the #of index entries. For this reason an index partition which has
     * undergone a lot of deletes will not underflow until it has gone through a
     * build to purge the deleted index entries. This is true even when all
     * index entries in the index partition have been deleted!
     */
    public boolean shouldJoin(IIndex ndx) {

        final long rangeCount = ndx.rangeCount(null, null);
        
        final boolean shouldJoin = rangeCount <= getMinimumEntryCount();
        
        if (INFO)
            log.info("shouldJoin=" + shouldJoin + " : rangeCount=" + rangeCount
                    + ", minimumEntryCount=" + getMinimumEntryCount());
        
        return shouldJoin;
        
    }
    
    /**
     * 
     * 
     */
    static void assertSplitJoinStable(int minimumEntryCount,
            int entryCountPerSplit, double underCapacityMultiplier) {

        if (minimumEntryCount >= underCapacityMultiplier * entryCountPerSplit) {
            
            throw new IllegalArgumentException("minimumEntryCount("
                    + minimumEntryCount + ") exceeds underCapacityMultiplier("
                    + underCapacityMultiplier + ") * entryCountPerSplit("
                    + entryCountPerSplit + ")");
            
        }

    }

    /**
     * The minimum #of index entries before the index partition becomes eligible
     * to be joined.
     */
    public int getMinimumEntryCount() {

        return minimumEntryCount;
        
    }

    public void setMinimumEntryCount(int minimumEntryCount) {

        assertSplitJoinStable(minimumEntryCount, getEntryCountPerSplit(),
                getUnderCapacityMultiplier());

        this.minimumEntryCount = minimumEntryCount;
        
    }

    /**
     * The target maximum #of index entries in an index partition.
     */
    public int getEntryCountPerSplit() {

        return entryCountPerSplit;

    }

    public void setEntryCountPerSplit(int entryCountPerSplit) {

        if (entryCountPerSplit <= Options.MIN_BRANCHING_FACTOR) {

            throw new IllegalArgumentException();

        }

        assertSplitJoinStable(getMinimumEntryCount(), entryCountPerSplit,
                getUnderCapacityMultiplier());
        
        this.entryCountPerSplit = entryCountPerSplit;
        
    }

    /**
     * The #of samples per estimated #of splits.
     */
    public int getSampleRate() {

        return sampleRate;

    }

    public void setSampleRate(int sampleRate) {

        this.sampleRate = sampleRate;

    }

    /**
     * The threshold for splitting an index is the
     * {@link #getOverCapacityMultiplier()} times
     * {@link #getEntryCountPerSplit()}. If there are fewer than this many
     * entries in the index then it will not be split.
     */
    public double getOverCapacityMultiplier() {

        return overCapacityMultiplier;

    }

    /**
     * 
     * @param overCapacityMultiplier
     *            A value in [1.0:2.0].
     */
    public void setOverCapacityMultiplier(double overCapacityMultiplier) {

        final double min = 1.0;
        final double max = 2.0;

        if (overCapacityMultiplier < min || overCapacityMultiplier > max) {

            throw new IllegalArgumentException("Must be in [" + min + ":" + max
                    + "], but was " + overCapacityMultiplier);

        }

        this.overCapacityMultiplier = overCapacityMultiplier;

    }

    /**
     * This is the target under capacity rate for a new index partition. For
     * example, if the {@link #getEntryCountPerSplit()} is 5M and this
     * property is <code>.75</code> then an attempt will be made to divide
     * the index partition into N splits such that each split is at 75% of
     * the {@link #getEntryCountPerSplit()} capacity.
     */
    public double getUnderCapacityMultiplier() {

        return underCapacityMultiplier;

    }

    /**
     * 
     * @param underCapacityMultiplier
     *            A value in [0.5,1.0).
     */
    public void setUnderCapacityMultiplier(double underCapacityMultiplier) {

        final double min = 0.5;
        final double max = 1.0;

        if (underCapacityMultiplier < min || underCapacityMultiplier >= max) {

            throw new IllegalArgumentException("Must be in [" + min + ":" + max
                    + "), but was " + underCapacityMultiplier);

        }

        assertSplitJoinStable(getMinimumEntryCount(), getEntryCountPerSplit(),
                underCapacityMultiplier);
        
        this.underCapacityMultiplier = underCapacityMultiplier;

    }

    public boolean shouldSplit(IIndex view) {

        /*
         * Range count the index. Will overestimate if deleted entries
         * or overwritten entries exist.
         */
        final long rangeCount = view.rangeCount(null, null);

        /*
         * Recommend split if the range count equals or exceeds the overcapacity
         * multiplier.
         */

        if (rangeCount >= (getOverCapacityMultiplier() * entryCountPerSplit)) {

            if(INFO)
            log.info("Recommending split: rangeCount(" + rangeCount
                    + ") >= (entryCountPerSplit(" + entryCountPerSplit
                    + ") * overCapacityMultiplier("
                    + getOverCapacityMultiplier() + "))");

            return true;

        }

        return false;

    }
    
    /**
     * A sample collected from a key-range scan.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class Sample {

        /**
         * A key from the index.
         */
        final byte[] key;

        /**
         * The origin zero (0) offset at which that key was found
         * (interpretation is that the key was visited by the Nth
         * {@link ITuple}).
         */
        final int offset;

        public Sample(byte[] key, int offset) {

            assert key != null;

            assert offset >= 0;

            this.key = key;

            this.offset = offset;

        }

        public String toString() {

            return super.toString() + "{offset=" + offset + ", key="
                    + Arrays.toString(key) + "}";

        }

    }

    /**
     * Sample index using a range scan choosing ({@link #getSampleRate()} x
     * N) {@link Sample}s. The key range scan will filter out both
     * duplicates and deleted index entries. The scan will halt if the index
     * entry offsets would exceed an int32 value.
     * 
     * @return An ordered array of {@link Sample}s as an aid to choosen the
     *         split points for the view.
     */
    public Sample[] sampleIndex(IIndex ndx, AtomicLong nvisited) {

        final int rangeCount = (int) Math.min(ndx.rangeCount(null, null),
                Integer.MAX_VALUE);

        final ITupleIterator itr = ndx.rangeIterator(null, null,
                0/* capacity */, IRangeQuery.KEYS, null/* filter */);

        ITuple tuple = null;

        // The estimated #of splits based on the range count.
        final int numSplitsEstimate = Math.max(1,rangeCount / getEntryCountPerSplit());
        
        // Compute the #of samples to take (the sample rate is the #of samples per split).
        final int numSamplesEstimate = numSplitsEstimate * getSampleRate();
        
        final int sampleEveryNTuples = rangeCount / numSamplesEstimate;
        
        if(INFO)
        log.info("Estimating " + numSplitsEstimate + " with sampleRate="
                + getSampleRate() + " yeilding ~ " + numSamplesEstimate
                + " samples with one sample every " + sampleEveryNTuples
                + " tuples");
        
        final List<Sample> samples = new ArrayList<Sample>(numSamplesEstimate);

        while (itr.hasNext()) {

            tuple = itr.next();

            final long offset = tuple.getVisitCount() - 1;

            if (offset == Integer.MAX_VALUE) {

                /*
                 * This covers an extreme condition. If the split offsets
                 * would exceed an int32 value then we do not continue. Such
                 * views can be broken down by multiple passes, e.g., on
                 * subsequent overflows of a journal.
                 */

                log.warn("Aborting sample - offsets would exceed int32.");

                break;

            }

            if ((offset % sampleEveryNTuples) == 0) {

                // take a sample.

                final Sample sample = new Sample(tuple.getKey(), (int) offset);

                if(INFO)
                log.info("samples[" + samples.size() + "] = " + sample);

                samples.add(sample);

            }

        }

        assert samples.size() > 0;

        assert samples.get(0).offset == 0 : "Expecting offset := 0 for 1st sample, not "
                + samples.get(0).offset;

        // the actual #of index entries in the view.
        nvisited.set(tuple == null ? 0L : tuple.getVisitCount());

        if(INFO)
        log.info("Collected " + samples.size() + " samples from " + nvisited
                + " index entries; estimatedSplitCount=" + numSplitsEstimate
                + ", sampleRate=" + getSampleRate() + ", sampling every "
                + sampleEveryNTuples);

        return samples.toArray(new Sample[samples.size()]);

    }

    /**
     * Note: There are configuation parameters so that you can choose to let
     * the index partition grow until it reaches e.g., 150-200% of its
     * maximum entry count and then split it into N index partitions each of
     * which is e.g., 50-75% full.
     * <p>
     * Note: If the index partition has more than int32 index entries then
     * the last split will have a zero (0) toIndex since we don't know how
     * many index entries will go into that split.
     * 
     * @param ndx
     *            The source index partition.
     * 
     * @return A {@link Split}[] array contains everything that we need to
     *         define the new index partitions <em>except</em> the
     *         partition identifiers.
     * 
     * @see #getSplits(IIndex, int, Sample[])
     */
    public Split[] getSplits(IResourceManager resourceManager, IIndex ndx) {

        final AtomicLong nvisited = new AtomicLong();

        final Sample[] samples = sampleIndex(ndx, nvisited);

        if (nvisited.get() < overCapacityMultiplier * getEntryCountPerSplit()) {

            if(INFO)
            log.info("Will not split : nvisited=" + nvisited + " is less than "
                    + overCapacityMultiplier + " * entryCountPerSplit("
                    + entryCountPerSplit + ")");

            return null;

        }

        /*
         * Compute the actual #of splits
         */
        final int nsplits = (int) Math
                .floor((nvisited.get() / getUnderCapacityMultiplier())
                        / getEntryCountPerSplit());

        if (nsplits < 2) {

            /*
             * Split rejected based on insufficient capacity in the result
             * splits for the configured undercapacity multiplier.
             */

            if(INFO)
            log.info("Will not split : nsplits(" + nsplits
                    + ") := floor(nvisited(" + nvisited
                    + ") / underCapacityMultiplier("
                    + getUnderCapacityMultiplier() + ") / entryCountPerSplit("
                    + +entryCountPerSplit + ")");

            return null;

        }

        return getSplits(resourceManager, ndx, nsplits, samples, nvisited.get());

    }

    /**
     * Examine the {@link Sample}s choosing {@link Split}s that best capture
     * the target #of splits to be made.
     * <p>
     * Note: If you are trying to write a custom split rule then consider
     * subclassing this method and adjust the split points so as to obey any
     * application constraint, such as not splitting a primary key across index
     * partitions. In general, the split rule can scan forward or backward until
     * it finds a suitable adjusted split point.
     * <p>
     * Note: The caller MUST disregard the {@link IResourceMetadata}[] attached
     * to {@link Split#pmd} since we do not have that information on hand here.
     * The correct {@link IResourceMetadata}[] is available locally to
     * {@link AtomicUpdateSplitIndexPartitionTask}.
     * 
     * @param ndx
     *            The source index partition.
     * @param nsplits
     *            The target #of splits. If necessary or desired, the #of splits
     *            MAY be changed simply by returning an array with a different
     *            #of splits -or- <code>null</code> iff you decide that you do
     *            not want the index partition to be split at this time.
     * @param samples
     *            An ordered array of samples from the index partition. See
     *            {@link #sampleIndex(IIndex, AtomicLong)}.
     * @param nvisited
     *            The #of index entries that were visited when generating those
     *            samples. This is capped at {@link Integer#MAX_VALUE} by
     *            {@link #sampleIndex(IIndex, AtomicLong)}.
     * 
     * @return A {@link Split}[] array containing everything that we need to
     *         define the new index partitions (including the new partition
     *         identifiers assigned by the {@link IMetadataService}) -or-
     *         <code>null</code> if a more detailed examination reveals that
     *         the index SHOULD NOT be split at this time.
     * 
     * @see #getEntryCountPerSplit()
     * @see #getUnderCapacityMultiplier()
     * 
     * @todo there are a lot of edge conditions in this -- write tests!
     */
    protected Split[] getSplits(final IResourceManager resourceManager, final IIndex ndx,
            final int nsplits, final Sample[] samples, final long nvisited) {

        // The source index partition metadata.
        final IndexMetadata indexMetadata = ndx.getIndexMetadata();

        // The target #of index entries per split.
        final int targetCapacity = (int) (getEntryCountPerSplit() * getUnderCapacityMultiplier());

        // The splits that we will generate.
        final List<Split> splits = new ArrayList<Split>(nsplits);

        // The metadata for the index partition that is being split. 
        final LocalPartitionMetadata oldpmd = ndx.getIndexMetadata().getPartitionMetadata();
        
        // index into the samples[].
        int j = 0;
        // #of index entries assigned into splits so far.
        int nused = 0;
        // begin at index zero into the source index partition.
        int fromIndex = 0;
        // begin with the leftSeparator for the source index partition.
        byte[] fromKey = oldpmd.getLeftSeparatorKey();

        for (int i = 0; i < nsplits; i++) {

            Sample sample = null;

            // consider remaining samples.
            for (; j < samples.length; j++) {

                sample = samples[j];

                final int count = sample.offset - nused;

                if (count >= targetCapacity) {

                    if(INFO)
                    log.info("Filled split[" + i + "] with " + count
                            + " entries: targetCapacity=" + targetCapacity
                            + ", samples[j]=" + sample);

                    j++; // consumed.

                    nused += count;

                    break;

                }

            }

            final int toIndex;
            if (sample == null) {

                assert j == samples.length : "j="+j+", samples.length="+samples.length;

                toIndex = 0;

            } else {

                toIndex = sample.offset;

            }

            if (fromIndex == toIndex) {

                /*
                 * Note: I've seen what appears to be an empty Split, which is
                 * illegal, so I added this to get some more information on when
                 * that occurs and added detail to the asserts in the Split()
                 * ctor. If you see this warning look into it a bit further and
                 * see what the fence post conditions are.
                 */
                
                log
                        .warn("Skipping over an empty split: fromIndex="
                                + fromIndex + ", toIndex=" + toIndex + ", j="
                                + j + ", nused=" + nused + ", fromKey="
                                + fromKey + ", sample=" + sample);

                continue;
                
            }
            
            final byte[] toKey;
            if (i == nsplits - 1) {

                // Note: always assign the rightSeparator to the last split.

                toKey = ndx.getIndexMetadata().getPartitionMetadata()
                        .getRightSeparatorKey();

            } else {

                assert sample != null;

                toKey = sample.key;

            }

            // Get the next partition identifier for the named scale-out index.
            final IMetadataService mds = resourceManager.getFederation().getMetadataService();
            final int partitionId;
            try {

                partitionId = mds.nextPartitionId(indexMetadata.getName());

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

            final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                    partitionId, fromKey, toKey,
                    /*
                     * Note: no resources for an index segment
                     */
                    null,//
                    oldpmd.getHistory()+
                    "chooseSplitPoint(oldPartitionId="
                            + oldpmd.getPartitionId() + ",nsplits=" + nsplits
                            + ",newPartitionId=" + partitionId + ") "
                    );

            splits.add( new Split(pmd, fromIndex, toIndex) );

            fromKey = toKey;

            fromIndex = toIndex;

        } // next split.

        final int splitCount = splits.size();
        
        if (splitCount <= 1) {

            log.warn("No splits! splitCount="+splitCount);
            
            return null;
            
        }
        
        return splits.toArray(new Split[splitCount]);

    }

}
