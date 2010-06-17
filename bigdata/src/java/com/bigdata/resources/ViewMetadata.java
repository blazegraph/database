package com.bigdata.resources;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.IndexSegment;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.service.Event;
import com.bigdata.service.Params;
import com.bigdata.util.InnerCause;

/**
 * Adds additional metadata to a {@link BTreeMetadata} that deals with the index
 * partition view, including its fast rangeCount, its {@link ISplitHandler},
 * etc.
 * <p>
 * Note: There is overhead in opening a view comprised of more than just the
 * mutable {@link BTree}. That is why there is a separation between the
 * {@link BTreeMetadata} class and the {@link ViewMetadata}. The latter will
 * force any {@link IndexSegment} in the view to be (re-)opened.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class ViewMetadata extends BTreeMetadata implements Params {

    /**
     * Set <code>true</code> iff the index partition view is requested and the
     * various additional data are collected from that view (e.g., range count).
     * This is done in order to prevent re-initialization of such lazily
     * obtained data.
     */
    private boolean initView = false;

    /**
     * Cached fast range count once initialized from view.
     */
    private volatile long rangeCount;
    
    /**
     * Cached index partition count once initialized from the view.
     */
    private volatile long partitionCount;

    /**
     * The adjusted nominal size of an index partition. Index partitions are
     * split once {@link #sumSegBytes} is GT this value. This value MAY be
     * adjusted down by an "acceleration" factor.
     * 
     * @see OverflowManager.Options#NOMINAL_SHARD_SIZE
     */
    private volatile long adjustedNominalShardSize;

    /**
     * Cached estimated percentage of a split once initialized from the view.
     * <p>
     * Note: the percentOfSplit when based on sumSegBytes is not 100% predictive
     * unless we have a compact view since the size on disk of the compact
     * segment can be much less. This is especially true when many deleted
     * tuples are purged by the compacting merge, in which case the segment
     * could shrink to zero during the merge.
     */
    private volatile double percentOfSplit;

    /**
     * Cached decision whether or not a tail split is warranted once initialized
     * from the view.
     */
    private volatile boolean tailSplit;
    
    /**
     * A {@link SoftReference} is used to cache the view since we really want to
     * hold onto the reference until we get around to finishing overflow
     * processing for this index partition. However, you SHOULD clear the
     * reference using {@link #clearRef()} as soon as you have handled
     * asynchronous overflow for this view.
     */
    private volatile SoftReference<ILocalBTreeView> ref;

    /**
     * Open the historical view of that index at that time (not just the mutable
     * BTree but the full view). The view is cached. If the reference has been
     * cleared then the view is re-opened. This also initializes values
     * requiring additional effort which are not available until this method is
     * invoked including {@link #getRangeCount()}, etc.
     */
    public ILocalBTreeView getView() {

        // double checked locking.
        ILocalBTreeView view = ref == null ? null : ref.get();

        if (view == null) {

            synchronized (this) {

                view = ref == null ? null : ref.get();

                if (view == null) {

                    view = (ILocalBTreeView) resourceManager.getIndex(name,
                            commitTime);

                    ref = new SoftReference<ILocalBTreeView>(view);

                    initView(view);

                }

            }

        }

        assert view != null : toString();
        
        return view;

    }

    /**
     * Release the {@link SoftReference} for the index partition view.
     * 
     * @todo refactor into clearBTreeRef() and clearViewRef(). The latter calls
     *       the former.  check all usage to make sure that we are invoking the
     *       correct method.
     */
    public void clearRef() {
        
        synchronized(this) {
            
            if(ref != null) { 
                
                ref.clear();
        
            }

        }

        super.clearRef();
        
    }

    /**
     * Initialize additional data with higher latency (range count, #of index
     * partitions, the adjusted split handler, etc.).
     * 
     * @param view
     *            The view.
     */
    synchronized private void initView(final ILocalBTreeView view) {

        if (view == null) {
            
            throw new AssertionError("View not found? " + this);
            
        }
        
        if (initView) {

            /*
             * This stuff only has to be done once even if the view is released.
             */
            
            return;
            
        }

        /*
         * Obtain the #of index partitions for this scale-out index.
         * 
         * Note: This may require RMI, but the metadata index is also heavily
         * cached by the {@link AbstractFederation}.
         * 
         * Note: This must be done before we obtain the adjusted split handler
         * as [npartitions] is an input to that process.
         */
        {
            long npartitions;
            try {

                final IMetadataIndex mdi = resourceManager.getFederation()
                        .getMetadataIndex(indexMetadata.getName(), commitTime);

                if (mdi == null) {

                    log.warn("No metadata index: running in test harness?");

                    npartitions = 1L;

                } else {

                    npartitions = mdi.rangeCount();

                    if (npartitions == 0) {

                        /*
                         * There must always be at least one index partition for
                         * a scale-out index so this is an error condition.
                         */
                        log.error("No partitions? name="
                                + indexMetadata.getName());

                    }

                }

            } catch (Throwable t) {

                if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                    // don't trap interrupts.
                    throw new RuntimeException(t);

                }

                /*
                 * Traps any RMI failures (or anything else), logs a warning,
                 * and reports npartitions as -1L.
                 */

                log.error("name=" + indexMetadata.getName(), t);

                npartitions = -1L;

            }

            this.partitionCount = npartitions;

        }

        /*
         * This computes the target size on the disk for a compact index segment
         * for the shard. The calculation concerns an acceleration factor based
         * on some desired minimum number of shards for the index and uses the
         * nominalShardSize if the minimum has been satisfied.
         */
        {

            final int accelerateSplitThreshold = resourceManager.accelerateSplitThreshold;

            if (accelerateSplitThreshold == 0) {

                this.adjustedNominalShardSize = resourceManager.nominalShardSize;

            } else {

                // discount: given T=100, will be 1 when N=100; 10 when N=10,
                // and
                // 100 when N=1.
                final double d = (double) partitionCount
                        / accelerateSplitThreshold;

                this.adjustedNominalShardSize = (long) (resourceManager.nominalShardSize * d);

                if (log.isInfoEnabled())
                    log.info("npartitions=" + partitionCount + ", discount=" + d
                            + ", threshold=" + accelerateSplitThreshold
                            + ", adjustedNominalShardSize="
                            + this.adjustedNominalShardSize
                            + ", nominalShardSize="
                            + resourceManager.nominalShardSize);
            }

        }

        /*
         * Range count for the view (fast.
         */
        this.rangeCount = view.rangeCount();

        /*
         * The percentage of a full index partition fulfilled by this view.
         * 
         * Note: the percentOfSplit when based on sumSegBytes is not 100%
         * predictive unless we have a compact view since the size on disk of
         * the compact segment can be much less. This is especially true when
         * many deleted tuples are purged by the compacting merge, in which case
         * the segment could shrink to zero during the merge.
         */
        this.percentOfSplit = super.sumSegBytes / (double) adjustedNominalShardSize;

        /*
         * true iff this is a good candidate for a tail split.
         */
        this.tailSplit = //
        this.percentOfSplit > resourceManager.percentOfSplitThreshold && //
        super.percentTailSplits > resourceManager.tailSplitThreshold//
        ;

        initView = true;

    }

    /**
     * The fast range count of the view (cached).
     * 
     * @throws IllegalStateException
     *             unless {@link #getView()} has been invoked.
     */
    public long getRangeCount() {

        if(!initView) {

            // materialize iff never initialized.
            getView();
            
        }

        return rangeCount;

    }

    /**
     * Return the #of index partitions for this scale-out index. The value is
     * computed once per overflow event and then cached.
     */
    public long getIndexPartitionCount() {
        
        if(!initView) {
            
            // materialize iff never initialized.
            getView();
            
        }
        
        return partitionCount;
        
    }

    /**
     * The adjusted nominal size on disk of a shard after a compacting merge
     * (cached). This factors in an optional acceleration factor which causes
     * shards to be split when they are smaller unless a minimum #of shards
     * exist for that index.
     * 
     * @see OverflowManager.Options#NOMINAL_SHARD_SIZE
     */
    public long getAdjustedNominalShardSize() {

        if(!initView) {
            
            // materialize iff never initialized
            getView();
            
        }
        
        return adjustedNominalShardSize;
        
    }
    
    /**
     * Estimated percentage of a split based on the size on disk (cached).
     * <p>
     * Note: the percentOfSplit is not 100% predictive unless we have a compact
     * view since the size on disk of the compact segment can be much less. This
     * is especially true when many deleted tuples are purged by the compacting
     * merge, in which case the segment could shrink to zero during the merge.
     */
    public double getPercentOfSplit() {
        
        if (!initView) {

            // materialize iff never initialized.
            getView();

        }

        return this.percentOfSplit;

    }

    /**
     * Return <code>true</code> if the index partition satisfies the criteria
     * for a tail split (heavy writes on the tail of the index partition and the
     * size of the index partition is large enough to warrant a tail split).
     * 
     * @see OverflowManager.Options#TAIL_SPLIT_THRESHOLD
     * @see OverflowManager.Options#PERCENT_OF_SPLIT_THRESHOLD
     */
    public boolean isTailSplit() {
        
        if(!initView) {
            
            getView();
            
        }
        
        return tailSplit;
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Note: The ctor intentionally does not force the materialization of the
     * view or perform any RMI. Those operations are done lazily in order to not
     * impose their latency during synchronous overflow.
     */
    public ViewMetadata(
            final ResourceManager resourceManager, final long commitTime,
            final String name, final BTreeCounters btreeCounters) {

        super(resourceManager, commitTime, name, btreeCounters);

    }

    /**
     * Extended for more metadata.
     */
    protected void toString(final StringBuilder sb) {

        if (initView) {

            sb.append(", rangeCount=" + rangeCount);

            sb.append(", partitionCount=" + partitionCount);

            sb.append(", adjustedNominalShardSize=" + adjustedNominalShardSize);

            sb.append(", percentOfSplit=" + percentOfSplit);

            sb.append(", tailSplit=" + tailSplit);

        }

    }
    
    /**
     * Returns all the interesting properties in a semi-structured form which
     * can be used to log an {@link Event}.
     */
    public Map<String,Object> getParams() {

        final Map<String, Object> m = new HashMap<String, Object>();

        /*
         * Fields from the BTreeMetadata class.
         */

        m.put("name", name);

        m.put("action", getAction());

        m.put("entryCount", entryCount);

        m.put("sourceCount", sourceCount);

        m.put("journalSourceCount", sourceJournalCount);

        m.put("segmentSourceCount", sourceSegmentCount);

        m.put("mergePriority", mergePriority);

//        m.put("splitPriority", splitPriority);

        m.put("manditoryMerge", mandatoryMerge);

        m.put("#leafSplit", btreeCounters.leavesSplit);

        m.put("#headSplit", btreeCounters.headSplit);

        m.put("#tailSplit", btreeCounters.tailSplit);

        m.put("percentHeadSplits", percentHeadSplits);

        m.put("percentTailSplits", percentTailSplits);

        /*
         * Fields from the ViewMetadata class.
         */
        
        m.put("rangeCount", rangeCount);

        m.put("partitionCount", partitionCount);

        m.put("adjustedNominalShardSize", adjustedNominalShardSize);

        m.put("percentOfSplit", percentOfSplit);

        m.put("tailSplit", tailSplit);

        return m;
        
    }
    
}
