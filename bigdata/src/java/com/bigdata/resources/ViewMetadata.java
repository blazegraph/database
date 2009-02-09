package com.bigdata.resources;

import java.lang.ref.SoftReference;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.service.IMetadataService;
import com.bigdata.util.InnerCause;

/**
 * Adds additional metadata to a {@link BTreeMetadata} that deals with the
 * index partition view, including its rangeCount, its {@link ISplitHandler},
 * etc.
 * <p>
 * Note: There is overhead in opening a view comprised of more than just the
 * mutable {@link BTree}. That is why there is a separation between the
 * {@link BTreeMetadata} class and the {@link ViewMetadata}. The latter
 * will force any {@link IndexSegment} in the view to be (re-)opened.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class ViewMetadata extends BTreeMetadata {

    /**
     * Set <code>true</code> iff the index partition view is requested and the
     * various additional data are collected from that view (e.g., range count).
     * This is done in order to prevent re-initialization of such lazily
     * obtained data.
     */
    private boolean initView = false;
    
    /**
     * Cached range count once initialized from view.
     */
    private long rangeCount;
    
    /**
     * Cached adjusted split handler once initialized from view.
     */
    private ISplitHandler adjustedSplitHandler;
    
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
     * invoked including {@link #getRangeCount()} ,
     * {@link #getAdjustedSplitHandler()}, etc.
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
     * Initialize additional data with higher latency (range count and the
     * adjusted split handler).
     * 
     * @param view
     *            The view.
     */
    synchronized private void initView(final ILocalBTreeView view) {

        if(view == null) {
            
            throw new AssertionError("View not found? " + this);
            
        }
        
        if (initView) {

            /*
             * This stuff only has to be done once even if the view is released.
             */
            
            return;
            
        }

        /*
         * Handler decides when and where to split an index partition.
         * 
         * Note: This is deferred since it requires RMI to the metadata service.
         * Even though that RMI is heavily cached it still makes sense to do
         * this outside of synchronous overflow.
         */
        this.adjustedSplitHandler = getSplitHandler();

        /*
         * range count for the view (fast, but slower when an index segment is
         * used by more than one index since the partition bounds are a subset
         * of the index segment key range).
         */
        this.rangeCount = view.rangeCount();

        this.percentOfSplit = adjustedSplitHandler.percentOfSplit(rangeCount);

        // true iff this is a good candidate for a tail split.
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
     * The adjusted split handler (cached).
     * 
     * @throws IllegalStateException
     *             unless {@link #getView()} has been invoked.
     */
    public ISplitHandler getAdjustedSplitHandler() {

        if(!initView) {

            // materialize iff never initialized.
            getView();
            
        }
        
        return adjustedSplitHandler;

    }

    public double getPercentOfSplit() {
        
        if (!initView) {

            // materialize iff never initialized.
            getView();

        }

        return this.percentOfSplit;

    }

    private volatile double percentOfSplit;

    /**
     * Return <code>true</code> if the index partition satisifies the criteria
     * for a tail split. 
     */
    public boolean isTailSplit() {
        
        if(!initView) {
            
            getView();
            
        }
        
        return tailSplit;
        
    }
    private volatile boolean tailSplit;
    
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

            sb.append(", percentOfSplit=" + percentOfSplit);

            sb.append(", adjustedSplitHandler=" + adjustedSplitHandler);

        }

    }
    
    /**
     * Return an adjusted split handler. The split handler will be adjusted to
     * be heavily biased in favor of splitting an index partition when the #of
     * index partitions for a scale-out index is small. This adjustment is
     * designed to very rapidly (re-)distribute a new scale-out index until
     * there are at least 10 index partitions and rapidly (re-)distribute a
     * scale-out index until they are at least 100 index partitions. Thereafter
     * the as configured behavior will be observed.
     * <p>
     * Note: The potential parallelism of a data service is limited by the #of
     * index partitions on that data service as well as by the workload of the
     * application. By partitioning new and young indices aggressively we ensure
     * that the index is broken into multiple index partitions on the initial
     * data service. Those index partitions will be re-distributed across the
     * available data services based on recommendations made by the load
     * balancer. Both breaking an index into multiple partitions on a single
     * data service and re-distributing those index partitions among the hosts
     * of a cluster will increase the resources (CPU, DISK, RAM) which can be
     * brought to bear on the index. In particular, there is a constraint of a
     * single core per index partition (for writes). Therefore breaking a new
     * index into 2 pieces doubles the potential concurrency for a data service
     * on a host with at least 2 cores. This can be readily extrapolated to a
     * cluster with 8 cores x 16 machines, etc.
     * <p>
     * Note: The adjustment is proportional to the #of existing index partitions
     * and is adjusted using a floating point discount factor. This should
     * prevent a split triggering a subsequent join on the next overflow.
     * 
     * @param indexMetadata
     *            The {@link IndexMetadata} for an index partition being
     *            considered for a split or join.
     * @param commitTime
     *            The commit time that is used to query the
     *            {@link IMetadataService} for the index partition count.
     * 
     * @return The adjusted split handler.
     * 
     * @todo only supports the {@link DefaultSplitHandler}.
     */
    private ISplitHandler getSplitHandler() {

        if (resourceManager == null)
            throw new IllegalArgumentException();

        if (indexMetadata == null)
            throw new IllegalArgumentException();

        final ISplitHandler splitHandler = indexMetadata.getSplitHandler();

        if (splitHandler == null)
            return splitHandler;

        final int accelerateSplitThreshold = resourceManager.accelerateSplitThreshold;

        if (accelerateSplitThreshold == 0) {

            // feature is disabled.
            return splitHandler;
            
        }
        
        if (splitHandler instanceof DefaultSplitHandler) {

            final long npartitions;
            try {

                /*
                 * The #of index partitions for this scale-out index.
                 * 
                 * Note: This may require RMI, but the metadata index is also
                 * heavily cached by the client.
                 */

                npartitions = resourceManager.getFederation().getMetadataIndex(
                        indexMetadata.getName(), commitTime).rangeCount();

            } catch (Throwable t) {

                if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                    // don't trap interrupts.
                    throw new RuntimeException(t);

                }

                /*
                 * Traps any RMI failures (or anything else), logs a warning,
                 * and returns the default splitHandler instead.
                 */

                log.warn("name=" + indexMetadata.getName(), t);

                return splitHandler;

            }

            if (npartitions == 0) {

                /*
                 * There must always be at least one index partition for a
                 * scale-out index so this is an error condition.
                 */
                log.error("No partitions? name=" + indexMetadata.getName());

                return splitHandler;

            }

            if (npartitions >= accelerateSplitThreshold) {

                /*
                 * There are plenty of index partitions. Use the original split
                 * handler.
                 * 
                 * Note: This also prevents our "discount" from becoming an
                 * "inflation" factor!
                 */

                return splitHandler;

            }

            // the split handler as configured.
            final DefaultSplitHandler s = (DefaultSplitHandler) splitHandler;

            // discount: given T=100, will be 1 when N=100; 10 when N=10, and
            // 100 when N=1.
            final double d = (double) npartitions / accelerateSplitThreshold;

            try {

                // adjusted split handler.
                final DefaultSplitHandler t = new DefaultSplitHandler(//
                        (int) (s.getMinimumEntryCount() * d), //
                        (int) (s.getEntryCountPerSplit() * d), //
                        s.getOverCapacityMultiplier(), // unchanged 
                        s.getUnderCapacityMultiplier(), // unchanged
                        s.getSampleRate() // unchanged
                );

                if (INFO)
                    log.info("Adjusted splitHandler:  name="
                            + indexMetadata.getName() + ", npartitions="
                            + npartitions + ", threshold="
                            + accelerateSplitThreshold + ", discount=" + d
                            + ", adjustedSplitHandler=" + t);

                return t;

            } catch (IllegalArgumentException ex) {

                /*
                 * The adjustment violated some constraint. Log a warning and
                 * use the original splitHandler since it was at least valid.
                 */

                log.warn("Adjustment failed: name=" + indexMetadata.getName()
                        + ", npartitions=" + npartitions + ", discount=" + d
                        + ", splitHandler=" + splitHandler, ex);

                return splitHandler;

            }

        }

        return splitHandler;

    }

}
