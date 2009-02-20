package com.bigdata.resources;

import java.lang.ref.SoftReference;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.mdi.IMetadataIndex;
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
     * Cached estimated percentage of a split once initialized from the view.
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

        /*
         * true iff this is a good candidate for a tail split.
         * 
         * @todo this does not pay attention to the #of tuples at which the
         * index partition would underflow. instead it assumes that underflow is
         * a modest distance from a full split. for example, we could also test
         * NOT(isJoinCandidate) to determine if the tail split would underflow.
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

    /**
     * Return <code>true</code> if the index partition satisifies the criteria
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

            sb.append(", percentOfSplit=" + percentOfSplit);

            sb.append(", adjustedSplitHandler=" + adjustedSplitHandler);

        }

    }
    
    /**
     * Return an adjusted split handler.
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

        if (!(splitHandler instanceof DefaultSplitHandler)) {
            
            return splitHandler;
            
        }
        
        final int accelerateSplitThreshold = resourceManager.accelerateSplitThreshold;

        if (accelerateSplitThreshold == 0) {

            // feature is disabled.
            return splitHandler;
            
        }
        
        final long npartitions;
        try {

            /*
             * The #of index partitions for this scale-out index.
             * 
             * Note: This may require RMI, but the metadata index is also
             * heavily cached by the client.
             */

            final IMetadataIndex mdi = resourceManager.getFederation()
                    .getMetadataIndex(indexMetadata.getName(), commitTime);
            
            if (mdi == null) {
                
                log.warn("No metadata index: running in test harness?");
                
                npartitions = 1L;
                
            } else {
            
                npartitions = mdi.rangeCount();
                
            }

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

        return ((DefaultSplitHandler) splitHandler).getAdjustedSplitHandler(
                accelerateSplitThreshold, npartitions);
        
    }

}
