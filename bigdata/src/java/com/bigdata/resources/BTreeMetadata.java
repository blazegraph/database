package com.bigdata.resources;

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.StoreManager.ManagedJournal;

/**
 * Class encapsulates a bunch of metadata used to make decisions about how to
 * handle an index partition during asynchronous overflow.
 * <p>
 * Note: This class uses {@link SoftReference}s to hold onto the mutable
 * {@link BTree}. The {@link SoftReference} was chosen because it is important
 * to keep these {@link BTree}s open so that we do not loose their buffers
 * until we have finish asynchronous overflow for a given {@link BTree}. Once
 * asynchronous overflow processing is complete for the {@link BTree} you SHOULD
 * use {@link #clearRef()} to release your hold those buffers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class BTreeMetadata {

    static protected final Logger log = Logger.getLogger(BTreeMetadata.class);
    
    /**
     * The object which may be used to (re-)open the {@link BTree} and the index
     * partition view.
     */
    protected final ResourceManager resourceManager;

    /**
     * The commit time associated with the {@link BTree} and the index partition
     * view.
     */
    public final long commitTime;
    
    /**
     * Name of the local index (the index partition name).
     */
    public final String name;
    
    /**
     * A {@link SoftReference} is used to cache the {@link BTree} reference
     * since we really want to hold onto the reference until we get around to
     * finishing overflow processing for this index partition. However, you
     * SHOULD clear the reference using {@link #clearRef()} as soon as you
     * have handled asynchronous overflow for this view.
     */
    private volatile SoftReference<BTree> ref;

    /**
     * Open the mutable {@link BTree}. The {@link BTree} reference is cached by
     * a {@link SoftReference}. If the reference has been cleared then the
     * {@link BTree} is re-opened from the backing journal.
     */
    final public BTree getBTree() {

        // double checked locking.
        BTree btree = ref == null ? null : ref.get();

        if (btree == null) {

            synchronized (this) {

                btree = ref == null ? null : ref.get();

                /*
                 * The mutable btree on the journal associated with the
                 * commitTime, not the full view of that index.
                 */

                final AbstractJournal store = resourceManager
                        .getJournal(commitTime);

                btree = (BTree) resourceManager.getIndexOnStore(name,
                        commitTime, store);

                if (btree == null)
                    throw new IllegalArgumentException();

                ref = new SoftReference<BTree>(btree);

            }
            
        }

        return btree;

    }

    /**
     * Release the {@link SoftReference} for the {@link BTree}.
     */
    public void clearRef() {
        
        synchronized(this) {
            
            if(ref != null) { 
                
                ref.clear();
        
            }

        }
        
    }

    public final IndexMetadata indexMetadata;
    
    public final LocalPartitionMetadata pmd;
    
    /**
     * The #of journals and index segments in the view.
     */
    public final int sourceCount;
    
    /**
     * The #of journals in the view.
     */
    public final int sourceJournalCount;

    /**
     * The #of index segments in the view.
     */
    public final int sourceSegmentCount;

    /**
     * The sum of the size on disk across the index segments in the view.
     */
    public final long sumSegBytes;

    /**
     * These constants are used to compute the {@link #mergePriority}.
     * 
     * See src/architecture/mergePriority.xls.
     */
    final private int A = 3, B = 1;//, C = 10;

    /**
     * This is the inverse of the {@link #mergePriority}. If the merge priority
     * is ZERO (0), then this is 1.0 (which is greater than the build priority
     * which associated with any index partition with a non-zero merge
     * priority).
     */
    public final double buildPriority;

    /**
     * The computed merge priority is based on the complexity of the view. This
     * is ZERO (0) if there is no reason to perform a merge.
     */
    public final double mergePriority;

//    /**
//     * The split priority is based solely on {@link #sumSegBytes} (it is the
//     * ratio of that value to the nominal shard size) and is ZERO (0) until
//     * {@link #sumSegBytes} is GTE the {@link OverflowManager#nominalShardSize}.
//     *
//     * @deprecated This did not consider the adjusted nominal shard size.
//     */
//    public final double splitPriority;

    /**
     * <code>true</code> iff this index partition meets the criteria for a
     * mandatory compacting merge (too many journals in the view, too many index
     * segments in the view, or too many sources in the view).
     * 
     * @deprecated by {@link #mergePriority}.
     *             <p>
     *             Note: This field can be dropped once we are running split,
     *             tailSplit, and scatterSplit as merge after actions. At the
     *             same time, make sure that those tasks will not accept a view
     *             unless it is a {@link #compactView}.
     */
    public final boolean mandatoryMerge;

    /**
     * <code>true</code> iff there are two sources in the view and the second
     * source is an {@link IndexSegment} (the first will be the {@link BTree} on
     * some {@link ManagedJournal}).
     */
    public final boolean compactView;
    
    /**
     * The entry count for the {@link BTree} itself NOT the view.
     */
    public final int entryCount;

    /**
     * The counters for the index partition view.
     */
    public final BTreeCounters btreeCounters;
    
    /**
     * The percentage of leaf splits which occurred at or near the head of the
     * {@link BTree}.
     */
    public final double percentHeadSplits;

    /**
     * The percentage of leaf splits which occurred at or near the tail of the
     * {@link BTree}.
     */
    public final double percentTailSplits;

    /**
     * A package private lock used to ensure that decisions concerning which
     * index partition operation (build, merge, split, etc) to execute are
     * serialized.
     * 
     * @see OverflowMetadata#setAction(String, OverflowActionEnum)
     */
    final ReentrantLock lock = new ReentrantLock();

    /**
     * The action taken and <code>null</code> if no action has been taken for
     * this local index.
     * <p>
     * Note: An {@link AtomicReference} is used to permit inspection of the
     * value without holding the {@link #lock}. If you want to make an atomic
     * decision based on this value, then make sure that you are holding the
     * {@link #lock} before you look at the value.
     */
    private final AtomicReference<OverflowActionEnum> actionRef = new AtomicReference<OverflowActionEnum>();

    /**
     * The action taken and <code>null</code> if no action has been taken for
     * this local index (non-blocking). 
     * <p>
     * Note: An {@link AtomicReference} is used to permit inspection of the
     * value without holding the {@link #lock}. If you want to make an atomic
     * decision based on this value, then make sure that you are holding the
     * {@link #lock} before you look at the value.
     */
    public OverflowActionEnum getAction() {
        
        return actionRef.get();
        
    }

    /**
     * Set the action to be taken.
     * 
     * @param action
     *            The action.
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalMonitorStateException
     *             unless the {@link #lock} is held by the caller.
     * @throws IllegalStateException
     *             if the action has already been set.
     */
    public void setAction(final OverflowActionEnum action) {
        
        if(action == null)
            throw new IllegalArgumentException();

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (actionRef.get() != null) {

            throw new IllegalStateException("Already set: " + actionRef.get()
                    + ", given=" + action);
            
        }
        
        actionRef.set(action);

    }

    /**
     * Used to force clear a {@link OverflowActionEnum#Copy} action
     * when we will force a compacting merge.  This allows us to do
     * compacting merges on shard views which would otherwise simply
     * be copied onto the new journal.
     */
    void clearCopyAction() {

	lock.lock();
	try {
	    if(actionRef.get().equals(OverflowActionEnum.Copy)) {
		actionRef.set(null/*clear*/);
	    }
	} finally {
	    lock.unlock();
	}

    }
    
    /**
     * 
     * @param resourceManager
     *            Used to (re-)open the {@link BTree} as necessary.
     * @param commitTime
     *            The commit time corresponding to the desired commit point.
     * @param name
     *            The name of the {@link BTree}.
     * @param btreeCounters
     *            The aggregated counters for the {@link BTree} or the
     *            {@link ILocalBTreeView index partition view} as reported by
     *            the {@link IndexManager}
     */
    public BTreeMetadata(final ResourceManager resourceManager,
            final long commitTime, final String name,
            final BTreeCounters btreeCounters) {

        if (resourceManager == null)
            throw new IllegalArgumentException();

        if (name == null)
            throw new IllegalArgumentException();

        if (btreeCounters == null)
            throw new IllegalArgumentException();
        
        this.resourceManager = resourceManager;

        this.commitTime = commitTime;
        
        this.name = name;

        // eager resolution to put a SoftReference in place.
        final BTree btree = getBTree();

        // index metadata for that index partition.
        indexMetadata = btree.getIndexMetadata();

        // index partition metadata
        pmd = indexMetadata.getPartitionMetadata();

        if (pmd == null)
            log.warn("Not an index partition: " + name);

        // #of sources in the view (very fast).
        int sourceCount = 0, sourceJournalCount = 0, sourceSegmentCount = 0;
        long sumSegBytes = 0L;
        boolean secondSourceIsSeg = false;
        if (pmd != null) {
            for (IResourceMetadata x : pmd.getResources()) {
                sourceCount++;
                if (x.isJournal()) {
                    sourceJournalCount++;
                } else {
                    sourceSegmentCount++;
                    /*
                     * Note: This opens the backing segment store in order to
                     * determine its size on the disk. This is a fairly light
                     * weight operation (the nodes region is not read, just the
                     * checkpoint record and the IndexMetadata record).
                     * 
                     * Note: This does not use IResourceMetadata#getFile() to
                     * determine the size of the file in the backing file system
                     * because it is not an absolute file path.
                     */
                    final IRawStore store = resourceManager.openStore(x.getUUID());
                    sumSegBytes += store.size();//new File(x.getFile()).length();
                    if (sourceCount == 2) {
                        secondSourceIsSeg = true;
                    }
                }
            }
        }
        this.sourceCount = sourceCount;
        this.sourceJournalCount = sourceJournalCount;
        this.sourceSegmentCount = sourceSegmentCount;
        this.sumSegBytes = sumSegBytes;
        this.compactView = sourceCount == 2 && secondSourceIsSeg;

        if (sourceJournalCount + sourceSegmentCount < 2) {
            /*
             * Nothing to merge. The build priority is 1.0 for this case. The
             * maximum build priority for an index partition with a non-zero
             * mergePriority will always be less than one.
             */
            this.mergePriority = 0d;
            this.buildPriority = 1d;
        } else {
            /*
             * Compute a score that will be used to prioritize compacting merges
             * vs builds for index partitions where either option is allowable.
             * The higher the score, the more we want to make sure that we do a
             * compacting merge for that index.
             * 
             * Note: The main purpose of an index partition build is to convert
             * from a write-order to a read-order and permit the release of the
             * old journal. However, applications which require frequent access
             * to historical commit points on the old journals will continue to
             * rely on the write-order journals.
             * 
             * Note: I have removed the sumSegBytes term from this formula since
             * that would tend to cause the priority of merge to increase for a
             * view until a split is performed, with the likelihood that
             * repeated merges would be performed for the same view just when it
             * is nearing its largest extent. Instead I have modified the
             * formula to consider only the view complexity for merge.
             * 
             * @todo if the application requires access to modest amounts of
             * history then consider a policy where the buffers are retained for
             * old journals up to the minReleaseAge. Of course, this can run
             * into memory constraints so that needs to be traded off against
             * IOWAIT.
             */
            this.mergePriority = (sourceJournalCount - 1) * A
                    + (sourceSegmentCount * B)
                    //+ ((sumSegBytes / resourceManager.nominalShardSize) * C)
                    ;
            this.buildPriority = 1. / mergePriority;
        }

//        /*
//         * The splitPriority considers only sumSegBytes.
//         * 
//         * @todo This does not consider the adjustedNominalShardSize.
//         */
//        this.splitPriority = (sumSegBytes < resourceManager.nominalShardSize) ? 0
//                : (sumSegBytes / (double) resourceManager.nominalShardSize);

        this.mandatoryMerge //
            =  sourceJournalCount >= resourceManager.maximumJournalsPerView //
            || sourceSegmentCount >= resourceManager.maximumSegmentsPerView //
        ;
        
        // BTree's directly maintained entry count (very fast).
        this.entryCount = btree.getEntryCount();

        this.btreeCounters = btreeCounters;

        // Note: +1 in the denominator to avoid divide by zero.
        this.percentHeadSplits = btreeCounters.headSplit
                / (btreeCounters.leavesSplit + 1d);

        // Note: +1 in the denominator to avoid divide by zero.
        this.percentTailSplits = btreeCounters.tailSplit
                / (btreeCounters.leavesSplit + 1d);

    }
        
    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append("name=" + name);

        sb.append(", action=" + actionRef.get());
        
        sb.append(", entryCount=" + entryCount);

        sb.append(", sumSegBytes=" + sumSegBytes);

        sb.append(", mergePriority=" + mergePriority);

//        sb.append(", splitPriority=" + splitPriority);

        sb.append(", manditoryMerge=" + mandatoryMerge);
        
        sb.append(", sourceCounts=" + "{all=" + sourceCount + ",journals="
                + sourceJournalCount + ",segments=" + sourceSegmentCount + "}");
        
        sb.append(", #leafSplit=" + btreeCounters.leavesSplit);
        
        sb.append(", #headSplit=" + btreeCounters.headSplit);
        
        sb.append(", #tailSplit=" + btreeCounters.tailSplit);

        sb.append(", percentHeadSplits=" + percentHeadSplits);

        sb.append(", percentTailSplits=" + percentTailSplits);
        
        toString(sb);

        return sb.toString();

    }

    /**
     * Permits extension of {@link #toString()} in subclass.
     * 
     * @param sb
     */
    protected void toString(final StringBuilder sb) {
        
        // NOP
        
    }
    
}
