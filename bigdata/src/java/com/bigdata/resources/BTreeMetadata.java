package com.bigdata.resources;

import java.lang.ref.SoftReference;

import javax.swing.text.View;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;

/**
 * Class encapsulates a bunch of metadata used to make decisions about how to
 * handle an index partition during asynchronous overflow.
 * <p>
 * Note: This class uses {@link SoftReference}s to hold onto the mutable
 * {@link BTree}. The {@link SoftReference} was choosen because it is important
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
    
    static protected final boolean INFO = log.isInfoEnabled();
    
    static protected final boolean DEBUG = log.isDebugEnabled();
    
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
    
    public final int sourceCount;
    
    public final int sourceJournalCount;
    
    public final int sourceSegmentCount;

    /**
     * <code>true</code> iff this index partition meets the criteria for a
     * manditory compacting merge (too many journals in the view, too many
     * index segments in the view, or too many sources in the view).
     */
    public final boolean manditoryMerge;
    
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
     * The action taken and <code>null</code> if no action has been taken for
     * this local index.
     */
    public OverflowActionEnum action;
    
    /**
     * 
     * @param resourceManager
     *            Used to (re-)open the {@link BTree} as necessary.
     * @param commitTime
     *            The commit time corresponding to the desired commit point.
     * @param name
     *            The name of the {@link BTree}.
     * @param btreeCounters
     *            The aggregated counters for the {@link BTree} or the index
     *            partition {@link View} as reported by the
     *            {@link ConcurrencyManager}
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
        if (pmd != null) {
            for (IResourceMetadata x : pmd.getResources()) {
                sourceCount++;
                if (x.isJournal()) {
                    sourceJournalCount++;
                } else {
                    sourceSegmentCount++;
                }
            }
        }
        this.sourceCount = sourceCount;
        this.sourceJournalCount = sourceJournalCount;
        this.sourceSegmentCount = sourceSegmentCount;

        this.manditoryMerge //
            =  sourceJournalCount > resourceManager.maximumJournalsPerView //
            || sourceSegmentCount > resourceManager.maximumSegmentsPerView //
//          || sourceCount > resourceManager.maximumSourcesPerView//
        ;
        
        // BTree's directly maintained entry count (very fast).
        this.entryCount = btree.getEntryCount();

        this.btreeCounters = btreeCounters;

        if (btreeCounters != null) {

            // Note: +1 in the denominator to avoid divide by zero.
            this.percentHeadSplits = btreeCounters.headSplit
                    / (btreeCounters.leavesSplit + 1d);

            // Note: +1 in the denominator to avoid divide by zero.
            this.percentTailSplits = btreeCounters.tailSplit
                    / (btreeCounters.leavesSplit + 1d);

        } else {
            
            this.percentHeadSplits = 0d;

            this.percentTailSplits = 0d;
            
        }

    }
    
    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append("name=" + name);

        sb.append(", entryCount=" + entryCount);

        sb.append(", sourceCounts=" + "{all=" + sourceCount + ",journals="
                + sourceJournalCount + ",segments=" + sourceSegmentCount + "}");

        sb.append(", manditoryMerge=" + manditoryMerge);
        
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
