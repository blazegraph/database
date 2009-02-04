package com.bigdata.resources;

import java.lang.ref.SoftReference;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.Name2Addr.Entry;
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
    
    protected final OverflowMetadata omd;

    /**
     * This is where we can (re-)load the {@link BTree}.
     */
    private final AbstractJournal oldJournal;
    
    /**
     * This is the {@link Checkpoint} record address on that journal.
     */
    private final long checkpointAddr;
    
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
                 * The mutable btree on the old journal, not the full view of
                 * that index.
                 */

                btree = (BTree) oldJournal.getIndex(checkpointAddr);

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
        
        ref.clear();
        
    }

    public final IndexMetadata indexMetadata;
    
    public final LocalPartitionMetadata pmd;
    
    public final int sourceCount;
    
    public final int sourceJournalCount;
    
    public final int sourceSegmentCount;
    
    /**
     * The entry count for the {@link BTree} itself NOT the view.
     */
    public final int entryCount;

    /**
     * The action taken and <code>null</code> if no action has been taken
     * for this local index.
     */
    public OverflowActionEnum action;
    
    public void setAction(final OverflowActionEnum action) {

        if (action == null)
            throw new IllegalArgumentException();
        
        omd.setAction(name, action);
        
    }
    
    /**
     * 
     * @param omd
     * @param oldJournal
     * @param entry
     */
    public BTreeMetadata(final OverflowMetadata omd, final AbstractJournal oldJournal, final Entry entry) {

        if (omd == null)
            throw new IllegalArgumentException();

        if (oldJournal == null)
            throw new IllegalArgumentException();

        if (entry == null)
            throw new IllegalArgumentException();

        this.omd = omd;
        
        this.oldJournal = oldJournal;
        
        this.checkpointAddr = entry.checkpointAddr;

        this.name = entry.name;

        // eager resolution to put a SoftReference in place.
        final BTree btree = getBTree();
        
        // index metadata for that index partition.
        indexMetadata = btree.getIndexMetadata();

        // index partition metadata
        pmd = indexMetadata.getPartitionMetadata();

        if (pmd == null)
            PostProcessOldJournalTask.log.warn("Not an index partition: " + name);

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
        
        // BTree's directly maintained entry count (very fast).
        entryCount = btree.getEntryCount(); 
        
    }

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append("name=" + name);

        sb.append(", entryCount=" + entryCount);

        sb.append(", sourceCounts=" + "{all=" + sourceCount + ",journals="
                + sourceJournalCount + ",segments=" + sourceSegmentCount + "}");

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
