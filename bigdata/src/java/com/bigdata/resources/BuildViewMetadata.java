package com.bigdata.resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexSegment;
import com.bigdata.service.Event;

/**
 * Helper class examines an index partition view and returns a view for which we
 * can quickly do an incremental build. The given view is always the real index
 * partition view. The identified view will include one or more of the sources
 * in the given view. The first component of the given view is always included
 * in the identified view and will be the {@link BTree} from a journal.
 * Additional sources are incorporated into the identified view until the
 * specified thresholds would be exceeded. The sources are ordered and the order
 * of the sources is maintained. If all sources can be incorporated without
 * exceeding the specified thresholds, then the view is flagged as a compacting
 * merge rather than an incremental build and deleted tuples will be purged from
 * the view.
 * <p>
 * For example, given a view with a {@link BTree} on a journal followed by 2
 * {@link IndexSegment}s, the output view might include just the {@link BTree}
 * or the {@link BTree} followed by the first {@link IndexSegment}, or the
 * {@link BTree} followed by both of the {@link IndexSegment}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class BuildViewMetadata {

    /**
     * The maximum #of bytes of {@link IndexSegment}s to allow into the view
     * (from the ctor).
     */
    public final long maxSumSegBytes;
    
    /** #of sources in the given view. */
    public final int nsources;

    /** #of sources in the accepted view. */
    public final int naccepted;

    /**
     * <code>true</code> iff all sources were incorporated into the accepted
     * view, in which case the build will be a compacting merge and deleted
     * tuples will be purged.
     */
    public final boolean compactingMerge;

    /** #of journals incorporated into the accepted view. */
    public final int journalCount;

    /** #of index segments incorporated into the accepted view. */
    public final int segmentCount;

    /**
     * The sum of the entryCount for each source incorporated into the accepted
     * view. The entryCount will reflect both deleted and undeleted tuples for
     * that source. There may also exist a tuple for the same key in more than
     * one source. Therefore this does not give the #of distinct tuples in the
     * accepted view.
     */
    public final long sumEntryCount;

    /**
     * The #of bytes in the index segments incorporated into the accepted view.
     */
    public final long sumSegBytes;

    /**
     * The accepted view.
     */
    public final ILocalBTreeView acceptedView;

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getSimpleName());
        sb.append("{nsources="+nsources);
        sb.append(",naccepted="+naccepted);
        sb.append(",compactingMerge="+compactingMerge);
        sb.append(",journalCount="+journalCount);
        sb.append(",segmentCount="+segmentCount);
        sb.append(",sumEntryCount="+sumEntryCount);
        sb.append(",sumSegBytes="+sumSegBytes);
        sb.append("}");

        return sb.toString();

    }

    /**
     * Figure out which sources we want to include. We MUST always include the
     * 1st source in the view since that is the mutable BTree on the old
     * journal. We continue to include sources in the view until incorporating a
     * source into the view would exceed the specified thresholds. We stop there
     * because the goal is to keep down the #of components in the view without
     * doing the heavy work of processing a large source (which winds up copying
     * a lot of tuples). We only do that additional work when a compacting merge
     * was selected as the action rather than an incremental build.
     * 
     * @param src
     *            The source view.
     * @param maxSumSegBytes
     *            The maximum #of index segment bytes that will be incorporated
     *            into the accepted view.
     * @param parentEvent
     *            The parent event.
     */
    public BuildViewMetadata(final ILocalBTreeView src,
            final long maxSumSegBytes, final Event parentEvent) {

        if (src == null)
            throw new IllegalArgumentException();

        if (maxSumSegBytes < 0)
            throw new IllegalArgumentException();
      
        final Event e = parentEvent.newSubEvent(OverflowSubtaskEnum.ChooseView)
                .start();

        try {

            this.maxSumSegBytes = maxSumSegBytes;
            
            final AbstractBTree[] sources = src.getSources();

            this.nsources = sources.length;

            final List<AbstractBTree> accepted = new ArrayList<AbstractBTree>(
                    nsources);

            // the mutable BTree on the old journal.
            accepted.add(sources[0]);

            int journalCount = 1;
            int segmentCount = 0;
            long sumEntryCount = sources[0].getEntryCount();
            long sumSegBytes = 0L;
            for (int i = 1; i < sources.length; i++) {

                /*
                 * Values for this source (2nd+ source only).
                 */
                final AbstractBTree s = sources[i];
                final int entryCount = s.getEntryCount();
                final boolean isJournal = !(s instanceof IndexSegment);
                final long segBytes = (isJournal ? 0L : s.getStore().size());

                /*
                 * Terminate if too much data to include in an incremental
                 * build.
                 */
                // final int BUILD_MAX_JOURNAL_COUNT = 3;
                // final long BUILD_MAX_SUM_ENTRY_COUNT = Bytes.megabyte * 10;
                // if (journalCount > BUILD_MAX_JOURNAL_COUNT)
                // break;
                // if (sumEntryCount > BUILD_MAX_SUM_ENTRY_COUNT)
                // break;
                if (sumSegBytes + segBytes > maxSumSegBytes)
                    break;

                /*
                 * Update the running totals.
                 */
                sumEntryCount += entryCount;

                if (isJournal)
                    journalCount++;
                else
                    segmentCount++;

                sumSegBytes += segBytes;

                // accept another source into the view for the build.
                accepted.add(s);

            }

            /*
             * Set final totals on the instance fields.
             */
            this.journalCount = journalCount;
            this.segmentCount = segmentCount;
            this.sumEntryCount = sumEntryCount;
            this.sumSegBytes = sumSegBytes;

            /*
             * Note: If ALL sources are accepted, then we are actually doing a
             * compacting merge and we set the flag appropriately!
             */
            compactingMerge = accepted.size() == sources.length;

            naccepted = accepted.size();

            /*
             * Create the accepted view.
             */

            if (naccepted == 1) {

                acceptedView = (BTree) accepted.get(0);

            } else {

                acceptedView = new FusedView(accepted
                        .toArray(new AbstractBTree[naccepted]));

            }

            e.addDetails(getParams());
            
        } finally {

            e.end();

        }

    }

    public Map<String, Object> getParams() {

        final Map<String, Object> m = new HashMap<String, Object>();

        m.put("maxSumSegBytes", maxSumSegBytes);
        
        m.put("nsources", nsources);
        
        m.put("naccepted", naccepted);
        
        m.put("compactingMerge", compactingMerge);
        
        m.put("journalCount", journalCount);
        
        m.put("segmentCount", segmentCount);
        
        m.put("sumEntryCount", sumEntryCount);
        
        m.put("sumSegBytes", sumSegBytes);

        return m;

    }

}
