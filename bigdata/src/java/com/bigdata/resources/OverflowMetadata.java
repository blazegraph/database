package com.bigdata.resources;

import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.service.IDataService;

/**
 * Metadata on the entire synchronous and asynchronous overflow task.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class OverflowMetadata {

    protected static final Logger log = Logger.getLogger(OverflowMetadata.class);
    
    /**
     * The resource manager.
     */
    public final ResourceManager resourceManager;
    
    /**
     * The last commit time on the old journal. This identifies the commit point
     * on which synchronous and asynchronous overflow will read.
     */
    public final long lastCommitTime;

//    /**
//     * The names of any index partitions that were copied onto the new journal
//     * during synchronous overflow processing.
//     */
//    private final Set<String> copied = new HashSet<String>();

    /**
     * Set <code>true</code> iff asynchronous post-processing should be
     * performed. Flag is set iff some indices are NOT copied onto the new
     * journal such that asynchronous post-processing should be performed.
     */
    public boolean postProcess;

    /*
     * Stuff relating to the performance counters for the indices on the old
     * journal.
     */
    
    /**
     * The raw write score computed based on the net change in the aggregated
     * {@link BTreeCounters}s for each index partition since the last overflow.
     * 
     * @see BTreeCounters#computeRawWriteScore()
     */
    private double totalRawStore;

    /**
     * Scores computed for each named index in order by ascending score
     * (increased activity).
     */
    private final Score[] scores;

    /**
     * Random access to the index {@link Score}s.
     */
    private final Map<String, Score> scoreMap;

    /*
     * Metadata for the BTree and index partition views.
     */
    
    /**
     * Note: Since this is a linked hash map it will maintain the order in which
     * we populate it so views() will also be in index name order.
     */
    private final LinkedHashMap<String, ViewMetadata> views;

    /**
     * Clears the {@link ViewMetadata} map, clearing the {@link SoftReference}s
     * from each {@link BTreeMetadata} and {@link ViewMetadata} instance. This
     * is used by the {@link AsynchronousOverflowTask} to encourage the eager GC
     * of resources once that task is finished.
     */
    void clearViews() {
        
        for(ViewMetadata view : views.values()) {
        
            view.clearRef();
            
        }
        
        views.clear();
        
    }
    
    /**
     * Random lookup of the {@link ViewMetadata}.
     * 
     * @param name
     *            The index name.
     * 
     * @return The {@link ViewMetadata} and <code>null</code> if none is
     *         found.
     * 
     * @throws IllegalStateException
     *             if {@link #collectMoreData(ResourceManager)} has not been
     *             called.
     */
    public final ViewMetadata getViewMetadata(final String name) {
        
        if (name == null)
            throw new IllegalArgumentException();

        final ViewMetadata vmd = views.get(name);

        return vmd;
        
    }

    /**
     * The views that are being processed in index name order.
     */
    public Iterator<ViewMetadata> views() {

        return Collections.unmodifiableMap(views).values().iterator();

    }

    /**
     * The #of indices on the old journal.
     */
    public int getIndexCount() {
        
        return views.size();
        
    }

    /**
     * True if the tuples for the index were copied to the new live journal
     * during synchronous overflow.
     * 
     * @param name
     *            The name of the index partition.
     * 
     * @return
     */
    public boolean isCopied(final String name) {
       
        if (name == null)
            throw new IllegalArgumentException();

        final ViewMetadata vmd = views.get(name);

        if (vmd == null)
            throw new IllegalArgumentException();

        return OverflowActionEnum.Copy.equals(vmd.getAction());
        
    }
    
//    /**
//     * The action to take / taken for the index partition.
//     * 
//     * @param name
//     *            The name of the index partition.
//     * 
//     * @return The action taken or <code>null</code> if no action has been
//     *         selected.
//     */
//    public OverflowActionEnum getAction(final String name) {
//
//        if (name == null)
//            throw new IllegalArgumentException();
//
//        final ViewMetadata vmd = views.get(name);
//
//        if (vmd == null)
//            throw new IllegalArgumentException();
//
//        return vmd.getAction();
//        
//    }
    
    /**
     * Specify the action to be taken.
     * 
     * @param name
     *            The index partition name.
     * @param action
     *            The action.
     * 
     * @throws IllegalStateException
     *             if an action has already been specified.
     */
    public void setAction(final String name, final OverflowActionEnum action) {

        if (name == null)
            throw new IllegalArgumentException();

        final ViewMetadata vmd = views.get(name);

        if (vmd == null)
            throw new IllegalArgumentException();

        /*
         * Note: The caller generally should already be holding this lock so
         * they can make an atomic decision regarding the action to take for
         * this index partition.
         */
        vmd.lock.lock();
        try {

            // set on the ViewMetadata object itself.
            vmd.setAction(action);

            // update counters.
            synchronized (actionCounts) {
                
                AtomicInteger counter = actionCounts.get(action);

                if (counter == null) {

                    // new counter.
                    counter = new AtomicInteger();
                    
                    actionCounts.put(action, counter);

                }
                
                // increment counter.
                counter.incrementAndGet();

            }
            
        } finally {

            vmd.lock.unlock();
            
        }

    }

    /**
     * Return the #of index partitions for which the specified action was
     * chosen.
     * 
     * @param action
     *            The action.
     * 
     * @return The #of index partitions where that action was chosen.
     * 
     * @todo This is only used by the unit tests as a means of verifying that
     *       they have set {@link OverflowManager#copyIndexThreshold} to zero.
     *       This stands in the way of decoupling asynchronous overflow from a
     *       fixed cycle such that index partition build, merge, split, etc.
     *       tasks can run at any time (other than while some other task is
     *       running).
     *       <p>
     *       Note: we must also give
     *       {@link OverflowManager#doSynchronousOverflow()} exclusive access so
     *       it can propagate the index partition declarations without blocking.
     *       Right now, asynchronous index tasks do not execute during
     *       synchronous overflow so there is no problem. This does have the
     *       consequence that the set of locks is per {@link OverflowMetadata}
     *       instance (and hence only valid for the epoch between one
     *       synchronous overflow and the next).
     */
    public int getActionCount(final OverflowActionEnum action) {

        if (action == null)
            throw new IllegalArgumentException();
        
        synchronized (actionCounts) {
            
            final AtomicInteger counter = actionCounts.get(action);

            if (counter == null) {

                return 0;

            }
            
            return counter.get();

        }
        
    }
    private final Map<OverflowActionEnum, AtomicInteger> actionCounts = new HashMap<OverflowActionEnum, AtomicInteger>();

    /**
     * Captures various metadata about the live journal in preparation for a
     * synchronous overflow operation.
     * <p>
     * Note: This captures metadata which has low latency and does not force the
     * materialization of the fused view of an index partition but may force the
     * loading of the {@link BTree} from the old journal, which it itself a very
     * light weight operation.
     * 
     * @param resourceManager
     */
    public OverflowMetadata(final ResourceManager resourceManager) {

        if (resourceManager == null)
            throw new IllegalArgumentException();

        this.resourceManager = resourceManager;
        
        // note: assumes the live journal is the one that we want!
        final AbstractJournal oldJournal = resourceManager.getLiveJournal();

        // timestamp of the last commit on the old journal.
        lastCommitTime = oldJournal.getRootBlockView().getLastCommitTime();

        /*
         * This will be the aggregate of the net change in the btree performance
         * counters since the last overflow operation (sum across [delta]).
         * Together with the per-index partition performance counters, this is
         * used to decide which indices are "hot" and which are not.
         */
        final BTreeCounters totalCounters = new BTreeCounters();

        /*
         * Generate the metadata summaries of the index partitions that we will
         * need to process.
         */
        {

            // using read-historical view of Name2Addr
            final int numIndices = (int) oldJournal
                    .getName2Addr(lastCommitTime).rangeCount();

            views = new LinkedHashMap<String, ViewMetadata>(numIndices);

            // the name2addr view as of the last commit time.
            final ITupleIterator<?> itr = oldJournal.getName2Addr(lastCommitTime)
                    .rangeIterator();
            
            final Map<String, BTreeCounters> delta = resourceManager
                    .markAndGetDelta();

            while (itr.hasNext()) {

                final ITuple<?> tuple = itr.next();

                final Entry entry = EntrySerializer.INSTANCE
                        .deserialize(new DataInputBuffer(tuple.getValue()));

                /*
                 * Obtain the delta in the btree performance counters for this
                 * index partition since the last overflow.
                 */
                BTreeCounters btreeCounters = delta.get(entry.name);
                
                if (btreeCounters == null) {
                 
                    // use empty counters if none were reported (paranoia).
                    log.error("No performance counters? index=" + entry.name);

                    btreeCounters = new BTreeCounters();
                    
                }

                totalCounters.add(btreeCounters);
                
                views.put(entry.name, new ViewMetadata(resourceManager,
                        lastCommitTime, entry.name, btreeCounters));

            }

        }

        /*
         * Compute the "Score" for each index partition.
         */
        {

            final int nscores = views.size();

            this.scores = new Score[nscores];

            this.scoreMap = new HashMap<String/* name */, Score>(nscores);

            this.totalRawStore = totalCounters.computeRawWriteScore();

            if (nscores > 0) {

                final Iterator<Map.Entry<String, ViewMetadata>> itr = views
                        .entrySet().iterator();

                int i = 0;

                while (itr.hasNext()) {

                    final Map.Entry<String, ViewMetadata> entry = itr.next();

                    final String name = entry.getKey();

                    final ViewMetadata vmd = entry.getValue();

                    final BTreeCounters btreeCounters = vmd.btreeCounters;
                    
                    scores[i] = new Score(name, btreeCounters, totalRawStore);

                    i++;

                }

                // sort into ascending order (increasing activity).
                Arrays.sort(scores);

                for (i = 0; i < scores.length; i++) {

                    scores[i].rank = i;

                    scores[i].drank = ((double) i) / scores.length;

                    scoreMap.put(scores[i].name, scores[i]);

                }

                if (log.isDebugEnabled()) {

                    log.debug("The most active index was: "
                            + scores[scores.length - 1]);

                    log.debug("The least active index was: " + scores[0]);

                }

            }

        }

    }

    /**
     * The #of active index partitions on this data service.
     */
    public int getActiveCount() {

        return scores.length;

    }

    /**
     * The scores in order from least active to most active.
     */
    public List<Score> getScores() {

        return Arrays.asList(scores);

    }

    /**
     * Return <code>true</code> if the named index partition is "warm" for
     * {@link ITx#UNISOLATED} and/or {@link ITx#READ_COMMITTED} operations.
     * <p>
     * Note: This method informs the selection of index partitions that will be
     * moved to another {@link IDataService}. The preference is always to move
     * an index partition that is "warm" rather than "hot" or "cold". Moving a
     * "hot" index partition causes more latency since more writes will have
     * been buffered and unisolated access to the index partition will be
     * suspended during the atomic part of the move operation. Likewise, "cold"
     * index partitions are not consuming any resources other than disk space
     * for their history, and the history of an index is not moved when the
     * index partition is moved.
     * <p>
     * Since the history of an index partition is not moved when the index
     * partition is moved, the determination of cold, warm or hot is made in
     * terms of the resources consumed by {@link ITx#READ_COMMITTED} and
     * {@link ITx#UNISOLATED} access to named index partitions. If an index
     * partition is hot for historical read, then your only choices are to shed
     * other index partitions from the data service, to read from a failover
     * data service having the same index partition, or possibly to increase the
     * replication count for the index partition.
     * 
     * @param name
     *            The name of an index partition.
     * 
     * @return The index {@link Score} -or- <code>null</code> iff the index
     *         was not touched for read-committed or unisolated operations.
     */
    public Score getScore(final String name) {

        final Score score = scoreMap.get(name);

        if (score == null) {

            /*
             * Index was not touched for read-committed or unisolated
             * operations.
             */

            if (log.isDebugEnabled())
                log.debug("Index is cold: " + name);

            return null;

        }

        if (log.isDebugEnabled())
            log.debug("Index score: " + score);

        return score;

    }

}
