/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Mar 24, 2008
 */

package com.bigdata.resources;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IDirtyListener;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.cache.LRUCache;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.BuildIndexSegmentTask.BuildResult;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.MetadataService;

/**
 * Class encapsulates logic and handshaking for tracking which indices (and
 * their backing stores) are recently and currently referenced.  This information
 * is used to coordinate the close out of index resources (and their backing stores)
 * on an LRU basis by the {@link ResourceManager}.
 * 
 * 
 * FIXME The {@link AbstractTask} needs to explicitly coordinate with this class
 * (or an IndexManager) so that we know which index views are in use.
 * AbstractTask has the advantage that all concurrent access to any index views
 * will go through that class and it knows when indices are opened and when the
 * task completes so it is the ideal point from which to maintain reference
 * counts on read-only and read-write indices. This is important for buffer
 * management for the indices. Indices should be on an "touch" style LRU (like
 * the nodes and leaves of a btree) together with a reference count so that they
 * get closed out once they are no longer active and also on a weak reference
 * cache so that we have a canonicalizing mapping. There is logic for all of
 * this already in the journal and btree classes and it could probably be
 * refactored to create an IndexManager.
 * <P>
 * review use of synchronization and make sure that there is no way in which we
 * can double-open a store or index.
 * <P>
 * Use a hard reference queue to track recently used AbstractBTrees (and
 * stores?). Add a public referenceCount field on AbstractBTree and close the
 * AbstractBTree on eviction from the hard reference queue iff the
 * referenceCount is zero (no references to that AbstractBTree remain on the
 * hard reference queue).
 * <p>
 * re-examine the caching for B+Trees from the perspective of the
 * {@link ResourceManager}. Ideally a checkpoint operation will not discard the
 * per-btree node / leaf cache (the write retention and/or read retention
 * queues). Equally, it would be nice if read-committed and historical reads for
 * "hot" points (such as the lastCommitTime of the old journal or an intensive
 * tx) were able to benefit from a read-cache at the node/leaf or record level.
 * Also, note that {@link IndexSegment}s may be reused in a number of views,
 * e.g., both the unisolated and read-committed view of an index, but that all
 * of those views share the same {@link IndexSegment} instance and hence the
 * same read cache. This makes it worth while to fully buffer the nodes of the
 * index segment, but since the branching factor is larger the write/read
 * retention queue should be smaller.
 * <P>
 * consider handling close out of index partitions "whole at once" to include
 * all index segments in the current view of that partition. this probably does
 * not matter but might be a nicer level of aggregation than the individual
 * index segment. It's easy enough to identify the index segments from the btree
 * using the partition metadata record. However, it is harder to go the other
 * way (and in fact impossible since the same index segment can be used in
 * multiple index partition views as the partition definition evolves - in
 * contrast 1st btree in the index partition view always has the current
 * partition metadata description and therefore could be used to decrement the
 * usage counters on the other components of that view (but not to directly
 * close them out)).
 * <p>
 * this still does not suggest a mechanism for close by timeout. one solutions
 * is to just close down all open indices if the server quieses. if the server
 * is not quiesent then unused indices will get shutdown in any case (this is
 * basically how we are closing btrees and index segments now, so they remain
 * available but release their resources).
 * 
 * <pre>
 * 
 * 
 *  Cache of added/retrieved btrees by _name_. This cache is ONLY used by the
 *  &quot;live&quot; {@link Name2Addr} instance.
 * <p>
 *  Map from the name of an index to a weak reference for the corresponding
 *  &quot;live&quot; version of the named index. Entries will be cleared from this map
 *  after they have become only weakly reachable. In order to prevent dirty
 *  indices from being cleared, we register an {@link IDirtyListener}. When
 *  it is informed that an index is dirty it places a hard reference to that
 *  index into the {@link #commitList}.
 * <p>
 *  Note: The capacity of the backing hard reference LRU effects how many
 *  _clean_ indices can be held in the cache. Dirty indices remain strongly
 *  reachable owing to their existence in the {@link #commitList}.
 * 
 *     private WeakValueCache&lt;String, BTree&gt; indexCache = new WeakValueCache&lt;String, BTree&gt;(
 *             new LRUCache&lt;String, BTree&gt;(cacheCapacity));
 * </pre>
 * 
 * @todo Scale-out index import and index recovery
 *       <p>
 *       Note that key range partitioned indices are simply registered under a
 *       name that reflects both the scale-out index name and the index
 *       partition#. The metadata index provides the integrating structure for
 *       those individual index partitions. A {@link ClientIndexView} uses the
 *       {@link MetadataIndex} to provide transparent access to the scale-out
 *       index.
 *       <p>
 *       It should be possible to explicitly convert an index that supports
 *       delete markers into a scale-out index. The right model might be to
 *       "import" the index into an existing federation since there needs to be
 *       an explicit {@link MetadataService} on hand. There will only be a
 *       single partition of the index initially, but that can be broken down
 *       either because it is too large or because it becomes too large. The
 *       import can be realized by moving all of the data off of the journal
 *       onto one or more {@link IndexSegment}s, moving those index segment
 *       files into a selected data service, and then doing an "index recovery"
 *       operation that hooks up the index segment(s) into an index and
 *       registers the metadata index for that index. (Note that we can't make
 *       an index into a key-range partitioned index unless there is a metadata
 *       index lying around somewhere.)
 *       <p>
 *       Work through a federated index recovery where we re-generate the
 *       metadata index from the on hand data services.
 *       
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class IndexManager extends StoreFileManager {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(IndexManager.class);

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

    /**
     * This cache is used to provide remote clients with an unambiguous
     * indication that an index partition has been rather than simply not
     * existing or having been dropped.
     *<p> 
     * The keys are the name of an index partitions that has been split, joined,
     * or moved. Such index partitions are no longer available and have been
     * replaced by one or more new index partitions (having a distinct partition
     * identifier) either on the same or on another data service. The value is a
     * reason, e.g., "split", "join", or "move".
     */
    private final LRUCache<String/*name*/, String/*reason*/> staleIndexCache = new LRUCache<String, String>(10000);  
    
    /**
     * Return non-<code>null</code> iff <i>name</i> is the name of an index
     * partition that was located on this data service but which is now gone.
     * <p>
     * Note: this information is based on an LRU cache with a large fixed
     * capacity. It is expected that the cache size is sufficient to provide
     * good information to clients having queued write tasks.  If the index
     * partition split/move/join changes somehow outpace the cache size then
     * the client would see a {@link NoSuchIndexException} instead.
     * 
     * @param name
     *            The name of an index partition.
     * 
     * @return The reason (split, join, or move) -or- <code>null</code> iff
     *         the index partition is not known to be gone.
     */
    protected String getIndexPartitionGone(String name) {
    
        synchronized(staleIndexCache) {
        
            return staleIndexCache.get(name);
            
        }
        
    }
    
    /**
     * Notify the {@link ResourceManager} that the named index partition was
     * split, joined or moved. This effects only the unisolated view of that
     * index partition. Historical views will continue to exist and reside as
     * before.
     * 
     * @param name
     *            The name of the index partition.
     * @param reason
     *            The reason (split, join, or move).
     */
    protected void setIndexPartitionGone(String name,String reason) {
        
        assert name != null;
        
        assert reason != null;
        
        synchronized(staleIndexCache) {
        
            log.warn("name="+name+", reason="+reason);
            
            staleIndexCache.put(name, reason, true);
            
        }
        
    }
    
    protected IndexManager(Properties properties) {
        
        super(properties);
        
    }
    
    public void start() {

        super.start();
        
    }

    public void shutdown() {
        
        super.shutdown();
        
    }
    
    public void shutdownNow() {
        
        super.shutdownNow();
        
    }
    
    /**
     * Return a reference to the named index as of the specified timestamp on
     * the identified resource.
     * <p>
     * Note: The returned index is NOT isolated.
     * <p>
     * Note: When the index is a {@link BTree} associated with a historical
     * commit point (vs the live unisolated index) it will be marked as
     * {@link AbstractBTree#isReadOnly()} and
     * {@link AbstractBTree#getLastCommitTime()} will reflect the commitTime of
     * the {@link ICommitRecord} from which that {@link BTree} was loaded. Note
     * further that the commitTime MAY NOT be the same as the specified
     * <i>timestamp</i> for a number of reasons. First, <i>timestamp</i> MAY
     * be negative to indicate a historical read vs a transactional read.
     * Second, the {@link ICommitRecord} will be the record having the greatest
     * commitTime LTE the specified <i>timestamp</i>.
     * <p>
     * Note: An {@link IndexSegment} is always read-only and always reports the
     * commitTime associated with the view from which it was constructed.
     * 
     * @param name
     *            The index name.
     * @param timestamp
     *            The startTime of an active transaction, <code>0L</code> for
     *            the current unisolated index view, or <code>-timestamp</code>
     *            for a historical view no later than the specified timestamp.
     * @param store
     *            The store from which the index will be loaded.
     * 
     * @return A reference to the index -or- <code>null</code> if the index
     *         was not registered on the resource as of the timestamp or if the
     *         store has no data for that timestamp.
     * 
     * FIXME add hard reference queue for {@link AbstractBTree} to the journal
     * and track the #of instances of each {@link AbstractBTree} on the queue
     * using #referenceCount and "touch()", perhaps in Name2Addr; write tests.
     * consider one queue for mutable btrees and another for index segments,
     * partitioned indices, metadata indices, etc. consider the meaning of
     * "queue length" here and how to force close based on timeout. improve
     * reporting of index segments by name and partition.<br>
     * Mutable indices are low-cost to close/open. Closing them once they are no
     * longer receiving writes can release some large buffers and reduce the
     * latency of commits since dirty nodes will already have been flushed to
     * disk. The largest cost on re-open is de-serializing nodes and leaves for
     * subsequent operations. Those nodes will be read from a fully buffered
     * store, so the latency will be small even though deserialization is CPU
     * intensive. <br>
     * Close operations on unisolated indices need to be queued in the
     * {@link #writeService} so that they are executed in the same thread as
     * other operations on the unisolated index.<br>
     * Make sure that we close out old {@link Journal}s that are no longer
     * required by any open index. This will require a distinct referenceCount
     * on the {@link Journal}.
     */
    public AbstractBTree getIndexOnStore(String name, long timestamp, IRawStore store) {

        if (name == null)
            throw new IllegalArgumentException();

        if (store == null)
            throw new IllegalArgumentException();
        
        final AbstractBTree btree;

        if (store instanceof IJournal) {

            // the given journal.
            final AbstractJournal journal = (AbstractJournal) store;

            if (timestamp == ITx.UNISOLATED) {

                /*
                 * Unisolated index.
                 */

                // MAY be null.
                btree = (BTree) journal.getIndex(name);

            } else if( timestamp == ITx.READ_COMMITTED ) {

                /*
                 * Read committed operation against the most recent commit point.
                 * 
                 * Note: This commit record is always defined, but that does not
                 * mean that any indices have been registered.
                 */

                final ICommitRecord commitRecord = journal.getCommitRecord();

                final long ts = commitRecord.getTimestamp();

                if (ts == 0L) {

                    log.warn("Nothing committed: read-committed operation.");

                    return null;

                }

                // MAY be null.
                btree = journal.getIndex(name, commitRecord);
                
                if (btree != null) {

                    /*
                     * Mark the B+Tree as read-only and set the lastCommitTime
                     * timestamp from the commitRecord.
                     */
                    
                    ((BTree)btree).setReadOnly(true);
                    
                    ((BTree)btree).setLastCommitTime(commitRecord.getTimestamp());
                    
                }

            } else {

                /*
                 * A specified historical index commit point.
                 */

                // use absolute value in case timestamp is negative.
                final long ts = Math.abs(timestamp);

                // the corresponding commit record on the journal.
                final ICommitRecord commitRecord = journal.getCommitRecord(ts);

                if (commitRecord == null) {

                    log.warn("Resource has no data for timestamp: name=" + name
                            + ", timestamp=" + timestamp + ", resource="
                            + store.getResourceMetadata());

                    return null;
                    
                }

                // open index on that journal (MAY be null).
                btree = (BTree) journal.getIndex(name, commitRecord);

                if (btree != null) {

                    /*
                     * Mark the B+Tree as read-only and set the lastCommitTime
                     * timestamp from the commitRecord.
                     */
                    
                    ((BTree)btree).setReadOnly(true);
                    
                    ((BTree)btree).setLastCommitTime(commitRecord.getTimestamp());
                    
                }

            }

        } else {

            final IndexSegmentFileStore segStore = ((IndexSegmentFileStore) store);

            if (timestamp != ITx.READ_COMMITTED && timestamp != ITx.UNISOLATED) {
            
                // use absolute value in case timestamp is negative.
                final long ts = Math.abs(timestamp);

                if (segStore.getCheckpoint().commitTime > ts) {

                    log.warn("Resource has no data for timestamp: name=" + name
                            + ", timestamp=" + timestamp + ", store=" + store);

                    return null;

                }

            }
            
            // Open an index segment.
            btree = segStore.load();

        }

        log.info("name=" + name + ", timestamp=" + timestamp + ", store="
                + store + " : " + btree);

        return btree;

    }

    /**
     * Return the ordered {@link AbstractBTree} sources for an index or a view
     * of an index partition. The {@link AbstractBTree}s are ordered from the
     * most recent to the oldest and together comprise a coherent view of an
     * index partition.
     * 
     * @param name
     *            The name of the index.
     * @param timestamp
     *            The startTime of an active transaction, <code>0L</code> for
     *            the current unisolated index view, or <code>-timestamp</code>
     *            for a historical view no later than the specified timestamp.
     * 
     * @return The sources for the index view -or- <code>null</code> if the
     *         index was not defined as of the timestamp.
     * 
     * @see FusedView
     */
    public AbstractBTree[] getIndexSources(String name, long timestamp) {

        log.info("name=" + name + ", timestamp=" + timestamp);

        /*
         * Open the index on the journal for that timestamp.
         */
        final AbstractBTree btree;
        {

            // the corresponding journal (can be the live journal).
            final AbstractJournal journal = getJournal(timestamp);

            if(journal == null) {
                
                log.warn("No journal with data for timestamp: name="+name+", timestamp="+timestamp);
                
                return null;
                
            }
            
            btree = getIndexOnStore(name, timestamp, journal);

            if (btree == null) {

                log.warn("No such index: name=" + name + ", timestamp="
                        + timestamp);

                return null;

            }

            log.info("View based on " + journal.getResourceMetadata());

        }

        if (btree == null) {

            // No such index.

            return null;

        }

        /*
         * Get the index partition metadata (if any). If defined, then we know
         * that this is an index partition and that the view is defined by the
         * resources named in that index partition. Otherwise the index is
         * unpartitioned.
         */
        final LocalPartitionMetadata pmd = btree.getIndexMetadata()
                .getPartitionMetadata();

        if (pmd == null) {

            // An unpartitioned index (one source).

            log.info("Unpartitioned index: name=" + name + ", ts=" + timestamp);

            return new AbstractBTree[] { btree };

        }

        /*
         * An index partition.
         */
        final AbstractBTree[] sources;
        {

            // live resources for that index partition.
            final IResourceMetadata[] a = pmd.getResources();

            assert a != null : "No resources: name="+name+", pmd="+pmd;
            
            sources = new AbstractBTree[a.length];

            // the most recent is this btree.
            sources[0/* j */] = btree;

            for (int i = 1; i < a.length; i++) {

                final IResourceMetadata resource = a[i];

                final IRawStore store = openStore(resource.getUUID());

                final AbstractBTree ndx = getIndexOnStore(name, timestamp, store);

                if (ndx == null) {

                    throw new RuntimeException(
                            "Could not load component index: name=" + name
                                    + ", timestamp=" + timestamp
                                    + ", resource=" + resource);

                }

                log.info("Added to view: " + resource);

                sources[i] = ndx;

            }

        }

        log.info("Opened index partition:  name=" + name + ", timestamp="
                + timestamp);

        return sources;

    }
    
    public String getStatistics(String name, long timestamp) {
        
        AbstractBTree[] sources = getIndexSources(name, timestamp);
        
        if(sources==null) {
            
            return null;
            
        }
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("name="+name+", timestamp="+timestamp);
        
        for(int i=0; i<sources.length; i++) {
         
            sb.append("\n"+sources[i].getStatistics());
            
        }
        
        return sb.toString();
        
    }
    
    /**
     * Note: logic duplicated by {@link Journal#getIndex(String, long)}
     */
    public IIndex getIndex(String name, long timestamp) {
        
        if (name == null) {

            throw new IllegalArgumentException();

        }

        final boolean isTransaction = timestamp > ITx.UNISOLATED;
        
        final ITx tx = (isTransaction ? getConcurrencyManager()
                .getTransactionManager().getTx(timestamp) : null); 
        
        if(isTransaction) {

            if(tx == null) {
                
                log.warn("Unknown transaction: name="+name+", tx="+timestamp);
                
                return null;
                    
            }
            
            if(!tx.isActive()) {
                
                // typically this means that the transaction has already prepared.
                log.warn("Transaction not active: name=" + name + ", tx="
                        + timestamp + ", prepared=" + tx.isPrepared()
                        + ", complete=" + tx.isComplete() + ", aborted="
                        + tx.isAborted());

                return null;
                
            }
                                
        }
        
        if( isTransaction && tx == null ) {
        
            /*
             * Note: This will happen both if you attempt to use a transaction
             * identified that has not been registered or if you attempt to use
             * a transaction manager after the transaction has been either
             * committed or aborted.
             */
            
            log.warn("No such transaction: name=" + name + ", tx=" + tx);

            return null;
            
        }
        
        final boolean readOnly = (timestamp < ITx.UNISOLATED)
                || (isTransaction && tx.isReadOnly());

        final IIndex tmp;

        if (isTransaction) {

            /*
             * Isolated operation.
             * 
             * Note: The backing index is always a historical state of the named
             * index.
             */

            final IIndex isolatedIndex = tx.getIndex(name);

            if (isolatedIndex == null) {

                log.warn("No such index: name="+name+", tx="+timestamp);
                
                return null;

            }

            tmp = isolatedIndex;

        } else {
            
            /*
             * historical read -or- read-committed operation.
             */

            if (readOnly) {

                final AbstractBTree[] sources = getIndexSources(name, timestamp);

                if (sources == null) {

                    log.warn("No such index: name="+name+", timestamp="+timestamp);
                    
                    return null;

                }

                assert sources.length > 0;

                assert sources[0].isReadOnly();

                if (sources.length == 1) {

                    tmp = sources[0];

                } else {

                    tmp = new FusedView(sources);
                    
                }
                
            } else {
                
                /*
                 * Writable unisolated index.
                 * 
                 * Note: This is the "live" mutable index. This index is NOT
                 * thread-safe. A lock manager is used to ensure that at most
                 * one task has access to this index at a time.
                 */

                assert timestamp == ITx.UNISOLATED;
                
                // Check to see if an index partition was split, joined or moved.
                final String reason = getIndexPartitionGone(name);

                if (reason != null) {

                    // Notify client of stale locator.
                    throw new StaleLocatorException(name, reason);
                    
                }
                
                final AbstractBTree[] sources = getIndexSources(name, ITx.UNISOLATED);
                
                if (sources == null) {

                    log.warn("No such index: name="+name+", timestamp="+timestamp);
                    
                    return null;
                    
                }

                assert ! sources[0].isReadOnly();
                
                if (sources.length == 1) {

                    tmp = sources[0];
                    
                } else {
                    
                    tmp = new FusedView( sources );
                    
                }

            }

        }
        
        return tmp;

    }

    /**
     * Dump index metadata as of the timestamp.
     * 
     * @param timestamp
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is positive (a transaction identifier).
     */
    public String listIndexPartitions(long timestamp) {

        if (timestamp > 0)
            throw new IllegalArgumentException();
        
        StringBuilder sb = new StringBuilder();

        final AbstractJournal journal = getJournal(timestamp);

        sb.append("timestamp="+timestamp+"\njournal="+journal.getResourceMetadata());

        final ITupleIterator itr = journal.getName2Addr().rangeIterator(
                null, null);
        
        while (itr.hasNext()) {

            final ITuple tuple = itr.next();

            final Entry entry = EntrySerializer.INSTANCE
                    .deserialize(new DataInputBuffer(tuple.getValue()));

            // the name of an index to consider.
            final String name = entry.name;

            /*
             * Open the historical view of that index at that time (not just
             * the mutable BTree but the full view).
             */
            final IIndex view = getIndex(name, timestamp);

            if (view == null) {

                throw new AssertionError(
                        "Index not found? : name=" + name
                                + ", timestamp=" + timestamp);

            }

            // index metadata for that index partition.
            final IndexMetadata indexMetadata = view.getIndexMetadata();

            // index partition metadata
            final LocalPartitionMetadata pmd = indexMetadata
                    .getPartitionMetadata();

            sb.append("\nname="+name+", pmd="+pmd);
    
        }

        return sb.toString();
        
    }

    /**
     * Build an index segment from an index partition.
     * 
     * @param name
     *            The name of the index partition (not the name of the scale-out
     *            index).
     * @param src
     *            A view of the index partition as of the <i>createTime</i>.
     * @parma outFile The file on which the {@link IndexSegment} will be
     *        written.
     * @param createTime
     *            The timestamp of the view. This is typically the
     *            lastCommitTime of the old journal after an
     *            {@link #overflow(boolean, boolean)} operation.
     * @param fromKey
     *            The lowest key that will be counted (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will not be counted (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @return A {@link BuildResult} identifying the new {@link IndexSegment}
     *         and the source index.
     * 
     * @throws IOException
     */
    public BuildResult buildIndexSegment(String name, IIndex src, File outFile,
            long createTime, byte[] fromKey, byte[] toKey) throws IOException {

        if (name == null)
            throw new IllegalArgumentException();
        
        if (src == null)
            throw new IllegalArgumentException();
        
        if (outFile == null)
            throw new IllegalArgumentException();

        if (createTime <= 0L)
            throw new IllegalArgumentException();
        
        // metadata for that index / index partition.
        final IndexMetadata indexMetadata = src.getIndexMetadata();

        // the branching factor for the generated index segment.
        final int branchingFactor = indexMetadata
                .getIndexSegmentBranchingFactor();

//         // Note: truncates nentries to int.
//        final int nentries = (int) Math.min(src.rangeCount(fromKey, toKey),
//                Integer.MAX_VALUE);

        /*
         * Use the range iterator to get an exact entry count for the view.
         * 
         * Note: We need the exact entry count for the IndexSegmentBuilder. It
         * requires the exact #of index entries when it creates its plan for
         * populating the index segment.
         */
        final int nentries;
        {

            final ITupleIterator itr = src
                    .rangeIterator(fromKey, toKey, 0/* capacity */,
                            0/*no flags*/, null/* filter */);

            int i = 0;

            while(itr.hasNext()) {
                
                itr.next();
                
                i++;
                
            }
            
            nentries = i;
            
            log.info("There are "+nentries+" non-deleted index entries: "+name);
            
        }
        
         /*
          * Note: Delete markers are ignored so they will NOT be transferred to
          * the new index segment (compacting merge).
          */
         final ITupleIterator itr = src
                 .rangeIterator(fromKey, toKey, 0/* capacity */,
                         IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);
         
         // Build index segment.
         final IndexSegmentBuilder builder = new IndexSegmentBuilder(//
                 outFile, //
                 getTmpDir(), //
                 nentries,//
                 itr, //
                 branchingFactor,//
                 indexMetadata,//
                 createTime//
         );

         // report event
         notifyIndexSegmentBuildEvent(builder);

         /*
          * Describe the index segment.
          */
         final SegmentMetadata segmentMetadata = new SegmentMetadata(//
                 "" + outFile, //
                 outFile.length(),//
                 builder.segmentUUID,//
                 createTime//
                 );

         /*
          * notify the resource manager so that it can find this file.
          * 
          * @todo once the index segment has been built the resource manager
          * should notice it in a restart manner and put it into play if it
          * has not already been used to update the view.
          */

         addResource(segmentMetadata, outFile);

         return new BuildResult(name, indexMetadata, segmentMetadata);
         
    }

}
