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
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.cache.WeakValueCache.IClearReferenceListener;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.Tx;
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
 * their backing stores) are recently and currently referenced. This information
 * is used to coordinate the close out of index resources (and their backing
 * stores) on an LRU basis by the {@link ResourceManager}.
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
abstract public class IndexManager extends StoreManager {

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
     * Options understood by the {@link IndexManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends StoreManager.Options {
     
        /**
         * The capacity of the LRU cache of open {@link IndexSegment}s. The
         * capacity of this cache indirectly controls how many
         * {@link IndexSegment}s will be held open. The main reason for keeping
         * an {@link IndexSegment} open is to reuse its buffers, including its
         * node and leaf cache, if another request arrives "soon" which would
         * read on that {@link IndexSegment}.
         * <p>
         * The effect of this parameter is indirect owning to the semantics of
         * weak references and the control of the JVM over when they are
         * cleared. Once an index becomes weakly reachable, the JVM will
         * eventually GC the index object, thereby effectively closing it (or at
         * least releasing all resources associated with that index). Since
         * indices which are strongly reachable are never "closed" this provides
         * our guarentee that indices are never closed if they are in use.
         * <p>
         * Note: {@link IndexSegment}s have a reference to the
         * {@link IndexSegmentStore} and an {@link IClearReferenceListener} is
         * used to see to it that the {@link IndexSegmentStore} is also closed,
         * thereby releasing its buffers and the associated file handle.
         * 
         * @see #DEFAULT_INDEX_SEGMENT_CACHE_CAPACITY
         */
        String INDEX_SEGMENT_CACHE_CAPACITY = "indexSegmentCacheCapacity";

        /**
         * The default for the {@link #INDEX_SEGMEWNT_CACHE_CAPACITY} option.
         */
        String DEFAULT_INDEX_SEGMENT_CACHE_CAPACITY = "60";

    }
    
    /**
     * A canonicalizing cache for {@link IndexSegment}.
     * 
     * FIXME make sure this cache purges entries that have not been touched in
     * the last N seconds, where N might be 60.
     * 
     * @see Options#INDEX_SEGMENT_CACHE_CAPACITY
     */
    final private WeakValueCache<UUID, IndexSegment> indexSegmentCache;

    /**
     * The #of entries in the hard reference cache for {@link IndexSegment}s.
     * There MAY be more {@link IndexSegment}s open than are reported by this
     * method if there are hard references held by the application to those
     * {@link IndexSegment}s. {@link IndexSegment}s that are not fixed by a
     * hard reference will be quickly finalized by the JVM.
     */
    public int getIndexSegmentCacheSize() {
        
        return indexSegmentCache.size();
        
    }
    
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
//    private // @todo exposed for counters - should be private.
    protected final LRUCache<String/*name*/, String/*reason*/> staleLocatorCache = new LRUCache<String, String>(1000);  
    
    /**
     * Note: this information is based on an LRU cache with a large fixed
     * capacity. It is expected that the cache size is sufficient to provide
     * good information to clients having queued write tasks.  If the index
     * partition split/move/join changes somehow outpace the cache size then
     * the client would see a {@link NoSuchIndexException} instead.
     */
    public String getIndexPartitionGone(String name) {
    
        synchronized(staleLocatorCache) {
        
            return staleLocatorCache.get(name);
            
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
        
        synchronized(staleLocatorCache) {
        
            log.info("name="+name+", reason="+reason);
            
            staleLocatorCache.put(name, reason, true);
            
        }
        
    }

    /**
     * The #of entries in the stale locator LRU.
     */
    protected int getStaleLocatorCount() {

        return staleLocatorCache.size();
        
    }
    
    protected IndexManager(Properties properties) {
        
        super(properties);
     
        /*
         * indexSegmentCacheCapacity
         */
        {

            final int indexSegmentCacheCapacity = Integer.parseInt(properties.getProperty(
                    Options.INDEX_SEGMENT_CACHE_CAPACITY,
                    Options.DEFAULT_INDEX_SEGMENT_CACHE_CAPACITY));

            log.info(Options.INDEX_SEGMENT_CACHE_CAPACITY+"="+indexSegmentCacheCapacity);

            if (indexSegmentCacheCapacity <= 0)
                throw new RuntimeException(Options.INDEX_SEGMENT_CACHE_CAPACITY
                        + " must be non-negative");

            indexSegmentCache = new WeakValueCache<UUID, IndexSegment>(
//                    WeakValueCache.INITIAL_CAPACITY,//
//                    WeakValueCache.LOAD_FACTOR, //
                    new LRUCache<UUID, IndexSegment>(indexSegmentCacheCapacity)
//                    new WeakCacheEntryFactory<UUID,IndexSegment>()
//                    new ClearReferenceListener()
                    );

        }

    }
    
    /**
     * Return a reference to the named index as of the specified timestamp on
     * the identified resource.
     * <p>
     * Note: The returned index is NOT isolated. Isolation is handled by the
     * {@link Tx}.
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
     * @todo this might have to be private since we assume that the store is in
     *       {@link StoreManager#openStores}.
     */
    public AbstractBTree getIndexOnStore(final String name,
            final long timestamp, final IRawStore store) {

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

//                    /*
//                     * Mark the B+Tree as read-only.
//                     */
//                    
//                    ((BTree)btree).setReadOnly(true);
                    
                    assert ((BTree) btree).getLastCommitTime() != 0;
//                    ((BTree)btree).setLastCommitTime(commitRecord.getTimestamp());
                    
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

//                    /*
//                     * Mark the B+Tree as read-only.
//                     */
//                    
//                    ((BTree)btree).setReadOnly(true);
                    
                    assert ((BTree) btree).getLastCommitTime() != 0;
//                    ((BTree)btree).setLastCommitTime(commitRecord.getTimestamp());
                    
                }

            }

        } else {

            /*
             * An IndexSegmentStore containing a single IndexSegment.
             */
            
            final IndexSegmentStore segStore = ((IndexSegmentStore) store);

            if (timestamp != ITx.READ_COMMITTED && timestamp != ITx.UNISOLATED) {
            
                // use absolute value in case timestamp is negative.
                final long ts = Math.abs(timestamp);

                if (segStore.getCheckpoint().commitTime > ts) {

                    log.warn("Resource has no data for timestamp: name=" + name
                            + ", timestamp=" + timestamp + ", store=" + store);

                    return null;

                }

            }

            {
                
                final IResourceMetadata resourceMetadata = store.getResourceMetadata();
                
                final UUID storeUUID = resourceMetadata.getUUID();

                /*
                 * Note: synchronization is required to have the semantics of an
                 * atomic get/put.
                 * 
                 * FIXME if the WeakValueCache used an atomic hash map then we
                 * could increase the concurrency here. The load of the index
                 * segment from the store can have significiant latency and this
                 * code is single threaded during the load.
                 */
                synchronized (indexSegmentCache) {

                    // check the cache first.
                    IndexSegment seg = indexSegmentCache.get(storeUUID);

                    if (seg == null) {

                        log.info("Loading index segment from store: name="
                                        + name + ", file="
                                        + resourceMetadata.getFile());

                        // Open an index segment.
                        seg = segStore.loadIndexSegment();

                        indexSegmentCache
                                .put(storeUUID, seg, false/* dirty */);

                    }
                    
                    btree = seg;

                }
            
            }

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

        if(INFO)
        log.info("name=" + name + ", timestamp=" + timestamp);

        /*
         * Open the index on the journal for that timestamp.
         */
        final BTree btree;
        {

            // the corresponding journal (can be the live journal).
            final AbstractJournal journal = getJournal(timestamp);

            if(journal == null) {
                
                log.warn("No journal with data for timestamp: name="+name+", timestamp="+timestamp);
                
                return null;
                
            }
            
            btree = (BTree) getIndexOnStore(name, timestamp, journal);

            if (btree == null) {

                if(INFO)
                log.info("No such index: name=" + name + ", timestamp="
                        + timestamp);

                return null;

            }

            if (INFO)
                log.info("name=" + name + ", timestamp=" + timestamp
                        + ", counter=" + btree.getCounter().get()
                        + ", journal=" + journal.getResourceMetadata());

        }

        if (btree == null) {

            // No such index.

            return null;

        }
        
        return getIndexSources(name,timestamp,btree);

    }

    public AbstractBTree[] getIndexSources(String name, long timestamp, BTree btree) {
        
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

                /*
                 * Interpret UNISOLATED and READ_COMMITTED for a historical
                 * store as the last committed data on that store.
                 */
                final long ts;
                if (timestamp == ITx.UNISOLATED
                        || timestamp == ITx.READ_COMMITTED) {

                    if (store instanceof IndexSegmentStore) {

                        // there is only one timestamp for an index segment store.
                        ts = ((IndexSegmentStore) store).getCheckpoint().commitTime;

                    } else {

                        // the live journal should never appear in the 2nd+ position of a view.
                        assert store != getLiveJournal();
                        
                        // the last commit time on the historical journal.
                        ts = ((AbstractJournal) store).getRootBlockView()
                                .getLastCommitTime();

                    }
                    
                } else {
                    
                    ts = timestamp;
                    
                }
                
                assert ts != ITx.UNISOLATED;
                assert ts != ITx.READ_COMMITTED;
                
                final AbstractBTree ndx = getIndexOnStore(name, ts, store);

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
             * 
             * Note: Tx#getIndex(String name) serializes concurrent requests for
             * the same index (thread-safe).
             */

            final IIndex isolatedIndex = tx.getIndex(name);

            if (isolatedIndex == null) {

                log.warn("No such index: name="+name+", tx="+timestamp);
                
                return null;

            }

            tmp = isolatedIndex;

        } else {

            /*
             * Note: serializes concurrent requests for the same index
             * (thread-safe).
             * 
             * FIXME In fact, this serializes all requests for any index which
             * limits concurrency. Change to use a per (name) lock using a
             * LockManager to serialize the requests. Use a different lock for
             * the historical read and the unisolated requests since they access
             * different views.
             * 
             * Note: neither a per (name,timestamp) lock nor a per
             * (name,commitRecord) lock makes sense since they fail to capture
             * the essential distinction which is {journal, checkpointAddr}. The
             * journal is identified by the timestamp and then the
             * checkpointAddr is identified using the Name2Addr for the
             * commitRecord on that journal identified by the timestamp.
             * 
             * Is there an API change to getIndexSources() which would let us
             * use this distinction and thereby only serialize requests for
             * exactly the same historical view?
             * 
             * FIXME Make sure that the synchronization changes are also made to
             * Journal.
             * 
             * FIXME Make sure that we properly synchronize getIndexSources(),
             * getJournal(), and getIndexOnStore().
             * 
             * @todo review use of synchronization and make sure that there is
             * no way in which we can double-open a store or index.
             */
//            synchronized (this) 
            {

                /*
                 * historical read -or- read-committed operation.
                 */

                if (readOnly) {

                    final AbstractBTree[] sources = getIndexSources(name,
                            timestamp);

                    if (sources == null) {

                        log.info("No such index: name=" + name + ", timestamp="
                                + timestamp
//                                , new RuntimeException("stacktrace")
                        );

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
                     * thread-safe. A lock manager is used to ensure that at
                     * most one task has access to this index at a time.
                     */

                    assert timestamp == ITx.UNISOLATED;

                    // Check to see if an index partition was split, joined or
                    // moved.
                    final String reason = getIndexPartitionGone(name);

                    if (reason != null) {

                        // Notify client of stale locator.
                        throw new StaleLocatorException(name, reason);

                    }

                    final AbstractBTree[] sources = getIndexSources(name,
                            ITx.UNISOLATED);

                    if (sources == null) {

                        log.info("No such index: name=" + name + ", timestamp="
                                + timestamp);

                        return null;

                    }

                    assert !sources[0].isReadOnly();

                    if (sources.length == 1) {

                        tmp = sources[0];

                    } else {

                        tmp = new FusedView(sources);

                    }

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
     * 
     * @return The dump.
     * 
     * @throws IllegalStateException
     *             if the live journal is closed when this method is invoked.
     * @throws RuntimeException
     *             if the live journal is closed asynchronously while this
     *             method is running.
     */
    public String listIndexPartitions(long timestamp) {

        if (timestamp == ITx.UNISOLATED || timestamp == ITx.READ_COMMITTED) {

            timestamp = getLiveJournal().getRootBlockView().getLastCommitTime();

        }
        
        StringBuilder sb = new StringBuilder();

        final AbstractJournal journal = getJournal(timestamp);
        
        sb.append("timestamp="+timestamp+"\njournal="+journal.getResourceMetadata());

        // historical view of Name2Addr as of that timestamp.
        final ITupleIterator itr = journal.getName2Addr(timestamp).rangeIterator(
                null, null);
        
        while (itr.hasNext()) {

            final ITuple tuple = itr.next();

            final Entry entry = EntrySerializer.INSTANCE
                    .deserialize(new DataInputBuffer(tuple.getValue()));

            // the name of an index to consider.
            final String name = entry.name;

            /*
             * Open the mutable BTree only (not the full view since we don't
             * want to force the read of index segments from the disk).
             */
            final BTree btree = journal.getIndex(entry.checkpointAddr);

            assert btree != null : entry.toString();
            
            // index metadata for that index partition.
            final IndexMetadata indexMetadata = btree.getIndexMetadata();

            // index partition metadata
            final LocalPartitionMetadata pmd = indexMetadata
                    .getPartitionMetadata();

            sb.append("\nname="+name+", checkpoint="+btree.getCheckpoint()+", pmd="+pmd);
    
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
                 outFile, //
                 outFile.length(),//
                 builder.segmentUUID,//
                 createTime//
                 );

         /*
          * notify the resource manager so that it can find this file.
          */

         addResource(segmentMetadata, outFile);

         return new BuildResult(name, indexMetadata, segmentMetadata);
         
    }

}
