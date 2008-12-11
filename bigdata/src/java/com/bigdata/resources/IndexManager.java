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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.ReadCommittedView;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.cache.ICacheEntry;
import com.bigdata.cache.LRUCache;
import com.bigdata.concurrent.NamedLock;
import com.bigdata.counters.Instrument;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.Tx;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IClientIndex;
import com.bigdata.service.IDataService;
import com.bigdata.service.MetadataService;
import com.bigdata.util.NT;

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
    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isInfoEnabled();

    /**
     * Options understood by the {@link IndexManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends StoreManager.Options {
     
        /**
         * The capacity of the LRU cache of open {@link IIndex}s. The capacity
         * of this cache indirectly controls how many {@link IIndex}s will be
         * held open. The main reason for keeping an {@link IIndex} open is to
         * reuse its buffers, including its node and leaf cache, if another
         * request arrives "soon" which would require on that {@link IIndex}.
         * <p>
         * The effect of this parameter is indirect owning to the semantics of
         * weak references and the control of the JVM over when they are
         * cleared. Once an index becomes weakly reachable, the JVM will
         * eventually GC the index object, thereby effectively closing it (or at
         * least releasing all resources associated with that index). Since
         * indices which are strongly reachable are never "closed" this provides
         * our guarentee that indices are never closed if they are in use.
         * <p>
         * Note: The {@link IIndex}s managed by this class are a
         * {@link FusedView} of {@link AbstractBTree}s. Each
         * {@link AbstractBTree} has a hard reference to the backing
         * {@link IRawStore} and will keep the {@link IRawStore} from being
         * finalized as long as a hard reference exists to the
         * {@link AbstractBTree} (the reverse is not true - an {@link IRawStore}
         * reference does NOT hold a hard reference to {@link AbstractBTree}s
         * on that {@link IRawStore}).
         * 
         * @see #DEFAULT_INDEX_CACHE_CAPACITY
         */
        String INDEX_CACHE_CAPACITY = IndexManager.class.getName()
                + ".indexCacheCapacity";

        String DEFAULT_INDEX_CACHE_CAPACITY = "20";

        /**
         * The time in milliseconds before an entry in the index cache will be
         * cleared from the backing {@link HardReferenceQueue} (default
         * {@value #DEFAULT_INDEX_CACHE_TIMEOUT}). This property controls how
         * long the index cache will retain an {@link IIndex} which has not been
         * recently used. This is in contrast to the cache capacity.
         */
        String INDEX_CACHE_TIMEOUT = IndexManager.class.getName()
                + ".indexCacheTimeout";

        String DEFAULT_INDEX_CACHE_TIMEOUT = ""+(60*1000); // One minute.
        
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
         * Note: {@link IndexSegment}s have a hard reference to the backing
         * {@link IndexSegmentStore} and will keep the {@link IndexSegmentStore}
         * from being finalized as long as a hard reference exists to the
         * {@link IndexSegment} (the reverse is not true - the
         * {@link IndexSegmentStore} does NOT hold a hard reference to the
         * {@link IndexSegment}).
         * 
         * @see #DEFAULT_INDEX_SEGMENT_CACHE_CAPACITY
         */
        String INDEX_SEGMENT_CACHE_CAPACITY = IndexManager.class.getName()
                + ".indexSegmentCacheCapacity";

        /**
         * The default for the {@link #INDEX_SEGMENT_CACHE_CAPACITY} option.
         */
        String DEFAULT_INDEX_SEGMENT_CACHE_CAPACITY = "60";

        /**
         * The time in milliseconds before an entry in the index segment cache
         * will be cleared from the backing {@link HardReferenceQueue} (default
         * {@value #DEFAULT_INDEX_SEGMENT_CACHE_TIMEOUT}). This property
         * controls how long the index segment cache will retain an
         * {@link IndexSegment} which has not been recently used. This is in
         * contrast to the cache capacity.
         */
        String INDEX_SEGMENT_CACHE_TIMEOUT = IndexManager.class.getName()
                + ".indexCacheTimeout";

        String DEFAULT_INDEX_SEGMENT_CACHE_TIMEOUT = "" + (60 * 1000); // One
                                                                        // minute.

    }

    /**
     * Performance counters for the {@link IndexManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IIndexManagerCounters {

        String StaleLocatorCacheCapacity = "Stale Locator Cache Capacity";

        String StaleLocatorCacheSize = "Stale Locator Cache Size";

        String StaleLocators = "Stale Locators";

        String IndexCacheCapacity = "Index Cache Capacity";

        String IndexCacheSize = "Index Cache Size";

        String IndexSegmentCacheCapacity = "Index Segment Cache Capacity";

        String IndexSegmentCacheSize = "Index Segment Cache Size";

    }

    /**
     * Cache of added/retrieved btrees by name and timestamp.
     * <p>
     * Map from the name and timestamp of an index to a weak reference for the
     * corresponding {@link IIndex}. Entries will be cleared from this map
     * after they have become only weakly reachable. Entries are associated with
     * a timestamp based on their last use and entries whose timestamp exceeds
     * the {@link Options#INDEX_CACHE_TIMEOUT} will be cleared from the backing
     * {@link HardReferenceQueue}. If they become weakly reachable they will
     * then be cleared from the cache as well.
     * <p>
     * Note: The capacity of the backing {@link HardReferenceQueue} effects how
     * many _clean_ indices can be held in the cache. Dirty indices remain
     * strongly reachable owing to their existence in the
     * {@link Name2Addr#commitList}.
     * <p>
     * Note: {@link ITx#READ_COMMITTED} indices MUST NOT be allowed into this
     * cache. Each time there is a commit for a given {@link BTree}, the
     * {@link ITx#READ_COMMITTED} view of that {@link BTree} needs to be
     * replaced by the most recently committed view, which is a different
     * {@link BTree} object and is loaded from a different checkpoint record.
     * 
     * @see Options#INDEX_CACHE_CAPACITY
     * @see Options#INDEX_CACHE_TIMEOUT
     * 
     * @todo alternatively, if such views are allowed in then this cache must be
     *       encapsulated by logic that examines the view when the timestamp is
     *       {@link ITx#READ_COMMITTED} to make sure that the BTree associated
     *       with that view is current (as of the last commit point). If not,
     *       then the entire view needs to be regenerated since the index view
     *       definition (index segments in use) might have changed as well.
     */
//    final private WeakValueCache<NT, IIndex> indexCache;
    final private ConcurrentWeakValueCache<NT, IIndex> indexCache;
    
    /**
     * A canonicalizing cache for {@link IndexSegment}s.
     * 
     * @see Options#INDEX_SEGMENT_CACHE_CAPACITY
     * @see Options#INDEX_SEGMENT_CACHE_TIMEOUT
     */
    final private ConcurrentWeakValueCache<UUID, IndexSegment> indexSegmentCache;
//    final private WeakValueCache<UUID, IndexSegment> indexSegmentCache;

    /**
     * Provides locks on a per-{name+timestamp} basis for higher concurrency.
     */
    private final transient NamedLock<NT> namedLock = new NamedLock<NT>();

    /**
     * Provides locks on a per-{@link IndexSegment} UUID basis for higher
     * concurrency.
     * <p>
     * Note: The UUID is the unique key for the {@link #indexSegmentCache}.
     * <p>
     * Note: The index name + timestamp is NOT a good basis for locking for the
     * {@link #indexSegmentCache} because many different timestamps will be
     * mapped onto the same {@link IndexSegment}.
     */
    private final transient NamedLock<UUID> segmentLock = new NamedLock<UUID>();
    
    /**
     * The #of entries in the hard reference cache for {@link IIndex}s. There
     * MAY be more {@link IIndex}s open than are reported by this method if
     * there are hard references held by the application to those {@link IIndex}s.
     * {@link IIndex}s that are not fixed by a hard reference will be quickly
     * finalized by the JVM.
     */
    public int getIndexCacheSize() {
        
        return indexCache.size();
        
    }
    
    /**
     * The configured capacity of the index cache.
     * 
     * @see Options#INDEX_CACHE_CAPACITY
     */
    public int getIndexCacheCapacity() {
        
        return indexCache.capacity();
        
    }
    
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
     * The configured capacity of the index segment cache.
     * 
     * @see Options#INDEX_SEGMENT_CACHE_CAPACITY
     */
    public int getIndexSegmentCacheCapacity() {
        
        return indexSegmentCache.capacity();
        
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
    // private // @todo exposed for counters - should be private.
    protected final LRUCache<String/* name */, StaleLocatorReason/* reason */> staleLocatorCache = new LRUCache<String, StaleLocatorReason>(
            1000);  
    
    /**
     * Note: this information is based on an LRU cache with a large fixed
     * capacity. It is expected that the cache size is sufficient to provide
     * good information to clients having queued write tasks.  If the index
     * partition split/move/join changes somehow outpace the cache size then
     * the client would see a {@link NoSuchIndexException} instead.
     */
    public StaleLocatorReason getIndexPartitionGone(String name) {
    
        return staleLocatorCache.get(name);
        
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
    protected void setIndexPartitionGone(String name, StaleLocatorReason reason) {
        
        if (name == null)
            throw new IllegalArgumentException();
        
        if (reason == null)
            throw new IllegalArgumentException();
        
        if (INFO)
            log.info("name=" + name + ", reason=" + reason);

        staleLocatorCache.put(name, reason, true);
        
    }

    /**
     * The #of entries in the stale locator LRU.
     */
    protected int getStaleLocatorCount() {

        return staleLocatorCache.size();
        
    }
    
    protected IndexManager(Properties properties) {
        
        super(properties);
     
        {
            
            final int indexCacheCapacity = Integer.parseInt(properties.getProperty(
                    Options.INDEX_CACHE_CAPACITY,
                    Options.DEFAULT_INDEX_CACHE_CAPACITY));

            if (INFO)
                log.info(Options.INDEX_CACHE_CAPACITY + "="
                        + indexCacheCapacity);

            if (indexCacheCapacity <= 0)
                throw new RuntimeException(Options.INDEX_CACHE_CAPACITY
                        + " must be positive");

            final long indexCacheTimeout = Long.parseLong(properties
                    .getProperty(Options.INDEX_CACHE_TIMEOUT,
                            Options.DEFAULT_INDEX_CACHE_TIMEOUT));

            if (INFO)
                log.info(Options.INDEX_CACHE_TIMEOUT + "=" + indexCacheTimeout); 

            if (indexCacheTimeout < 0)
                throw new RuntimeException(Options.INDEX_CACHE_TIMEOUT
                        + " must be non-negative");
            
//            indexCache = new WeakValueCache<NT, IIndex>(
//                    new LRUCache<NT, IIndex>(indexCacheCapacity));

            indexCache = new ConcurrentWeakValueCache<NT, IIndex>(
                    new HardReferenceQueue<IIndex>(null/* evictListener */,
                            indexCacheCapacity,
                            HardReferenceQueue.DEFAULT_NSCAN,
                            TimeUnit.MILLISECONDS.toNanos(indexCacheTimeout)),
                    .75f/* loadFactor */, 16/* concurrencyLevel */, true/* removeClearedEntries */);

        }
        
        /*
         * indexSegmentCacheCapacity
         */
        {

            final int indexSegmentCacheCapacity = Integer.parseInt(properties.getProperty(
                    Options.INDEX_SEGMENT_CACHE_CAPACITY,
                    Options.DEFAULT_INDEX_SEGMENT_CACHE_CAPACITY));

            if (INFO)
                log.info(Options.INDEX_SEGMENT_CACHE_CAPACITY + "="
                        + indexSegmentCacheCapacity);

            if (indexSegmentCacheCapacity <= 0)
                throw new RuntimeException(Options.INDEX_SEGMENT_CACHE_CAPACITY
                        + " must be positive");

            final long indexSegmentCacheTimeout = Long.parseLong(properties
                    .getProperty(Options.INDEX_SEGMENT_CACHE_TIMEOUT,
                            Options.DEFAULT_INDEX_SEGMENT_CACHE_TIMEOUT));

            if (INFO)
                log.info(Options.INDEX_SEGMENT_CACHE_TIMEOUT + "="
                        + indexSegmentCacheTimeout);

            if (indexSegmentCacheTimeout < 0)
                throw new RuntimeException(Options.INDEX_SEGMENT_CACHE_TIMEOUT
                        + " must be non-negative");

            indexSegmentCache = new ConcurrentWeakValueCache<UUID, IndexSegment>(
                    new HardReferenceQueue<IndexSegment>(
                            null/* evictListener */,
                            indexSegmentCacheCapacity,
                            HardReferenceQueue.DEFAULT_NSCAN,
                            TimeUnit.MILLISECONDS
                                    .toNanos(indexSegmentCacheTimeout)),
                    .75f/* loadFactor */, 16/* concurrencyLevel */, true/* removeClearedEntries */);

//            indexSegmentCache = new WeakValueCache<UUID, IndexSegment>(
////                    WeakValueCache.INITIAL_CAPACITY,//
////                    WeakValueCache.LOAD_FACTOR, //
//                    new LRUCache<UUID, IndexSegment>(indexSegmentCacheCapacity)
////                    new WeakCacheEntryFactory<UUID,IndexSegment>()
////                    new ClearReferenceListener()
//                    );

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
                 * atomic get/put against the WeakValueCache.
                 * 
                 * Note: The load of the index segment from the store can have
                 * significiant latency. The use of a per-UUID lock allows us to
                 * load index segments for different index views concurrently.
                 * 
                 * Note: We DO NOT use a name+timestamp lock here because many
                 * different timestamp values will be served by the same
                 * IndexSegment.
                 */
                final Lock lock = segmentLock.acquireLock(storeUUID);
                
                try {

                    // check the cache first.
                    IndexSegment seg = indexSegmentCache.get(storeUUID);

                    if (seg == null) {

                        if (INFO)
                            log.info("Loading index segment from store: name="
                                    + name + ", file="
                                    + resourceMetadata.getFile());

                        // Open an index segment.
                        seg = segStore.loadIndexSegment();

                        indexSegmentCache.put(storeUUID, seg);
//                        indexSegmentCache
//                                .put(storeUUID, seg, false/* dirty */);

                    }
                    
                    btree = seg;

                } finally {
                    
                    lock.unlock();
                    
                }
            
            }

        }

        if (INFO)
            log.info("name=" + name + ", timestamp=" + timestamp + ", store="
                    + store + " : " + btree);

        return btree;

    }

    public AbstractBTree[] getIndexSources(String name, long timestamp) {

        if (INFO)
            log.info("name=" + name + ", timestamp=" + timestamp);

        /*
         * Open the index on the journal for that timestamp.
         */
        final BTree btree;
        {

            // the corresponding journal (can be the live journal).
            final AbstractJournal journal = getJournal(timestamp);

            if (journal == null) {

                log.warn("No journal with data for timestamp: name=" + name
                        + ", timestamp=" + timestamp);

                return null;

            }

            btree = (BTree) getIndexOnStore(name, timestamp, journal);

            if (btree == null) {

                if (INFO)
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
        
        return getIndexSources(name, timestamp, btree);

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

            if (INFO)
                log.info("Unpartitioned index: name=" + name + ", ts="
                        + timestamp);

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

                final IRawStore store;
                try {
                    
                    store = openStore(resource.getUUID());
                    
                } catch (NoSuchStoreException ex) {
                    
                    /*
                     * There is dependency for that index that is on a resource
                     * (a ManagedJournal or IndexSegment) that is no longer
                     * available.
                     */
                    
                    throw new RuntimeException("Could not load index: name="
                            + name + ", timestamp=" + timestamp
                            + " from resource=" + resource.getFile(), ex);
                    
                }

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

                if (INFO)
                    log.info("Added to view: " + resource);

                sources[i] = ndx;

            }

        }

        if (INFO)
            log.info("Opened index partition:  name=" + name + ", timestamp="
                    + timestamp);

        return sources;

    }
    
    /**
     * Note: An {@link ITx#READ_COMMITTED} view returned by this method WILL NOT
     * update if there are intervening commits. This decision was made based on
     * the fact that views are requested from the {@link IndexManager} by an
     * {@link AbstractTask} running on the {@link ConcurrencyManager}. Such
     * tasks, and hence such views, have a relatively short life. However, the
     * {@link Journal} implementation of this method is different and will
     * return a {@link ReadCommittedView} precisely because objects are directly
     * requested from a {@link Journal} by the application and the application
     * can hold onto a read-committed view for an arbitrary length of time. This
     * has the pragmatic effect of allowing us to cache read-committed views in
     * the application and in the {@link IBigdataClient}. For the
     * {@link IBigdataClient}, the view acquires its read-committed semantics
     * because an {@link IClientIndex} generates {@link AbstractTask}(s) for
     * each {@link IIndex} operation and submits those task(s) to the
     * appropriate {@link IDataService}(s) for evaluation. The
     * {@link IDataService} will resolve the index using this method, and it
     * will always see the then-current read-committed view and the
     * {@link IClientIndex} will appear to have read-committed semantics.
     * 
     * @see Journal#getIndex(String, long)
     */
    public IIndex getIndex(String name, long timestamp) {
        
        if (name == null) {

            throw new IllegalArgumentException();

        }

        final NT nt = new NT(name, timestamp);
        
        final Lock lock = namedLock.acquireLock(nt);

        try {

            if (timestamp != ITx.READ_COMMITTED) {
             
                // test the indexCache.
//                synchronized (indexCache) {

                    final IIndex ndx = indexCache.get(nt);

                    if (ndx != null) {

                        if (INFO)
                            log.info("Cache hit: " + nt);

                        return ndx;

                    }

//                }
                
            }

            // is this a transactional view?
            final boolean isTransaction = timestamp > ITx.UNISOLATED;

            // lookup transaction iff transactional view.
            final ITx tx = (isTransaction ? getConcurrencyManager()
                    .getTransactionManager().getTx(timestamp) : null);

            if (isTransaction) {

                /*
                 * Handle transactional views.
                 */
                
                if (tx == null) {

                    log.warn("Unknown transaction: name=" + name + ", tx="
                            + timestamp);

                    return null;

                }

                if (!tx.isActive()) {

                    // typically this means that the transaction has already
                    // prepared.
                    log.warn("Transaction not active: name=" + name + ", tx="
                            + timestamp + ", prepared=" + tx.isPrepared()
                            + ", complete=" + tx.isComplete() + ", aborted="
                            + tx.isAborted());

                    return null;

                }

            }

            if (isTransaction && tx == null) {

                /*
                 * Note: This will happen both if you attempt to use a
                 * transaction identified that has not been registered or if you
                 * attempt to use a transaction manager after the transaction
                 * has been either committed or aborted.
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
                 * Note: The backing index is always a historical state of the
                 * named index.
                 * 
                 * Note: Tx#getIndex(String name) serializes concurrent requests
                 * for the same index (thread-safe).
                 */

                final IIndex isolatedIndex = tx.getIndex(name);

                if (isolatedIndex == null) {

                    log.warn("No such index: name=" + name + ", tx="
                            + timestamp);

                    return null;

                }

                tmp = isolatedIndex;

            } else {

                /*
                 * Non-transactional view.
                 */

                if (readOnly) {

                    /*
                     * historical read -or- read-committed operation.
                     */

                    final AbstractBTree[] sources = getIndexSources(name,
                            timestamp);

                    if (sources == null) {

                        if (INFO)
                            log.info("No such index: name=" + name
                                    + ", timestamp=" + timestamp);

                        return null;

                    }

//                    System.err.println("getIndex(name="+name+", timestamp="+timestamp);
                    
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

                    /*
                     * Check to see if an index partition was split, joined or
                     * moved.
                     */
                    final StaleLocatorReason reason = getIndexPartitionGone(name);

                    if (reason != null) {

                        // Notify client of stale locator.
                        throw new StaleLocatorException(name, reason);

                    }

                    final AbstractBTree[] sources = getIndexSources(name,
                            ITx.UNISOLATED);

                    if (sources == null) {

                        if (INFO)
                            log.info("No such index: name=" + name
                                    + ", timestamp=" + timestamp);

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

            if (timestamp != ITx.READ_COMMITTED) {

                // update the indexCache.
                if (INFO)
                    log.info("Adding to cache: " + nt);

//                synchronized (indexCache) {

//                    indexCache.put(nt, tmp, true/* dirty */);
                    indexCache.put(nt, tmp);

//                }

            }
            
            return tmp;

        } finally {

            lock.unlock();

        }

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

            timestamp = getLiveJournal().getLastCommitTime();

        }
        
        final StringBuilder sb = new StringBuilder();

        final AbstractJournal journal = getJournal(timestamp);
        
        sb.append("timestamp="+timestamp+"\njournal="+journal.getResourceMetadata());

        // historical view of Name2Addr as of that timestamp.
        final ITupleIterator itr = journal.getName2Addr(timestamp)
                .rangeIterator();
        
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
     * Build an {@link IndexSegment} from an index partition. Delete markers are
     * propagated to the {@link IndexSegment} unless <i>compactingMerge</i> is
     * <code>true</code>.
     * 
     * @param name
     *            The name of the index partition (not the name of the scale-out
     *            index).
     * @param src
     *            A view of the index partition as of the <i>createTime</i>.
     * @param outFile
     *            The file on which the {@link IndexSegment} will be written.
     *            The file MAY exist, but if it exists then it MUST be empty.
     * @param compactingMerge
     *            When <code>true</code> the caller asserts that <i>src</i>
     *            is a {@link FusedView} and deleted index entries WILL NOT be
     *            included in the generated {@link IndexSegment}. Otherwise, it
     *            is assumed that the only select component(s) of the index
     *            partition view are being exported onto an {@link IndexSegment}
     *            and deleted index entries will therefore be propagated to the
     *            new {@link IndexSegment}.
     * @param createTime
     *            The timestamp of the view. This is typically the
     *            lastCommitTime of the old journal after an
     *            {@link #overflow(boolean, boolean)} operation.
     * @param fromKey
     *            The lowest key that will be included (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will not be included (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @return A {@link BuildResult} identifying the new {@link IndexSegment}
     *         and the source index.
     * 
     * @throws Exception
     *             if any errors are encountered then the file (if it exists)
     *             will be deleted as a side-effect before returning control to
     *             the caller.
     */
    public BuildResult buildIndexSegment(final String name, final IIndex src,
            final File outFile, final boolean compactingMerge,
            final long createTime, final byte[] fromKey, final byte[] toKey)
            throws Exception {

        final IndexMetadata indexMetadata;
        final SegmentMetadata segmentMetadata;
        
        try {

            // new builder.
            final IndexSegmentBuilder builder = IndexSegmentBuilder
                    .newInstance(name, src, outFile, tmpDir, compactingMerge,
                            createTime, fromKey, toKey);

            // metadata for that index / index partition.
            indexMetadata = src.getIndexMetadata();

            // build the index segment.
            builder.call();

            // report event
            notifyIndexSegmentBuildEvent(builder);

            // Describe the index segment.
            segmentMetadata = new SegmentMetadata(//
                    outFile, //
                    builder.segmentUUID, //
                    createTime //
            );

            // Notify the resource manager so that it can find this file.
            addResource(segmentMetadata, outFile);

        } catch (Throwable t) {

            if (outFile != null && outFile.exists()) {

                try {

                    outFile.delete();

                } catch (Throwable t2) {

                    log.warn(t2.getLocalizedMessage(), t2);

                }

            }

            if (t instanceof Exception)
                throw (Exception) t;

            throw new RuntimeException(t);

        }

        /*
         * Note: Now that the resource is registered with the StoreManager we
         * have to handle errors somewhat differently.
         */
        
        try {

            if (INFO)
                log.info("built index segment: " + name + ", file=" + outFile);

            return new BuildResult(name, indexMetadata, segmentMetadata);

        } catch (Throwable t) {

            try {

                deleteResource(segmentMetadata.getUUID(), false/* isJournal */);
                
            } catch (Throwable t2) {
                
                log.warn(t2.getLocalizedMessage(), t2);
                
            }

            if (t instanceof Exception)
                throw (Exception) t;

            throw new RuntimeException(t);

        }

    }

}
