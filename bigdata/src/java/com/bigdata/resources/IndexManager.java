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
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentCheckpoint;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.ReadCommittedView;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.cache.LRUCache;
import com.bigdata.concurrent.NamedLock;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
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
import com.bigdata.journal.TimestampUtility;
import com.bigdata.journal.Tx;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.Event;
import com.bigdata.service.EventType;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IDataService;
import com.bigdata.service.jini.util.DumpFederation;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.util.NT;

/**
 * Class encapsulates logic and handshaking for tracking which indices (and
 * their backing stores) are recently and currently referenced. This information
 * is used to coordinate the close out of index resources (and their backing
 * stores) on an LRU basis by the {@link ResourceManager}.
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
     * Options understood by the {@link IndexManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends StoreManager.Options {
     
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
         * <p>
         * Note: The retention of the {@link BTree}s on the live
         * {@link ManagedJournal}s is governed by
         * {@link com.bigdata.journal.Options#LIVE_INDEX_CACHE_CAPACITY}.
         * <p>
         * Note: The retention of the {@link BTree}s on the open historical
         * {@link ManagedJournal}s is governed by
         * {@link com.bigdata.journal.Options#HISTORICAL_INDEX_CACHE_CAPACITY}.
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

        /**
         * The parent under which the per-index partition performance counters
         * are listed.
         */
        String Indices = "indices";

        /**
         * The capacity of the cache of stale locators.
         * 
         * @see StaleLocatorException
         */
        String StaleLocatorCacheCapacity = "Stale Locator Cache Capacity";

        /**
         * The #of stale locators in the cache.
         * 
         * @see StaleLocatorException
         */
        String StaleLocatorCacheSize = "Stale Locator Cache Size";

        /**
         * The stale locators, including the {@link StaleLocatorReason} for each
         * one.
         */
        String StaleLocators = "Stale Locators";

        /**
         * The capacity of the index cache.
         */
        String IndexCacheCapacity = "Index Cache Capacity";

        /**
         * The approximate #of open indices.
         */
        String IndexCacheSize = "Index Cache Size";

        /**
         * The capacity of the {@link IndexSegment} cache.
         */
        String IndexSegmentCacheCapacity = "Index Segment Cache Capacity";

        /**
         * The approximate #of open {@link IndexSegment}.
         */
        String IndexSegmentCacheSize = "Index Segment Cache Size";

        /**
         * The approximate #of {@link IndexSegment} leaves that are buffered in
         * memory.
         */
        String IndexSegmentOpenLeafCount = "Index Segment Open Leaf Count";

        /**
         * The #of bytes on disk occupied by the {@link IndexSegment} leaves
         * which are currently loaded into memory (their in-memory profile can
         * not be directly captured by the java runtime, but you can get it from
         * a heap dump). Likewise, you can directly obtain the #of bytes on disk
         * per leaf from the {@link IndexSegmentCheckpoint} or from
         * {@link DumpFederation}.
         */
        String IndexSegmentOpenLeafByteCount = "Index Segment Open Leaf Byte Count";
        
    }

    /**
     * Cache of added/retrieved {@link IIndex}s by name and timestamp.
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
     * Note: Read-historical and read-committed tasks need to hold a read lock
     * on the local resources in order to prevent their being released if there
     * is a concurrent commit followed by a request to the StoreManager to
     * purgeResources. This problem is very similar to the problem of the
     * transaction manager which needs to manage the global release time.
     * <p>
     * Note: {@link ITx#READ_COMMITTED} indices MUST NOT be allowed into this
     * cache. Each time there is a commit for a given {@link BTree}, the
     * {@link ITx#READ_COMMITTED} view of that {@link BTree} needs to be
     * replaced by the most recently committed view, which is a different
     * {@link BTree} object and is loaded from a different checkpoint record.
     * <p>
     * Note: {@link ITx#UNISOLATED} indices have a related problem. Those views
     * are no longer valid after synchronous overflow since a new view is
     * defined by that process. Likewise, the various atomic update tasks during
     * asynchronous overflow also change the definition of the view. Therefore I
     * have modified the IndexManager to NOT permit UNISOLATES views into the
     * index cache. Note however that the Journal still retains a live index
     * cache and that we still have a separate cache for index segment stores.
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
    final private IndexCache indexCache;

    /**
     * The earliest timestamp that MUST be retained for the read-historical
     * indices in the cache and {@link Long#MAX_VALUE} if there are NO
     * read-historical indices in the cache.
     * 
     * @see StoreManager#indexCacheLock
     */
    protected long getIndexRetentionTime() {

        final long t = indexCache.getRetentionTime();

        assert t > 0 : "t=" + t;

        return t;
        
    }
    
    /**
     * Extends the {@link ConcurrentWeakValueCache} to track the earliest
     * timestamp from which any local {@link IIndex} view is reading. This
     * timestamp is reported by {@link #getRetentionTime()}. The
     * {@link StoreManager} uses this in
     * {@link StoreManager#purgeOldResources()} to provide a "read lock" such
     * that resources for in use views are not released.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class IndexCache extends
            ConcurrentWeakValueCacheWithTimeout<NT, ILocalBTreeView> {

        /**
         * The earliest timestamp that must be retained for the read-historical
         * indices in the cache, {@link Long#MAX_VALUE} if there a NO
         * read-historical indices in the cache, and (-1L) if the value is not
         * known and must be computed by scanning the index cache.
         * 
         * @todo an alternative is to maintain a sorted set of the timestamps
         *       (breaking ties with the index name) and then always return the
         *       first entry (lowest value) from the set. in fact the tx service
         *       does something similar, but it has an easier time since the
         *       timestamps are unique where the same timestamp may be used for
         *       multiple indices here.
         */
        private AtomicLong retentionTime = new AtomicLong(-1L);

        /**
         * The earliest timestamp that MUST be retained for the read-historical
         * indices in the cache and {@link Long#MAX_VALUE} if there are NO
         * read-historical indices in the cache.
         * <p>
         * Note: Due to the concurrent operations of the garbage collector, this
         * method MAY return a time that is earlier than necessary. This
         * possibility arises because {@link WeakReference} values may be
         * cleared at any time. There is no negative consequence to this
         * behavior - it simply means that fewer resources will be released than
         * might otherwise be possible. This "hole" can not be closed by polling
         * the {@link ReferenceQueue} since it is not when the entries are
         * removed from the map which matters, but when their
         * {@link WeakReference} values are cleared. However, periodically
         * clearing stale references using {@link #clearStaleRefs()} will keep
         * down the size of that "hole".
         */
        public long getRetentionTime() {

//            /*
//             * This will tend to clear stale references and cause them to be
//             * cleared in the cache, but probably not in time for the changes to
//             * be noticed within the loop below.
//             */
//
//            clearStaleRefs();
            
            synchronized (retentionTime) {

                // @todo remove debug flag.
                final boolean debug = false;
                
                if (retentionTime.get() == -1L) {

                    // note: default if nothing is found.
                    long tmp = Long.MAX_VALUE;

                    int n = 0;

                    final long now = System.currentTimeMillis();
                    
                    final Iterator<Map.Entry<NT, WeakReference<ILocalBTreeView>>> itr = entryIterator();

                    while (itr.hasNext()) {

                        final Map.Entry<NT, WeakReference<ILocalBTreeView>> entry = itr
                                .next();

                        if (entry.getValue().get() == null) {

                            // skip cleared references.
                            continue;

                        }

                        final long timestamp = entry.getKey().getTimestamp();

                        if (timestamp == ITx.UNISOLATED) {

                            // ignore unisolated indices.
                            continue;

                        }

                        if(debug)
                            log.warn("considering: " + entry.getKey());
                        
                        if (timestamp < tmp) {

                            // choose the earliest timestamp for any index.
                            tmp = timestamp;
                            
                            if(debug)
                            log.warn("Earliest so far: "
                                    + tmp
                                    + " (age="
                                    + TimeUnit.MILLISECONDS
                                            .toSeconds(tmp - now) + "secs)");

                        }
                        
                        n++;

                    } // next entry

                    retentionTime.set(tmp);

                    if(debug)
                        log.warn("Considered: " + n + " indices: retentionTime="
                            + tmp + " (age="
                            + TimeUnit.MILLISECONDS.toSeconds(tmp - now)
                            + "secs)");
                    
                } // end search.

                return retentionTime.get();
                
            } // synchronized(retentionTime)

        }

        public ILocalBTreeView put(final NT k, final ILocalBTreeView v) {

            synchronized (retentionTime) {

                if (retentionTime.get() > k.getTimestamp()) {

                    // found an earlier timestamp, so use that.
                    retentionTime.set(k.getTimestamp());

                }

            }

            return super.put(k, v);

        }

        public ILocalBTreeView putIfAbsent(final NT k, final ILocalBTreeView v) {
            
            synchronized(retentionTime) {
                
                if (retentionTime.get() > k.getTimestamp()) {

                    // found an earlier timestamp, so use that.
                    retentionTime.set(k.getTimestamp());
                    
                }
                
            }

            return super.putIfAbsent(k, v);
            
        }
        
        /**
         * Overriden to clear the {@link #retentionTime} if the map entry
         * corresponding to that timestamp is being removed from the map.
         */
        protected WeakReference<ILocalBTreeView> removeMapEntry(final NT k) {
            
            synchronized(retentionTime) {
                
                if (retentionTime.get() == k.getTimestamp()) {

                    /*
                     * Removed the earliest timestamp so we will need to
                     * explicitly search for the new minimum timestamp.
                     */
                    
                    retentionTime.set(-1L);
                    
                }
                
            }

            return super.removeMapEntry(k);
            
        }
        
        public IndexCache(final int cacheCapacity, final long cacheTimeout) {

            super(cacheCapacity, TimeUnit.MILLISECONDS.toNanos(cacheTimeout));

        }
        
    }
    
    /**
     * A canonicalizing cache for {@link IndexSegment}s.
     * <p>
     * Note: {@link IndexSegmentStore} already makes the {@link IndexSegment}s
     * canonical and the {@link StoreManager#storeCache} makes the
     * {@link IndexSegmentStore}s canonical so what this really does is give
     * you a cache which lets you exert some more control over the #of
     * {@link IndexSegment}s that are open.
     * 
     * FIXME It might be better to break this down as a journalCache and a
     * segmentCache on the {@link StoreManager}.  That is more explicit and
     * there is less interaction between the configuration choices with that
     * breakdown.
     * 
     * @see Options#INDEX_SEGMENT_CACHE_CAPACITY
     * @see Options#INDEX_SEGMENT_CACHE_TIMEOUT
     */
    final private ConcurrentWeakValueCacheWithTimeout<UUID, IndexSegment> indexSegmentCache;

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
     * Statistics about the {@link IndexSegment}s open in the cache.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class IndexSegmentStats {

        public long leafCount;
        
        public long leafByteCount;

    }

    /**
     * The approximate #of {@link IndexSegment} leaves in memory.
     */
    public int getIndexSegmentOpenLeafCount() {

        final Iterator<WeakReference<IndexSegment>> itr = indexSegmentCache
                .iterator();

        int leafCount = 0;
        
        while (itr.hasNext()) {

            final IndexSegment seg = itr.next().get();

            if (seg != null) {

                leafCount += seg.getOpenLeafCount();
                
            }
            
        }
        
        return leafCount;
        
    }

    /**
     * The #of bytes on disk occupied by the {@link IndexSegment} leaves which
     * are currently loaded into memory (their in-memory profile can not be
     * directly captured by the java runtime, but you can get it from a heap
     * dump). Likewise, you can directly obtain the #of bytes on disk per leaf
     * from the {@link IndexSegmentCheckpoint} or from {@link DumpFederation}.
     */
    public long getIndexSegmentOpenLeafByteCount() {

        final Iterator<WeakReference<IndexSegment>> itr = indexSegmentCache
                .iterator();

        long leafByteCount = 0;
        
        while (itr.hasNext()) {

            final IndexSegment seg = itr.next().get();

            if (seg != null) {

                leafByteCount += seg.getOpenLeafByteCount();
                
            }
            
        }
        
        return leafByteCount;
        
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
    public StaleLocatorReason getIndexPartitionGone(final String name) {
    
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
     * 
     * FIXME Should also include "deleted" and handle case where a scale-out
     * index is deleted and then re-created so that we don't get the
     * {@link StaleLocatorException} after the recreate.
     */
    protected void setIndexPartitionGone(final String name,
            final StaleLocatorReason reason) {
        
        if (name == null)
            throw new IllegalArgumentException();
        
        if (reason == null)
            throw new IllegalArgumentException();
        
        if (log.isInfoEnabled())
            log.info("name=" + name + ", reason=" + reason);

        staleLocatorCache.put(name, reason, true);
        
        // clear from the index counters.
        indexCounters.remove(name);
        
    }

    /**
     * The #of entries in the stale locator LRU.
     */
    protected int getStaleLocatorCount() {

        return staleLocatorCache.size();
        
    }
    
    protected IndexManager(final Properties properties) {
        
        super(properties);
     
        /*
         * indexCache
         */
        {
            
            final int indexCacheCapacity = Integer.parseInt(properties.getProperty(
                    Options.INDEX_CACHE_CAPACITY,
                    Options.DEFAULT_INDEX_CACHE_CAPACITY));

            if (log.isInfoEnabled())
                log.info(Options.INDEX_CACHE_CAPACITY + "="
                        + indexCacheCapacity);

            if (indexCacheCapacity <= 0)
                throw new RuntimeException(Options.INDEX_CACHE_CAPACITY
                        + " must be positive");

            final long indexCacheTimeout = Long.parseLong(properties
                    .getProperty(Options.INDEX_CACHE_TIMEOUT,
                            Options.DEFAULT_INDEX_CACHE_TIMEOUT));

            if (log.isInfoEnabled())
                log.info(Options.INDEX_CACHE_TIMEOUT + "=" + indexCacheTimeout); 

            if (indexCacheTimeout < 0)
                throw new RuntimeException(Options.INDEX_CACHE_TIMEOUT
                        + " must be non-negative");

            indexCache = new IndexCache(indexCacheCapacity, indexCacheTimeout);

        }
        
        /*
         * indexSegmentCache
         */
        {

            final int indexSegmentCacheCapacity = Integer.parseInt(properties.getProperty(
                    Options.INDEX_SEGMENT_CACHE_CAPACITY,
                    Options.DEFAULT_INDEX_SEGMENT_CACHE_CAPACITY));

            if (log.isInfoEnabled())
                log.info(Options.INDEX_SEGMENT_CACHE_CAPACITY + "="
                        + indexSegmentCacheCapacity);

            if (indexSegmentCacheCapacity <= 0)
                throw new RuntimeException(Options.INDEX_SEGMENT_CACHE_CAPACITY
                        + " must be positive");

            final long indexSegmentCacheTimeout = Long.parseLong(properties
                    .getProperty(Options.INDEX_SEGMENT_CACHE_TIMEOUT,
                            Options.DEFAULT_INDEX_SEGMENT_CACHE_TIMEOUT));

            if (log.isInfoEnabled())
                log.info(Options.INDEX_SEGMENT_CACHE_TIMEOUT + "="
                        + indexSegmentCacheTimeout);

            if (indexSegmentCacheTimeout < 0)
                throw new RuntimeException(Options.INDEX_SEGMENT_CACHE_TIMEOUT
                        + " must be non-negative");

            indexSegmentCache = new ConcurrentWeakValueCacheWithTimeout<UUID, IndexSegment>(
                    indexSegmentCacheCapacity, TimeUnit.MILLISECONDS
                            .toNanos(indexSegmentCacheTimeout));

        }

    }
    
    /**
     * Return a reference to the named index as of the specified timestamp on
     * the identified resource.
     * <p>
     * Note: {@link AbstractTask} handles the load of the {@link ITx#UNISOLATED}
     * index from the live journal in such a manner as to provide ACID semantics
     * for add/drop of indices.
     * <p>
     * Note: The returned index is NOT isolated. Isolation is handled by the
     * {@link Tx}.
     * 
     * @param name
     *            The index name.
     * @param timestamp
     *            A transaction identifier, {@link ITx#UNISOLATED} for the
     *            unisolated index view, {@link ITx#READ_COMMITTED}, or
     *            <code>timestamp</code> for a historical view no later than
     *            the specified timestamp.
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

            /*
             * A BTree on this Journal.
             */

            btree = getIndexOnJournal(name, timestamp, (AbstractJournal) store);

        } else {

            /*
             * An IndexSegmentStore containing a single IndexSegment.
             */

            btree = getIndexOnSegment(name, timestamp,
                    (IndexSegmentStore) store);

        }

        if (btree != null) {

            /*
             * Make sure that it is using the canonical counters for that index.
             * 
             * Note: AbstractTask also does this for UNISOLATED indices which it
             * loads by itself as part of providing ACID semantics for add/drop
             * of indices.
             */

            btree.setBTreeCounters(getIndexCounters(name));

        }

        if (log.isInfoEnabled())
            log.info("name=" + name + ", timestamp=" + timestamp + ", found="
                    + (btree != null) + ", store=" + store + " : " + btree);

        return btree;

    }

    final private AbstractBTree getIndexOnJournal(final String name,
            final long timestamp, final AbstractJournal journal) {

        final AbstractBTree btree;

        if (timestamp == ITx.UNISOLATED) {

            /*
             * Unisolated index.
             */

            // MAY be null.
            btree = (BTree) journal.getIndex(name);

        } else if (timestamp == ITx.READ_COMMITTED) {

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

                assert ((BTree) btree).getLastCommitTime() != 0;
                
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
                        + journal.getResourceMetadata());

                return null;

            }

            // open index on that journal (MAY be null).
            btree = (BTree) journal.getIndex(name, commitRecord);

            if (btree != null) {

                assert ((BTree) btree).getLastCommitTime() != 0;

            }

        }

        // MAY be null.
        return btree;

    }
    
    final private IndexSegment getIndexOnSegment(final String name,
            final long timestamp, IndexSegmentStore segStore) {

        final IndexSegment btree;

        if (timestamp != ITx.READ_COMMITTED && timestamp != ITx.UNISOLATED) {

            // use absolute value in case timestamp is negative.
            final long ts = Math.abs(timestamp);

            if (segStore.getCheckpoint().commitTime > ts) {

                log.warn("Resource has no data for timestamp: name=" + name
                        + ", timestamp=" + timestamp + ", store=" + segStore);

                return null;

            }

        }

        {

            final IResourceMetadata resourceMetadata = segStore
                    .getResourceMetadata();

            final UUID storeUUID = resourceMetadata.getUUID();

            /*
             * Note: synchronization is required to have the semantics of an
             * atomic get/put against the WeakValueCache.
             * 
             * Note: The load of the index segment from the store can have
             * significant latency. The use of a per-UUID lock allows us to load
             * index segments for different index views concurrently.
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

                    if (log.isInfoEnabled())
                        log
                                .info("Loading index segment from store: name="
                                        + name + ", file="
                                        + resourceMetadata.getFile());

                    // Open an index segment.
                    seg = segStore.loadIndexSegment();

                    indexSegmentCache.put(storeUUID, seg);

                }

                btree = seg;

            } finally {

                lock.unlock();

            }

        }

        // MAY be null.
        return btree;
        
    }

    public AbstractBTree[] getIndexSources(final String name,
            final long timestamp) {

        if (log.isInfoEnabled())
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

                if (log.isInfoEnabled())
                    log.info("No such index: name=" + name + ", timestamp="
                            + timestamp);

                return null;

            }

            if (log.isInfoEnabled())
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

    public AbstractBTree[] getIndexSources(final String name,
            final long timestamp, final BTree btree) {
        
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

            if (log.isInfoEnabled())
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
                    // add some more information to the error message.
                    throw new NoSuchStoreException(
                            "Could not load index: name=" + name
                                    + ", timestamp=" + timestamp
                                    + ", storeUUID=" + resource.getUUID()
                                    + ", storeFile=" + resource.getFile()
                                    + ", pmd=" + pmd + " : " + ex, ex);
                    
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

                if (log.isInfoEnabled())
                    log.info("Added to view: " + resource);

                sources[i] = ndx;

            }

        }

        if (log.isInfoEnabled())
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
    public ILocalBTreeView getIndex(final String name, /*final*/ long timestamp) {
        
        if (name == null) {

            throw new IllegalArgumentException();

        }
        
        /*
         * Note: Contention is with purgeResources().
         */
        indexCacheLock.readLock().lock();
        
        try {

        if (timestamp == ITx.READ_COMMITTED) {

            /*
             * @todo experimental alternative gives a view based on the most
             * recent commit time. The only drawback about this approach is that
             * each request by the same operation will return the then most
             * recently committed view, well and the IIndex will report the
             * actual timestamp used. The upside is that the view is cached
             * since it has a normal timestamp and we need do nothing more to
             * provide a read lock for read-committed requests. In fact, if we
             * simply did this when the task began to execute then it would use
             * a consistent timestamp for all of its index views.
             */
            
            timestamp = getLiveJournal().getRootBlockView().getLastCommitTime();
            
        }
        
        final NT nt = new NT(name, timestamp);
        
        final Lock lock = namedLock.acquireLock(nt);

        try {

            if (timestamp != ITx.READ_COMMITTED) {
             
                // test the indexCache.
//                synchronized (indexCache) {

                    final ILocalBTreeView ndx = indexCache.get(nt);

                    if (ndx != null) {

                        if (log.isInfoEnabled())
                            log.info("Cache hit: " + nt);

                        return ndx;

                    }

//                }
                
            }

            // is this a read-write transactional view?
            final boolean isReadWriteTx = TimestampUtility.isReadWriteTx(timestamp);

            // lookup transaction iff transactional view.
            final ITx tx = (isReadWriteTx ? getConcurrencyManager()
                    .getTransactionManager().getTx(timestamp) : null);

            if (isReadWriteTx) {

                /*
                 * Handle fully isolated (read-write) transactional views.
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

            if (isReadWriteTx && tx == null) {

                /*
                 * Note: This will happen both if you attempt to use a
                 * transaction identified that has not been registered or if you
                 * attempt to use a transaction manager after the transaction
                 * has been either committed or aborted.
                 */

                log.warn("No such transaction: name=" + name + ", tx=" + tx);

                return null;

            }

            final boolean readOnly = TimestampUtility.isReadOnly(timestamp);
//                        || (isReadWriteTx && tx.isReadOnly());

            final ILocalBTreeView tmp;

            if (isReadWriteTx) {

                /*
                 * Isolated operation.
                 * 
                 * Note: The backing index is always a historical state of the
                 * named index.
                 * 
                 * Note: Tx#getIndex(String name) serializes concurrent requests
                 * for the same index (thread-safe).
                 */

                final ILocalBTreeView isolatedIndex = tx.getIndex(name);

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

                    if (timestamp == ITx.READ_COMMITTED) {
                        
                        /*
                         * Check to see if an index partition was split, joined
                         * or moved.
                         */
                        final StaleLocatorReason reason = getIndexPartitionGone(name);

                        if (reason != null) {

                            // Notify client of stale locator.
                            throw new StaleLocatorException(name, reason);

                        }

                    }
                    
                    final AbstractBTree[] sources = getIndexSources(name,
                            timestamp);

                    if (sources == null) {

                        if (log.isInfoEnabled())
                            log.info("No such index: name=" + name
                                    + ", timestamp=" + timestamp);

                        return null;

                    }

//                    System.err.println("getIndex(name="+name+", timestamp="+timestamp);
                    
                    assert sources.length > 0;

                    assert sources[0].isReadOnly();

                    if (sources.length == 1) {

                        tmp = (BTree) sources[0];

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

                    assert timestamp == ITx.UNISOLATED : "timestamp="
                                + timestamp;

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

                        if (log.isInfoEnabled())
                            log.info("No such index: name=" + name
                                    + ", timestamp=" + timestamp);

                        return null;

                    }

                    assert !sources[0].isReadOnly();

                    if (sources.length == 1) {

                        tmp = (BTree) sources[0];

                    } else {

                        tmp = new FusedView(sources);

                    }

                }

            }

            if (timestamp != ITx.READ_COMMITTED
                        && timestamp != ITx.UNISOLATED) {

                // update the indexCache.
                if (log.isInfoEnabled())
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
        
        } finally {
            
            indexCacheLock.readLock().unlock();
            
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
     * <p>
     * Note: {@link IndexSegment}s are registered with the {@link StoreManager}
     * by this method but are also placed into a hard reference collection (the
     * <i>retentionSet</i>) in order to prevent their being released before
     * they are put to use by incorporating them into an index partition view.
     * The caller MUST remove the {@link IndexSegment} from that hard reference
     * collection once the index has been incorporated into an index partition
     * view or is no longer required (e.g., has been MOVEd). However, the caller
     * MUST NOT remove the {@link IndexSegment} from the hard reference
     * collection until after the commit point for the task which incoporates it
     * into the index partition view. In practice, this means that those tasks
     * must be encapsulated with either a post-condition action or wrapped by a
     * caller which provides the necessary after-action in a finally{} clause.
     * 
     * @param indexPartitionName
     *            The name of the index partition (not the name of the scale-out
     *            index).
     * @param src
     *            A view of the index partition as of the <i>createTime</i>.
     *            This may be a partial view of comprised from only the first N
     *            sources in the view, in which case <i>compactingMerge := false</code>.
     * @param compactingMerge
     *            When <code>true</code> the caller asserts that <i>src</i>
     *            is a {@link FusedView} and deleted index entries WILL NOT be
     *            included in the generated {@link IndexSegment}. Otherwise, it
     *            is assumed that the only select component(s) of the index
     *            partition view are being exported onto an {@link IndexSegment}
     *            and deleted index entries will therefore be propagated to the
     *            new {@link IndexSegment}.
     * @param commitTime
     *            The commit time associated with the view from which the
     *            {@link IndexSegment} is being generated. This value is written
     *            into {@link IndexSegmentCheckpoint#commitTime}.
     * @param fromKey
     *            The lowest key that will be included (inclusive). When <code>null</code>
     *            there is no lower bound.
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
     * 
     * @see StoreManager#purgeOldResources(long, boolean)
     */
    public BuildResult buildIndexSegment(final String indexPartitionName,
            final ILocalBTreeView src, final boolean compactingMerge,
            final long commitTime, final byte[] fromKey, final byte[] toKey,
            final Event parentEvent) throws Exception {

        if (indexPartitionName == null)
            throw new IllegalArgumentException();

        if (src == null)
            throw new IllegalArgumentException();
        
        if (parentEvent == null)
            throw new IllegalArgumentException();

        final Event e;
        {
            final Map<String, Object> m = new HashMap<String, Object>();
            m.put("name", indexPartitionName);
            m.put("merge", compactingMerge);
            m.put("#sources", src.getSourceCount());

            e = parentEvent.newSubEvent(EventType.IndexSegmentBuild, m).start();
        }

        File outFile = null;
        try {
            final IndexMetadata indexMetadata;
            final SegmentMetadata segmentMetadata;

            final IndexSegmentBuilder builder;
            try {

                // metadata for that index / index partition.
                indexMetadata = src.getIndexMetadata();

                // the file to be generated.
                outFile = getIndexSegmentFile(indexMetadata);

                // new builder.
                builder = IndexSegmentBuilder.newInstance(indexPartitionName, src, outFile,
                        tmpDir, compactingMerge, commitTime, fromKey, toKey);

                // build the index segment.
                builder.call();

                /*
                 * Report on a bulk merge/build of an {@link IndexSegment}.
                 */
                {

                    final int nentries = builder.plan.nentries;

                    final long nbytes = builder.getCheckpoint().length;
                    
                    // data rate in MB/sec.
                    float mbPerSec = builder.mbPerSec;

                    // add more event details.
                    e.addDetail("filename", outFile);
                    e.addDetail("nentries", nentries);
                    e.addDetail("commitTime", commitTime);
                    e.addDetail("elapsed", +builder.elapsed);
                    e.addDetail("MB", fpf
                            .format(((double) nbytes / Bytes.megabyte32)));
                    e.addDetail("MB/s", fpf.format(mbPerSec));
            
                }

                // Describe the index segment.
                segmentMetadata = new SegmentMetadata(//
                        outFile, //
                        builder.segmentUUID, //
                        commitTime //
                );

                /*
                 * Add to the retention set so the newly built index segment
                 * will not be deleted before it is put to use.
                 */
                retentionSetAdd(segmentMetadata.getUUID());

                /*
                 * Now that the file is protected from release, notify the
                 * resource manager so that it can find this file.
                 */
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
             * Note: Now that the resource is registered with the StoreManager
             * we have to handle errors somewhat differently.
             */

            try {

                final BuildResult tmp = new BuildResult(indexPartitionName, compactingMerge,
                        src.getSources(), indexMetadata, segmentMetadata,
                        builder);

                if (log.isInfoEnabled())
                    log.info("built index segment: " + tmp);

                return tmp;

            } catch (Throwable t) {
                
                try {
                
                    // make it releasable.
                    retentionSetRemove(segmentMetadata.getUUID());
                    
                } catch (Throwable t2) {

                    log.warn(t2.getLocalizedMessage(), t2);

                }

                try {

                    // release it.
                    deleteResource(segmentMetadata.getUUID(), false/* isJournal */);

                } catch (Throwable t2) {

                    log.warn(t2.getLocalizedMessage(), t2);

                }

                if (t instanceof Exception)
                    throw (Exception) t;

                throw new RuntimeException(t);

            }

        } finally {

            e.end();

        }

    }

    /*
     * Per index counters.
     */

    /**
     * Canonical per-index partition {@link BTreeCounters}. These counters are
     * set on each {@link AbstractBTree} that is materialized by
     * {@link #getIndexOnStore(String, long, IRawStore)}. The same
     * {@link BTreeCounters} object is used for the unisolated, read-committed,
     * read-historical and isolated views of the index partition and for each
     * source in the view regardless of whether the source is a mutable
     * {@link BTree} on the live journal, a read-only {@link BTree} on a
     * historical journal, or an {@link IndexSegment}.
     * 
     * FIXME An {@link IndexSegment} can be used by more than one view of an
     * index partition. This is not a problem and no double counting,
     * misassignment of credit, or lost counters will result. However, if an
     * {@link IndexSegment} is used by different index partitions (which might
     * well be allowed in a post-split scenario but is not possible for a post-
     * move or post-join scenario, and those are the three ways in which a new
     * index partition can be created (other than by registering a new scale-out
     * index) then the {@link BTreeCounters} will only reflect all activity on
     * an {@link IndexSegment} in the index partition which last (re-)opened
     * that {@link IndexSegment}.
     * 
     * FIXME Index partitions which have been dropped should be cleared from the
     * map at overflow unless they have been re-registered since. Use
     * {@link #getIndexPartitionGone(String)} to figure out if each index
     * partition has been dropped during synchronous overflow. Then cross check
     * to verify that it does not still exist.
     * <p>
     * Slightly better would be to reset the index counters at the drop (except
     * that they will immediately disappear) or best yet to always reset the
     * index counters on add and to clear at overflow if split/moved/deleted or
     * otherwise gone.
     * <p>
     * When a scale-out index is deleted clear out the entries in
     * {@link #getIndexPartitionGone(String)} so that we do not run into trouble
     * if the index is re-registered!
     */
    final private ConcurrentHashMap<String/* name */, BTreeCounters> indexCounters = new ConcurrentHashMap<String, BTreeCounters>();

    /**
     * The aggregated performance counters for each unisolated index partition
     * view as of the time when the old journal was closed for writes. This is
     * used to compute the delta for each unisolated index partition view at the
     * end of the life cycle for the new live journal.
     */
    private Map<String/*name*/, BTreeCounters> mark = new HashMap<String, BTreeCounters>(); 

    /**
     * Return the {@link BTreeCounters} for the named index. If none exist, then
     * a new instance is atomically created and returned to the caller.
     * 
     * @param name
     *            The name of the index.
     * 
     * @return The counters for that index and never <code>null</code>.
     */
    public BTreeCounters getIndexCounters(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        // first test for existence.
        BTreeCounters t = indexCounters.get(name);

        if (t == null) {

            // not found.  create a new instance.
            t = new BTreeCounters();

            // put iff absent.
            final BTreeCounters oldval = indexCounters.putIfAbsent(name, t);

            if (oldval != null) {

                // someone else got there first so use their instance.
                t = oldval;

            } else {
                
                if (log.isInfoEnabled())
                    log.info("New counters: indexPartitionName=" + name);
                
            }
            
        }

        assert t != null;
        
        return t;
        
    }
    
    /**
     * Snapshots the index partition performance counters and returns a map
     * containing the net change in the performance counters for each index
     * partition since the last time this method was invoked (it is invoked by
     * {@link #overflow()}).
     * <p>
     * Note: This method has a side effect of setting a new mark. It SHOULD NOT
     * be used except at overflow since the "mark" is used to determine the net
     * change in the per-index partition performance counters. If used other
     * than at overflow the net change will be under-reported.
     * 
     * @return A map containing the net change in the index partition
     *         performance counters for each index partition.
     */
    synchronized protected Map<String, BTreeCounters> markAndGetDelta() {

        final Map<String/*name*/, BTreeCounters> newMark = new HashMap<String, BTreeCounters>(); 
        
        /*
         * The net change in the performance counters for each unisolated index
         * partition view over the life cycle of the old journal. This is used
         * to determine the amount of activity on each index partition during
         * the life cycle of the old journal. That is used to compute the
         * {@link Score} for each index partition. Those {@link Score}s inform
         * the choice of the index partition moves.
         */
        final Map<String/* name */, BTreeCounters> delta = new HashMap<String, BTreeCounters>();

        final Iterator<Map.Entry<String,BTreeCounters>> itr = indexCounters.entrySet().iterator(); 

        while(itr.hasNext()) {
            
            final Map.Entry<String, BTreeCounters> entry = itr.next();

            // name of the index partition.
            final String name = entry.getKey();
            
            // current counters (strictly increasing over time).
            final BTreeCounters current = entry.getValue();

            // the previous total for this index partition (if any).
            final BTreeCounters prior = this.mark.get(name);

            if (prior == null) {

                // first total for this index partition.
                delta.put(name, current);

                if (log.isInfoEnabled())
                    log.info("First time: " + name);

            } else {

                // compute the delta for this index partition.
                delta.put(name, current.subtract(prior));

                if (log.isInfoEnabled())
                    log.info("Computed delta: " + name);

            }
            
            // record the total for use in the new mark.
            newMark.put(name, current);

        }

        // replace the old mark with the new one.
        this.mark = newMark;

        // return summary of the net change in activity for each index partition.
        return delta;
        
    }

    /**
     * Return a {@link CounterSet} reflecting use of the named indices. When an
     * index partition is in use, its {@link CounterSet} is reported under a
     * path formed from name of the scale-out index and partition identifier.
     * 
     * @return A new {@link CounterSet} reflecting the use of the named indices.
     */
    public CounterSet getIndexCounters() {

        final CounterSet tmp = new CounterSet();

        final Iterator<Map.Entry<String, BTreeCounters>> itr = indexCounters
                .entrySet().iterator();

        while (itr.hasNext()) {

            final Map.Entry<String, BTreeCounters> entry = itr.next();

            final String name = entry.getKey();

            final BTreeCounters btreeCounters = entry.getValue();

            assert btreeCounters != null : "name=" + name;

//            // non-null iff this is an index partition.
//            final LocalPartitionMetadata pmd = viewCounters.pmd;

            /*
             * Note: this is a hack. We parse the index name in order to
             * recognize whether or not it is an index partition since we want
             * to know that even if the we get a StaleLocatorException from the
             * ResourceManager. This will work fine as long as the the basename
             * of the index does not use a '#' character.
             */
            final String path;
            final int indexOf = name.lastIndexOf('#');
            if (indexOf != -1) {

                path = name.substring(0, indexOf) + ICounterSet.pathSeparator
                        + name;

            } else {

                path = name;

            }

            /*
             * Note: The code below works and avoids re-opening a closed index
             * but it makes the presence of the additional counters dependent on
             * recent state in a manner that I do not like.
             */

            // IIndex view = null;
            // try {
            // if (resourceManager instanceof ResourceManager) {
            // /*
            // * Get the live index object from the cache and [null]
            // * if it is not in the cache. When the view is not in
            // * the cache we simply do not update our counters from
            // * the view.
            // *
            // * Note: Using the cache prevents a request for the
            // * counters from forcing the index to be re-loaded.
            // *
            // * Note: This is the LIVE index object. We DO NOT hold
            // * an exclusive lock. Therefore we MUST NOT use most of
            // * its API, but we are only concerned with its counters
            // * here and that is thread-safe.
            // */
            // final ResourceManager rmgr = ((ResourceManager) resourceManager);
            // view = rmgr.indexCache.get(new NT(name, ITx.UNISOLATED));
            // final StaleLocatorReason reason =
            // rmgr.getIndexPartitionGone(name);
            // if (reason != null) {
            // // Note that the index partition is gone.
            // t.addCounter("pmd" + ICounterSet.pathSeparator+"StaleLocator",
            // new OneShotInstrument<String>(reason.toString()));
            // }
            // } else {
            // /*
            // * Get the live index object from Name2Addr's cache. It
            // * will be [null] if the index is not in the cache. When
            // * the index is not in the cache we simply do not update
            // * our counters from the view.
            // */
            // final Journal jnl = ((Journal)resourceManager);
            // synchronized(jnl.name2Addr) {
            // view = jnl.name2Addr.getIndexCache(name);
            // // view = jnl.getIndex(name, ITx.READ_COMMITTED);
            // }
            // }
            // } catch (Throwable ex) {
            // log.error("Could not update counters: name=" + name + " : "
            // + ex, ex);
            // // fall through - [view] will be null.
            // }
            //
            // if (view == null) {
            //
            // /*
            // * Note: the view can be unavailable either because the
            // * index was concurrently registered and has not been
            // * committed yet or because the index has been dropped.
            // *
            // * Note: an index partition that moved, split, or joined is
            // * handled above.
            // */
            //
            // // t.addCounter("No data", new OneShotInstrument<String>(
            // // "Read committed view not available"));
            //
            // continue;
            //
            // }
            // create counter set for this index / index partition.
            final CounterSet t = tmp.makePath(path);

            /*
             * Attach the aggregated counters for the index / index partition.
             */
            t.attach(btreeCounters.getCounters());

//            if (pmd != null) {
//
//                /*
//                 * A partitioned index.
//                 */
//
//                final CounterSet pmdcs = t.makePath("pmd");
//
//                pmdcs.addCounter("leftSeparatorKey",
//                        new OneShotInstrument<String>(BytesUtil.toString(pmd
//                                .getLeftSeparatorKey())));
//
//                pmdcs.addCounter("rightSeparatorKey",
//                        new OneShotInstrument<String>(BytesUtil.toString(pmd
//                                .getRightSeparatorKey())));
//
//                pmdcs.addCounter("history", new OneShotInstrument<String>(pmd
//                        .getHistory()));
//
//                final IResourceMetadata[] resources = pmd.getResources();
//
//                for (int i = 0; i < resources.length; i++) {
//
//                    final IResourceMetadata resource = resources[i];
//
//                    final CounterSet rescs = pmdcs.makePath("resource[" + i
//                            + "]");
//
//                    rescs.addCounter("file", new OneShotInstrument<String>(
//                            resource.getFile()));
//
//                    rescs.addCounter("uuid", new OneShotInstrument<String>(
//                            resource.getUUID().toString()));
//
//                    rescs.addCounter("createTime",
//                            new OneShotInstrument<String>(Long
//                                    .toString(resource.getCreateTime())));
//
//                }
//
//            }

        }

        return tmp;

    }

}
