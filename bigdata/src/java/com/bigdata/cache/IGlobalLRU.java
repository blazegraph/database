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
 * Created on Sep 13, 2009
 */

package com.bigdata.cache;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.LRUNexus;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.data.INodeData;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.io.IDataRecord;
import com.bigdata.io.IDataRecordAccess;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.IWORM;

/**
 * Factory for per-{@link IRawStore} caches backed by a global LRU allowing
 * competition for buffer space across all requests for decompressed records,
 * including B+Tree {@link INodeData} and {@link ILeafData} objects.
 * <p>
 * The vast majority of all data in the stores are B+Tree {@link INodeData} and
 * {@link ILeafData} records. This factory provides per-{@link IRawStore} cache
 * instances which can dramatically reduce read IO for B+Trees, and thus for the
 * entire database. This also serves as the "leafCache" for the
 * {@link IndexSegment}, which uses linked-leaf navigation for
 * {@link ITupleIterator} and {@link ITupleCursor}s and does not otherwise
 * buffer leaves when performing key range scans (versus top-down navigation).
 * <p>
 * There is one cache per store. All cache instances are backed by a
 * <em>shared</em> Least Recently Used (LRU) policy. Entries will be evicted
 * from the appropriate cache as they fall of the end of the LRU. A global LRU
 * ensures that all cache instances compete for the same memory resources.
 * <p>
 * Each per-store cache provides a canonicalizing mapping from the {@link Long}
 * address of a record to a {@link WeakReference} value. The referent of the
 * {@link WeakReference} may be an {@link IDataRecord}, {@link INodeData},
 * {@link ILeafData}, or other object whose persistent state was coded by (or
 * serialized by) the record having that address.
 * <p>
 * Note: The individual cache instances for each {@link IRawStore} reclaim JVM
 * heap space as the {@link WeakReference} values are cleared. The set of such
 * instances is also a {@link ConcurrentWeakValueCache} with a backing LRU so we
 * can hold onto the canonicalizing mappings for closed stores which might be
 * reopened.
 * <p>
 * Note: The global LRU may have impact on GC of the old generation since there
 * will be a tendency of the backing byte[]s to be held long enough to become
 * tenured. Newer incremental garbage collections are designed to address this
 * problem.
 * <p>
 * Note: While caching compressed records would have a smaller memory footprint,
 * this class provides for caching decompressed records since high-level data
 * structures (such as the B+Trees) do not retain references to the compressed
 * data records. Therefore a compressed data record cache based on weak
 * reference semantics for the compressed {@link ByteBuffer}s would be of little
 * utility.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo convert <K> to <code>long</code>? By getting rid of this generic type
 *       as can allow native long hash map implementations.
 */
public interface IGlobalLRU<K,V> {

    /**
     * Interface for cache of read-only records from a specific
     * {@link IRawStore} backed by a shared LRU.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <K>
     *            The key (the address in an {@link IRawStore})
     * @param <V>
     *            The value (typically an {@link IDataRecordAccess}
     *            implementation).
     */
    public interface ILRUCache<K, V> {

        /**
         * The {@link UUID} of the associated store.
         */
        public UUID getStoreUUID();

        /**
         * An {@link IAddressManager} which can be used to decode the byte count
         * of a record on the disk given the <code>long</code> address of that
         * record. This MUST NOT cause the corresponding {@link IRawStore}
         * reference to be maintained as that would prevent many
         * {@link IRawStore} from being closed by their finalizer.
         */
        public IAddressManager getAddressManager();

        /**
         * Insert or "touch" this object in the cache. If an object already
         * exists for the given key, then the access order of the entry is
         * updated and the existing object is returned.
         * <p>
         * Note: For a {@link IWORM} store, the address is always new so there
         * will not be an entry in the cache for that address.
         * <p>
         * Note: For a IRW store, the addresses can be reused and the delete of
         * the old address MUST have cleared the entry for that address from the
         * store's cache.
         * 
         * @param k
         *            The key.
         * @param v
         *            The value.
         * @return The existing object in the cache -or- <code>null</code> iff
         *         this given object was inserted into the cache.
         * 
         * @exception IllegalStateException
         *                If a different object is in the cache under the
         *                specified object identifier.
         */
        public V putIfAbsent(K k, V v);

        /**
         * Return the object under the key from the cache or <code>null</code>
         * if there is no entry in the cache for that key.
         * 
         * @param k
         *            The key.
         * 
         * @return The object or <code>null</code> iff it is not in cache.
         */
        public V get(K k);

        /**
         * Remove the object under the key from the cache.
         * 
         * @param k
         *            The key.
         * 
         * @return The object in the cache for that key or <code>null</code> if
         *         there was no entry for that key.
         */
        public V remove(K k);

        /**
         * Removes all entries from the cache and the backing LRU.
         */
        public void clear();

        /**
         * Return the #of entries in the cache.
         */
        public int size();

    }

    /**
     * An canonicalizing factory for cache instances supporting random access to
     * decompressed {@link IDataRecord}s, higher-level data structures wrapping
     * those decompressed data records ({@link INodeData} and {@link ILeafData}
     * ), or objects deserialized from those {@link IDataRecord}s.
     * 
     * @param storeUUID
     *            The store's {@link UUID}.
     * @param am
     *            The {@link IAddressManager} for that store (optional, but used
     *            to compute the bytes on disk for record and hence highly
     *            desired).
     * 
     * @return The cache for records in the store.
     * 
     * @see AbstractBTree#readNodeOrLeaf(long)
     * @see IndexSegmentStore#reopen()
     * @see LRUNexus#getCache(IRawStore)
     */
    public ILRUCache<K, V> getCache(final UUID storeUUID, IAddressManager am);

    /**
     * Remove the cache for the {@link IRawStore} from the set of caches
     * maintained by this class and clear any entries in that cache. This method
     * SHOULD be used when the persistent resources for the store are deleted.
     * It SHOULD NOT be used if a store is simply closed in a context when the
     * store COULD be re-opened. The method DOES NOT guarantee consistency if
     * there are concurrent requests against that cache instance.
     * 
     * @param storeUUID
     *            The store's {@link UUID}.
     * 
     * @see IRawStore#destroy()
     * @see IRawStore#deleteResources()
     */
    public void deleteCache(final UUID storeUUID);

    /**
     * Clear all per-{@link IRawStore} cache instances. This may be used if all
     * bigdata instances in the JVM are closed, but SHOULD NOT be invoked if you
     * are just closing some {@link IRawStore}. The method DOES NOT guarantee
     * consistency if there are concurrent requests against the
     * {@link IGlobalLRU}.
     */
    public void discardAllCaches();

    /** The counters for the global LRU. */
    public CounterSet getCounterSet();

    /*
     * Various performance counters.
     */
    
    /**
     * The #of records in memory across all cache instances.
     */
    public int getRecordCount();

    /**
     * The #of records which have been evicted from memory to date across all
     * cache instances.
     */
    public long getEvictionCount();

    /**
     * The #of bytes in memory across all cache instances.
     */
    public long getBytesInMemory();

    /**
     * The #of bytes on the disk for the records in memory across all cache
     * instances.
     */
    public long getBytesOnDisk();

    /**
     * The configured value for the maximum #of bytes in memory across all cache
     * instances.
     */
    public long getMaximumBytesInMemory();

    /**
     * Return the #of cache instances.
     */
    public int getCacheSetSize();
    
    /**
     * Interface defines some standard counters for the global LRU.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IGlobalLRUCounters extends ICounterSet {

        /**
         * The concurrency level of the cache.
         */
        final String CONCURRENCY_LEVEL = "Concurrency Level";

        /**
         * The #of bytes on disk for the records that are buffered by the cache.
         */
        final String BYTES_ON_DISK = "Bytes On Disk";

        /**
         * The decompressed byte length for the records that are buffered by the
         * cache.
         */
        final String BYTES_IN_MEMORY = "Bytes In Memory";

        /**
         * The maximum bytes in memory that may be used by the cache. Note that
         * the constraint is strictly in terms of the value reported by
         * {@link #BYTES_IN_MEMORY} and DOES NOT reflect any additional overhead
         * required to represent the objects associated with the buffered data
         * records.
         */
        final String MAXIMUM_ALLOWED_BYTES_IN_MEMORY = "Bytes In Memory Maximum Allowed";

        /**
         * The percentage of the maximum allowed bytes in memory that is used by
         * the cache.
         */
        final String PERCENT_BYTES_IN_MEMORY = "Bytes In Memory Percent Used";

        /**
         * The #of records buffered by the cache (current value).
         */
        final String BUFFERED_RECORD_COUNT = "Buffered Record Count";

        /**
         * The #of records that have been evicted from the cache to date.
         */
        final String BUFFERED_RECORD_EVICTION_COUNT = "Buffered Record Eviction Count";

        /**
         * The #of bytes for records that have been evicted from the cache to date.
         */
        final String BUFFERED_RECORD_EVICTION_BYTE_COUNT = "Buffered Record Eviction Byte Count";

        /**
         * The average size on disk for the records buffered in the cache
         * (current value).
         */
        final String AVERAGE_RECORD_SIZE_ON_DISK = "Average Record Size On Disk";

        /**
         * The average decompressed byte length for the records buffered in the
         * cache (current value).
         */
        final String AVERAGE_RECORD_SIZE_IN_MEMORY = "Average Record Size In Memory";

        /**
         * The #of per-{@link IRawStore} cache instances (current value). Some
         * implementations MAY supply additional counters for each active cache
         * instance.
         */
        final String CACHE_COUNT = "Cache Count";
        
    }

}
