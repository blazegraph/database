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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.BigdataStatics;
import com.bigdata.LRUNexus.CacheSettings;
import com.bigdata.btree.IndexSegment;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.IDataRecordAccess;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;

/**
 * A canonicalizing collection of strongly held hard reference hash maps backed
 * by a single Least Recently Used (LRU) ordering over entries. This is used to
 * impose a shared LRU policy on the cache for a set of {@link IRawStore}s. The
 * LRU {@link Entry} is recycled on eviction as the MRU {@link Entry}, which
 * requires tests against the inner {@link LRUCacheImpl}'s map to be made while
 * holding the lock. This allows us to use a {@link LinkedHashMap}, which has a
 * faster iterator, but requiring the lock to test the inner cache limits
 * concurrency and has been observed to limit throughput by about 10%.
 * <p>
 * The "explicit delete required" refers to the {@link #cacheSet}, which uses
 * hard references to the per-{@link IRawStore} cache instances. Those cache
 * instances must therefore be explicitly deleted using
 * {@link #deleteCache(UUID)} rather than relying on weak references to the
 * {@link IRawStore} instances to remove entries from the {@link #cacheSet}.
 * This allows us to retain items in the cache for closed {@link IndexSegment}s
 * which stand a reasonable likelihood of being reopened in a timely manner.
 * Cache instances are deleted only when the corresponding {@link IRawStore} is
 * itself deleted.
 * 
 * @version $Id: HardReferenceGlobalLRURecyclerExplicitDeleteRequired.java 2265
 *          2009-10-26 12:51:06Z thompsonbry $
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 * @param <K>
 *            The generic type of the key.
 * @param <V>
 *            The generic type of the value.
 * 
 * @todo ... Disallow anything where <V> does not extend
 *       {@link IDataRecordAccess} since we can not measure the bytesInMemory
 *       for those objects and hence the LRU eviction policy will not account
 *       for their memory footprint?
 */
public class HardReferenceGlobalLRURecyclerExplicitDeleteRequired<K, V> implements
        IHardReferenceGlobalLRU<K, V> {

//    /**
//     * A canonicalizing mapping for per-{@link IRawStore} caches. Cache
//     * instances MAY be retained when the backing store is closed. However,
//     * cache instances will be lost if their {@link WeakReference} is cleared
//     * and this will typically happen once the {@link IRawStore} is no longer
//     * strongly referenced.
//     */
//  private final ConcurrentWeakValueCache<UUID, LRUCacheImpl<K,V>> cacheSet;
    /**
     * A canonicalizing mapping for per-{@link IRawStore} caches. Cache
     * instances are retained when the backing store is closed and even if its
     * reference is cleared. A cache instance MUST be explicitly removed from
     * the map using {@link #deleteCache(UUID)}.
     */
    private final ConcurrentHashMap<UUID, LRUCacheImpl<K,V>> cacheSet;

    /**
     * The counters for the shared LRU.
     */
    private final LRUCounters counters = new LRUCounters();
    
    /**
     * Lock used to gate access to changes in the LRU ordering. A fair policy is
     * NOT selected in the hopes that the cache will have higher throughput.
     */
    private final ReentrantLock lock = new ReentrantLock(false/* fair */);

    /**
     * The maximum bytes in memory for the LRU across all cache instances.
     */
    private final long maximumBytesInMemory;

    /**
     * The minimum bytes available in the LRU after an eviction. 
     */
    private final long minCleared;
    
    /**
     * The initial capacity for each cache instance.
     */
    private final int initialCacheCapacity;
    
    /**
     * The load factor for each cache instance.
     */
    private final float loadFactor;

    /**
     * The current LRU linked list size (the entry count) across all cache
     * instances.
     */
    private volatile int size = 0;
    
    /**
     * The entry which is first in the ordering (the
     * <em>least recently used</em>) and <code>null</code> iff the cache is
     * empty.
     */
    private volatile Entry<K, V> first = null;

    /**
     * The entry which is last in the ordering (the <em>most recently used</em>)
     * and <code>null</code> iff the cache is empty.
     */
    private volatile Entry<K, V> last = null;

    /**
     * The designated constructor used by {@link CacheSettings}.
     * 
     * @param s
     *            The {@link CacheSettings}.
     */
    public HardReferenceGlobalLRURecyclerExplicitDeleteRequired(
            final CacheSettings s) {

        this(s.maximumBytesInMemory, s.minCleared, s.minCacheSetSize,
                s.initialCacheCapacity, s.loadFactor);

    }

    /**
     * 
     * @param maximumBytesInMemory
     *            The maximum bytes in memory for the cached records across all
     *            cache instances.
     * @param minCleared
     *            The minimum number of bytes that will be cleared when evicting
     *            the LRU entry. When zero, only as many records will be evicted
     *            as are necessary to bring the bytes in memory below the
     *            configured maximum. When greater than zero, "batch" evictions
     *            can be performed. For example, several MB worth of records can
     *            be evicted each time the LRU is at its maximum capacity.
     * @param minimumCacheSetCapacity
     *            The #of per-{@link IRawStore} {@link ILRUCache} instances that
     *            will be maintained by hard references unless their cache is
     *            explicitly discarded.
     * @param initialCacheCapacity
     *            The initial capacity of each new cache instance.
     * @param loadFactor
     *            The load factor for the cache instances.
     */
    public HardReferenceGlobalLRURecyclerExplicitDeleteRequired(
            final long maximumBytesInMemory,
            final long minCleared,
            final int minimumCacheSetCapacity, final int initialCacheCapacity,
            final float loadFactor) {

        if (maximumBytesInMemory <= 0)
            throw new IllegalArgumentException();

        if (minCleared < 0)
            throw new IllegalArgumentException();

        if (minCleared > maximumBytesInMemory)
            throw new IllegalArgumentException();

        this.maximumBytesInMemory = maximumBytesInMemory;

        this.minCleared = minCleared;

        this.initialCacheCapacity = initialCacheCapacity;

        this.loadFactor = loadFactor;

//        cacheSet = new ConcurrentWeakValueCache<UUID, LRUCacheImpl<K, V>>(
//                minimumCacheSetCapacity);
        
        cacheSet = new ConcurrentHashMap<UUID, LRUCacheImpl<K, V>>(
                minimumCacheSetCapacity);

    }

    public ILRUCache<K, V> getCache(final UUID uuid, final IAddressManager am) {

        if (uuid == null)
            throw new IllegalArgumentException();

        LRUCacheImpl<K,V> cache = cacheSet.get(uuid);

        if (cache == null) {

            cache = new LRUCacheImpl<K, V>(uuid, am, this,
                    initialCacheCapacity, loadFactor);

            final LRUCacheImpl<K, V> oldVal = cacheSet.putIfAbsent(uuid,
                    cache);

            if (oldVal == null) {

//                if (BigdataStatics.debug)
//                    System.err.println("New store: " + store + " : file="
//                            + store.getFile());
                
            } else {

                // concurrent insert.
                cache = oldVal;

            }

        }

        return cache;
        
    }

    public int getRecordCount() {

        return size;

    }

    public long getEvictionCount() {

        return counters.evictionCount.get();
        
    }
    
    public long getEvictionByteCount() {

        return counters.evictionByteCount.get();
        
    }
    
    public long getBytesOnDisk() {

        return counters.bytesOnDisk.get();
        
    }
    
    public long getBytesInMemory() {

        return counters.bytesInMemory.get();
        
    }
    
    public long getMaximumBytesInMemory() {

        return maximumBytesInMemory;
        
    }

    public int getCacheSetSize() {
        
        return cacheSet.size();
        
    }
    
    public void deleteCache(final UUID uuid) {

        if (uuid == null)
            throw new IllegalArgumentException();

        // remove cache from the cacheSet.
        final LRUCacheImpl<K, V> cache = cacheSet.remove(uuid);

        if (cache != null) {

            // if cache exists, the clear it.
            cache.clear();

            if (BigdataStatics.debug)
                System.err.println("Cleared cache: " + uuid);

        } else {

            if (BigdataStatics.debug)
                System.err.println("No cache: " + uuid);

        }
        
    }

    public void discardAllCaches() {
        
        lock.lock();
        try {

//            final Iterator<WeakReference<LRUCacheImpl<K, V>>> itr = cacheSet
//            .iterator();
            final Iterator<LRUCacheImpl<K, V>> itr = cacheSet.values().iterator();

            while (itr.hasNext()) {

//                final LRUCacheImpl<K, V> cache = itr.next().get();
                final LRUCacheImpl<K, V> cache = itr.next();

                if (cache == null) {

                    // weak reference was cleared.
                    continue;

                }

                cache.clear();

            }

            assert size == 0;
            assert first == null;
            assert last == null;
            
//            size = 0;
//
//            first = last = null;
            
            counters.clear();
            
        } finally {

            lock.unlock();
            
        }
        
    }

    public CounterSet getCounterSet() {

        final CounterSet root = counters.getCounterSet();

//        final Iterator<WeakReference<LRUCacheImpl<K, V>>> itr = cacheSet
//                .iterator();

        final Iterator<LRUCacheImpl<K, V>> itr = cacheSet.values().iterator();

        while (itr.hasNext()) {

//            final LRUCacheImpl<K, V> cache = itr.next().get();

            final LRUCacheImpl<K, V> cache = itr.next();

            if (cache == null) {
             
                // weak reference was cleared.
                continue;

            }

            // add the per-cache counters.
            root.makePath(cache.storeUUID.toString()).attach(
                    cache.counters.getCounters());

        }


        return root;
        
    }
    
    /**
     * Counters for the {@link HardReferenceGlobalLRURecyclerExplicitDeleteRequired}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private class LRUCounters {

        /**
         * {@link #bytesOnDisk} is the sum of the compressed storage on the disk
         * for the buffered data records.
         */
        private final AtomicLong bytesOnDisk = new AtomicLong();

        /**
         * {@link #bytesInMemory} is the sum of the decompressed byte[] lengths.
         * In fact, the memory footprint is always larger than bytesInMemory.
         * The ratio of bytesOnDisk to bytesInMemory reflects the degree of
         * "active" compression.
         */
        private final AtomicLong bytesInMemory = new AtomicLong();

        /**
         * The #of cache entries that have been evicted.
         */
        private final AtomicLong evictionCount = new AtomicLong();
        
        /**
         * The #of bytes for cache entries that have been evicted.
         */
        private final AtomicLong evictionByteCount = new AtomicLong();
        
        public void clear() {
            
            bytesOnDisk.set(0L);
            
            bytesInMemory.set(0L);
            
            evictionCount.set(0L);

            evictionByteCount.set(0L);
                
        }
        
        public CounterSet getCounterSet() {

            final CounterSet counters = new CounterSet();

            counters.addCounter(IGlobalLRU.IGlobalLRUCounters.BYTES_ON_DISK,
                    new Instrument<Long>() {
                        @Override
                        protected void sample() {
                            setValue(bytesOnDisk.get());
                        }
                    });

            counters.addCounter(IGlobalLRU.IGlobalLRUCounters.BYTES_IN_MEMORY,
                    new Instrument<Long>() {
                        @Override
                        protected void sample() {
                            setValue(bytesInMemory.get());
                        }
                    });

            counters.addCounter(
                    IGlobalLRU.IGlobalLRUCounters.PERCENT_BYTES_IN_MEMORY,
                    new Instrument<Double>() {
                        @Override
                        protected void sample() {
                            setValue(((int) (10000 * bytesInMemory.get() / (double) HardReferenceGlobalLRURecyclerExplicitDeleteRequired.this.maximumBytesInMemory)) / 10000d);
                        }
                    });

            counters
                    .addCounter(
                            IGlobalLRU.IGlobalLRUCounters.MAXIMUM_ALLOWED_BYTES_IN_MEMORY,
                            new OneShotInstrument<Long>(
                                    HardReferenceGlobalLRURecyclerExplicitDeleteRequired.this.maximumBytesInMemory));

            counters.addCounter(
                    IGlobalLRU.IGlobalLRUCounters.BUFFERED_RECORD_COUNT,
                    new Instrument<Integer>() {
                        @Override
                        protected void sample() {
                            setValue(HardReferenceGlobalLRURecyclerExplicitDeleteRequired.this.size);
                        }
                    });

            counters.addCounter(
                    IGlobalLRU.IGlobalLRUCounters.BUFFERED_RECORD_EVICTION_COUNT,
                    new Instrument<Long>() {
                        @Override
                        protected void sample() {
                            setValue(evictionCount.get());
                        }
                    });

            counters.addCounter(
                    IGlobalLRU.IGlobalLRUCounters.BUFFERED_RECORD_EVICTION_BYTE_COUNT,
                    new Instrument<Long>() {
                        @Override
                        protected void sample() {
                            setValue(evictionByteCount.get());
                        }
                    });

            counters
                    .addCounter(
                            IGlobalLRU.IGlobalLRUCounters.AVERAGE_RECORD_SIZE_IN_MEMORY,
                            new Instrument<Integer>() {
                                @Override
                                protected void sample() {
                                    final long tmp = HardReferenceGlobalLRURecyclerExplicitDeleteRequired.this.size;
                                    if (tmp == 0) {
                                        setValue(0);
                                        return;
                                    }
                                    setValue((int) (bytesInMemory.get() / tmp));
                                }
                            });

            counters.addCounter(
                    IGlobalLRU.IGlobalLRUCounters.AVERAGE_RECORD_SIZE_ON_DISK,
                    new Instrument<Integer>() {
                        @Override
                        protected void sample() {
                            final long tmp = HardReferenceGlobalLRURecyclerExplicitDeleteRequired.this.size;
                            if (tmp == 0) {
                                setValue(0);
                                return;
                            }
                            setValue((int) (bytesOnDisk.get() / tmp));
                        }
                    });

            counters.addCounter(
                    IGlobalLRU.IGlobalLRUCounters.CACHE_COUNT,
                    new Instrument<Integer>() {
                        @Override
                        protected void sample() {
                            setValue(cacheSet.size());
                        }
                    });

            return counters;

        }
        
        public String toString() {
            
            return getCounterSet().toString();
            
        }

    }

    public String toString() {
        
        return getCounterSet().toString();

    }

    /**
     * A (key,value) pair for insertion into an {@link LRUCacheImpl} with a
     * (prior,next) reference used to maintain a double-linked list across all
     * {@link LRUCacheImpl}s for a given {@link HardReferenceGlobalLRURecyclerExplicitDeleteRequired}.
     * 
     * @version $Id$
     * @author thompsonbry
     */
    final private static class Entry<K, V> {

        private Entry<K, V> prior;

        private Entry<K, V> next;

        private K k;

        private V v;

        /** The owning cache for this entry. */
        private volatile LRUCacheImpl<K,V> cache;
        
        /** The bytes in memory for this entry. */
        private int bytesInMemory;
        
        /** The bytes on disk for this entry. */
        private int bytesOnDisk;
        
        Entry() {
        }
        
        private void set(final LRUCacheImpl<K,V> cache, final K k, final V v) {

            this.k = k;
            
            this.v = v;

            this.cache = cache;
            
            if(v instanceof IDataRecordAccess) {

                bytesInMemory = ((IDataRecordAccess)v).data().len();
                
            } else {
                
                // Can not track w/o IDataRecord.
                bytesInMemory = 0;
                
            }

            if (cache.am != null) {

                bytesOnDisk = cache.am.getByteCount((Long) k);

            } else {
                
                // Can not track w/o IAddressManager.
                bytesOnDisk = 0;
                
            }

        }

        /**
         * Human readable representation used for debugging in test cases.
         */
        public String toString() {
            return "Entry{key=" + k + ",val=" + v + ",prior="
                    + (prior == null ? "N/A" : "" + prior.k) + ",next="
                    + (next == null ? "N/A" : "" + next.k) + ",bytesInMemory="
                    + bytesInMemory + ",bytesOnDisk=" + bytesOnDisk + "}";
        }

    } // class Entry

    /**
     * Add an Entry to the tail of the linked list (the MRU position).
     */
    private void addEntry(final Entry<K, V> e) {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        if (first == null) {
            first = e;
            last = e;
        } else {
            last.next = e;
            e.prior = last;
            last = e;
        }
        size++;
        counters.bytesInMemory.addAndGet(e.bytesInMemory);
        counters.bytesOnDisk.addAndGet(e.bytesOnDisk);
    }

    /**
     * Remove an {@link Entry} from linked list that maintains the LRU ordering.
     * The {@link Entry} is modified but not deleted. The key and value fields
     * on the {@link Entry} are not modified. The {@link #first} and
     * {@link #last} fields are updated as necessary. This method is used when
     * the LRU entry is being evicted and the {@link Entry} will be recycled
     * into the MRU position in the ordering. You must also remove the entry
     * under that key from the hash map.
     * 
     * @return The value associate with the entry before it was removed from the
     *         LRU ordering.
     */
    private V removeEntry(final Entry<K, V> e) {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        if(e.cache==null) return null;
        final Entry<K, V> prior = e.prior;
        final Entry<K, V> next = e.next;
        if (e == first) {
            first = next;
        }
        if (last == e) {
            last = prior;
        }
        if (prior != null) {
            prior.next = next;
        }
        if (next != null) {
            next.prior = prior;
        }
        final V clearedValue = e.v;
        e.prior = null;
        e.next = null;
        e.cache = null; // clear reference to the cache.
        e.k = null; // clear the key.
        e.v = null; // clear the value reference.
        size--;
        counters.bytesInMemory.addAndGet(-e.bytesInMemory);
        counters.bytesOnDisk.addAndGet(-e.bytesOnDisk);
        e.bytesInMemory = e.bytesOnDisk = 0;
        return clearedValue;
    }

    /**
     * Move the entry to the end of the linked list (the MRU position).
     */
    private void touchEntry(final Entry<K, V> e) {

        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (last == e) {

            return;

        }

        // unlink entry
        // removeEntry(e);
        {
            final Entry<K, V> prior = e.prior;
            final Entry<K, V> next = e.next;
            if (e == first) {
                first = next;
            }
            if (last == e) {
                last = prior;
            }
            if (prior != null) {
                prior.next = next;
            }
            if (next != null) {
                next.prior = prior;
            }
        }

        // link entry as the new tail.
        // addEntry(e);
        {
            if (first == null) {
                first = e;
                last = e;
            } else {
                last.next = e;
                e.prior = last;
                e.next = null; // must explicitly set to null.
                last = e;
            }
        }
        
    }

    /**
     * A hard reference hash map backed by a shared Least Recently Used (LRU)
     * ordering over entries.
     * <p>
     * Note: Thread-safety is enforced using {@link HardReferenceGlobalLRURecyclerExplicitDeleteRequired#lock}
     * . Nested locking, such as using <code>synchronized</code> on the
     * instances of this class can cause deadlocks because evictions may be made
     * from any {@link LRUCacheImpl} when the LRU entry is evicted from the
     * shared LRU.
     * 
     * @version $Id$
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
     *         </a>
     * @param <K>
     *            The generic type of the key.
     * @param <V>
     *            The generic type of the value.
     */
    private static class LRUCacheImpl<K, V> implements ILRUCache<K, V> {

        /**
         * Counters for a {@link LRUCacheImpl} instance.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         * 
         * @todo Do these counters need to be volatile or {@link AtomicLong}s in
         *       order to be consistent? They are updated inside of a
         *       {@link Lock}, but that does not help with their visibility,
         *       does it?
         */
        private class LRUCacheCounters {

            /**
             * The largest #of entries in the cache to date.
             */
            private int highTide = 0;

            /** The #of inserts into the cache. */
            private long ninserts = 0;

            /** The #of cache tests (get())). */
            private long ntests = 0;

            /**
             * The #of cache hits (get() returns non-<code>null</code>).
             */
            private long nsuccess = 0;

            /**
             * Reset the counters.
             */
            public void clear() {

                highTide = 0;
                
                ninserts = ntests = nsuccess = 0;
                
            }

            /**
             * A new {@link CounterSet} instance for this cache instance.
             */
            public CounterSet getCounters() {

                final CounterSet c = new CounterSet();

                // The maximum #of entries in this per-store cache.
                c.addCounter("highTide", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(highTide);
                    }
                });

                // The size of this per-store cache.
                c.addCounter("size", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(size());
                    }
                });

                // The #of inserts into the cache (does not count touches).
                c.addCounter("ninserts", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(ninserts);
                    }
                });

                // The #of cache tests (get()).
                c.addCounter("ntests", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(ntests);
                    }
                });

                // The #of successful cache tests.
                c.addCounter("nsuccess", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(nsuccess);
                    }
                });

                // The percentage of lookups which are satisfied by the cache.
                c.addCounter("hitRatio", new Instrument<Double>() {
                    @Override
                    protected void sample() {
                        final long tmp = ntests;
                        setValue(tmp == 0 ? 0 : (double) nsuccess / tmp);
                    }
                });

                return c;

            }

            public String toString() {
                
                return getCounters().toString();
                
            }
            
        }

        /**
         * Counters for this cache instance.
         */
        private final LRUCacheCounters counters = new LRUCacheCounters();

        /**
         * The {@link UUID} of the associated {@link IRawStore}.
         */
        private final UUID storeUUID;
        
        /**
         * The {@link IRawStore} implementation class.
         */
//        private final Class<? extends IRawStore> cls;

        /**
         * An {@link IAddressManager} that can decode the record byte count from
         * the record address without causing the {@link IRawStore} reference to
         * be retained.
         */
        private final IAddressManager am;

        /**
         * The shared LRU.
         */
        private final HardReferenceGlobalLRURecyclerExplicitDeleteRequired<K, V> globalLRU;

        /**
         * The hash map from keys to entries wrapping cached object references.
         * <p>
         * Note: A {@link ConcurrentHashMap} may be used to permit concurrent
         * tests against the map without requiring us to hold the shared
         * {@link HardReferenceGlobalLRURecyclerExplicitDeleteRequired#lock} IFF the
         * {@link #putIfAbsent(Object, Object)} method is modified to NOT
         * recycle the LRU {@link Entry}. Otherwise use a {@link LinkedHashMap}
         * for faster iterator traversal. When using the {@link LinkedHashMap}
         * note that ALL access must be protected, including {@link #size}.
         */
        private final LinkedHashMap<K, Entry<K, V>> map;

        /**
         * Create an LRU cache with the specific initial capacity and load
         * factor.
         * 
         * @param storeUUID
         *            The {@link UUID} of the associated {@link IRawStore}.
         * @param am
         *            The <em>delegate</em> {@link IAddressManager} associated
         *            with the {@link IRawStore} whose records are being cached.
         *            This is used to track the bytesOnDisk buffered by the
         *            cache using {@link IAddressManager#getByteCount(long)}. DO
         *            NOT provide a reference to an {@link IRawStore} here as
         *            that will cause the {@link IRawStore} to be retained by a
         *            hard reference!
         * @param initialCapacity
         *            The capacity of the cache (must be positive).
         * @param loadFactor
         *            The load factor for the internal hash table.
         */
        public LRUCacheImpl(final UUID storeUUID,
                final IAddressManager am,
                final HardReferenceGlobalLRURecyclerExplicitDeleteRequired<K, V> lru,
                final int initialCapacity, final float loadFactor) {

            if (storeUUID == null)
                throw new IllegalArgumentException();
            
            // [am] MAY be null.
            
            /*
             * This would cause the IRawStore to be retained by a hard
             * reference!
             */
            assert !(am instanceof IRawStore) : am.getClass().getName()
                    + " implements " + IRawStore.class.getName();

            if (lru == null)
                throw new IllegalArgumentException();

            this.storeUUID = storeUUID;

            this.am = am;
            
            this.globalLRU = lru;

//            this.map = new ConcurrentHashMap<K, Entry<K, V>>(initialCapacity,
//                    loadFactor);
            this.map = new LinkedHashMap<K, Entry<K, V>>(initialCapacity,
                    loadFactor);

        }

        public IAddressManager getAddressManager() {

            return am;
            
        }

        public UUID getStoreUUID() {
            
            return storeUUID;
            
        }

        /**
         * Discards each entry in this cache and resets the statistics for this
         * cache, but does not remove the {@link LRUCacheImpl} from the 
         * {@link HardReferenceGlobalLRURecyclerExplicitDeleteRequired#cacheSet}.
         */
        public void clear() {

            globalLRU.lock.lock();
            
            try {
            
                final Iterator<Entry<K, V>> itr = map
                        .values().iterator();
                
                while (itr.hasNext()) {

                    final Entry<K, V> e = itr.next();

                    // remove entry from the map.
                    itr.remove();

                    // unlink entry from the LRU.
                    globalLRU.removeEntry(e);

                }

                counters.clear();
                
            } finally {
                
                globalLRU.lock.unlock();
                
            }
            
        }

        /**
         * The #of entries in the cache.
         */
        public int size() {

            globalLRU.lock.lock();
            
            try {
                
                return map.size();
                
            } finally {
                
                globalLRU.lock.unlock();
                
            }

        }

        /**
         * {@inheritDoc}
         * 
         * Note: When the LRU is full and the given key is not in the cache,
         * this implementation will <em>recycle</em> the evicted LRU
         * {@link Entry} for the incoming MRU {@link Entry}. This means that the
         * (key,value) pair stored in an {@link Entry} can change at any time
         * unless you are holding the shared {@link ReentrantLock}.
         */
        public V putIfAbsent(final K k, final V v) {

            if (k == null)
                throw new IllegalArgumentException();

            if (v == null)
                throw new IllegalArgumentException();

            globalLRU.lock.lock();
            
            try {

                Entry<K, V> entry = map.get(k);

                if (entry != null) {

                    /*
                     * There is an existing entry under the key.
                     */

                    // Update entry ordering.
                    globalLRU.touchEntry(entry);

                    // Return the old value.
                    return entry.v;

                }

                /*
                 * There is no entry under that key.
                 */
                
                if (globalLRU.counters.bytesInMemory.get() >= globalLRU.maximumBytesInMemory) {

                    /*
                     * The global LRU is over capacity.
                     * 
                     * Purge the the LRU position until the cache is just under
                     * capacity.
                     * 
                     * Note: MUST use the same test (GTE) or [entry] can be null
                     * afterwards!
                     */

                    // evict records until the #of bytes in memory falls below
                    // this threshold.
                    final long threshold = globalLRU.maximumBytesInMemory
                            - globalLRU.minCleared;

                    assert threshold >= 0;

                    while (globalLRU.counters.bytesInMemory.get() >= threshold) {

                        // entry in the LRU position.
                        entry = globalLRU.first;
                        
                        // the key associated with the entry to be evicted.
                        final K evictedKey = entry.k;

                        // The cache from which the entry will be evicted.
                        final LRUCacheImpl<K,V> evictedFromCache = entry.cache; 
                    
                        final int bytesOnDisk = entry.bytesOnDisk;

                        // remove LRU entry from ordering.
                        globalLRU.removeEntry(entry);

                        // remove entry under that key from hash map for that
                        // store.
                        evictedFromCache.remove(evictedKey);//entry.k);

                        globalLRU.counters.evictionCount.incrementAndGet();

                        globalLRU.counters.evictionByteCount.addAndGet(bytesOnDisk);

                    }

                    /*
                     * Recycle the last cache entry that we purged.
                     */

                    assert entry != null;
                    
                    // set key and object on LRU entry.
                    entry.set(this, k, v);

                    // add entry into the hash map.
                    map.put(k, entry);

                    // add entry into MRU position in ordering.
                    globalLRU.addEntry(entry);

                    counters.ninserts++;

                    // return [null] since there was no entry under the key.
                    return null;

                }

                /*
                 * The map was not over capacity.
                 * 
                 * Create a new entry and link into the MRU position.
                 */

                entry = new Entry<K, V>();

                entry.set(this, k, v);

                map.put(k, entry);

                globalLRU.addEntry(entry);

                final int count = map.size();

                if (count > counters.highTide) {

                    counters.highTide = count;

                }

                counters.ninserts++;

                // return [null] since there was no entry under the key.
                return null;

            } finally {

                globalLRU.lock.unlock();

            }

        }

        public V get(final K key) {

            if (key == null)
                throw new IllegalArgumentException();

//            final Entry<K, V> entry = map.get(key);
//
//            if (entry == null) {
//
//                return null;
//
//            }

            globalLRU.lock.lock();

            try {

                /*
                 * Note: This test needs to be done while holding the global
                 * lock since the LRU can reuse the LRU Entry instance when it
                 * is evicted as the MRU Entry object. If you want to do this
                 * test outside of the lock, then the code needs to be modified
                 * to allocate a new Entry object on insert. If the test is done
                 * outside of the lock, then you can use a ConcurrentHashMap for
                 * the map to avoid concurrent modification issues. Otherwise,
                 * use a LinkedHashMap for a faster iterator.
                 */
 
                final Entry<K, V> entry = map.get(key);

                counters.ntests++;

                if (entry == null) {

                    return null;

                }

                globalLRU.touchEntry(entry);

                counters.nsuccess++;

                return entry.v;
                
            } finally {
                
                globalLRU.lock.unlock();
                
            }

        }

        public V remove(final K key) {

            if (key == null)
                throw new IllegalArgumentException();

            globalLRU.lock.lock();

            try {

                /*
                 * Note: must be invoked while holding the lock since the map is
                 * not thread safe and since the LRU Entry can be recycled at
                 * any time if we are not holding the lock.
                 */
                
                final Entry<K, V> entry = map.remove(key);

                if (entry == null)
                    return null;

                return globalLRU.removeEntry(entry);
                
            } finally {
                
                globalLRU.lock.unlock();
                
            }

        }

        public String toString() {
            
            return super.toString() + "{" + counters.toString() + "}";
            
        }
        
    }

}
