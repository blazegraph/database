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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.BigdataStatics;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.IDataRecordAccess;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;

/**
 * Global LRU implementation based on a single map whose keys include both the
 * {@link IRawStore#getUUID()} and the <code>long addr</code> of the record
 * within the store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Implement a variant of the {@link HardReferenceGlobalLRU} and this
 *       class which uses a single {@link ConcurrentHashMap} but also maintains
 *       the linked-list LRU ordering.
 */
public class StoreAndAddressLRUCache<V> implements IHardReferenceGlobalLRU<Long,V> {

    /**
     * Keys of the map combine the store's {@link UUID} and the within store
     * <code>long addr</code>. The hash code is precomputed based on bits
     * selected from the hash of the {@link UUID} and the hash of the
     * <code>long addr</code>.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static private class K {
        
        /**
         * Mask to capture bits from the {@link UUID} hash code (the mask
         * selects every other bit).
         */
        static final int mask1 = 0x10101010;

        /**
         * Mask used to capture bits from the <code>long addr</code>'s hash code
         * (this is the negation of the other mask, so it selects every other
         * bit, but with a one bit offset).
         */
        static final int mask2 = ~mask1;

        final UUID storeUUID;

        final long addr;

        final int hash;

        int bytesInMemory, bytesOnDisk;

        /**
         * Ctor used for map tests (get()).
         * @param storeUUID
         * @param addr
         */
        public K(final UUID storeUUID, final long addr) {
            this.storeUUID = storeUUID;
            this.addr = addr;
            final int addrHash = ((int) (addr ^ (addr >>> 32)));
            this.hash = (storeUUID.hashCode() & mask1) | (addrHash & mask2);
        }

        /**
         * Ctor used for map modifications (putIfAbsent(), remove()).
         * @param storeUUID
         * @param addr
         * @param bytesOnDisk
         * @param bytesInMemory
         */
        public K(final UUID storeUUID, final long addr, final int bytesOnDisk,
                final int bytesInMemory) {
            this(storeUUID,addr);
            this.bytesOnDisk = bytesOnDisk;
            this.bytesInMemory = bytesInMemory;
        }

        /**
         * Pre-computed hash code.
         */
        public int hashCode() {
            
            return hash;
            
        }
        
        /**
         * Equality test required to disambiguate keys on hash collision.
         */
        public boolean equals(final Object o) {

            if (this == o)
                return true;

            if (!(o instanceof K))
                return false;

            final K o1 = (K) o;

            return addr == o1.addr && storeUUID.equals(o1.storeUUID);

        }
        
    }
    
    /**
     * The maximum bytesInMemory before the LRU record will be evicted.
     */
    private final long maximumBytesInMemory;
        
    /**
     * A canonicalizing mapping for per-{@link IRawStore} caches. Cache
     * instances MAY be retained when the backing store is closed. However,
     * cache instances will be lost if their {@link WeakReference} is cleared
     * and this will typically happen once the {@link IRawStore} is no longer
     * strongly referenced.
     */
    private final ConcurrentWeakValueCache<UUID, InnerCacheImpl> cacheSet;

    /**
     * The map containing all entries for all stores.
     * <p>
     * Note: In order to make iterators or a sequence of operations consistent,
     * the caller MUST synchronize on this {@link #map}.
     * 
     * @see Collections#synchronizedCollection(java.util.Collection)
     */
    private final Map<K,V> map;

    /** The counters. */
    private final LRUCounters counters = new LRUCounters();

    /**
     * 
     * @param maximumBytesInMemory
     *            The maximum bytes in memory for the cached records across all
     *            cache instances.
     * @param minimumCacheSetCapacity
     *            The #of per-{@link IRawStore} {@link ILRUCache} instances that
     *            will be maintained by hard references unless their cache is
     *            explicitly discarded.
     * @param initialCacheCapacity
     *            The initial capacity of each new cache instance (in fact, a
     *            single map is shared by all cache instances - the cache
     *            instances are just logical views onto the shared map).
     * @param loadFactor
     *            The load factor for the cache instances.
     */
    public StoreAndAddressLRUCache(final long maximumBytesInMemory,
            final int minimumCacheSetCapacity, final int initialCacheCapacity,
            final float loadFactor) {

        if (maximumBytesInMemory <= 0)
            throw new IllegalArgumentException();

        this.maximumBytesInMemory = maximumBytesInMemory;
        
        cacheSet = new ConcurrentWeakValueCache<UUID, InnerCacheImpl>(
                minimumCacheSetCapacity);

        map = Collections.synchronizedMap(new LinkedHashMap<K, V>(
                initialCacheCapacity, loadFactor, true/* accessOrder */) {

            private static final long serialVersionUID = 1L;

            protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {

                if (counters.bytesInMemory.get() >= StoreAndAddressLRUCache.this.maximumBytesInMemory) {

                    // Subtract out bytesOnDisk and bytesInMemory
                    counters.bytesInMemory.addAndGet(eldest.getKey().bytesInMemory);
                    counters.bytesOnDisk.addAndGet(eldest.getKey().bytesOnDisk);
                    counters.evictionCount.incrementAndGet();
                 
                    // implementation will remove the entry.
                    return true;
                    
                }
                
                return false;

            }

        });

    }
    
    public int getRecordCount() {

        // FIXME Is this constant time or near constant time?
        return map.size();

    }

    public long getEvictionCount() {

        return counters.evictionCount.get();
        
    }

    public long getEvictionByteCount() {

        // FIXME getEvictionByteCount
        return 0L;
//        return counters.evictionByteCount.get();
        
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
        
    /**
     * Return the #of entries in the backing LRU across all {@link IRawStore}s.
     */
    public int size() {

        return map.size();
        
    }

    public ILRUCache<Long, V> getCache(final UUID uuid, final IAddressManager am) {

        if (uuid == null)
            throw new IllegalArgumentException();

        InnerCacheImpl cache = cacheSet.get(uuid);

        if (cache == null) {

            cache = new InnerCacheImpl(uuid, am);

            final InnerCacheImpl oldVal = cacheSet.putIfAbsent(uuid, cache);

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

    public void deleteCache(final UUID uuid) {
        
        if (uuid == null)
            throw new IllegalArgumentException();

        // remove cache from the cacheSet.
        final InnerCacheImpl cache = cacheSet.remove(uuid);

        if (cache != null) {

            // if cache exists, the clear it.
            cache.clear();

        }

    }

    /**
     * Removes all entries from the cache and the backing LRU for all
     * {@link IRawStore}s.
     */
    public void discardAllCaches() {
        
        synchronized (map) {
        
            /*
             * @todo if we track per InnerCacheImpl counters then we must also
             * reset those counters here.
             */
            
            cacheSet.clear();
            
            map.clear();
            
            counters.clear();
    
        }
    }

    public CounterSet getCounterSet() {
     
        return counters.getCounterSet();
        
    }
    
    /**
     * Counters for the {@link HardReferenceGlobalLRU}.
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
        
        public void clear() {
            
            bytesOnDisk.set(0L);
            
            bytesInMemory.set(0L);
            
            evictionCount.set(0L);
                
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
                            setValue(((int) (10000 * bytesInMemory.get() / (double) StoreAndAddressLRUCache.this.maximumBytesInMemory)) / 10000d);
                        }
                    });

            counters
                    .addCounter(
                            IGlobalLRU.IGlobalLRUCounters.MAXIMUM_ALLOWED_BYTES_IN_MEMORY,
                            new OneShotInstrument<Long>(
                                    StoreAndAddressLRUCache.this.maximumBytesInMemory));

            counters.addCounter(
                    IGlobalLRU.IGlobalLRUCounters.BUFFERED_RECORD_COUNT,
                    new Instrument<Integer>() {
                        @Override
                        protected void sample() {
                            setValue(StoreAndAddressLRUCache.this.size());
                        }
                    });

            counters.addCounter(
                    IGlobalLRU.IGlobalLRUCounters.BUFFERED_RECORD_COUNT,
                    new Instrument<Long>() {
                        @Override
                        protected void sample() {
                            setValue(evictionCount.get());
                        }
                    });

            counters
                    .addCounter(
                            IGlobalLRU.IGlobalLRUCounters.AVERAGE_RECORD_SIZE_IN_MEMORY,
                            new Instrument<Integer>() {
                                @Override
                                protected void sample() {
                                    final long tmp = StoreAndAddressLRUCache.this
                                            .size();
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
                            final long tmp = StoreAndAddressLRUCache.this
                                    .size();
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
        
        final String t = getCounterSet().toString();
        
        if(!BigdataStatics.debug) {
            
            return t;
            
        }
        
        final StringBuilder sb = new StringBuilder();

        sb.append(t);

        final Iterator<WeakReference<InnerCacheImpl>> itr = cacheSet
                .iterator();

        while (itr.hasNext()) {

            final InnerCacheImpl cache = itr.next().get();

            if (cache == null) {
                // weak reference was cleared.
                continue;
            }

            sb.append("\ncache: storeClass=" + cache.getStoreUUID() + ", size="
                    + cache.size());

        }

        return sb.toString();

    }

    /**
     * A flyweight skin for a specific {@link IRawStore}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private class InnerCacheImpl implements ILRUCache<Long,V> {
        
        private final UUID storeUUID;

        private final IAddressManager am;

        public IAddressManager getAddressManager() {
            return am;
        }

        public UUID getStoreUUID() {
            return storeUUID;
        }

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
         */
        public InnerCacheImpl(final UUID storeUUID,
                final IAddressManager am
                ) {

            if (storeUUID == null)
                throw new IllegalArgumentException();
            
            // [am] MAY be null.
            
            /*
             * This would cause the IRawStore to be retained by a hard
             * reference!
             */
            assert !(am instanceof IRawStore) : am.getClass().getName()
                    + " implements " + IRawStore.class.getName();

            this.storeUUID = storeUUID;
            
            this.am = am;
            
        }

        /**
         * Note: this performance a full LRU scan, removing any entry for the
         * store UUID.
         */
        public void clear() {
            synchronized (map) {
                final Iterator<Map.Entry<K, V>> itr = map.entrySet().iterator();
                while (itr.hasNext()) {
                    final Map.Entry<K, V> e = itr.next();
                    if (e.getKey().storeUUID.equals(this.storeUUID)) {
                        /*
                         * Note: itr.remove() invokes map.remove(k) which
                         * updates bytesOnDisk/bytesInMemory.
                         */
                        itr.remove();
                    }
                }
            }
        }

        public V get(final Long k) {
            return map.get(new K(storeUUID, k));
        }

        public V putIfAbsent(final Long k, final V v) {
            synchronized (map) {
                final K k1 = new K(storeUUID,k,bytesOnDisk(k),bytesInMemory(v));
                final V old = map.get(k);
                if (old != null)
                    return old;
                // update bytesOnDisk/bytesInMemory
                counters.bytesOnDisk.addAndGet(k1.bytesOnDisk);
                counters.bytesInMemory.addAndGet(k1.bytesInMemory);
                return map.put(k1, v);
            }
        }

        public V remove(final Long k) {
            // synchronize across counter updates?
            synchronized (map) {
                final V v = map.remove(new K(storeUUID, k));
                if(v != null) {
                    // update bytesOnDisk/bytesInMemory
                    counters.bytesOnDisk.addAndGet(bytesOnDisk(k));
                    counters.bytesInMemory.addAndGet(bytesInMemory(v));
                }
                return v;
            }
        }

        private int bytesOnDisk(final long addr) {
            
            if (am != null) {

                return am.getByteCount(addr);

            }
                
            // Can not track w/o IAddressManager.
            return 0;
                
        }
        
        private int bytesInMemory(final V v) {

            if(v instanceof IDataRecordAccess) {

                return ((IDataRecordAccess)v).data().len();
                
            }
            
            // Can not track w/o IDataRecord.
            return 0;

        }

        /**
         * @todo Returns the <em>total</em> size of the LRU (does not conform to
         *       the API).
         *       <p>
         *       Note: The per store size is NOT available unless we use a
         *       canonicalizing mapping for the {@link InnerCacheImpl} and track
         *       the size ourselves. If we do this, then we wind up having to
         *       resolve the appropriate view inside of
         *       <code>removeEldestEntry()</code>.
         */
        public int size() {
            return map.size();
        }
        
    }

}
