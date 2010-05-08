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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.BigdataStatics;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.IDataRecordAccess;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;

/**
 * A mostly non-blocking cache based on a {@link ConcurrentHashMap} and batched
 * updates to its access policy. This approach encapsulates:
 * <ul>
 * <li>
 * (a) an unmodified ConcurrentHashMap (CHM); combined with</li>
 * <li>
 * (b) non-thread-safe thread-local buffers (TLB) for touches, managed by an
 * inner CHM<ThreadId,TLB> instance. The reason for this inner map is to allow
 * the TLB instances to be discarded by clear(); The TLBs get batched onto;</li>
 * <li>
 * (c) a shared non-thread safe access policy (LIRS, LRU) built on double-linked
 * nodes (DLN) stored in the inner CHM. Updates are deferred until holding the
 * lock (d). The DLN reference to the cached value is final. The (prior, next,
 * delete) fields are only read or written while holding the lock (d). Other
 * fields could be defined by subclassing a newDLN() method to support LIRS,
 * etc. The access policy will need [head, tail] or similar fields, which would
 * also be guarded by the lock (d);</li>
 * <li>
 * (d) a single lock guarding mutation on the access policy. Since there is only
 * one lock, there can be no lock ordering problems. Both batching touches onto
 * (c) and eviction (per the access policy) require access to the lock, but that
 * is the only lock. If the access policy batches evictions, then lock requests
 * will be rare and the whole cache will be non-blocking, wait free, and not
 * spinning on CAS locks 99% of the time; and</li>
 * <li>
 * (e) explicit management of the threads used to access the cache. e.g., by
 * queuing accepted requests and servicing them out of a thread pool, which has
 * the benefit of managing the workload imposed by the clients.</li>
 * </ul>
 * <p>
 * This should have the best possible performance and the simplest
 * implementation. (b) The TLB could be a DLN[] or other simple data structure.
 * The access policy (c) is composed from linking DLN instances together while
 * holding the lock.
 * <ul>
 * <li>
 * A get() on the outer class looks up the DLN on the inner CHM and places it
 * into the TLB (if found).</li>
 * <li>
 * A put() or putIfAbsent() on the outer class creates a new DLN and either
 * unconditionally or conditionally puts it into the inner CHM. The new DLN is
 * added to the TLB IFF it was added to the inner CHM. The access order is NOT
 * updated at this time.</li>
 * <li>
 * A remove() on the outer class acquires the lock (d), looks up the DLN in the
 * cache, and synchronously unlinks the DLN if found and sets its [deleted]
 * flag. I would recommend that the clients do not call remove() directly, or
 * that an outer remove() method exists which only removes the DLN from the
 * inner CHM and queues up remove requests to be processed the next time any
 * thread batches its touches through the lock. The inner remove() method would
 * synchronously update the DLNs.</li>
 * <li>
 * A clear() clear the ConcurrentHashMap<Key,DLN<Val>> map. It would also clear
 * the inner ConcurrentHashMap<ThreadId,TLB> map, which would cause the existing
 * TLB instances to be discarded. It would have to obtain the lock in order to
 * clear the [head,tail] or related fields for the access policy.</li>
 * </ul>
 * When batching touches through the lock, only the access order is updated by
 * the appropriate updates of the DLN nodes. If the [deleted] flag is set, then
 * the DLN has been removed from the cache and its access order is NOT updated.
 * If the cache is over its defined maximums, then evictions are batched while
 * holding the lock. Evictions are only processed when batching touches through
 * the lock.
 * <p>
 * Note: This implementation was derived from
 * {@link HardReferenceGlobalLRURecyclerExplicitDeleteRequired}. However, this
 * implementation DOES NOT permit recycling of the {@link DLN}s in order to have
 * a better guarantee of thread-safety.
 * 
 * @todo Support LIRS as well as LRU.
 * 
 * @todo A main issue with this approach is managing the pool of threads used to
 *       access the TLBs in a manner which does not leak TLBs. If we do a hand
 *       off of the request from the caller's thread to an inner thread pool
 *       then that hand off can become a bottleneck. We could use a striped
 *       lock, but that is the same issue all over again. Maybe fork/join has
 *       sufficient concurrency for handing off requests? Another approach is to
 *       make the inner CHM<hash(ThreadId),TLB> and to impose locking for the
 *       TLB. That will stripe access to the TLB instances. Or if we bound the
 *       #of threads in join processing, then we may not have a problem.
 *       <p>
 *       This could be handled by a striped lock granting access to a specific
 *       {@link TLB} instance out of a pool of {@link TLB}s (which would be a
 *       {@link ConcurrentHashMap}, naturally). The caller would acquire the
 *       {@link TLB} and release the {@link TLB} inside of get(), putIfAbsent(),
 *       and remove(). Since there are many such TLBs, there is less contention
 *       for each one. Since they are guarded by a lock, each TLB is still
 *       thread-safe without further synchronization.
 *       <p>
 *       There still needs to be a global read/write lock in order to execute
 *       operations against all the {@link TLB}s without deadlock (such as
 *       {@link #discardAllCaches()}). Without that global lock, any TLB which
 *       was already held could attempt to batch updates through to the shared
 *       access policy which could then seek to obtain the outer lock. If normal
 *       operations take the read() lock and global update operations take the
 *       write lock when we will avoid deadlocks based on such lock ordering
 *       problems.
 * 
 * @todo Support tx isolation. This will involve a per-tx cache I believe, so
 *       maybe the tx gets a {@link UUID}? We can then delete that cache if the
 *       tx aborts().
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BCHMGlobalLRU2<K,V> implements IHardReferenceGlobalLRU<K,V> {

    /**
     * A canonicalizing mapping for per-{@link IRawStore} caches. Cache
     * instances are retained when the backing store is closed and even if its
     * reference is cleared. A cache instance MUST be explicitly removed from
     * the map using {@link #deleteCache(UUID)}.
     */
    private final ConcurrentHashMap<UUID, LRUCacheImpl<K, V>> cacheSet;

    /*
     * Thread local buffer stuff.
     */

    /**
     * The concurrency level of the cache -or- ZERO (0) if <em>per-thread</em>
     * buffers will be used. When non-zero, this is the #of striped locks and
     * lock will protect a {@link TLB} instance.
     * 
     * @see #add(DLN)
     */
    private final int concurrencyLevel;

    /**
     * The capacity of the thread-local buffers.
     * 
     * @todo config : threadLocalBufferCapacity
     */
    private final int threadLocalBufferCapacity = 64;

    /**
     * When our inner queue has this many entries we will invoke tryLock() and
     * batch the updates if we can barge in on the lock.
     * 
     * @todo config : threadLocalBufferTryLockSize
     */
    private final int threadLocalBufferTryLockSize = threadLocalBufferCapacity >> 1;

    /**
     * A double-linked node having a (key,value) pair with a (prior,next)
     * reference used to maintain a double-linked list reflecting an access
     * policy (LRU or LIRS). This {@link #prior} and {@link #next} fields are
     * protected by the {@link BCHMGlobalLRU2#lock}. The other fields are final
     * (no recycler).
     * 
     * @version $Id$
     * @author thompsonbry
     */
    private static class DLN<K, V> {

        /** The owning cache for this entry. */
        private final LRUCacheImpl<K, V> cache;

        /** The bytes in memory for this entry. */
        private final int bytesInMemory;

        /** The bytes on disk for this entry. */
        private final int bytesOnDisk;

        private final K k;

        private final V v;

        /**
         * The prior, next fields are protected by the [lock] on the outer
         * class. Unlike the rest of the fields, these are mutable and will be
         * changed when the access order is updated.
         */
        private DLN<K, V> prior, next;

        /**
         * When the delete flag is set the {@link DLN} will be unlinked when it
         * is processed. Otherwise, the {@link DLN} will be added if
         * {@link #prior} and {@link #next} are <code>null</code> (since they
         * are <code>null</code> iff it has not yet been linked) and otherwise
         * it will be relinked into the MRU position.
         */
        volatile boolean delete; 

        DLN(final LRUCacheImpl<K, V> cache, final K k, final V v) {

            this.k = k;

            this.v = v;

            this.cache = cache;

            if (v instanceof IDataRecordAccess) {

                bytesInMemory = ((IDataRecordAccess) v).data().len();

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
            return "DLN{key=" + k + ",val=" + v + ",prior="
                    + (prior == null ? "N/A" : "" + prior.k) + ",next="
                    + (next == null ? "N/A" : "" + next.k) + ",bytesInMemory="
                    + bytesInMemory + ",bytesOnDisk=" + bytesOnDisk + "}";
        }

    } // class DLN

    /**
     * A thread-local buffer.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <K>
     * @param <V>
     * @param <T>
     */
    abstract private static class TLB<T> {

        /**
         * The identifier for this instance.
         */
        public final int id;
        
        /**
         * The capacity of the thread-local buffers.
         */
        private final int capacity;

        /**
         * When our inner queue has this many entries we will invoke tryLock()
         * and batch the updates if we can barge in on the lock.
         */
        private final int tryLockSize;

        /**
         * The lock protected the shared access order.
         */
        private final Lock lock;

        /**
         * The local buffer, which is lazily initialized so we can have it
         * strongly typed.
         */
        private T[] a;
        
        /**
         * The current #of elements in {@link #a}.
         * <p>
         * Note: Since this is thread-local we DO NOT need any synchronization.
         */
        private int size;

        /**
         * @param id
         *            The instance identifier.
         * @param capacity
         *            The capacity of the internal array.
         * @param tryLockSize
         *            The threshold at which an attempt will be made using
         *            tryLock() to batch the references in the array through the
         *            lock.
         * @param lock
         *            The lock.
         */
        protected TLB(final int id, final int capacity, final int tryLockSize, final Lock lock) {

            this.id = id;
            this.capacity = capacity;
            this.tryLockSize = tryLockSize;
            this.lock = lock;
//            this.a = new Object[capacity];
            this.size = 0;

        }

        /**
         * Delegates to {@link #doEvict()} to batch the references through the
         * {@link #lock} and then clears the references in the {@link #a array}
         * to <code>null</code> and resets the {@link #size} to ZERO (0).
         * <p>
         * Note: The references in the array are cleared to facilitate GC of the
         * references.
         */
        protected void evict() {

            doEvict(size, a);

            clearBuffer();
            
        }

        /**
         * Clears the references in the buffer, setting the size back to ZERO
         * (0).
         */
        protected void clearBuffer() {

            while (size > 0) {

                a[size--] = null;
                
            }

        }

        /**
         * Dispatch the first <i>n</i> references from the array. The lock
         * specified to the constructor will be held by the caller across this
         * call (you do not need to acquire it yourself).
         * 
         * @param n
         *            The #of references in the buffer.
         * @param a
         *            The array of references.
         */
        abstract protected void doEvict(int n, T[] a);

        /**
         * Add a reference to the thread local buffer. The references in the
         * buffer will be batched through {@link #evict()} if (a) the buffer is
         * full; or (b) there are {@link #tryLockSize} elements in the buffer.
         * {@link #evict()} clears the buffer size back to
         * 
         * @param ref
         *            The reference.
         */
        @SuppressWarnings("unchecked")
        public void add(final T ref) {

            if (a == null) {

                // Allocate strongly typed array.
                a = (T[]) java.lang.reflect.Array.newInstance(ref.getClass(),
                        capacity);

            }
            
            if (tryLockSize != 0 && size == tryLockSize) {

                if (lock.tryLock()) {

                    try {

                        evict();

                    } finally {

                        lock.unlock();

                    }

                }

                return;

            }

            if (size + 1 == capacity) {

                lock.lock();

                try {

                    evict();

                } finally {

                    lock.unlock();

                }

            }

            a[size++] = ref;

        }
        
    }

    protected TLB<DLN<K, V>> newTLB(final int id, final int capacity,
            final int tryLockSize, final Lock lock) {

        return new TLB<DLN<K, V>>(id, threadLocalBufferCapacity,
                threadLocalBufferTryLockSize, lock) {

            /**
             * Batch the touches through to the {@link AccessPolicy}.
             */
            @Override
            protected void doEvict(int n, DLN<K, V>[] a) {

                for (int i = 0; i < n; i++) {

                    BCHMGlobalLRU2.this.accessPolicy.relink(a[i]);

                }

            }

        };
                
    }

    /**
     * Return a thread-local buffer which may be used to batch updates to the
     * access order for the outer class. The use of a thread-local buffer can
     * substantially reduce contention for the lock which protects the outer
     * access order.
     * <P>
     * Note: The returned buffer handles synchronization internally using the
     * outer class's {@link #lock}.
     * 
     * @return The thread-local buffer used to batch updates to <i>this</i>
     *         {@link IGlobalLRU}.
     */
    final private TLB<DLN<K, V>> getTLB() {

        final Thread t = Thread.currentThread();

        TLB<DLN<K, V>> tmp = threadLocalBuffers.get(t);// id);

        if (tmp == null) {

            if (threadLocalBuffers.put(t, tmp = newTLB(0/* idIsIgnored */,
                    threadLocalBufferCapacity, threadLocalBufferTryLockSize,
                    lock)) != null) {

                /*
                 * Note: Since the key is the thread it is not possible for
                 * there to be a concurrent put of an entry under the same key
                 * so we do not have to use putIfAbsent().
                 */

                throw new AssertionError();

            }

        }

        return tmp;

    }

    /**
     * Acquire a {@link TLB} from an internal array of {@link TLB} instances
     * using a striped lock pattern.
     * <p>
     * Note: Contention can definitely arise with {@link #acquire()} on the
     * backing {@link Semaphore}. For the synthetic test, implementing using
     * per-thread {@link TLB}s scores <code>2694</code> ops/ms whereas
     * implementing using striped locks the performance score is only
     * <code>2033</code> (on a 2 core laptop with 3 threads). One thread on the
     * laptop has a throughput of <code>1405</code>, so <code>2800</code> is the
     * maximum possible throughput for 2 threads and is very nearly achieved by
     * the implementation based on thread-local {@link TLB}s. The actual
     * performance of the striped locks approach depends on the degree of
     * collision in the {@link Thread#getId()} values and the #of {@link TLB}
     * instances in the array.
     * <p>
     * While striped locks clearly have less throughput when compared to thread-
     * local {@link TLB}s, the striped lock performance is still nearly twice
     * the best performance of any other {@link IGlobalLRU} implementation and,
     * with striped locks, we do not have to worry about references on
     * {@link TLB}s "escaping" when we rarely see requests for some threads.
     * 
     * @return The {@link TLB}.
     * 
     * @throws InterruptedException
     */
    private TLB<DLN<K,V>> acquire() throws InterruptedException {
        
        // Note: Thread.getId() is a positive integer.
        final int i = (int) (Thread.currentThread().getId() % concurrencyLevel);

        permits[i].acquire();

        return buffers[i];
        
    }

    /**
     * Release a {@link TLB} obtained using {@link #acquire()}.
     * 
     * @param b
     *            The {@link TLB}.
     */
    private void release(final TLB<DLN<K, V>> b) {

        permits[b.id].release();

    }

    /**
     * Buffer an access policy update on a {@link TLB}. If the
     * {@link DLN#delete} flag is set, then it is removed from the access
     * policy. Otherwise, if the {@link DLN#prior} and {@link DLN#next}
     * references of the entry are <code>null</code> then it is inserted into
     * the access policy. Otherwise it is relinked within the access policy
     * according to the semantics of the {@link AccessPolicy} (LRU, LIRS, etc).
     * 
     * @param entry
     *            An entry in the access policy.
     */
    private void add(final DLN<K, V> entry) {

        if (concurrencyLevel == 0) {

            /*
             * Per-thread buffers.
             */
            getTLB().add(entry);
            
        } else {
            
            /*
             * Striped locks.
             */
            
            TLB<DLN<K, V>> t = null;
            try {

                t = acquire();

                t.add(entry);

            } catch (InterruptedException ex) {

                throw new RuntimeException(ex);

            } finally {

                if (t != null)
                    release(t);

            }
            
        }

    }
    
    /**
     * Visits all {@link TLB}s.
     * <p>
     * Note: You MUST hold a global lock in order to operate on the {@link TLB}s
     * without using {@link #acquire()} and {@link #release(TLB)}.
     * 
     * @todo do explicit acquire/release for each one? In that case, use direct
     *       access to the {@link #permits} and {@link #buffers}.
     */
    private Iterator<TLB<DLN<K,V>>> bufferIterator() {
        
        return Collections.unmodifiableList(Arrays.asList(buffers)).iterator();
        
    }
    
    /**
     * The counters for the shared LRU.
     */
    private final GlobalLRUCounters<K,V> globalLRUCounters;

    /**
     * The access policy. Changes in the access policy's internal state are
     * protected by the {@link #lock}.
     */
    private final AccessPolicy<K, V> accessPolicy;

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

    /*
     * Used iff striped locks are used.
     */
    /**
     * The striped locks and <code>null</code> if per-thread {@link TLB}s are
     * being used.
     */
    private final Semaphore[] permits;

    /**
     * The {@link TLB}s protected by the striped locks and <code>null</code> if
     * per-thread {@link TLB}s are being used.
     */
    private final TLB<DLN<K,V>>[] buffers;

    /*
     * Used iff true thread-local buffers are used.
     */

    /**
     * The per-thread {@link TLB}s and <code>null</code> if striped locks are
     * being used.
     */
    private final ConcurrentHashMap<Thread/* Thread */, TLB<DLN<K, V>>> threadLocalBuffers;

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
     * @param concurrencyLevel
     *            The concurrency level of the cache -or- ZERO (0) if
     *            <em>per-thread</em> buffers will be used. When non-zero, this
     *            is the #of striped locks and lock will protect a {@link TLB}
     *            instance.
     */
    public BCHMGlobalLRU2(final long maximumBytesInMemory,
            final long minCleared, final int minimumCacheSetCapacity,
            final int initialCacheCapacity, final float loadFactor,
            final int concurrencyLevel) {

        if (maximumBytesInMemory <= 0)
            throw new IllegalArgumentException();

        if (minCleared < 0)
            throw new IllegalArgumentException();

        if (minCleared > maximumBytesInMemory)
            throw new IllegalArgumentException();

        if (concurrencyLevel < 0)
            throw new IllegalArgumentException();

        this.maximumBytesInMemory = maximumBytesInMemory;

        this.minCleared = minCleared;

        this.initialCacheCapacity = initialCacheCapacity;

        this.loadFactor = loadFactor;

        this.concurrencyLevel = concurrencyLevel;
        
        this.globalLRUCounters = new GlobalLRUCounters<K, V>(this);

        this.accessPolicy = new LRUAccessPolicy<K, V>(lock, globalLRUCounters);
        
        cacheSet = new ConcurrentHashMap<UUID, LRUCacheImpl<K, V>>(
                minimumCacheSetCapacity);
        
        if (concurrencyLevel == 0) {
            /*
             * Per-thread buffers.
             */
            permits = null;
            buffers = null;
            threadLocalBuffers = new ConcurrentHashMap<Thread, TLB<DLN<K, V>>>();
        } else {
            /*
             * Striped locks.
             */
            permits = new Semaphore[concurrencyLevel];
            buffers = new TLB[concurrencyLevel];
            threadLocalBuffers = null;
            for (int i = 0; i < concurrencyLevel; i++) {
                permits[i] = new Semaphore(1, false/* fair */);
                buffers[i] = newTLB(i/* id */, threadLocalBufferCapacity,
                        threadLocalBufferTryLockSize, lock);
            }
        }
        
    }

    public ILRUCache<K, V> getCache(final UUID uuid, final IAddressManager am) {

        if (uuid == null)
            throw new IllegalArgumentException();

        LRUCacheImpl<K, V> cache = cacheSet.get(uuid);

        if (cache == null) {

            cache = new LRUCacheImpl<K, V>(uuid, am, this,
                    initialCacheCapacity, loadFactor);

            final LRUCacheImpl<K, V> oldVal = cacheSet.putIfAbsent(uuid, cache);

            if (oldVal == null) {

                // if (BigdataStatics.debug)
                // System.err.println("New store: " + store + " : file="
                // + store.getFile());

            } else {

                // concurrent insert.
                cache = oldVal;

            }

        }

        return cache;

    }

    /**
     * The concurrency level of the cache -or- ZERO (0) if <em>per-thread</em>
     * buffers will be used. When non-zero, this is the #of striped locks and
     * lock will protect a {@link TLB} instance.
     * 
     * @todo report as a one-shot counter.
     */
    public int getConcurrencyLevel() {
    
        return concurrencyLevel;
        
    }
    
    public int getRecordCount() {

        return accessPolicy.size();

    }

    public long getEvictionCount() {

        return globalLRUCounters.evictionCount.get();

    }

    public long getEvictionByteCount() {

        return globalLRUCounters.evictionByteCount.get();

    }

    public long getBytesInMemory() {

        return globalLRUCounters.bytesInMemory.get();

    }

    /**
     * The minimum bytes available in the {@link IGlobalLRU} after an eviction
     * (from the constructor).
     */
    public long getMinCleared() {
        
        return minCleared;
        
    }
    
    public long getMaximumBytesInMemory() {

        return maximumBytesInMemory;
        
    }

    public int getCacheSetSize() {

        return cacheSet.size();

    }

    /**
     * If the global LRU is over capacity (based on the #of bytes buffered) then
     * purge entries from the cache(s) based on the access policy eviction order
     * until at least {@link #getMinCleared()} bytes are available. This batch
     * eviction strategy helps to minimize contention for the {@link #lock} when
     * cache records must be evicted.
     * 
     * @see #getMinCleared()
     * @see #getMaximumBytesInMemory()
     */
    protected void purgeEntriesIfOverCapacity() {

        if (globalLRUCounters.bytesInMemory.get() < maximumBytesInMemory) {

            return;

        }

        lock.lock();

        try {

            /*
             * The global LRU is over capacity. Purge entries from the cache
             * until until the #of bytes in memory falls below [threshold].
             */
            final long threshold = maximumBytesInMemory - minCleared;

            assert threshold >= 0;

            while (globalLRUCounters.bytesInMemory.get() >= threshold) {

                accessPolicy.evictEntry();

            }

        } finally {

            lock.unlock();

        }

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

    /*
     * @todo We might need to use a read/write lock to clear the TLB instances
     * atomically.
     */
    public void discardAllCaches() {

        lock.lock();
        try {

            {

                final Iterator<LRUCacheImpl<K, V>> itr = cacheSet.values()
                        .iterator();

                while (itr.hasNext()) {

                    final LRUCacheImpl<K, V> cache = itr.next();

                    if (cache == null) {

                        // weak reference was cleared.
                        continue;

                    }

                    cache.clear();

                }

                // Verify the access policy state was reset.
                accessPolicy.assertEmpty();

            }

            // Discard any buffered references.
            {

                final Iterator<TLB<DLN<K, V>>> itr = bufferIterator();

                while (itr.hasNext()) {

                    final TLB<DLN<K, V>> t = itr.next();

                    t.clearBuffer();

                }

            }

            // reset the global counters.
            globalLRUCounters.clear();

        } finally {

            lock.unlock();

        }

    }

//    /**
//     * Flush all buffered touches to the backing access policy.
//     * <p>
//     * Note: This method is NOT required during normal operations. It's sole
//     * purpose is to bring counters associated with the access policy (rather
//     * than the {@link ConcurrentHashMap}) up to date by evicting the buffered
//     * references currently in the {@link TLB thread-local buffers} onto the
//     * shared access policy.
//     * 
//     * @todo Unless we acquire()/release() the {@link TLB}s their internal state
//     *       changes may not be visible to another thread.
//     */
//    public void flushBuffers() {
//        
//        lock.lock();
//        try {
//
//            final Iterator<TLB<DLN<K, V>>> itr = threadLocalBuffers.values()
//                    .iterator();
//
//            while (itr.hasNext()) {
//
//                final TLB<DLN<K, V>> t = itr.next();
//
//                t.evict();
//
//            }
//
//        } finally {
//            lock.unlock();
//        }
//        
//    }
    
    public CounterSet getCounterSet() {

        final CounterSet root = globalLRUCounters.getCounterSet();

        final Iterator<LRUCacheImpl<K, V>> itr = cacheSet.values().iterator();

        while (itr.hasNext()) {

            // final LRUCacheImpl<K, V> cache = itr.next().get();

            final LRUCacheImpl<K, V> cache = itr.next();

            if (cache == null) {

                // weak reference was cleared.
                continue;

            }

            // add the per-cache counters.
            root.makePath(cache.storeUUID.toString()).attach(
                    cache.cacheCounters.getCounters());

        }

        return root;

    }

    /**
     * Counters for the {@link BCHMGlobalLRU2}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static private class GlobalLRUCounters<K,V> {

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

        private final IHardReferenceGlobalLRU<K,V> cache;
        
        public GlobalLRUCounters(final BCHMGlobalLRU2<K,V> cache) {
            
            this.cache = cache;
            
        }
        
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
                            setValue(((int) (10000 * cache.getBytesInMemory() / (double) cache
                                    .getMaximumBytesInMemory())) / 10000d);
                        }
                    });

            counters
                    .addCounter(
                            IGlobalLRU.IGlobalLRUCounters.MAXIMUM_ALLOWED_BYTES_IN_MEMORY,
                            new OneShotInstrument<Long>(cache
                                    .getMaximumBytesInMemory()));

            counters.addCounter(
                    IGlobalLRU.IGlobalLRUCounters.BUFFERED_RECORD_COUNT,
                    new Instrument<Integer>() {
                        @Override
                        protected void sample() {
                            setValue(cache.getRecordCount());
                        }
                    });

            counters
                    .addCounter(
                            IGlobalLRU.IGlobalLRUCounters.BUFFERED_RECORD_EVICTION_COUNT,
                            new Instrument<Long>() {
                                @Override
                                protected void sample() {
                                    setValue(evictionCount.get());
                                }
                            });

            counters
                    .addCounter(
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
                                    final long tmp = cache.getRecordCount();
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
                            final long tmp = cache.getRecordCount();
                            if (tmp == 0) {
                                setValue(0);
                                return;
                            }
                            setValue((int) (bytesOnDisk.get() / tmp));
                        }
                    });

            counters.addCounter(IGlobalLRU.IGlobalLRUCounters.CACHE_COUNT,
                    new Instrument<Integer>() {
                        @Override
                        protected void sample() {
                            setValue(cache.getCacheSetSize());
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
     * An access policy maintains the {@link DLN}s in some particular order and
     * decides which {@link DLN}s should be evicted when the cache is full (that
     * is, when the memory limit on the cache has been reached since that is how
     * we determine "full").
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    interface AccessPolicy<K, V> {

        /**
         * Check the internal state and verify that the access policy is empty.
         * 
         * @throws AssertionError
         *             if the access policy is not empty.
         */
        void assertEmpty();
        
        /**
         * The approximate #of objects in the access policy.
         */
        int size();
        
        /**
         * Accept the entry for processing. If {@link DLN#delete} is
         * <code>true</code> then the entry will be unlinked. Otherwise, the
         * entry will be added if its (prior,next) links are <code>null</code>.
         * Otherwise, the entry will be relinked to update its location in the
         * access order.
         * 
         * @param e
         *            The entry.
         */
        void relink(DLN<K,V> e);
        
//        /**
//         * Add an entry to the access policy.
//         */
//        void addEntry(final DLN<K, V> e);

//        /**
//         * Update the order of the entry in the access policy.
//         */
//        void touchEntry(final DLN<K, V> e);
        
//        /**
//         * Remove the specified entry from the access policy.
//         */
//        void removeEntry(final DLN<K, V> e);

        /**
         * Evict an entry from the access policy.
         */
        DLN<K,V> evictEntry();
        
    }

    /**
     * LRU implementation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <K>
     *            The key type.
     * @param <V>
     *            The value type.
     */
    private static class LRUAccessPolicy<K,V> implements AccessPolicy<K, V> {
        
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
        private DLN<K, V> first = null;

        /**
         * The entry which is last in the ordering (the <em>most recently used</em>)
         * and <code>null</code> iff the cache is empty.
         */
        private DLN<K, V> last = null;

        /**
         * The lock protecting the mutable fields in this class.
         * <p>
         * Note: This is the lock on the outer class. It MUST be held across
         * updates to the access policy. Those updates are batched from
         * {@link TLB}s as they fill up. Since this lock is always held when
         * those fields are updated, the fields do not need to be [volatile].
         */
        private final ReentrantLock lock;

        private final GlobalLRUCounters<K, V> counters;

        /**
         * Return the #of objects in the LRU. This is non-blocking and relies on
         * a volatile read for visibility.
         */
        public int size() {
            
            return size;
            
        }
        
        public void assertEmpty() {

            /*
             * The caller needs to be holding the lock for changes to first and
             * last to be visible.
             */
            if (!lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();

            assert size == 0 : "size=" + size + ", first=" + first + ", last="
                    + last;
            assert first == null;
            assert last == null;

        }
        
        protected LRUAccessPolicy(final ReentrantLock lock,
                final GlobalLRUCounters<K, V> counters) {

            this.lock = lock;

            this.counters = counters;

        }

        public void relink(final DLN<K,V> e) {

            if (e.delete) {

                // unlink from the access order.
                removeEntry(e);
                
            } else {
                
                if (e.prior == null && e.next == null) {

                    // insert into the access order.
                    addEntry(e);
                    
                } else {
                
                    // update the position in the access order.
                    touchEntry(e);
                    
                }
                
            }
            
        }
        
        /**
         * Add an {@link DLN} to the tail of the linked list (the MRU position).
         */
        void addEntry(final DLN<K, V> e) {
            if (!lock.isHeldByCurrentThread())
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
         * Remove an {@link DLN} from linked list that maintains the LRU
         * ordering. The {@link DLN#prior} and {@link DLN#next} fields are
         * cleared. The {@link #first} and {@link #last} fields are updated as
         * necessary. This DOES NOT remove the entry under that key from the
         * hash map (typically this has already been done).
         */
        void removeEntry(final DLN<K, V> e) {
            if (!lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();
            if (e.cache == null)
                return;
            final DLN<K, V> prior = e.prior;
            final DLN<K, V> next = e.next;
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
//            final V clearedValue = e.v;
            e.prior = null;
            e.next = null;
            // e.cache = null; // clear reference to the cache.
            // e.k = null; // clear the key.
            // e.v = null; // clear the value reference.
            size--;
            counters.bytesInMemory.addAndGet(-e.bytesInMemory);
            counters.bytesOnDisk.addAndGet(-e.bytesOnDisk);
            // e.bytesInMemory = e.bytesOnDisk = 0;
//            return clearedValue;
        }

        /**
         * Move the entry to the end of the linked list (the MRU position).
         */
        void touchEntry(final DLN<K, V> e) {

            if (!lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();

            if (last == e) {

                return;

            }

            // unlink entry
            // removeEntry(e);
            {
                final DLN<K, V> prior = e.prior;
                final DLN<K, V> next = e.next;
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
         * Evict the LRU entry.
         */
        public DLN<K, V> evictEntry() {

            // entry in the LRU position.
            final DLN<K, V> entry = first;

            assert entry != null;
            
            // the key associated with the entry to be evicted.
            final K evictedKey = entry.k;

            // The cache from which the entry will be evicted.
            final LRUCacheImpl<K, V> evictedFromCache = entry.cache;

            final int bytesOnDisk = entry.bytesOnDisk;

            // remove LRU entry from ordering.
            removeEntry(entry);

            // remove entry under that key from hash map for that store.
            evictedFromCache.remove(evictedKey);// entry.k);

            counters.evictionCount.incrementAndGet();

            counters.evictionByteCount.addAndGet(bytesOnDisk);
            
            return entry;

        }
        
    } // LRUAccessPolicy

    /**
     * A hard reference hash map backed by a shared Least Recently Used (LRU)
     * ordering over entries.
     * <p>
     * Note: Thread-safety is enforced using {@link BCHMGlobalLRU2#lock} .
     * Nested locking, such as using <code>synchronized</code> on the instances
     * of this class can cause deadlocks because evictions may be made from any
     * {@link LRUCacheImpl} when the LRU entry is evicted from the shared LRU.
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
         * @version $Id: BCHMGlobalLRU2.java 2547 2010-03-24 20:44:07Z
         *          thompsonbry $
         * 
         *          FIXME Review visibility concerns for these counters.
         *          <p>
         *          Do these counters need to be volatile or {@link AtomicLong}s
         *          in order to be consistent? They are updated inside of a
         *          {@link Lock}, but that does not help with their visibility,
         *          does it?
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
        private final LRUCacheCounters cacheCounters = new LRUCacheCounters();

        /**
         * The {@link UUID} of the associated {@link IRawStore}.
         */
        private final UUID storeUUID;

        /**
         * The {@link IRawStore} implementation class.
         */
        // private final Class<? extends IRawStore> cls;

        /**
         * An {@link IAddressManager} that can decode the record byte count from
         * the record address without causing the {@link IRawStore} reference to
         * be retained.
         */
        private final IAddressManager am;

        /**
         * The shared LRU.
         */
        private final BCHMGlobalLRU2<K, V> globalLRU;

        /**
         * The hash map from keys to entries wrapping cached object references.
         */
        private final ConcurrentHashMap<K, DLN<K, V>> map;

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
        public LRUCacheImpl(final UUID storeUUID, final IAddressManager am,
                final BCHMGlobalLRU2<K, V> lru, final int initialCapacity,
                final float loadFactor) {

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

            this.map = new ConcurrentHashMap<K, DLN<K, V>>(initialCapacity,
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
         * {@link BCHMGlobalLRU2#cacheSet}.
         */
        public void clear() {

            globalLRU.lock.lock();

            try {

                /*
                 * While holding the [lock], batch through each of the TLBs so
                 * we do not lose any updates.
                 */
                {
                    
                    final Iterator<TLB<DLN<K, V>>> itr = globalLRU
                            .bufferIterator();

                    while (itr.hasNext()) {

                        final TLB<DLN<K, V>> t = itr.next();

                        t.evict();

                    }
                    
                }
                
                /*
                 * Now discard all entries in the selected cache.
                 */
                {

                    final Iterator<DLN<K, V>> itr = map.values().iterator();

                    while (itr.hasNext()) {

                        final DLN<K, V> e = itr.next();

                        // remove entry from the map.
                        itr.remove();

                        // unlink entry from the LRU.
                        e.delete = true;
                        globalLRU.accessPolicy.relink(e);

                    }

                }

                cacheCounters.clear();

            } finally {

                globalLRU.lock.unlock();

            }

        }

        /**
         * The #of entries in the cache (approximate, non-blocking).
         */
        public int size() {

            return map.size();

        }
        
        /**
         * {@inheritDoc}
         * 
         * Note: When the access policy is full (at or above the target maximum
         * memory) then a batch eviction will clear sufficient cache entries to
         * bring down the memory burden of the cache by a reasonable amount in
         * order to reduce contention for the lock due to eviction pressure.
         */
        public V putIfAbsent(final K k, final V v) {

            if (k == null)
                throw new IllegalArgumentException();

            if (v == null)
                throw new IllegalArgumentException();

            DLN<K, V> entry = map.get(k);

            if (entry != null) {

                /*
                 * There is an existing entry under the key.
                 */

                // buffer the touch on an existing entry.
                globalLRU.add(entry);

                // Return the old value.
                return entry.v;

            }

            /*
             * There is no entry under that key.
             */
            globalLRU.purgeEntriesIfOverCapacity();
            
            /*
             * The map is not over capacity.
             * 
             * Create a new entry and buffer the entry to be linked into the
             * access policy.
             */

            // new entry.
            entry = new DLN<K, V>(this, k, v);

            // put in the map.
            map.put(k, entry);

            // buffer access policy update.
            globalLRU.add(entry);

            final int count = map.size();

            if (count > cacheCounters.highTide) {

                cacheCounters.highTide = count;

            }

            cacheCounters.ninserts++;

            // return [null] since there was no entry under the key.
            return null;

        }

        public V get(final K key) {

            if (key == null)
                throw new IllegalArgumentException();

            /*
             * Note: This test needs to be done while holding the global lock
             * since the LRU can reuse the LRU DLN instance when it is evicted
             * as the MRU Entry object. If you want to do this test outside of
             * the lock, then the code needs to be modified to allocate a new
             * Entry object on insert. If the test is done outside of the lock,
             * then you can use a ConcurrentHashMap for the map to avoid
             * concurrent modification issues. Otherwise, use a LinkedHashMap
             * for a faster iterator.
             */

            final DLN<K, V> entry = map.get(key);

            cacheCounters.ntests++;

            if (entry == null) {

                return null;

            }

            // buffer access policy update.
            globalLRU.add(entry);

            cacheCounters.nsuccess++;

            return entry.v;

        }

        public V remove(final K key) {

            if (key == null)
                throw new IllegalArgumentException();

            // remove the cache entry (iff it exists).
            final DLN<K, V> entry = map.remove(key);

            if (entry == null)
                return null;

            // mark as deleted and add to TLB.
            entry.delete = true;
            globalLRU.add(entry);

            // return the old value.
            return entry.v;

        }

        public String toString() {

            return super.toString() + "{" + cacheCounters.toString() + "}";

        }

    }

}
