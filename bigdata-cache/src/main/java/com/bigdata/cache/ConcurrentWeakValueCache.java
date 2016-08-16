package com.bigdata.cache;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * A low-contention/high concurrency weak value cache. This class can offer
 * substantially less lock contention and hence greater performance than the
 * {@link WeakValueCache}.
 * <p>
 * The role of the {@link HardReferenceQueue} is to place a minimum upper bound
 * on the #of objects in the cache. If the application holds hard references to
 * more than this many objects, e.g., in various threads, collections, etc.,
 * then the cache size can be larger than the queue capacity. Likewise, if the
 * JVM takes too long to finalize weakly reachable references, then the cache
 * size can exceed this limit even if the application does not hold ANY hard
 * references to the objects in the cache. For these reasons, this class uses an
 * initialCapacity for the inner {@link ConcurrentHashMap} that is larger than
 * the <i>queueCapacity</i>. This helps to prevent resizing the
 * {@link ConcurrentHashMap} which is a relatively expensive operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <K>
 *            The generic type of the keys.
 * @param <V>
 *            The generic type of the values.
 */
public class ConcurrentWeakValueCache<K, V> implements
        IConcurrentWeakValueCache<K, V> {

    protected static transient final Logger log = Logger.getLogger(ConcurrentWeakValueCache.class);
    
    protected static transient final boolean INFO = log.isInfoEnabled();

    protected static transient final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * A concurrency-savvy map.
     */
    final private ConcurrentHashMap<K, WeakReference<V>> map;

    /**
     * Used to ensure that the [cacheCapacity] MRU weak references are not
     * finalized (optional).
     */
    final private IHardReferenceQueue<V> queue;

    /**
     * Reference queue for weak references in entered into the cache. A weak
     * reference will appear on this queue once the reference has been cleared.
     */
    final private ReferenceQueue<V> referenceQueue;

    /**
     * Return <code>true</code> iff a {@link ReferenceQueue} is being maintained
     * and entries will be removed from the map once the corresponding
     * {@link WeakReference} has been cleared. This behavior is controlled by a
     * constructor option.
     */
    public boolean isRemoveClearedReferences() {
        
        return referenceQueue != null;
        
    }
    
    /**
     * Returns the approximate number of keys in the map. Cleared references are
     * removed before reporting the size of the map, but the return value is
     * still approximate as garbage collection by the JVM can cause references
     * to be cleared at any time.
     */
    @Override
    public int size() {

        removeClearedEntries();
        
        return map.size();
        
    }

    /**
     * The capacity of the backing hard reference queue.
     */
    @Override
    public int capacity() {

        if (queue == null)
            return 0;
        
        return queue.capacity();
        
    }
    
    @Override
    public void clear() {

        if (queue != null) {

            // synchronize on the queue so that this operation is atomic.
            synchronized (queue) {

                // clear hard references so that we don't hold onto things.
                queue.clear(true);

                // clear the map entries (atomic).
                map.clear();

            }
        
        }
        
        /*
         * Note: We do not need to remove cleared references here since
         * removeClearedEntries() guards against removal of the entry under the
         * key if the WeakReference object itself has been changed.
         * 
         * However, it does not hurt do invoke it here either. And we do not
         * need to be synchronized when we do this.
         */

        removeClearedEntries();
        
    }

    /**
     * Uses the default queue capacity (16), load factor (0.75) and concurrency
     * level (16).
     */
    public ConcurrentWeakValueCache() {

        this(16/* queueCapacity */);

    }

    /**
     * Uses the default load factor (0.75) and concurrency level (16).
     * 
     * @param queueCapacity
     *            The {@link IHardReferenceQueue} capacity. When ZERO (0), there
     *            will not be a backing hard reference queue.
     */
    public ConcurrentWeakValueCache(final int queueCapacity) {

        this(queueCapacity, 0.75f/* loadFactor */, 16/* concurrencyLevel */);

    }

    /**
     * Uses the specified values.
     * 
     * @param queueCapacity
     *            The {@link IHardReferenceQueue} capacity. When ZERO (0), there
     *            will not be a backing hard reference queue.
     * @param loadFactor
     *            The load factor.
     * @param concurrencyLevel
     *            The concurrency level.
     */
    public ConcurrentWeakValueCache(final int queueCapacity,
            final float loadFactor, final int concurrencyLevel) {

        this(queueCapacity, loadFactor, concurrencyLevel, true/* removeClearedReferences */);

    }

    /**
     * Uses the specified values and creates a {@link HardReferenceQueue}
     * without a timeout.
     * 
     * @param queueCapacity
     *            The {@link HardReferenceQueue} capacity. When ZERO (0), there
     *            will not be a backing hard reference queue.
     * @param loadFactor
     *            The load factor.
     * @param concurrencyLevel
     *            The concurrency level.
     * @param removeClearedReferences
     *            When <code>true</code> the cache will remove entries for
     *            cleared references. When <code>false</code> those entries will
     *            remain in the cache.
     */
    public ConcurrentWeakValueCache(final int queueCapacity,
            final float loadFactor, final int concurrencyLevel,
            final boolean removeClearedReferences
            ) {
        
        this(queueCapacity == 0 ? null : new HardReferenceQueue<V>(
                null/* listener */, queueCapacity), loadFactor,
                concurrencyLevel, removeClearedReferences);
        
    }

    /**
     * Defaults the initial capacity of the map based on the capacity of the
     * optional {@link IHardReferenceQueue} and uses the Java default of
     * <code>16</code> if there is no queue.
     * 
     * @param queue
     *            The {@link IHardReferenceQueue} (optional).
     * @param loadFactor
     *            The load factor.
     * @param concurrencyLevel
     *            The concurrency level.
     * @param removeClearedReferences
     *            When <code>true</code> the cache will remove entries for
     *            cleared references. When <code>false</code> those entries will
     *            remain in the cache.
     */
    public ConcurrentWeakValueCache(final IHardReferenceQueue<V> queue,
          final float loadFactor, final int concurrencyLevel,
          final boolean removeClearedReferences
          ) {

        /*
         * This uses Math.max(16,queue.capacity()) as initialCapacity of the
         * map. The map CAN have more entries than the queue since it will
         * contain all non-cleared references inserted into the map. On the
         * other hand, the queue is normally used to bound the size of the map
         * in designs were map entries are cleared as references are evicted
         * from the queue.
         */
        this(queue, (queue == null ? 16 : Math.max(16, queue.capacity() / 2)),
                loadFactor, concurrencyLevel, removeClearedReferences);
        
    }

    /**
     * Uses the specified values.
     * 
     * @param queue
     *            The {@link IHardReferenceQueue} (optional).
     * @param initialCapacity
     *            The initial capacity of the backing hash map.
     * @param loadFactor
     *            The load factor.
     * @param concurrencyLevel
     *            The concurrency level.
     * @param removeClearedReferences
     *            When <code>true</code> the cache will remove entries for
     *            cleared references. When <code>false</code> those entries will
     *            remain in the cache.
     */
    public ConcurrentWeakValueCache(final IHardReferenceQueue<V> queue,
            final int initialCapacity, final float loadFactor,
            final int concurrencyLevel, final boolean removeClearedReferences) {

//        if (queue == null)
//            throw new IllegalArgumentException();
        
        this.queue = queue;
        
        /*
         * We set the initial capacity of the ConcurrentHashMap to be larger
         * than the capacity of the hard reference queue. This helps to avoid
         * resizing the ConcurrentHashMap, which is relatively expensive.
         */

        map = new ConcurrentHashMap<K, WeakReference<V>>(initialCapacity,
                loadFactor, concurrencyLevel);

        if (removeClearedReferences) {
            
            referenceQueue = new ReferenceQueue<V>();
            
        } else {
            
            referenceQueue = null;
            
        }

    }

    /**
     * Returns the value for the key.
     * 
     * @param k
     *            The key.
     * 
     * @return The value.
     * 
     * @todo If we can locate a ConcurrentRingBuffer then we could further
     *       improve performance by removing synchronization on the hard
     *       reference queue in this method.
     * 
     * @see http://en.wikipedia.org/wiki/Lock-free_and_wait-free_algorithms
     * @see http://www.audiomulch.com/~rossb/code/lockfree/
     */
    @Override
    public V get(final K k) {

        final WeakReference<V> ref = map.get(k);

        if (ref != null) {

            /*
             * There is an entry under the key, so get the reference paired to
             * the key.
             */
            
            final V v = ref.get();

            if (v != null) {

                /*
                 * The reference paired with the key has not been cleared so we
                 * append it to the queue so that the reference will be retained
                 * longer (a touch).
                 */
                
                if (queue != null) {
                    
                    synchronized (queue) {

                        queue.add(v);

                    }

                }

                return v;

            }

        }

        // Note: Done by put().
//        removeClearedEntries();
        
        return null;

    }

    /**
     * Return <code>true</code> iff the map contains an entry for the key
     * whose weak reference has not been cleared.
     * 
     * @param k
     *            The key.
     *            
     * @return <code>true</code> iff the map contains an entry for that key
     *         whose weak reference has not been cleared.
     */
    @Override
    public boolean containsKey(final K k) {

        final WeakReference<V> ref = map.get(k);

        if (ref != null) {

            /*
             * There is an entry under the key, so get the reference paired to
             * the key.
             */

            final V v = ref.get();

            if (v != null) {

                /*
                 * The reference paired with the key has not been cleared so we
                 * append it to the queue so that the reference will be retained
                 * longer (a touch).
                 */

                if (queue != null) {

                    synchronized (queue) {

                        queue.add(v);

                    }

                }
                
                return true;

            }

        }

        // Note: Done by put().
        // removeClearedEntries();

        return false;

    }

    /**
     * Adds the key-value mapping to the cache.
     * 
     * @param k
     *            The key.
     * @param v
     *            The value.
     * 
     * @return The old value under the key -or- <code>null</code> if there is
     *         no entry under the key or if the entry under the key has has its
     *         reference cleared.
     */
    @Override
    public V put(final K k, final V v) {

        try {
            
            // new reference.
            final WeakReference<V> ref = newWeakRef(k, v, referenceQueue);
//            final WeakReference<V> ref = referenceQueue == null //
//                ? new WeakReference<V>(v)
//                : new WeakRef<K, V>(k, v, referenceQueue);

            // add to cache.
            final WeakReference<V> oldRef = map.put(k, ref);

            final V oldVal = oldRef == null ? null : oldRef.get();

            if (queue != null) {

                synchronized (queue) {

                    // put onto the hard reference queue.
                    if (queue.add(v) && DEBUG) {

                        log.debug("put: key=" + k + ", val=" + v);

                    }

                }

            }
            
            // notification.
            didUpdate(k, ref, oldRef);

            return oldVal;
            
        } finally {
            
            removeClearedEntries();

        }
        
    }

    /**
     * Adds the key-value mapping to the cache iff there is no entry for that
     * key. Note that a cleared reference under a key is treated in exactly the
     * same manner as if there were no entry under the key (the entry under the
     * key is replaced atomically).
     * 
     * @param k
     *            The key.
     * @param v
     *            The value.
     * 
     * @return the previous value associated with the specified key, or
     *         <tt>null</tt> if there was no mapping for the key or if the
     *         entry under the key has has its reference cleared.
     */
    @Override
    public V putIfAbsent(final K k, final V v) {

        try {

            // new reference.
            final WeakReference<V> ref = newWeakRef(k, v, referenceQueue);
//            final WeakReference<V> ref = referenceQueue == null //
//                ? new WeakReference<V>(v)
//                : new WeakRef<K, V>(k, v, referenceQueue);

            // add to cache.
            final WeakReference<V> oldRef = map.putIfAbsent(k, ref);

            final V oldVal = oldRef == null ? null : oldRef.get();

            if (oldRef != null && oldVal == null) {

                /*
                 * There was an entry under the key but its reference has been
                 * cleared. A cleared value paired to the key is equivalent to
                 * there being no entry under the key (the entry will be cleared
                 * the next time removeClearedRefernces() is invoked). Therefore
                 * we attempt to replace the entry under the key. If this can be
                 * done atomically, then we have achieved putIfAbsent semantics
                 * for a key paired to a cleared reference.
                 */

                if (map.replace(k, oldRef, ref)) {

                    if (queue != null) {

                        // no reference under that key.
                        synchronized (queue) {

                            // put the new value onto the hard reference queue.
                            if (queue.add(v) && DEBUG) {

                                log.debug("put: key=" + k + ", val=" + v);

                            }

                        }

                    }
                    
                    // notification.
                    didUpdate(k, ref, oldRef);

                    // the old value for the key was a cleared reference.
                    return null;

                } else {
                    
                    /**
                     * We lost a potential concurrent data race, so make
                     * recursive call to ensure correct value is returned.
                     * 
                     * @see <a href="http://trac.blazegraph.com/ticket/1004">
                     *      Concurrent binding problem </a>
                     */

                    return putIfAbsent(k, v);
                    
                }

            }

            if (oldVal == null) {

                if (queue != null) {

                    // no reference under that key.
                    synchronized (queue) {

                        // put it onto the hard reference queue.
                        if (queue.add(v) && DEBUG) {

                            log.debug("put: key=" + k + ", val=" + v);

                        }

                    }

                }
                
                // notification.
                didUpdate(k, ref, null/* oldVal */);
                
                return null;

            }

            /*
             * There was a non-null reference paired to the key so we return the
             * old value and DO NOT update either the map or the hard reference
             * queue.
             */

            return oldVal;

        } finally {

            removeClearedEntries();

        }

    }

    /**
     * Notification method is invoked if a map entry is inserted or updated by
     * {@link #put(Object, Object)} or {@link #putIfAbsent(Object, Object)}.
     * This method IS NOT invoked if {@link #putIfAbsent(Object, Object)} did
     * not update the map.
     * 
     * @param k
     *            The key.
     * @param newRef
     *            The {@link WeakReference} for the new value. This was
     *            generated using
     *            {@link #newWeakRef(Object, Object, ReferenceQueue)}.
     *            {@link WeakReference#get()} will always return the value
     *            inserted into the map since it is on the stack and hence can
     *            not have been cleared during this callback.
     * @param oldRef
     *            The {@link WeakReference} for the old value. This will be
     *            <code>null</code> if there was no entry under the key for the
     *            map. If the entry for a cleared reference is updated then
     *            {@link WeakReference#get()} will return null for the
     *            <i>oldRef</i>.
     */
    protected void didUpdate(final K k, final WeakReference<V> newRef,
            final WeakReference<V> oldRef) {

        // NOP
        
    }
    
    @Override
    public V remove(final K k) {

        try {

            final WeakReference<V> ref = removeMapEntry(k);

            if (ref != null) {

                return ref.get();

            }

            return null;

        } finally {

            removeClearedEntries();

        }
        
    }
    
    /**
     * <p>
     * Remove any entries whose weak reference has been cleared from the
     * {@link #map}. This method does not block and only polls the
     * {@link ReferenceQueue}.
     * </p>
     * <p>
     * Note: This method does not clear entries from the hard reference queue
     * because it is not possible to have a weak or soft reference cleared by
     * the JVM while a hard reference exists, so it is not possible to have an
     * entry in the hard reference queue for a reference that is being cleared
     * here.
     * </p>
     */
    protected void removeClearedEntries()
    {
        
        if (referenceQueue == null)
            return;
        
        int counter = 0;

        for (Reference<? extends V> ref = referenceQueue.poll(); ref != null; ref = referenceQueue
                .poll()) {

            final K k = ((WeakRef<K, V>) ref).k;

            if (map.get(k) == ref) {

                if (DEBUG) {

                    log.debug("Removing cleared reference: key=" + k);

                }

                removeMapEntry(k);
                
                counter++;
                
            }
            
        }
        
        if( counter > 1 ) {
            
            if( INFO ) {
                
                log.info("Removed " + counter + " cleared references");
                
            }
            
        }
        
    }

    /**
     * Invoked when a reference needs to be removed from the map.
     * 
     * @param k
     *            The key.
     */
    protected WeakReference<V> removeMapEntry(final K k) {
        
        return map.remove(k);
        
    }

    /**
     * An iterator that visits the weak reference values in the map. You must
     * test each weak reference in order to determine whether its value has been
     * cleared as of the moment that you request that value. The entries visited
     * by the iterator are not "touched" so the use of the iterator will not
     * cause them to be retained any longer than they otherwise would have been
     * retained.
     */
    @Override
    public Iterator<WeakReference<V>> iterator() {

        return map.values().iterator();
        
    }

    /**
     * An iterator that visits the entries in the map. You must test the weak
     * reference for each entry in order to determine whether its value has been
     * cleared as of the moment that you request that value. The entries visited
     * by the iterator are not "touched" so the use of the iterator will not
     * cause them to be retained any longer than they otherwise would have been
     * retained.
     */
    @Override
    public Iterator<Map.Entry<K,WeakReference<V>>> entryIterator() {

        return map.entrySet().iterator();
        
    }

//    /**
//     * Clears stale references from the backing {@link HardReferenceQueue}.
//     * <p>
//     * Note: Evictions from the backing hard reference queue are driven by
//     * touches (get, put, remove). This means that the LRU entries in the map
//     * will not be cleared from the backing {@link HardReferenceQueue}
//     * regardless of their age. Applications may invoke this method occasionally
//     * (in general, with a delay of not less than the configured time) in order
//     * to ensure that stale references are cleared in the absence of touched and
//     * thus may become weakly reachable in a timely fashion.
//     * 
//     * @see HardReferenceQueue#evictStaleRefs()
//     */
//    public void clearStaleRefs() {
//        
//        synchronized(queue) {
//            
//            queue.evictStaleRefs();
//            
//        }
//
//        /*
//         * Note: I double that this will notice any stale references that we
//         * cleared above because the garbage collector is not synchronous with
//         * us here.
//         */
//        removeClearedEntries();
//        
//    }

    /**
     * Factory for new weak references. The default implementation uses a
     * {@link WeakReference} unless <code>referenceQueue!=null</code>, in which
     * case it uses {@link WeakRef} to pair the key with the
     * {@link WeakReference}. This may be extended if you need to track
     * additional information in the map entries.
     * 
     * @param k
     *            The key.
     * @param v
     *            The value.
     * @param referenceQueue
     *            The {@link ReferenceQueue} used to remove map entries whose
     *            {@link WeakReference}s have been cleared (optional).
     * 
     * @return An instance of a class extending {@link WeakReference} -or- an
     *         instance of a class extending {@link WeakRef} if
     *         <code>referenceQueue!=null</code>.
     */
    protected WeakReference<V> newWeakRef(final K k, final V v,
            final ReferenceQueue<V> referenceQueue) {

        if (referenceQueue == null) {

            return new WeakReference<V>(v);

        }

        return new WeakRef<K, V>(k, v, referenceQueue);

    }

    /**
     * Adds the key to the weak reference.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @param <K>
     * @param <V>
     */
    protected static class WeakRef<K,V> extends WeakReference<V> {
        
        private final K k;
        
        public WeakRef(final K k, final V v, final ReferenceQueue<V> queue) {

            super(v, queue);

            this.k = k;

        }

        @Override
        public String toString() {
			return super.toString() + "{key=" + k + ",val=" + get() + "}";
        }
        
    }

}
