package com.bigdata.cache;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * A low-contention/high concurrency weak value cache. This class can offer
 * substantially less lock contention and hence greater performance than the
 * {@link WeakValueCache}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <K>
 *            The generic type of the keys.
 * @param <V>
 *            The generic type of the values.
 */
public class ConcurrentWeakValueCache<K, V> {

    protected static final Logger log = Logger.getLogger(ConcurrentWeakValueCache.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * A concurrency-savvy map.
     */
    final private ConcurrentHashMap<K, WeakReference<V>> map;

    /**
     * Used to ensure that the [cacheCapacity] MRU weak references are not
     * finalized.
     */
    final private HardReferenceQueue<V> queue;

    /**
     * Reference queue for weak references in entered into the cache. A weak
     * reference will appear on this queue once the reference has been cleared.
     */
    final private ReferenceQueue<V> referenceQueue;
    
    /**
     * Uses the default queue capacity (16), load factor (0.75) and
     * concurrency level (16).
     */
    public ConcurrentWeakValueCache() {

        this(16/* queueCapacity */);

    }

    /**
     * Uses the default load factor (0.75) and concurrency level (16).
     * 
     * @param queueCapacity
     *            The queue capacity.
     */
    public ConcurrentWeakValueCache(final int queueCapacity) {

        this(queueCapacity, 0.75f/* loadFactor */, 16/* concurrencyLevel */);

    }

    /**
     * Uses the specified values.
     * 
     * @param queueCapacity
     *            The queue capacity.
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
     * Uses the specified values.
     * 
     * @param queueCapacity
     *            The capacity of the hard reference queue. This places a
     *            minimum upper bound on the #of objects in the cache. If the
     *            application holds hard references to more than this many
     *            objects, e.g., in various threads, collections, etc., then the
     *            cache size can be larger than the queue capacity. Likewise, if
     *            the JVM takes too long to finalize weakly reachable
     *            references, then the cache size can exceed this limit even if
     *            the application does not hold ANY hard references to the
     *            objects in the cache. For these reasons, this class uses an
     *            initialCapacity for the inner {@link ConcurrentHashMap} that
     *            is larger than the <i>queueCapacity</i>. This helps to
     *            prevent resizing the {@link ConcurrentHashMap} which is a
     *            relatively expensive operation.
     * @param loadFactor
     *            The load factor.
     * @param concurrencyLevel
     *            The concurrency level.
     * @param removeClearedReferences
     *            When <code>true</code> the cache will remove entries for
     *            cleared references. When <code>false</code> those entries
     *            will remain in the cache.
     */
    public ConcurrentWeakValueCache(final int queueCapacity,
            final float loadFactor, final int concurrencyLevel,
            final boolean removeClearedReferences) {

        /*
         * We set the initial capacity of the ConcurrentHashMap to be larger
         * than the capacity of the hard reference queue. This helps to avoid
         * resizing the ConcurrentHashMap, which is relatively expensive.
         */
        map = new ConcurrentHashMap<K, WeakReference<V>>(queueCapacity * 2,
                loadFactor, concurrencyLevel);

        queue = new HardReferenceQueue<V>(null/* listener */, queueCapacity);

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
    public V get(final K k) {

        final WeakReference<V> ref = map.get(k);

        if (ref != null) {

            final V v = ref.get();

            if (v != null) {

                synchronized (queue) {

                    queue.append(v);

                }

                return v;

            }

        }

        // Note: Done by put().
//        removeClearedEntries();
        
        return null;

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
    public V put(final K k, final V v) {

        // new reference.
        final WeakReference<V> ref = referenceQueue == null //
                ? new WeakReference<V>(v)
                : new WeakRef<K, V>(k, v, referenceQueue)
                ;
        
        // add to cache.
        final WeakReference<V> oldRef = map.putIfAbsent(k, ref);

        final V oldVal = oldRef == null ? null : oldRef.get();

        if (oldVal == null) {

            // no reference under that key.
            synchronized (queue) {

                // put it onto the hard reference queue.
                queue.append(v);

            }
            
            return null;

        }

        removeClearedEntries();
        
        return oldVal;
        
    }

    public V remove(K k) {
        
        final WeakReference<V> ref = map.remove( k );

        removeClearedEntries();
        
        if( ref != null ) {
            
            return ref.get(); 
            
        }
        
        return null;
        
    }
    
    /**
     * <p>
     * Invoked from various methods to remove any objects whose weak reference
     * has been cleared from the cache. This method does not block and only
     * polls the {@link ReferenceQueue}. We do not clear entries from the
     * delegate hard reference cache because it is not possible to have a weak
     * or soft reference cleared while a hard reference exists, so it is not
     * possible to have an entry in the hard reference cache for a reference
     * that is being cleared here.
     * </p>
     */
    final private void removeClearedEntries()
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

                map.remove(k);

// if (_clearReferenceListener != null) {
//
//                    _clearReferenceListener.cleared(k);
//
//                }
                
                counter++;
                
//                _nclear++;
                
            }
            
        }
        
        if( counter > 1 ) {
            
            if( INFO ) {
                
                log.info("Removed " + counter + " cleared references");
                
            }
            
        }
        
    }

    /**
     * Adds the key to the weak reference.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <K>
     * @param <V>
     */
    private static class WeakRef<K,V> extends WeakReference<V> {
        
        private final K k;
        
        public WeakRef(final K k, final V v, final ReferenceQueue<V> queue) {

            super(v, queue);

            this.k = k;

        }

    }

}
