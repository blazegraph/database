package com.bigdata.cache;

/**
 * Interface for reporting cache evictions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 */
public interface HardReferenceQueueEvictionListener<T> {

    /**
     * Notice that a reference is being evicted from the cache. This notice does
     * NOT imply that the cache no longer holds a reference to this object,
     * neither does it imply that the object is dirty.
     * 
     * @param cache
     *            The cache on which the listener is registered.
     * @param ref
     *            The reference that is being evicted from the cache.
     */
    public void evicted(HardReferenceQueue<T> cache, T ref);
    
}
