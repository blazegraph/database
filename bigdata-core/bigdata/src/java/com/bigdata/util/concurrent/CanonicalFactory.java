/*

 Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

 Contact:
 SYSTAP, LLC DBA Blazegraph
 2501 Calvert ST NW #106
 Washington, DC 20008
 licenses@blazegraph.com

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
package com.bigdata.util.concurrent;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;

import com.bigdata.cache.ConcurrentWeakValueCache;

/**
 * A pattern for a canonicalizing factory based on a map with weak values.
 *
 * @param <KEY>
 * @param <VAL>
 * @param <STATE>
 * 
 * @author thompsonbry
 */
abstract public class CanonicalFactory<KEY, VAL, STATE> {

	/**
	 * Canonicalizing mapping.
	 */
//	private WeakValueCache<KEY, VAL> cache;
	private ConcurrentWeakValueCache<KEY,VAL> cache;

	/**
	 * 
	 * @param queueCapacity
	 *            The capacity of the backing hard reference queue. This places
	 *            a lower bound on the #of instances which will be retained by
	 *            the factory.
	 */
	public CanonicalFactory(final int queueCapacity) {

//		cache = new WeakValueCache<KEY, VAL>(new LRUCache<KEY, VAL>(queueCapacity));
		cache = new ConcurrentWeakValueCache<KEY, VAL>(queueCapacity);

	}

	/**
	 * Canonical factory pattern.
	 * 
	 * @param key
	 *            The key.
	 * @param state
	 *            Additional state from the caller which will be passed through
	 *            to {@link #newInstance(Object, Object)} when creating a new
	 *            instance (optional).
	 * 
	 * @return The instance paired with that key.
	 * 
	 * @throws IllegalArgumentException
	 *             if the key is <code>null</code>.
	 */
	public VAL getInstance(final KEY key, final STATE state) {

		if (key == null)
			throw new IllegalArgumentException();

		// check first w/o lock.
		VAL val = cache.get(key);

		if (val != null) {
			/*
			 * Fast code path if entry exists for that key. This amortizes the
			 * lock costs by relying on the striped locks of the CHM to provide
			 * less lock contention.
			 */
			return val;
		}
		
		// obtain lock
		synchronized (cache) {

			// check with lock held
			val = cache.get(key);

			if (val == null) {

				// create an instance
				val = newInstance(key,state);

				// pair that instance with the key in the map.
//				cache.put(key, val, true/* dirty */);
				cache.put(key, val);

			}

			return val;

		}

	}

	/**
	 * Remove an entry from the cache.
	 * <p>
	 * Note: It is sometimes necessary to clear a cache entry. For example, if a
	 * persistent resource is destroyed it may be necessary to discard the cache
	 * entry to avoid inappropriate carry over via the cache if the resource is
	 * then recreated.
	 * 
	 * @param key
	 *            The key for the entry.
	 *            
	 * @throws IllegalArgumentException
	 *             if the key is <code>null</code>.
	 */
	public void remove(KEY key) {
		
        if (key == null)
            throw new IllegalArgumentException();
        
		cache.remove(key);
		
	}

	/**
	 * Create an instance which will be associated with the key in the
	 * {@link CanonicalFactory}.
	 * 
	 * @param key
	 *            The key.
	 * @param state
	 *            Additional state used to initialize the new instance
	 *            (optional).
	 * 
	 * @return The new instance to be paired with that key.
	 */
	abstract protected VAL newInstance(KEY key,STATE state);
	
    /**
     * Clear all entries from the cache.
     */
    public void clear() {
        
        cache.clear();
        
    }
    
    /**
     * An iterator that visits the entries in the map. You must test the weak
     * reference for each entry in order to determine whether its value has been
     * cleared as of the moment that you request that value. The entries visited
     * by the iterator are not "touched" so the use of the iterator will not
     * cause them to be retained any longer than they otherwise would have been
     * retained.
     */
    public Iterator<Map.Entry<KEY,WeakReference<VAL>>> entryIterator() {

        return cache.entryIterator();
        
    }

}
