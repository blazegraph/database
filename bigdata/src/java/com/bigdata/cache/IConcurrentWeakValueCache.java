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
 * Created on Feb 9, 2009
 */

package com.bigdata.cache;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <K>
 * @param <V>
 */
public interface IConcurrentWeakValueCache<K, V> {

    /**
     * Returns the approximate number of keys in the map. Cleared references are
     * removed before reporting the size of the map, but the return value is
     * still approximate as garbage collection by the JVM can cause references
     * to be cleared at any time.
     */
    int size();

    /**
     * The capacity of the backing hard reference queue.
     */
    int capacity();

    /**
     * Clear all entries in the map.
     */
    void clear();

    /**
     * Returns the value for the key.
     * 
     * @param k
     *            The key.
     * 
     * @return The value.
     */
    V get(final K k);

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
    boolean containsKey(final K k);

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
    V put(final K k, final V v);

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
    V putIfAbsent(final K k, final V v);

    /**
     * Remove the entry for the key.
     * 
     * @param k
     *            The key.
     * 
     * @return The previous value associated with the key, or <tt>null</tt> if
     *         there was no mapping for the key or if the entry under the key
     *         has has its reference cleared.
     */
    V remove(final K k);

    /**
     * An iterator that visits the weak reference values in the map. You must
     * test each weak reference in order to determine whether its value has been
     * cleared as of the moment that you request that value.
     */
    Iterator<WeakReference<V>> iterator();

    /**
     * An iterator that visits the entries in the map. You must test the weak
     * reference for each entry in order to determine whether its value has been
     * cleared as of the moment that you request that value.
     */
    Iterator<Map.Entry<K, WeakReference<V>>> entryIterator();

}
