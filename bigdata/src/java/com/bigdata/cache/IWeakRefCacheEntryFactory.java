/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Dec 13, 2005
 */
package com.bigdata.cache;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;

/**
 * Interface supports choice of either weak or soft references for cache
 * entries and makes it possible for the application to extend the metadata
 * associated with and entry in the {@link WeakValueCache}.
 * 
 * @author thompsonbry
 * @version $Id$
 */
public interface IWeakRefCacheEntryFactory<K,T>
{

    /**
     * Creates a weak reference object to serve as the value in the cache for
     * the given application object.
     * 
     * @param key
     *            The object identifier.
     * 
     * @param obj
     *            The application object.
     * 
     * @param queue
     *            The weak or soft reference object must be created such that it
     *            will appear on this queue when the reference is cleared by the
     *            garbage collector.
     * 
     * @return The new cache entry for that application object.
     * 
     * @see WeakReference
     * @see SoftReference
     */
    
    public IWeakRefCacheEntry<K,T> newCacheEntry( K key, T obj, ReferenceQueue<T> queue );
    
}
