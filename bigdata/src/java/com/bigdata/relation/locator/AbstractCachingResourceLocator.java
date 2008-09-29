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
 * Created on Jun 30, 2008
 */

package com.bigdata.relation.locator;

import java.lang.ref.WeakReference;

import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.util.NT;

/**
 * Abstract base class for {@link IResourceLocator}s with caching. The cache
 * uses {@link WeakReference}s so that cache entries will be cleared if the
 * referenced item is cleared.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractCachingResourceLocator<T extends ILocatableResource> implements IResourceLocator<T> {

    private transient WeakValueCache<NT, T> cache;

    private int capacity;
    
    /**
     * The default #of recently located resources whose hard references will be
     * retained by the {@link LRUCache}.
     */
    protected static transient final int DEFAULT_CACHE_CAPACITY = 10;

    /**
     * The cache capacity.
     */
    public int capacity() {
        
        return capacity;
        
    }

    protected AbstractCachingResourceLocator() {

        this(DEFAULT_CACHE_CAPACITY);

    }

    protected AbstractCachingResourceLocator(int capacity) {

        this.capacity = capacity;
        
        this.cache = new WeakValueCache<NT, T>(new LRUCache<NT, T>(capacity));

    }

    /**
     * Looks up the resource in the cache.
     * <p>
     * Note: The caller MUST be synchronized on the named resource.
     * 
     * @param namespace
     * 
     * @param timestamp
     * 
     * @return The relation -or- <code>null</code> iff it is not in the cache.
     */
    protected T get(String namespace, long timestamp) {

        if (namespace == null)
            throw new IllegalArgumentException();

        final T r = cache.get(new NT(namespace, timestamp));
        
        return r;

    }

    /**
     * Places the resource in the cache.
     * <p>
     * Note: The caller MUST be synchronized on the named resource.
     * 
     * @param resource
     *            The relation.
     */
    protected void put(T resource) {
        
        if (resource == null)
            throw new IllegalArgumentException();
        
        final String namespace = resource.getNamespace().toString();

        final long timestamp = resource.getTimestamp();
        
        cache.put(new NT(namespace, timestamp), resource, false/* dirty */);

    }

    /**
     * Clears any resource having the same namespace and timestamp from the
     * cache.
     * <p>
     * Note: The caller MUST be synchronized on the named resource.
     * 
     * @return <code>true</code> iff there was an entry in the cache for the
     *         same resource namespace and timestamp, in which case it was
     *         cleared from the cache.
     */
    protected boolean clear(String namespace, long timestamp) {
        
        if (namespace == null)
            throw new IllegalArgumentException();
        
        if(cache.remove(new NT(namespace,timestamp))!=null) {
            
            return true;
            
        }
        
        return false;
        
    }
    
}
