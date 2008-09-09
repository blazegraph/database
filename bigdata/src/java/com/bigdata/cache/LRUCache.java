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
 * Created on Apr 19, 2006
 */
package com.bigdata.cache;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.log4j.Logger;

/**
 * Hard reference hash map with Least Recently Used ordering over entries. The
 * keys are object identifiers. The values are cache entries wrapping the
 * identified objects.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 * 
 * @todo Consider removing synchronization for use in a single threaded context.
 * 
 * @todo This can be replaced by a hard reference ring buffer that scans the
 *       last N entries to minimize churn. See {@link HardReferenceQueue}. This
 *       will change the delegation interfaces since the ring buffer does NOT
 *       support random access by the identifier.
 */

public class LRUCache<K,T> implements ICachePolicy<K,T>
{
 
    protected static final Logger log = Logger.getLogger(LRUCache.class);
    
    /**
     * The maximum capacity of the cache.
     */
    private final int capacity;
    
    /**
     * The hash map from keys to entries wrapping cached object references.
     */
    private final Map<K,Entry<K,T>> map;

    /**
     * The entry which is first in the ordering (the
     * <em>least recently used</em>) and <code>null</code> iff the cache is
     * empty.
     */
    private Entry<K,T> first = null;

    /**
     * The entry which is last in the ordering (the <em>most recently used</em>)
     * and <code>null</code> iff the cache is empty.
     */
    private Entry<K,T> last = null;

    /**
     * The load factor for the internal hash table.
     */
    private final float loadFactor;

    private long highTide = 0, ninserts = 0, ntests = 0, nsuccess = 0;

    public double getHitRatio() {
        
        return ((double) nsuccess / ntests);
        
    }

    public long getInsertCount() {
        
        return ninserts;
        
    }
    
    public long getTestCount() {
        
        return ntests;
        
    }
    
    public long getSuccessCount() {
        
        return nsuccess;
        
    }
    
    /**
     * The cache eviction listener.
     */
    private ICacheListener<K,T> _listener = null;
    
    /**
     * Create an LRU cache with a default load factor of <code>0.75</code>.
     * 
     * @param capacity
     *            The capacity of the cache.
     */
    public LRUCache( int capacity )
    {
        this( capacity, 0.75f );
    }
    
    /**
     * Create an LRU cache with the specific capacity and load factor.
     * 
     * @param capacity
     *            The capacity of the cache (must be positive).
     * @param loadFactor
     *            The load factor for the internal hash table.
     */
    
    public LRUCache( int capacity, float loadFactor )
    {
        
        if( capacity <= 0 ) {
            
            throw new IllegalArgumentException();
            
        }
        
        this.capacity = capacity;
        
        this.loadFactor = loadFactor;
        
        this.map = new HashMap<K,Entry<K,T>>( capacity, loadFactor );
        
    }
    
    public void setListener(ICacheListener<K,T> listener) {
        _listener = listener;
    }

    public ICacheListener<K,T> getCacheListener() {
        return _listener;
    }
    
    /**
     * <p>
     * Visits objects in the cache in LRU ordering (the least recently used
     * object is visited first).
     * </p>
     * <p>
     * The returned iterator is NOT thread safe. It supports removal but does
     * NOT support concurrent modification of the cache state. Normally the
     * iterator is used during a commit and the framework guarentees that
     * concurrent
     * </p>
     */

    public Iterator<T> iterator() {
        return new LRUIterator<K,T>( this, true );
    }

    /**
     * <p>
     * Visits {@link ICacheEntry entries} in the cache in LRU ordering (the
     * least recently used object is visited first).
     * </p>
     * <p>
     * The returned iterator is NOT thread safe. It supports removal but does
     * NOT support concurrent modification of the cache state. Normally the
     * iterator is used during a commit and the framework guarentees that
     * concurrent
     * </p>
     */
    public Iterator<ICacheEntry<K,T>> entryIterator() {
        return new LRUIterator<K,T>( this, false );
    }
    
    public void clear() {
        map.clear();
        first = last = null;
        resetStatistics();
    }
    
    public void resetStatistics() {
        highTide = ninserts = ntests = nsuccess = 0;
    }
    
    /**
     * The #of entries in the cache.
     */
    synchronized public int size() {
        return map.size();
    }
    
    /**
     * The capacity of the cache.
     */
    public int capacity() {
        // this is final data and does not need to be synchronized.
        return capacity;
    }
    
    /**
     * Writes cache performance statistics.
     */
    protected void finalize() throws Throwable
    {
    
        if (log.isInfoEnabled())
            log.info(getStatistics());
        
    }

    public String getStatistics() {

        return "LRUCache" + //
                ": capacity=" + capacity + //
                ", loadFactor=" + loadFactor + //
                ", highTide=" + highTide + //
                ", ninserts=" + ninserts + //
                ", ntests=" + ntests + //
                ", nsuccess=" + nsuccess + //
                ", hitRatio=" + ((double) nsuccess / ntests)//
        ;
        
    }

    /**
	 * <p>
	 * Add the object to the hash map under the key if it is not already there
	 * and update the entry ordering (this can be used to touch an entry).
	 * </p>
	 * <p>
	 * Cache evictions are only performed at or <i>over</i> capacity, but not
	 * for reentrant invocations. If a cache eviction causes a nested
	 * {@link #put(long, Object, boolean)} cache enters a temporary
	 * <em>over capacity</em> condition. The nested eviction is effectively
	 * deferred and a new cache entry is created for the incoming object rather
	 * than recycling the LRU cache entry. This temporary over capacity state
	 * exists until the primary eviction event has been handled, at which point
	 * entries are purged from the cache until it has one free entry. That free
	 * entry is then used to cache the incoming object which triggered the outer
	 * eviction event.
	 * </p>
	 * <p>
	 * This is not the only coherent manner in which nested eviction events
	 * could be handled, but it is perhaps the simplest. This technique MUST NOT
	 * be used with an open array hash table since the temporary over capacity
	 * condition would not be supported.
	 * </p>
	 * 
	 * @param key
	 *            The object identifier.
	 * 
	 * @param obj
	 *            The object.
	 */

    synchronized public void put(K key, T obj, boolean dirty )
    {

        assert key != null;

		reentrantPutCounter++;

		try {

			Entry<K,T> entry = map.get(key);

			if (entry == null) {

				if (map.size() >= capacity && reentrantPutCounter == 1) {

					/*
					 * Purge the the LRU position until the cache is just under
					 * capacity.
					 */

					while (map.size() >= capacity) {

						// entry in the LRU position.
						entry = first;

						if (_listener != null) {

							// Notify listener of cache eviction.

							_listener.objectEvicted(entry);

						}

						// remove LRU entry from ordering.
						removeEntry(entry);

						// remove entry from hash map under that key.
						map.remove(entry.key);

					}

					/*
					 * Recycle the last cache entry that we purged.
					 */

					// set key and object on LRU entry.
					entry.key = key;
					entry.obj = obj;
					entry.dirty = dirty;

					// add entry into the hash map.
					map.put(key, entry);

					// add entry into MRU position in ordering.
					addEntry(entry);

				} else {

					/*
					 * Create a new entry and link into the MRU position.
					 * 
					 * Note: This also handles the case of a reentrant
					 * invocation, in which case the cache will be temporarily
					 * over capacity.
					 */

					entry = new Entry<K,T>(key, obj, dirty);

					map.put(key, entry);

					addEntry(entry);

					int count = map.size();

					if (count > highTide) {

						highTide = count;

					}

				}

				ninserts++;

			} else {

				if (entry.obj != obj) {

					throw new IllegalStateException(
							"can not change object under key");

				}

				// Update entry ordering.
				touchEntry(entry);

				// Update dirty flag.
				entry.dirty = dirty;

			}

		} finally {

			reentrantPutCounter--;
			
		}

	}
    
    /**
	 * Used to track and handle reentrant calls to put(). The value of the
	 * counter is the number of times that put() occurs in the stack frame.
	 * E.g., the counter value will be zero when put() is not in the stack
	 * frame, a non-recursive invocation will show a counter value of 1; and a
	 * counter value of 2 or more indicates a reentrant invocation is in
	 * progress.
	 * 
	 * @see #put(long, Object, boolean)
	 */
    private int reentrantPutCounter = 0;

    synchronized public T get( K key )
    {

        assert key != null;
        
        ntests++;
        
        Entry<K,T> entry = map.get( key );
        
        if( entry == null ) {
            
            return null;
            
        }

        touchEntry( entry );
        
        nsuccess++;
        
        return entry.getObject();
        
    }
    
//    synchronized public boolean isDirty( long key ) {
//
//        ntests++;
//        
//        Entry entry = (Entry) map.get( new Long( key ) );
//        
//        if( entry == null ) {
//            
//            return false;
//            
//        }
//        
//        return entry.isDirty();
//
//    }
    
    synchronized public T remove( K key )
    {
        
        assert key != null;

        Entry<K,T> entry = map.remove( key );
        
        if( entry == null ) return null;
        
        removeEntry( entry );
        
        return entry.getObject();
        
    }

    /**
	 * Registers a listener for removeEntry events. This is used by the
	 * {@link LRUIterator} to handle concurrent modifications of the cache
	 * ordering during traversal.
	 */
    
    synchronized protected void addCacheOrderChangeListener( ICacheOrderChangeListener<K,T> l ) {
    	if( _cacheOrderChangeListeners == null ) {
    		_cacheOrderChangeListeners = new CopyOnWriteArraySet<ICacheOrderChangeListener<K,T>>();
    	}
    	_cacheOrderChangeListeners.add(l);
    }
    
    /**
	 * Unregister the listener. This is safe to invoke when the listener is not
	 * registered.
	 * 
	 * @param l
	 *            The listener.
	 */
    synchronized protected void removeCacheOrderChangeListener( ICacheOrderChangeListener<K,T> l ) {
    	if (_cacheOrderChangeListeners == null)
			return;
    	_cacheOrderChangeListeners.remove(l);
    	if( _cacheOrderChangeListeners.size() == 0 ) {
    		_cacheOrderChangeListeners = null;
    	}
    }
    
    private void fireCacheOrderChangeEvent( boolean removed, ICacheEntry<K,T> entry ) {
    	if( _cacheOrderChangeListeners.size() == 0 ) return; 
//    	ICacheOrderChangeListener<T>[] listeners = _cacheOrderChangeListeners
//				.toArray(new ICacheOrderChangeListener[] {});
//    	for( int i=0; i<listeners.length; i++ ) {
//    		ICacheOrderChangeListener<T> l = listeners[ i ];
        Iterator<ICacheOrderChangeListener<K,T>> itr = _cacheOrderChangeListeners.iterator();
        while( itr.hasNext() ) {
            ICacheOrderChangeListener<K,T> l = itr.next();
    		if( removed ) {
    			l.willRemove( entry );
    		} else {
    			throw new UnsupportedOperationException(); // feature is not implemented.
    		}
    	}
	}

    /**
     * Lazily allocated and eagerly freed.
     */
    private CopyOnWriteArraySet<ICacheOrderChangeListener<K,T>> _cacheOrderChangeListeners = null;
    
    protected static interface ICacheOrderChangeListener<K,T> {
    	public void willRemove( ICacheEntry<K,T> entry );
//    	public void didAdd(ICacheEntryEntry);
    }
    
    /**
     * Add an Entry to the tail of the linked list (the MRU position).
     */
    private void addEntry(Entry<K,T> entry) {
        if (first == null) {
            first = entry;
            last = entry;
        } else {
            last.next = entry;
            entry.prior = last;
            last = entry;
        }
    }

    /**
     * Remove an {@link Entry} from linked list that maintains the LRU ordering.
     * The {@link Entry} is modified but not deleted.  The key and value fields 
     * on the {@link Entry} are not modified.  The {@link #first} and {@link #last}
     * fields are updated as necessary.  This method is used when the LRU entry is
     * being evicted and the {@link Entry} will be recycled into the MRU position
     * in the ordering.  You must also remove the entry under that key from the hash
     * map.
     */
    private void removeEntry(Entry<K,T> entry) {
    	if( _cacheOrderChangeListeners != null ) {
    		fireCacheOrderChangeEvent(true, entry);
    	}
        Entry<K,T> prior = entry.prior;
        Entry<K,T> next = entry.next;
        if (entry == first) {
            first = next;
        }
        if (last == entry) {
            last = prior;
        }
        if (prior != null) {
            prior.next = next;
        }
        if (next != null) {
            next.prior = prior;
        }
        entry.prior = null;
        entry.next = null;
    }

    /**
     * Move the entry to the end of the linked list (the MRU position).
     */
    private void touchEntry(Entry<K,T> entry) {

        if (last == entry) {

            return;

        }

        removeEntry(entry);

        addEntry(entry);

    }

    /**
     * Wraps an object with metadata to maintain the LRU ordering.
     * 
     * @version $Id$
     * @author thompsonbry
     */
    
    final static class Entry<K,T> implements ICacheEntry<K,T>
    {
        private K key;
        private T obj;
        private boolean dirty;
        private Entry<K,T> prior;
        private Entry<K,T> next;
        Entry( K key, T obj, boolean dirty )
        {
            this.key = key;
            this.obj = obj;
            this.dirty = dirty;
        }
        public boolean isDirty() {
            return dirty;
        }
        public void setDirty(boolean dirty) {
            this.dirty = dirty;
        }
        public K getKey() {
            return key;
        }
        public T getObject() {
            return obj;
        }
        /**
         * The prior entry (less recently used).
         * 
         * @return The next entry or <code>null</code>.
         */
        public Entry<K,T> getPrior() {
            return prior;
        }
        /**
         * The next entry (more recently used).
         * 
         * @return The next entry or <code>null</code>.
         */
        public Entry<K,T> getNext() {
            return next;
        }
        
        /**
         * Human readable representation used for debugging in test cases.
         */
        public String toString() {
            return "Entry{key=" + key + ",obj=" + obj + ",dirty=" + dirty
					+ ",prior=" + (prior == null ? "N/A" : "" + prior.key)
					+ ",next=" + (next == null ? "N/A" : "" + next.key)+ "}";
        }
    }

    /**
     * <p>
     * Visits entries in the {@link LRUCache} in their natural ordering from LRU
     * to MRU. The iterator will optionally resolve the entries to the
     * application objects associated with each entry. The iterator supports
     * removal but is NOT thread-safe and does not support concurrent
     * modification of the {@link LRUCache}.
     * </p>
     * <p>
     * This class provide fast visitation from LRU to MRU by chasing references.
     * In order to support concurrent modification of the cache order during
     * traversal, an instance uses a protocol by it is informed of changes in
     * the cache order. There are two basic operations that effect the cache
     * order: addEntry and removeEntry. addEntry always inserts the entry in the
     * MRU position, so it can not effect the visitation order. However,
     * removeEntry could unlink the entry that the iterator will use to reach
     * the next entry (via its next reference). The iterator must therefore
     * receive notice when a cache entry is about to be removed. If the entry is
     * the same entry that the iterator would visit next in the LRU to MRU
     * ordering, then the iterator advances its state to the next entry that it
     * would visit. When removeEntry then removes the entry from the cache
     * ordering, the iterator correctly visits the next cache entry in the new
     * ordering.
     * </p>
     * 
     * @version $Id$
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
     *         </a>
     * 
     * @todo In order to support generics, we might need to break this down into
     *       two implementations so that we can have a strongly typed iterator
     *       for both T and ICacheEntry<T>.
     */
    static class LRUIterator<K,T> implements Iterator, ICacheOrderChangeListener<K,T>
    {

        private final LRUCache<K,T> cache;
        private final boolean resolveObjects;
        private Entry<K,T> next;
        private Entry<K,T> lastVisited = null;
        
        /**
         * An iterator that traverses the {@link LRUCache} in the its natural
         * ordering.
         * 
         * @param cache
         *            The {@link LRUCache}.
         *            
         * @param resolveObjects
         *            When true, the iterator will visit the application objects
         *            in the cache. When false it will visit the {@link ICache}
         *            entries themselves.
         */
        LRUIterator( LRUCache<K,T> cache, boolean resolveObjects )
        {
            this.cache = cache;
            this.next = cache.first;
            this.resolveObjects = resolveObjects;
            cache.addCacheOrderChangeListener(this);
        }

        public boolean hasNext() {
            return next != null;
        }

        public Object next() {
            if( next == null ) {
            	removeListener();
                throw new NoSuchElementException();
            }
            /* Optionally resolve the application object vs the entry for that
             * object.
             */
            Object ret = (resolveObjects ? next.obj : next);
            /*
             * Advance the internal state of the iterator.
             */
            lastVisited = next;
            next = next.next;
            if( next == null ) {
            	removeListener();
            }
            /*
             * Return either the cache entry or the resolve application object.
             */
            return ret;
        }

        /**
         * Removes the last visited entry from the cache.
         * 
         * @exception IllegalStateException
         *                if no entry has been visited yet.
         */
        public void remove() {
            if( lastVisited == null ) {
                throw new IllegalStateException();
            }
            cache.map.remove( lastVisited.key );
            cache.removeEntry( lastVisited );
        }

		public void willRemove(ICacheEntry entry) {
			if( entry == next ) {
				next = next.next;
			}
		}
        
		/**
		 * Unregister the iterator as a cache order change listener.  This is
		 * safe to invoke multiple times.
		 */
		private void removeListener() {
			cache.removeCacheOrderChangeListener(this);
		}
    }
    
}
