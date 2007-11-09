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

import java.util.Iterator;

/**
 * <p>
 * Interface for cache policy.
 * </p>
 * <p>
 * The semantics of this interface are generally specified in terms of a hard
 * reference cache backing a weak (or soft) reference cache. Examples of methods
 * on the interface whose semantics are determined by the hard reference cache
 * include:
 * <ul>
 * <li>{@link #iterator()} - visitation order and membership is determined by
 * the backing hard reference cache.</li>
 * <li>{@link #entryIterator()} - visitation order and membership is determined
 * by the backing hard reference cache.</li>
 * <li>{@link #size()} - membership is determined by the backing hard reference
 * cache.</li>
 * <li>{@link #capacity()} - the capacity of the backing hard reference cache.</li>
 * <li>{@link #setListener()} - eviction notices are fired when entries are
 * evicted from the hard reference cache.</li>
 * </ul>
 * The rationale for this is that the cache iterator methods are used to perform
 * installs of dirty objects from the cache onto the persitence layer during a
 * commit. Since clear references are not reachable, eviction notices are fired
 * when entries are evicted from the hard reference cache. Those notices must be
 * used to install dirty objects onto the persistence layer, in which case the
 * entry for that object in the weak reference cache is marked as <em>clean</em>.
 * When properly integrated with the persistence layer, this provides a
 * guarentee that the iterators will never fail to visit a dirty entry in the
 * cache. For consistency with the iterator methods, {@link #size()} returns the
 * number of entries in the hard reference cache. Neither visitation of nor
 * counting of all weak cache entries in and of itself is not a forseen use case
 * and those semantics not supported by this interface.
 * </p>
 * 
 * @version $Id$
 * 
 * @author thompsonbry
 * 
 * @todo long oid to Object oid? The advantages of this are: (1) it allows us to
 *       create a flyweight object a long integer and pass it around, thereby
 *       avoiding many conditions under which we mint new Longs; (2) the OID
 *       math may be encapsulated on that flyweight object; (3) we can use a
 *       different OID object for each backend if necessary; (4) we can change
 *       the size of the OID more transparently since more code will not depend
 *       on it being a [long] data type. (The IGenericData would still use long
 *       internally.) (extSer might accept an Object for its writePackedOID()
 *       method).
 * 
 * @todo expose a means to test whether a cache entry is dirty, but consider the
 *       interactions implied for the weak value cache. one way to approach this
 *       is to expose getEntry( long key ). The problem with this is that it
 *       does not encapsulate the odder semantics of the weak value cache.
 */

public interface ICachePolicy<K,T>
{
    
    /**
	 * Sets the cache eviction listener on the hard reference cache. Eviction
	 * notices are fired when objects are evicted from the hard reference cache.
	 * 
	 * @param listener
	 *            The listener or <code>null</code> to remove any listener.
	 */
    public void setListener( ICacheListener<K,T> listener );
    
    /**
     * Return the cache eviction listener.
     */
    public ICacheListener<K,T> getCacheListener();
    
    /**
     * Insert or "touch" this object in the cache.
     * 
     * @param oid
     *            The object identifier.
     * 
     * @param obj
     *            The object.
     * 
     * @param dirty True iff the object is dirty.
     * 
     * @exception IllegalStateException
     *                If a different object is in the cache under the specified
     *                object identifier.
     */
    public void put( K oid, T obj, boolean dirty );
    
    /**
     * Return the indicated object from the cache or null if the object is not
     * in cache.
     * 
     * @param oid
     *            The object identifier.
     * 
     * @return The object or null iff it is not in cache.
     */
    public T get( K oid );
    
//    /**
//     * Return true iff there is a dirty entry in the cache under that key. The
//     * cache ordering is NOT updated by this method.
//     * 
//     * @param oid
//     *            The key
//     * 
//     * @return True iff there is an entry in the cache under that key and its
//     *         dirty flag is true.
//     */
//    public boolean isDirty( long oid );
    
    /**
     * Remove the indicated object from the cache.
     * 
     * @param oid
     *            The object identifier.
     * 
     * @return The object in the cache for that object identifier or
     *         <code>null</code> if there was no object under that identifier.
     */
    public T remove( K oid );

    /**
     * Clear all objects from the cache. This method may be used to reset the
     * cache when a transaction is being rolledback.  Cache eviction notices
     * are NOT fired when this method is called.
     */
    public void clear();

    /**
	 * <p>
	 * Return an iterator that will visit the application objects in the cache.
	 * The visitation order is determined by the hard reference cache policy. If
	 * the cache policy is ordered, then the visitation order reflects that
	 * order.
	 * </p>
	 * 
	 * @see #entryIterator()
	 */
    public Iterator<T> iterator();

    /**
     * <p>
     * Return an iterator that will visit the {@link ICacheEntry} objects in the
     * cache. The visitation order is determined by the hard reference cache
     * policy. If the cache policy is ordered, then the visitation order
     * reflects that order.
     * </p>
     * <p>
     * Note: This method is used to force dirty objects in the cache to the
     * persistence layer during a transaction commit since it visits entries and
     * not referants, thereby providing access to the cache entry metadata.
     * </p>
     * 
     * @return Iterator visiting {@link ICacheEntry} objects. If this is a weak
     *         reference cache, then the iterator visits the entries in the
     *         delegate hard reference cache.
     * 
     * @see ICacheEntry
     * @see #iterator()
     */
    public Iterator<ICacheEntry<K,T>> entryIterator();

    /**
     * Return the #of entries in the hard reference cache.
     * 
     * @return The #of entries in the hard reference cache.
     */
    public int size();
    
    /**
     * Return the capacity of the hard reference cache.
     * 
     * @return The capacity of the hard reference cache.
     */
    public int capacity();
    
}
