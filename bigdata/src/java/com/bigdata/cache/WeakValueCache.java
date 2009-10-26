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

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;

/**
 * <p>
 * A memory sensitive cache using weak references for its values and object ids
 * for its keys and backed by the CRUD operations of the persistence layer,
 * which is assumed to implement a hard reference LRU or similar cache policy.
 * </p>
 * <p>
 * Performance can be dramatically effect by the selection of the size of the
 * LRU cache and by the choice of soft vs. weak references. The interaction of
 * these parameters and the choice of the load factor for the weak value and LRU
 * caches is non-trivial. Good performance is observed for default settings, but
 * you can see 2-4x or better improvements through a tuned cache. You need a
 * performance benchmark in order to tune your cache. The best performance
 * benchmark is a solid and representative sample of real operations performed
 * by your application, e.g., loading data, navigating data, and querying data.
 * </p>
 * <p>
 * If you use soft references then the garbage collector will defer collection
 * of application objects until it determines that it needs the memory. In
 * contrast the garbage collector ignores the presence of weak references when
 * deciding whether or not to clear the references for an object. The backing
 * LRU cache policy MUST be using hard references, so in any case nothing will
 * be removed from the weak value cache until it has been first evicted from the
 * backing LRU.
 * </p>
 * <p>
 * Finalizers MUST NOT be relied on to effect the persistent state of objects in
 * this cache since (a) finalizers may never run; and (b) finalizers will only
 * run after references to the object in the cache have been cleared.
 * </p>
 * <p>
 * Since dirty objects that are evicted from the hard reference cache are
 * installed on the database, it is NOT possible to have an object in the weak
 * cache that is (a) dirty; and (b) not also in the hard reference cache.
 * Therefore the <i>dirty</i> flag is <em>only</em> maintained by the hard
 * reference cache.  When an object no longer exists in the hard reference cache
 * but it still present in the weak reference cache, a {@link #get(long)} will 
 * cause the object to be installed into the hard reference cache and the object
 * will be marked as <strong>clean</strong>.
 * <p>
 * This implementation is synchronized.
 * </p>
 * 
 * @param K The key type.
 * @param T The value type.
 * 
 * @version $Id$
 * @author thompsonbry
 * 
 * @todo Consider removing synchronization for use in a single threaded context.
 */
final public class WeakValueCache<K,T>
    implements ICachePolicy<K,T>
{

    protected static final Logger log = Logger.getLogger(WeakValueCache.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * Default value for the initial capacity (1000).
     */
    static public final int INITIAL_CAPACITY = 1000;
    
    /**
     * Default value for the load factor (0.75).
     */
    static public final float LOAD_FACTOR = 0.75f;
    
    /**
     * The initial capacity of the cache.
     */
    private int _initialCapacity; 
    
    /**
     * The load factor for the cache.
     */
    private float _loadFactor;
    
    /**
     * #of "put" operations. Put operations either result in an insert or a
     * "touch".
     */
    private int _nput     = 0;
    
    /**
     * #of "put" operations that wind up inserting the object into the weak
     * value cache.
     */
    private int _ninsert  = 0;
    
    /**
     * #of "put" operations that wind up touching the object in the weak value
     * cache. A touch in the weak value cache does not cause any change in the
     * state of the weak value cache.
     */
    private int _ntouch   = 0;
    
    /**
     * #of cache tests.
     */
    private int _ntest    = 0;
    
    /**
     * #of successful cache tests.
     */
    private int _nsuccess = 0;
    
    /**
     * #of cache entries which were cleared.
     */
    private int _nclear   = 0;
    
    /**
     * #of cache entries which were explicitly removed.
     */
    private int _nremove  = 0;

    /**
     * The high tide for the cache is the largest size which it achieves. The
     * size of the cache grows as objects are added to the cache and shrinks as
     * references are cleared and as objects are explicitly removed from the
     * cache.
     */
    private int _highTide = 0;
        
    /**
     * The hash map which is the basis for the cache. The keys are the object
     * identifiers. The values are weak references to the objects inserted into
     * the cache.
     * 
     * @see WeakCacheEntry
     */
    private Map<K,IWeakRefCacheEntry<K,T>> _cache;
    
    /**
     * Reference queue for weak references in entered into the cache.
     * A weak reference will appear on this queue once the reference
     * has been cleared.
     */
    private ReferenceQueue<T> _queue;
    
    /**
     * The delegate for the hard reference cache policy implemented by the
     * persistence layer.
     */
    final private ICachePolicy<K,T> _delegate;
    
    final private IWeakRefCacheEntryFactory<K,T> _factory;

    final private IClearReferenceListener<K> _clearReferenceListener;
    
    /**
     * An optional listener that is invoked when we notice a cleared reference.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IClearReferenceListener<K> {
        
        public void cleared(K key);
        
    }
    
    public WeakValueCache( ICachePolicy<K,T> delegate )
    {
        
        this( INITIAL_CAPACITY, LOAD_FACTOR, delegate, new WeakCacheEntryFactory<K,T>() );
        
    }

    public WeakValueCache( ICachePolicy<K,T> delegate, IWeakRefCacheEntryFactory<K,T> factory )
    {
    
        this( INITIAL_CAPACITY, LOAD_FACTOR, delegate, factory );

    }
    
    /**
	 * Designated constructor.
	 * 
	 * @param initialCapacity
	 *            May be used to reduce re-hashing by starting with a larger
	 *            initial capacity in the {@link HashMap} with weak values.
	 *            (The capacity of the delegate hard reference cache map is
	 *            configured separately.)
	 * 
	 * @param loadFactor
	 *            May be used to bias for speed vs space in the {@link HashMap}.
	 * 
	 * @param delegate
	 *            The delegate which MUST implement a hard reference cache
	 *            policy such as LRU.
	 * 
	 * @param factory
	 *            The factory which will generate the weak reference value
	 *            objects for this cache.
	 * 
	 * @exception IllegalArgumentException
	 *                if the initialCapacity is negative.
	 * @exception IllegalArgumentException
	 *                if the loadFactor is non-positive.
	 */
    
    public WeakValueCache
    	( int initialCapacity,
    	  float loadFactor,
    	  ICachePolicy<K,T> delegate,
    	  IWeakRefCacheEntryFactory<K,T> factory
    	  ) {
        
        this(initialCapacity, loadFactor, delegate, factory, null/*clearReferenceListener*/);
        
    }
    
    public WeakValueCache
    ( int initialCapacity,
      float loadFactor,
      ICachePolicy<K,T> delegate,
      IWeakRefCacheEntryFactory<K,T> factory,
      IClearReferenceListener<K> clearReferenceListener
      )
    {
    
        if( delegate == null || factory == null ) {
            
            throw new IllegalArgumentException();
           
        }
        
        _initialCapacity = initialCapacity;
        
        _loadFactor = loadFactor;

        /*
         * FIXME Changing to concurrent hash map and remove some of the
         * synchronization constraints, e.g., by defining an atomic
         * putIfAbsent() method. (I have not review the coupling between the
         * outer hash map and the inner LRU, but it seems likely that the LRU
         * can be an "approximate" least-recently-used order so all we need is a
         * consistent way in which to update that order. Also, the
         * ConcurrentHashMap requires an estimate of the concurrency level when
         * using a loadFactor. There is going to be a relationship between the
         * size of the LRU, the #of distinct objects that are likely in the map,
         * the probability that N threads will have an overlapping "touch" set,
         * and the maximum reasonable concurrency.)
         */
        _cache = new HashMap<K,IWeakRefCacheEntry<K,T>>( initialCapacity, loadFactor );
        
        _queue = new ReferenceQueue<T>();
        
        _delegate = delegate;
        
        _factory = factory;
        
        _clearReferenceListener = clearReferenceListener;
        
    }

    /**
     * The delegate hard reference cache.
     */
    public ICachePolicy<K,T> getDelegate() {
        return _delegate;
    }
    
    synchronized public void clear()
    {

        reportStatistics( true );
        
        _cache = new HashMap<K,IWeakRefCacheEntry<K,T>>( _initialCapacity, _loadFactor );
        
        _queue = new ReferenceQueue<T>();
        
        _delegate.clear();

    }
    
    /**
     * Report and optionally clear the cache statistics.
     * 
     * @param resetCounters
     *            When true the counters will be reset to zero.
     * 
     * @todo modify to use {@link CounterSet}.
     */
    synchronized public void reportStatistics( boolean resetCounters )
    {
        
        int size = _cache.size();
        
        double hitRatio = ((double)_nsuccess/_ntest);
        
        if(INFO) log.info
           ( "WeakValueCache"+
             ": initialCapacity="+_initialCapacity+
             ", size="+size+
             ", highTide="+_highTide+
             ", nput="+_nput+
             ", ninsert="+_ninsert+
             ", ntouch="+_ntouch+
             ", nremove="+_nremove+
             ", nclear="+_nclear+
             ", ntest="+_ntest+
             ", nsuccess="+_nsuccess+
             ", hitRatio="+hitRatio
             );
        
        if( resetCounters ) {
            
            _highTide = _nput = _ninsert = _ntouch = _nremove = _nclear = _ntest = _nsuccess = 0;
            
        }
        
    }
    
    /**
	 * This reports some statistics gathered during the cache use. The execution
	 * of this finalizer is NOT required for the correct functioning of the
	 * cache.
	 */
    protected void finalize()
    	throws Throwable
    {
        
        super.finalize();
        
        reportStatistics( false );
        
    }
        
    synchronized public void put( K oid, T obj, boolean dirty )
    {

        _nput++;
        
        _delegate.put( oid, obj, dirty );
        
        /*
         * Note: To the extent that this is a common operation it does not make
         * sense to trade off speed vs space using the weak reference cache load
         * factor. If this method is called on each update() of a persistent
         * object, then speed is very important for _cache.
         */
        
        // Note: when _cache is an LRU this re-orders the entry in the LRU (if it exists). 
        IWeakRefCacheEntry<K,T> entry = _cache.get( oid );
        
        final Object value = ( entry == null ? null : entry.getObject() );

        if( value == null ) {

        	entry = _factory.newCacheEntry( oid, obj, _queue );
        	
            _cache.put( oid, entry );
            
            _ninsert++;

        } else {

            /*
             * Since the object already exists in the weak value cache verify
             * that we are not being asked to replace the object under this key
             * with a different object.
             */

            if( value != obj ) {
                    
                throw new IllegalStateException
                   ( "Attempting to change the object in cache under this key: key="+oid+", obj="+obj
                   );
                
            }

//            entry.setDirty( dirty );
            
            _ntouch++;

        }
        
        final int size = _cache.size();
        
        if( size > _highTide ) {
            
            _highTide = size;
            
        }
        
        removeClearedEntries();
        
    }
    
    synchronized public T get( K oid )
    {

        _ntest++;
        
        T obj = _delegate.get( oid );

        if( obj != null ) {
            
            _nsuccess++;
            
        } else {
            
            final IWeakRefCacheEntry<K,T> entry = _cache.get
               ( oid 
                 );
            
            if( entry != null ) {
                
                obj = entry.getObject();
                
                if( obj != null ) {
                    /*
                     * Note: Dirty objects are installed when they are evicted
                     * on the hard reference cache and it is impossible for an
                     * object to be dirty if it is not found on the hard
                     * reference cache. Therefore we always set the dirty flag
                     * to [false] when an object is found in the weak reference
                     * cache and needs to be installed on the inner hard
                     * reference cache.
                     */
                    _delegate.put( oid, obj, false );
                    
                    _nsuccess++;
                    
                }
                
            }
            
        }
        
        removeClearedEntries();

        return obj;
        
    }
    
    synchronized public T remove( K oid )
    {
        
        _delegate.remove( oid );
        
        IWeakRefCacheEntry<K,T> entry = _cache.remove( oid );

        _nremove++;
        
        removeClearedEntries();
        
        if( entry != null ) {
            
            return entry.getObject(); 
            
        } else {
            
            return null;
            
        }
        
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
    private void removeClearedEntries()
    {
        
        int counter = 0; 
        
        for( Reference<? extends T> ref = _queue.poll(); ref != null; ref = _queue.poll() ) {
            
            K oid = ((IWeakRefCacheEntry<K,T>)ref).getKey();
            
            if( _cache.get( oid ) == ref ) {
                
                if( DEBUG ) {

                    log.debug( "Removing cleared reference: key="+oid );
                    
                }
                    
                _cache.remove( oid );

                if(_clearReferenceListener!=null) {
                    
                    _clearReferenceListener.cleared( oid );
                    
                }
                
                counter++;
                
                _nclear++;
                
            }
            
        }
        
        if( counter > 1 ) {
            
            if( INFO ) {
                
                log.info( "Removed "+counter+" cleared references" );
                
            }
            
        }
        
    }

    /**
     * Sets the listener on the delegate.
     */
    public void setListener(ICacheListener<K,T> listener) {
       _delegate.setListener( listener ); 
    }
    
    /**
     * Return the listener on the delegate.
     */
    public ICacheListener<K,T> getCacheListener() {
        return _delegate.getCacheListener();
    }

    /**
     * <p>
     * Visits objects in the delegate cache in the order defined by the
     * delegate.
     * </p>
     * <p>
     * Note: Objects evicted from the hard reference cache that are still weakly
     * reachable are no longer accessible from the weak cache iterators. This is
     * consistent with the policy that dirty objects are installed onto pages in
     * the persistence layer when they are evicted from the inner hard reference
     * cache and provides a fast iterator mechanism for scanning the object
     * cache. While it means that you are not able to fully enumerate the
     * entries in the weak reference cache, when integrated with a persistence
     * layer handling installation of dirty objects onto pages, objects that are
     * not visitable are guaranteed to be clean.
     * </p>
     * <p>
     * Note: While the iterator supports removal, its behavior is delegated to
     * the backing hard reference cache. Therefore, removal causes the entry to
     * be removed from the backing hard reference cache but NOT the
     * {@link WeakValueCache}. In order to remove the entry from the
     * {@link WeakValueCache} as well you can use {@link #entryIterator()} and
     * then also delete the entry under the last visited key.
     * </p>
     * 
     * @return Iterator visiting objects from the delegate hard reference cache.
     */
    public Iterator<T> iterator() {
        return _delegate.iterator();
    }
    
    /**
     * <p>
     * Visits entries in the delegate cache in the order defined by the
     * delegate.
     * </p>
     * <p>
     * Note: Objects evicted from the hard reference cache that are still weakly
     * reachable are no longer accessible from the weak cache iterators. This is
     * consistent with the policy that dirty objects are installed onto pages in
     * the persistence layer when they are evicted from the inner hard reference
     * cache and provides a fast iterator mechanism for scanning the object
     * cache. While it means that you are not able to fully enumerate the
     * entries in the weak reference cache, when integrated with a persistence
     * layer handling installation of dirty objects onto pages, entries that are
     * not visitable are guaranteed to be clean.
     * </p>
     * <p>
     * Note: While the iterator supports removal, its behavior is delegated to
     * the backing hard reference cache. Therefore, removal causes the entry to
     * be removed from the backing hard reference cache but NOT the
     * {@link WeakValueCache}. In order to remove the entry from the
     * {@link WeakValueCache} you MUST also delete the entry under the last
     * visited key.
     * </p>
     * 
     * @return Iterator visiting {@link ICacheEntry} objects from the delegate
     *         hard reference cache.
     */
    public Iterator<ICacheEntry<K,T>> entryIterator() {
        return _delegate.entryIterator();
    }
    
//    /**
//	 * The #of entries in the weak reference cache. Entries may be
//	 * deterministically {@link #remove(long) removed} from the cache. However
//	 * entries are also cleared from the cache once the object in that entry
//	 * becomes weakly reachable. The number of entries in the cache is therefore
//	 * a non-deterministic function of the running system since entries whose
//	 * referents have become weakly reachable may or may not have been placed on
//	 * the {@link ReferenceQueue} used to detect and clear weakly reachable
//	 * entries. Once placed on the reference queue, they may be cleared
//	 * deterministically.
//	 * 
//	 * @return The #of entries in the map.
//	 */
    /**
     * The #of entries in the backing hard reference cache.  The weak reference
     * cache will often contain additional entries, but those entries are not
     * reported by this method and can not be visited by either {@link #iterator()}
     * or {@link #entryIterator()}.
     * 
     * @see ICachePolicy
     */
    public int size() {
//    	removeClearedEntries();
//    	return _cache.size();
    	return _delegate.size();
    }

    /**
     * The capacity of the backing hard reference cache.
     */
    public int capacity() {
    	return _delegate.capacity();
    }
    
}
