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
 * Created on Nov 8, 2006
 */

package com.bigdata.cache;

/**
 * <p>
 * A cache for hard references using an LRU policy. References are simply
 * cached, but objects are not recoverable from their reference. In order to
 * make an object recoverable, this cache must be wrapped by a weak reference
 * cache that implements a hash map for discovery of objects using their
 * persistent identifier. The {@link HardReferenceQueue} has a capacity that
 * determines the #of hard references that may be cached before an object is
 * evicted from the cache. Objects may be stored multiple times on the cache but
 * the nscan most recent references are always tested before appending a reference
 * to the cache in order to minimize cache churn when an object is touched
 * repeatedly in close succession. Likewise, eviction does not mean that the
 * object is no longer on the hard reference cache, nor does eviction mean that
 * no hard references to the object exist within the VM. However, eviction is
 * nevertheless used to drive persistence of the object. Since an object may be
 * evicted multiple times without being updated in between evictions, this
 * requires the object to implement a protocol for determining whether or not it
 * is dirty. Eviction is then contigent on that protocol and the dirty state of
 * the object is reset when it is serialized for eviction. In combination with
 * the object's dirty protocol, the hard reference cache can substitute for a
 * commit list.
 * </p>
 * <p>
 * Note: This implementation is NOT synchronized.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <T> The reference type stored in the cache.
 */
public class HardReferenceQueue<T> {

    /**
     * The listener to which cache eviction notices are reported.
     */
    private final HardReferenceQueueEvictionListener<T> listener;
    /**
     * The capacity of the cache.
     */
    private final int capacity;
    /**
     * The hard references. There is no guarentee that the references are
     * distinct. Unused enties are cleared to null so that we do not hold onto
     * hard references after they have been evicted.
     */
    private final T[] refs;
    /**
     * The head (the insertion point for the next reference).
     */
    private int head = 0;
    /**
     * The tail (LRU position).
     */
    private int tail = 0;
    /**
     * The #of references in the cache. The cache is empty when this field is
     * zero. The cache is full when this field equals the {@link #capacity}.
     */
    private int count = 0;

    /**
     * The #of references to scan backwards from the LRU position when testing
     * for whether or not a reference is already in the cache.
     */
    private final int nscan;
    
    /**
     * Defaults the #of references to scan on append requests to 10.
     * 
     * @param listener
     *            The listener on which cache evictions are reported.
     * @param capacity
     *            The maximum #of references that can be stored on the cache.
     *            There is no guarentee that all stored references are distinct.
     */
    public HardReferenceQueue(HardReferenceQueueEvictionListener<T> listener,
            int capacity) {
        
        this( listener, capacity, 10);
        
    }
    
    /**
     * @param listener
     *            The listener on which cache evictions are reported (optional).
     * @param capacity
     *            The maximum #of references that can be stored on the cache.
     *            There is no guarentee that all stored references are distinct.
     * @param nscan
     *            The #of references to scan from the MRU position before
     *            appended a reference to the cache. Scanning is used to reduce
     *            the chance that references that are touched several times in
     *            near succession from entering the cache more than once. The
     *            #of reference tests trads off against the latency of adding a
     *            reference to the cache.
     */
    public HardReferenceQueue(final HardReferenceQueueEvictionListener<T> listener,
            final int capacity, final int nscan) {

//        if (listener == null)
//            throw new IllegalArgumentException();

        if (capacity <= 0)
            throw new IllegalArgumentException();

        if (nscan < 0 || nscan > capacity)
            throw new IllegalArgumentException();
        
        this.listener = listener;
        
        this.capacity = capacity;
        
        this.nscan = nscan;
        
        this.refs = (T[])new Object[capacity];
        
    }

    /**
     * The  listener specified to the constructor.
     */
    final public HardReferenceQueueEvictionListener<T> getListener() {
        
        return listener;
        
    }
    
    /**
     * The cache capacity specified to the constructor.
     */
    final public int capacity() {
        
        return capacity;
        
    }

    /**
     * The #of references that are tested on append requests.
     */
    final public int nscan() {
        
        return nscan;
        
    }
    
    /**
     * The #of references in the cache.  Note that there is no guarentee that
     * the references are distinct.
     * 
     * @return
     */
    final public int size() {

        return count;
        
    }
    
    /**
     * True iff the cache is empty.
     */
    final public boolean isEmpty() {
        
        return count == 0;
        
    }

    /**
     * True iff the cache is full.
     */
    final public boolean isFull() {
        
        return count == capacity;
        
    }
    
    /**
     * Add a reference to the cache. If the reference was recently added to the
     * cache then this is a NOP. Otherwise the reference is appended to the
     * cache. If a reference is appended to the cache and then cache is at
     * capacity, then the LRU reference is first evicted from the cache.
     * 
     * @param ref
     *            The reference to be added.
     * 
     * @return True iff the reference was added to the cache and false iff the
     *         reference was found in a scan of the nscan MRU cache entries.
     */
    final public boolean append(T ref) {
        
        if( ref == null ) throw new IllegalArgumentException();
        
        /*
         * Scan the last nscan references for this reference. If found, return
         * immediately.
         */
        if (nscan > 0 && scanHead(nscan, ref)) {
            
            return false;

        }
        
        if (count == capacity) {

            /*
             * If at capacity, evict the LRU reference.
             */
            
            evict();
            
        }
        
        /*
         * append the reference.
         */

        assert (count < capacity);
        
        refs[head] = ref;
        
        head = (head + 1) % capacity;
        
        count++;
        
        return true;
        
    }

    /**
     * Evict the LRU reference. This is a NOP iff the cache is empty.
     * 
     * @return true iff a reference was evicted.
     * 
     * @see HardReferenceQueueEvictionListener
     */
    final public boolean evict() {
        
        // The cache must not be empty.
        if( count <= 0 ) return false;

        final T ref = refs[tail]; // LRU reference.
        refs[tail] = null; // drop reference.
        count--; // update #of references.
        tail = (tail + 1) % capacity; // update tail.

        // report eviction notice to listener.
        if (listener != null)
            listener.evicted(this, ref);
        
        return true;
        
    }

    /**
     * Clears the cache (sets the head, tail and count to zero) without
     * generating eviction notices.
     * 
     * @param clearRefs
     *            When <code>true</code> the references are explicitly set to
     *            <code>null</code> which can facilitate garbage collection.
     */
    final public void clear(boolean clearRefs ) {
     
        if( clearRefs ) {

            /*
             * Evict all references, clearing each as we go.
             */

            while( count > 0 ) {
                
                refs[tail] = null; // drop LRU reference.
                
                count--; // update #of references.
                
                tail = (tail + 1) % capacity; // update tail.
                
            }
            
        }
        
        // reset to initial conditions.
        
        head = tail = count = 0;
        
    }
    
    /**
     * Evict all references, starting with the LRU reference and proceeding to
     * the MRU reference.
     * 
     * @param clearRefs
     *            When true, the reference are actually cleared from the cache.
     *            This may be false to force persistence of the references in
     *            the cache without actually clearing the cache.
     */
    final public void evictAll(boolean clearRefs) {

        if( clearRefs ) {

            /*
             * Evict all references, clearing each as we go.
             */
            while( count > 0 ) {
                
                evict();
                
            }
            
        } else {

            /*
             * Generate eviction notices but do NOT clear the references.
             * 
             * Note: This uses local variables to shadow the instance variables
             * so that we do not modify the state of the cache as a side effect.
             */
            
            int tail = this.tail;
            
            int count = this.count;
            
            while (count > 0) {
                
                final T ref = refs[tail]; // LRU reference.
                
                count--; // update #of references.
                
                tail = (tail + 1) % capacity; // update tail.

                // report eviction notice to listener.
                if (listener != null)
                    listener.evicted(this, ref);
                
            }
            
        }
        
    }

    /*
     * package private methods used to write the unit tests.
     */

    /**
     * The head index (the insertion point).
     */
    final int head() {
        return head;
    }
    
    /**
     * The tail index (the LRU position).
     */
    final int tail() {
        return tail;
    }
    
    /**
     * The backing array.
     */
    final T[] array() {
        return refs;
    }
    
    /**
     * The reference at the tail of the queue. This is the next reference that
     * will be evicted from the queue.
     * 
     * @todo We can also write getHead(), but note that the {@link #head} is the
     *       insertion point NOT the index of the last reference inserted.
     */
    final public T getTail() {
        
        return refs[tail];
        
    }

    /**
     * Scan the last nscan references for this reference. If found, return
     * immediately.
     * 
     * @param nscan
     *            The #of positions to scan, starting with the most recently
     *            added reference.
     * @param ref
     *            The reference for which we are scanning.
     *            
     * @return True iff we found <i>ref</i> in the scanned queue positions.
     */
    final public boolean scanHead(int nscan, T ref) {
        assert nscan > 0;
        assert ref != null;
        /*
         * Note: This loop goes backwards from the head.  Since the head is the
         * insertion point, we decrement the head position before testing the
         * reference.  If the head is zero, then we wrap around.  This carries
         * the head back to the last index in the array (capacity-1).
         *
         * Note: This uses local variables to shadow the instance variables
         * so that we do not modify the state of the cache as a side effect.
         */
        {

            int head = this.head;

            int count = this.count;

            for (int i = 0; i < nscan && count > 0; i++) {

                head = (head == 0 ? capacity-1 : head - 1); // update head.

                count--; // update #of references.

                if( refs[head] == ref ) {
                
                    // Found a match.

                    return true;

                }

            }

            return false;

        }
        
    }
    
    /**
     * Return true iff the reference is found in the first N positions scanning
     * backwards from the tail of the queue.
     * 
     * @param nscan
     *            The #of positions to be scanned. When one (1) only the tail of
     *            the queue is scanned.
     * @param ref
     *            The reference to scan for.
     * 
     * @return True iff the reference is found in the last N queue positions
     *         counting backwards from the tail.
     * 
     * FIXME Write unit tests for this method.
     */
    final public boolean scanTail(int nscan, T ref) {

        assert nscan > 0 ;
        
        assert ref != null;
        
        for( int n=0, i=tail; n<nscan; n++ ) {
            
            if( ref == refs[ i ] ) return true;
            
            i = (i + 1) % capacity; // update index.
            
        }
        
        return false;

    }
    
    /**
     * The references in the cache in order from LRU to MRU. This is a copy of
     * the relevant references from the backing array. Changes to this array
     * have NO effect on the state of the cache.
     */
    protected T[] toArray() {
        
        T[] ary = (T[])new Object[count];
        
        for( int n=0, i=tail; n<count; n++ ) {
            
            T ref = refs[ i ];
            
            assert ref != null;
            
            ary[ n ] = ref;
            
            i = (i + 1) % capacity; // update index.
            
        }
                
        return ary;
        
    }
    
    /**
     * Interface for reporting cache evictions.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <T>
     */
    public interface HardReferenceQueueEvictionListener<T> {
        
        /**
         * Notice that a reference is being evicted from the cache. This notice
         * does NOT imply that the cache no longer holds a reference to this
         * object, neither does it imply that the object is dirty.
         * 
         * @param cache
         *            The cache on which the listener is registered.
         * @param ref
         *            The reference that is being evicted from the cache.
         */
        abstract public void evicted(HardReferenceQueue<T> cache,T ref);
        
    }
    
}
