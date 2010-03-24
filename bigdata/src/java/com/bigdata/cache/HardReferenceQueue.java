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
 * the nscan most recent references are always tested before appending a
 * reference to the cache in order to minimize cache churn when an object is
 * touched repeatedly in close succession. Likewise, eviction does not mean that
 * the object is no longer on the hard reference cache, nor does eviction mean
 * that no hard references to the object exist within the VM. However, eviction
 * is nevertheless used to drive persistence of the object. Since an object may
 * be evicted multiple times without being updated in between evictions, this
 * requires the object to implement a protocol for determining whether or not it
 * is dirty. Eviction is then contingent on that protocol and the dirty state of
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
 * @param <T>
 *            The reference type stored in the cache.
 */
public class HardReferenceQueue<T> extends RingBuffer<T> implements IHardReferenceQueue<T> {

//    protected static final Logger log = Logger.getLogger(HardReferenceQueue.class);
    
//    protected static final boolean INFO = log.isInfoEnabled(); 
    
    /**
     * The listener to which cache eviction notices are reported.
     */
    private final HardReferenceQueueEvictionListener<T> listener;

    /**
     * The #of references to scan backwards from the LRU position when testing
     * for whether or not a reference is already in the cache.
     */
    protected final int nscan;
    
    /**
     * Uses the default #of references to scan on append requests.
     * 
     * @param listener
     *            The listener on which cache evictions are reported.
     * @param capacity
     *            The maximum #of references that can be stored on the cache.
     *            There is no guarantee that all stored references are distinct.
     */
    public HardReferenceQueue(final HardReferenceQueueEvictionListener<T> listener,
            final int capacity) {
        
        this(listener, capacity, Math.min(capacity, DEFAULT_NSCAN));
        
    }
    
    /**
     * Fully specified ctor.
     * 
     * @param listener
     *            The listener on which cache evictions are reported (optional).
     * @param capacity
     *            The maximum #of references that can be stored on the cache.
     *            There is no guarantee that all stored references are distinct.
     * @param nscan
     *            The #of references to scan from the MRU position before
     *            appended a reference to the cache. Scanning is used to reduce
     *            the chance that references that are touched several times in
     *            near succession from entering the cache more than once. The
     *            #of reference tests trades off against the latency of adding a
     *            reference to the cache.
     */
    public HardReferenceQueue(
            final HardReferenceQueueEvictionListener<T> listener,
            final int capacity, final int nscan) {

        super(capacity);
        
// if (listener == null)
//            throw new IllegalArgumentException();

        if (nscan < 0 || nscan > capacity)
            throw new IllegalArgumentException();

        this.listener = listener;
        
        this.nscan = nscan;
        
    }

    /**
     * The  listener specified to the constructor.
     */
    final public HardReferenceQueueEvictionListener<T> getListener() {
        
        return listener;
        
    }
    
    /**
     * The #of references that are tested on append requests.
     */
    final public int nscan() {
        
        return nscan;
        
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
    @Override
    public boolean add(final T ref) {
        
        // note: tested by the base class in offer().
//        if (ref == null)
//            throw new IllegalArgumentException();
        
        /*
         * Scan the last nscan references for this reference. If found, return
         * immediately.
         */
        if (nscan > 0 && scanHead(nscan, ref)) {
            
            return false;

        }
        
        super.add(ref);
        
        return true;
        
    }

    /**
     * Extended to evict the element at the tail of the buffer iff the buffer is
     * full.
     * <p>
     * Note: This hook is further extended to realize the stale reference
     * protocol in {@link SynchronizedHardReferenceQueueWithTimeout}.
     */
    protected void beforeOffer(final T ref) {

//        super.beforeOffer(ref);  // base class is NOP.
        
        if (size == capacity/* isFull() inline */) {

            /*
             * If at capacity, evict the LRU reference.
             */

            evict();

        }

    }

    /**
     * Evict the LRU reference. This is a NOP iff the cache is empty.
     * 
     * @return true iff a reference was evicted.
     * 
     * @see HardReferenceQueueEvictionListener
     */
    final public boolean evict() {

        final T ref = poll();

        if (ref == null) {

            // buffer is empty.
            return false;

        }

        if (listener != null) {

            // report eviction notice to listener.
            listener.evicted(this, ref);

        }

        return true;

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
    final public void evictAll(final boolean clearRefs) {

        if (clearRefs) {

            /*
             * Evict all references, clearing each as we go.
             */

            while (!isEmpty()) { // count > 0 ) {

                evict();

            }

        } else {

            /*
             * Generate eviction notices in LRU to MRU order but do NOT clear
             * the references.
             */

            final int size = size();

            for (int n = 0; n < size; n++) {

                final T ref = get(n); 

                if (listener != null) {

                    // report eviction notice to listener.
                    listener.evicted(this, ref);
                    
                }

            }
            
//            /*
//             * Note: This uses local variables to shadow the instance variables
//             * so that we do not modify the state of the cache as a side effect.
//             */
//
//            int tail = this.tail;
//
//            int count = this.size;
//
//            while (count > 0) {
//
//                final T ref = refs[tail]; // LRU reference.
//
//                count--; // update #of references.
//
//                tail = (tail + 1) % capacity(); // update tail.
//
//                if (listener != null) {
//
//                    // report eviction notice to listener.
//                    listener.evicted(this, ref);
//                    
//                }
//
//            }

        }

    }

}
