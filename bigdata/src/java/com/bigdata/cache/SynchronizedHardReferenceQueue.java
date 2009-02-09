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

import org.apache.log4j.Logger;

/**
 * Thread-safe version.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SynchronizedHardReferenceQueue<T> implements IHardReferenceQueue<T> {

    protected static final Logger log = Logger.getLogger(SynchronizedHardReferenceQueue.class);
    
    protected static final boolean INFO = log.isInfoEnabled(); 

    protected static final boolean DEBUG = log.isDebugEnabled(); 

    /**
     * Note: Synchronization for the inner {@link #queue} is realized using the
     * <strong>outer</strong> reference!
     */
    protected final HardReferenceQueue<T> queue;
    
    /**
     * Defaults the #of references to scan on append requests to 10.
     * 
     * @param listener
     *            The listener on which cache evictions are reported.
     * @param capacity
     *            The maximum #of references that can be stored on the cache.
     *            There is no guarentee that all stored references are distinct.
     */
    public SynchronizedHardReferenceQueue(final HardReferenceQueueEvictionListener<T> listener,
            final int capacity) {
        
        this(listener, capacity, DEFAULT_NSCAN);
        
    }
    
    /**
     * Core impl.
     * 
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
    public SynchronizedHardReferenceQueue(
            final HardReferenceQueueEvictionListener<T> listener,
            final int capacity, final int nscan) {

        this(new HardReferenceQueue<T>(listener, capacity, DEFAULT_NSCAN));
       
    }
    
    protected SynchronizedHardReferenceQueue(final HardReferenceQueue<T> innerQueue) {
        
        if(innerQueue == null)
            throw new IllegalArgumentException();
        
        this.queue = innerQueue;
        
    }

    /*
     * Methods which DO NOT require synchronization.
     */
    final public int capacity() {
        return queue.capacity();
    }

    public HardReferenceQueueEvictionListener<T> getListener() {
        return queue.getListener();
    }

    public int nscan() {
        return queue.nscan();
    }

    /*
     * Methods which DO require synchronization.
     */
    
    synchronized public boolean append(T ref) {
        return queue.append(ref);
    }

    synchronized public void clear(boolean clearRefs) {
        queue.clear(clearRefs);
    }

    synchronized public boolean evict() {
        return queue.evict();
    }

    synchronized public void evictAll(boolean clearRefs) {
        queue.evictAll(clearRefs);
    }

    synchronized public T getTail() {
        return queue.getTail();
    }

    synchronized public boolean isEmpty() {
        return queue.isEmpty();
    }

    synchronized public boolean isFull() {
        return queue.isFull();
    }

    synchronized public boolean scanHead(int nscan, T ref) {
        return queue.scanHead(nscan, ref);
    }

    synchronized public boolean scanTail(int nscan, T ref) {
        return queue.scanTail(nscan, ref);
    }

    synchronized public int size() {
        return queue.size();
    }

}
