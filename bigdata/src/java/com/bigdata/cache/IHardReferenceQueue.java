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

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 *            The generic type of the hard references in the queue.
 */
public interface IHardReferenceQueue<T> {

    /**
     * A resonable default for the #of references on the head of the queue that
     * should be tested before a reference is appended to the queue.
     */
    int DEFAULT_NSCAN = 10;

    /**
     * The cache capacity.
     */
    int capacity();

    /**
     * The #of references that are tested on append requests.
     */
    int nscan();

    /**
     * The #of references in the cache.  Note that there is no guarentee that
     * the references are distinct.
     */
    int size();

    /**
     * True iff the cache is empty.
     */
    boolean isEmpty();

    /**
     * True iff the cache is full.
     */
    boolean isFull();

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
    boolean append(final T ref);

    /**
     * Evict the LRU reference. This is a NOP iff the cache is empty.
     * 
     * @return true iff a reference was evicted.
     * 
     * @see HardReferenceQueueEvictionListener
     */
    boolean evict();

    /**
     * Clears the cache (sets the head, tail and count to zero) without
     * generating eviction notices.
     * 
     * @param clearRefs
     *            When <code>true</code> the references are explicitly set to
     *            <code>null</code> which can facilitate garbage collection.
     */
    void clear(final boolean clearRefs);

    /**
     * Evict all references, starting with the LRU reference and proceeding to
     * the MRU reference.
     * 
     * @param clearRefs
     *            When true, the reference are actually cleared from the cache.
     *            This may be false to force persistence of the references in
     *            the cache without actually clearing the cache.
     */
    void evictAll(boolean clearRefs);

    /**
     * The reference at the tail of the queue. This is the next reference that
     * will be evicted from the queue.
     */
    T getTail();

}
