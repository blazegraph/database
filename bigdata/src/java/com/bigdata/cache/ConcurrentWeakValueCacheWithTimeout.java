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

import java.util.concurrent.ConcurrentHashMap;

/**
 * Extends the basic behavior to clear stale references from a backing hard
 * reference queue. The weak reference values for entries whose hard reference
 * is cleared from the backing hard reference queue WILL NOT be cleared from
 * this map as long as those references remain strongly reachable.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see SynchronizedHardReferenceQueueWithTimeout
 */
public class ConcurrentWeakValueCacheWithTimeout<K, V> extends
        ConcurrentWeakValueCache<K, V> {

    /**
     * Ctor variant using a {@link SynchronizedHardReferenceQueueWithTimeout}
     * with the specified capacity and timeout for stale references.
     */
    public ConcurrentWeakValueCacheWithTimeout(final int queueCapacity,
            final long timeout) {

        this(queueCapacity, 0.75f/* loadFactor */, 16/* concurrencyLevel */,
                timeout);

    }

    /**
     * Ctor variant using a {@link SynchronizedHardReferenceQueueWithTimeout}
     * with the specified capacity, timeout for stale references, and the
     * specified concurrency level for the inner {@link ConcurrentHashMap} used
     * by this class.
     */
    public ConcurrentWeakValueCacheWithTimeout(final int queueCapacity,
            final float loadFactor, final int concurrencyLevel,
            final long timeout) {

        super(new SynchronizedHardReferenceQueueWithTimeout<V>(queueCapacity,
                timeout), loadFactor, concurrencyLevel, true/* removeClearedReferences */
        );

    }

}
