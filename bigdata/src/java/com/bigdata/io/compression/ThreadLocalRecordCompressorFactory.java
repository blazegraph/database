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
 * Created on May 2, 2009
 */

package com.bigdata.io.compression;

import java.util.concurrent.TimeUnit;

import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;

/**
 * An {@link IRecordCompressorFactory} with thread-local semantics based on an
 * internal weak value cache and providing instances based on a delegate
 * {@link IRecordCompressorFactory}. This is designed to work well when the
 * application is single-threaded as well as when there are concurrent threads
 * demanding instances from the delegate factory.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ThreadLocalRecordCompressorFactory<A extends RecordCompressor>
        implements IRecordCompressorFactory<A> {

    /**
     * Cache with timeout. A relatively small cache is used since the maximum
     * #of instances is bounded by the real concurrency of readers on a single
     * resource. A relatively short timeout is used so that the hard references
     * in the queue will be cleared quickly if the factory is in high demand.
     * That is by design since both read and write scenarios have high demand.
     */
    private final ConcurrentWeakValueCacheWithTimeout<Thread, A> cache = new ConcurrentWeakValueCacheWithTimeout<Thread, A>(
            10/* queueCapacity */, TimeUnit.SECONDS.toNanos(5));

    private final IRecordCompressorFactory<A> delegate;

    /**
     * 
     * @param delegate
     *            The factory used to create instances of the
     *            {@link IRecordCompressor} when there is none in the cache.
     */
    public ThreadLocalRecordCompressorFactory(final IRecordCompressorFactory<A> delegate) {

        if (delegate == null)
            throw new IllegalArgumentException();

        this.delegate = delegate;
        
    }
    
    /**
     * Return an instance for use by the current thread.
     */
    public A getInstance() {

        final Thread t = Thread.currentThread();

        // test cache.
        A a = cache.get(t);

        if (a == null) {

            /*
             * Not found - create new instance.
             * 
             * Note: Since the key is the Thread, it is not possible for a race
             * condition to exist in which a different Thread concurrently adds
             * an entry under our key.
             */
            
            a = newInstance();

            // add to the cache.
            if (cache.put(t, a) != null) {

                /*
                 * Per above, this should not be possible.
                 */

                throw new AssertionError();

            }

        }

        return a;
        
    }

    /**
     * Return a new {@link IRecordCompressor} instance from the delegate
     * {@link IRecordCompressorFactory}.
     */
    protected A newInstance() {
        
        return delegate.getInstance();
        
    }
    
}
