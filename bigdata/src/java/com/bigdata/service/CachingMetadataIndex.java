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
 * Created on Oct 3, 2008
 */

package com.bigdata.service;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;

import cutthecrap.utils.striterators.IFilter;

/**
 * Implementation caches all locators and then updates them on demand as stale
 * locators are discovered.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CachingMetadataIndex extends CacheOnceMetadataIndex {

    /**
     * The delegate from which we refresh our local copy when we see stale
     * locators.
     */
    private final NoCacheMetadataIndexView delegate;

    /**
     * Note: This class must impose synchronization on access to the B+Tree
     * caching the locators. That synchronization is required since the class
     * will re-fetch locators on demand in
     * {@link #staleLocator(PartitionLocator)}. Since the fetched locators will
     * be written onto the B+Tree cache and since the B+Tree is NOT thread-safe
     * if there is a writer, then we must synchronized access to that B+Tree for
     * the readers as well as the writer. We do this using a
     * {@link ReadWriteLock}, which permits higher concurrency for readers.
     * This should work out well as reads should far outway writes (stale
     * locators are relatively rare).
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    
    /**
     * Cache the index partition metadata in the client.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @return The cached partition metadata -or- <code>null</code> iff there
     *         is no such scale-out index.
     */
    public CachingMetadataIndex(AbstractScaleOutFederation fed, String name,
            long timestamp, MetadataIndexMetadata mdmd) {

        super(fed, name, timestamp, mdmd);
        
        this.delegate = new NoCacheMetadataIndexView(fed, name, timestamp, mdmd);

    }

    /**
     * Re-fetches the locator(s).
     */
    public void staleLocator(final PartitionLocator locator) {

        if (locator == null)
            throw new IllegalArgumentException();
        
        if (timestamp != ITx.UNISOLATED && timestamp != ITx.READ_COMMITTED) {
            
            /*
             * Stale locator exceptions should not be thrown for these views.
             */

            throw new RuntimeException(
                    "Stale locator, but views should be consistent? timestamp="
                            + TimestampUtility.toString(timestamp));

        }

        if(INFO)
            log.info(locator.toString());
        
        final Lock lock = readWriteLock.writeLock();
        
        lock.lock();

        try {

            /*
             * Now that we have a write lock, update the local cache for all
             * locators known to the remote index that are spanned by the
             * key-range partition associated with the stale locator.
             */

            cacheLocators(//
                    locator.getLeftSeparatorKey(), // fromKey
                    locator.getRightSeparatorKey() // toKey
            );

        } finally {

            lock.unlock();

        }
        
    }

    public PartitionLocator get(byte[] key) {

        final Lock lock = readWriteLock.readLock();

        lock.lock();

        try {
        
            return super.get(key);
            
        } finally {
            
            lock.unlock();
            
        }

    }

    public PartitionLocator find(byte[] key) {

        final Lock lock = readWriteLock.readLock();
        
        lock.lock();
        
        try {
        
            return super.find(key);
            
        } finally {
            
            lock.unlock();
            
        }

    }

    public long rangeCount() {
        
        final Lock lock = readWriteLock.readLock();
        
        lock.lock();
        
        try {
        
            return delegate.rangeCount();
            
        } finally {
            
            lock.unlock();
            
        }

    }

    public long rangeCount(byte[] fromKey, byte[] toKey) {

        final Lock lock = readWriteLock.readLock();
        
        lock.lock();
        
        try {
        
            return delegate.rangeCount(fromKey, toKey);
            
        } finally {
            
            lock.unlock();
            
        }

    }

    public long rangeCountExact(byte[] fromKey, byte[] toKey) {

        final Lock lock = readWriteLock.readLock();
        
        lock.lock();
        
        try {
        
            return delegate.rangeCountExact(fromKey, toKey);
            
        } finally {
            
            lock.unlock();
            
        }

    }

    public ITupleIterator rangeIterator() {

        return delegate.rangeIterator(null, null);

    }

    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        return rangeIterator(fromKey, toKey, 0/* capacity */,
                IRangeQuery.DEFAULT, null/*filter*/);

    }

    /**
     * FIXME this is wrong. The {@link #delegate} must be a
     * {@link UnisolatedReadWriteIndex} in order to provide correct locking for
     * the iterator. The class may have to be refactored in order to permit the
     * behavior to be gated by an {@link UnisolatedReadWriteIndex}.
     */
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, IFilter filter) {

        final Lock lock = readWriteLock.readLock();
        
        lock.lock();
        
        try {
        
            return delegate.rangeIterator(fromKey, toKey, capacity, flags, filter);
            
        } finally {
            
            lock.unlock();
            
        }

    }

}
