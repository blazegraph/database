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

import org.apache.log4j.Logger;

import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.service.ndx.RawDataServiceTupleIterator;

/**
 * Implementation caches all locators but does not allow stale locators. This is
 * useful for read-historical index views since locators can not become stale
 * for a historical view.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CacheOnceMetadataIndex implements IMetadataIndex {

    protected static final Logger log = Logger
            .getLogger(CacheOnceMetadataIndex.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * The federation.
     */
    protected final AbstractScaleOutFederation fed;

    /**
     * Name of the scale-out index.
     */
    protected final String name;

    /**
     * Timestamp of the view.
     */
    protected final long timestamp;

    /**
     * Cached metadata record for the metadata index.
     */
    protected final MetadataIndexMetadata mdmd;

    /**
     * Local copy of the remote {@link MetadataIndex}.
     */
    private final MetadataIndex mdi;

    /**
     * Caches the index partition locators.
     * 
     * @param name
     *            The name of the scale-out index.
     */
    public CacheOnceMetadataIndex(AbstractScaleOutFederation fed, String name,
            long timestamp, MetadataIndexMetadata mdmd) {

        this.fed = fed;

        this.name = name;

        this.timestamp = timestamp;

        /*
         * Allocate a cache for the defined index partitions.
         */
        this.mdi = MetadataIndex.create(//
                fed.getTempStore(),//
                mdmd.getIndexUUID(),// UUID of the metadata index.
                mdmd.getManagedIndexMetadata()// the managed index's metadata.
                );

        this.mdmd = mdmd;
        
        // cache all the locators.
        cacheLocators(null/* fromKey */, null/* toKey */);
        
    }

    /**
     * Bulk copy the partition definitions for the scale-out index into the
     * client.
     * <p>
     * Note: This assumes that the metadata index is NOT partitioned and DOES
     * NOT support delete markers.
     */
    protected void cacheLocators(byte[] fromKey, byte[] toKey) {

        long n = 0;

        /*
         * Note: before we can update the cache we need to delete any locators
         * in the specified key range. This is a NOP of course if we are just
         * caching everything, but there is a subclass that updates key-ranges
         * of the cache in response to stale locator notices, so we have to
         * first wipe out the old locator(s) before reading in the updated
         * locator(s)
         */
        mdi.rangeIterator(fromKey, toKey, 0/* capacity */,
                IRangeQuery.REMOVEALL, null/*filter*/);

        /*
         * Read the locators from the remote metadata service.
         */
        final ITupleIterator itr = new RawDataServiceTupleIterator(fed
                .getMetadataService(),//
                MetadataService.getMetadataIndexName(name), //
                timestamp,//
                true, // readConsistent
                fromKey, //
                toKey, //
                0, // capacity
                IRangeQuery.KEYS | IRangeQuery.VALS | IRangeQuery.READONLY, //
                null // filter
        );

        while (itr.hasNext()) {

            final ITuple tuple = itr.next();

            final byte[] key = tuple.getKey();

            final byte[] val = tuple.getValue();

            mdi.insert(key, val);

            n++;

        }

        if (INFO) {

            log.info("Copied " + n + " locator records: name=" + name);

        }

    }

    /**
     * @throws UnsupportedOperationException
     *             stale locators should not occur for read-historical views!
     */
    public void staleLocator(PartitionLocator locator) {

        throw new UnsupportedOperationException();

    }

    final public MetadataIndexMetadata getIndexMetadata() {

        return mdmd;

    }

    final public IndexMetadata getScaleOutIndexMetadata() {

        return getIndexMetadata().getManagedIndexMetadata();

    }

    public PartitionLocator get(byte[] key) {

        return mdi.get(key);

    }

    public PartitionLocator find(byte[] key) {

        return mdi.find(key);

    }

    public long rangeCount() {
        
        return mdi.rangeCount();

    }

    public long rangeCount(byte[] fromKey, byte[] toKey) {

        return mdi.rangeCount(fromKey, toKey);

    }

    public long rangeCountExact(byte[] fromKey, byte[] toKey) {

        return mdi.rangeCountExact(fromKey, toKey);

    }

    public long rangeCountExactWithDeleted(byte[] fromKey, byte[] toKey) {

        return mdi.rangeCountExactWithDeleted(fromKey, toKey);

    }

    public ITupleIterator rangeIterator() {

        return mdi.rangeIterator();

    }

    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, IFilterConstructor filter) {

        return mdi.rangeIterator(fromKey, toKey, capacity, flags, filter);

    }

    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        return mdi.rangeIterator(fromKey, toKey);

    }

}
