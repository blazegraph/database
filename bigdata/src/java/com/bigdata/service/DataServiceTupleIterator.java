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
 * Created on Sep 21, 2007
 */

package com.bigdata.service;

import java.util.Iterator;

import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.BatchRemove.BatchRemoveConstructor;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.ITx;
import com.bigdata.resources.StaleLocatorException;

/**
 * Class supports range query across against an unpartitioned index on an
 * {@link IDataService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServiceTupleIterator<E> extends RawDataServiceTupleIterator<E> {
    
//    protected static final transient Logger log = Logger
//            .getLogger(DataServiceTupleIterator.class);
//
//    protected static final boolean INFO = log.isInfoEnabled();
    
    /**
     * Used to submit delete requests to the scale-out index in a robust
     * manner.
     */
    protected final ClientIndexView ndx;
    
    /**
     * 
     * @param ndx
     *            The scale-out index view.
     * @param dataService
     *            The data service to be queried.
     * @param name
     *            The name of an index partition of that scale-out index on the
     *            data service.
     * @param timestamp
     *            The timestamp used for the reads. If
     *            {@link ITx#READ_COMMITTED}, then each read will be against
     *            the most recent commit point on the database. If you want
     *            read-consistent, then use
     *            {@link IIndexStore#getLastCommitTime()} rather than
     *            {@link ITx#READ_COMMITTED}.
     * @param fromKey
     * @param toKey
     * @param capacity
     * @param flags
     * @param filter
     */
    public DataServiceTupleIterator(final ClientIndexView ndx,
            final IDataService dataService, final String name,
            final long timestamp, final byte[] fromKey, final byte[] toKey,
            final int capacity, final int flags, final IFilterConstructor filter) {

        super(dataService, name, timestamp, false/* readConsistent */, fromKey,
                toKey, capacity, flags, filter);
        
        if (ndx == null) {

            throw new IllegalArgumentException();

        }

//        if (timestamp != ndx.getTimestamp()) {
//
//            throw new IllegalArgumentException();
//        
//        }

        this.ndx = ndx;

    }

    /**
     * This method (and no other method on this class) will throw a (possibly
     * wrapped) {@link StaleLocatorException} if an index partition is split,
     * joined or moved during traversal.
     * <p>
     * The caller MUST test any thrown exception. If the exception is, or wraps,
     * a {@link StaleLocatorException}, then the caller MUST refresh its
     * {@link IPartitionLocatorMetadata locators}s for the key range of the
     * index partition that it thought it was traversing, and then continue
     * traversal based on the revised locators(s).
     * <p>
     * Note: The {@link StaleLocatorException} CAN NOT arise from any other
     * method since only
     * {@link #getResultSet(byte[], byte[], int, int, IFilterConstructor)} actually
     * reads from the {@link IDataService} and ALL calls to that method are
     * driven by {@link #hasNext()}.
     * <p>
     * Note: The methods that handle delete-behind use the
     * {@link ClientIndexView} to be robust and therefore will never throw a
     * {@link StaleLocatorException}.
     */
    public boolean hasNext() {
        
        return super.hasNext();
        
    }
        
    @Override
    protected void deleteBehind(int n, Iterator<byte[]> itr) {

        final byte[][] keys = new byte[n][];
        
        int i = 0;
        
        while(itr.hasNext()) {
            
            keys[i] = itr.next();
            
        }
        
        /*
         * Let the index view handle the delete in a robust manner.
         */

        ndx.submit(0/*fromIndex*/,n/*toIndex*/, keys, null/* vals */,
                BatchRemoveConstructor.RETURN_NO_VALUES, null/* handler */);

    }

    @Override
    protected void deleteLast(byte[] key) {

        /*
         * Let the index view handle the delete in a robust manner.
         */

        ndx.submit(0/*fromIndex*/,1/*toIndex*/, new byte[][] { key }, null/* vals */,
                BatchRemoveConstructor.RETURN_NO_VALUES, null/* handler */);
        
    }
    
}
