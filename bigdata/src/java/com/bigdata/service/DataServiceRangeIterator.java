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

import org.apache.log4j.Logger;

import com.bigdata.btree.ITupleFilter;
import com.bigdata.btree.BatchRemove.BatchRemoveConstructor;
import com.bigdata.journal.NoSuchIndexException;

/**
 * Class supports range query across against an unpartitioned index on an
 * {@link IDataService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServiceRangeIterator extends RawDataServiceRangeIterator {
    
    public static final transient Logger log = Logger
            .getLogger(DataServiceRangeIterator.class);

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
     * @param timestamp The timestamp for that scale-out index view.
     * @param fromKey
     * @param toKey
     * @param capacity
     * @param flags
     * @param filter
     */
    public DataServiceRangeIterator(ClientIndexView ndx,
            IDataService dataService, String name, long timestamp,
            byte[] fromKey, byte[] toKey, int capacity, int flags,
            ITupleFilter filter) {

        super(dataService, name, timestamp, fromKey, toKey, capacity, flags,
                filter);
        
        if (ndx == null) {

            throw new IllegalArgumentException();

        }

        if (timestamp != ndx.getTimestamp())
            throw new IllegalArgumentException();

        this.ndx = ndx;

    }

    /**
     * This method (and no other method on this class) will throw a (possibly
     * wrapped) {@link NoSuchIndexException} if an index partition is split,
     * joined or moved during traversal.
     * <p>
     * The caller MUST test any thrown exception. If the exception is, or wraps,
     * a {@link NoSuchIndexException}, then the caller MUST refresh its
     * {@link IPartitionLocatorMetadata locators}s for the key range of the
     * index partition that it thought it was traversing, and then continue
     * traversal based on the revised locators(s).
     * <p>
     * Note: The {@link NoSuchIndexException} CAN NOT arise from any other
     * method since only
     * {@link #getResultSet(byte[], byte[], int, int, ITupleFilter)} actually
     * reads from the {@link IDataService} and ALL calls to that method are
     * driven by {@link #hasNext()}.
     * <p>
     * Note: The methods that handle delete-behind use the
     * {@link ClientIndexView} to be robust and therefore will never throw a
     * {@link NoSuchIndexException}.
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

        ndx.submit(n, keys, null/* vals */,
                BatchRemoveConstructor.RETURN_NO_VALUES, null/* handler */);

//        try {
//
//            dataService.submit(tx, name, new BatchRemove(n, 0/* offset */,
//                    keys, false/* returnOldValues */));
//
//        } catch (Exception e) {
//
//            throw new RuntimeException(e);
//
//        }

    }

    @Override
    protected void deleteLast(byte[] key) {

        /*
         * Let the index view handle the delete in a robust manner.
         */

        ndx.submit(1, new byte[][] { key }, null/* vals */,
                BatchRemoveConstructor.RETURN_NO_VALUES, null/* handler */);

//        try {
//            
//            dataService.submit(tx, name, new BatchRemove(1, 0/* offset */,
//                    new byte[][] { key }, false/* returnOldValues */));
//
//        } catch (Exception e) {
//            
//            throw new RuntimeException(e);
//            
//        }
        
    }

    
}
