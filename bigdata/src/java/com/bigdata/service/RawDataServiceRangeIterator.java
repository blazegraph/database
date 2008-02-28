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
 * Created on Feb 27, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractChunkedRangeIterator;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ITupleFilter;
import com.bigdata.btree.ResultSet;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IBlock;

/**
 * Class supports range query across against an unpartitioned index on an
 * {@link IDataService} but DOES NOT handle index partition splits, moves or
 * joins.
 * <p>
 * Note: This class exists mainly to support caching of the remove metadata
 * index, which does not use index partitions, by the bigdata federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RawDataServiceRangeIterator extends AbstractChunkedRangeIterator {
    
    public static final transient Logger log = Logger
            .getLogger(RawDataServiceRangeIterator.class);

    /**
     * Error message used by {@link #getKey()} when the iterator was not
     * provisioned to request keys from the data service.
     */
    static public transient final String ERR_NO_KEYS = "Keys not requested";
    
    /**
     * Error message used by {@link #getValue()} when the iterator was not
     * provisioned to request values from the data service.
     */
    static public transient final String ERR_NO_VALS = "Values not requested";
    
    /**
     * The data service for the index.
     * <p>
     * Note: Be careful when using this field since you take on responsibilty
     * for handling index partition splits, joins, and moves!
     * 
     * @todo this should failover if a data service dies.
     */
    protected final IDataService dataService;
    
    /**
     * The index on which the range query is being performed.
     */
    protected final String name;
    
    /**
     * The transaction identifier -or- zero iff the request is unisolated.
     */
    protected final long tx;

    /**
     * 
     * @param dataService
     *            The data service on which the index resides.
     * @param name
     *            The name of the index partition on that data service.
     * @param timestamp
     * @param fromKey
     * @param toKey
     * @param capacity
     * @param flags
     * @param filter
     */
    public RawDataServiceRangeIterator(IDataService dataService, String name,
            long timestamp, byte[] fromKey, byte[] toKey, int capacity, int flags,
            ITupleFilter filter) {

        super(fromKey, toKey, capacity, flags, filter);
        
        if (dataService == null) {

            throw new IllegalArgumentException();

        }

        if (name == null) {

            throw new IllegalArgumentException();
            
        }
        
        if (capacity < 0) {

            throw new IllegalArgumentException();
            
        }

        this.dataService = dataService;
        
        this.name = name;
        
        this.tx = timestamp;

    }

    /**
     * Atomic operation caches a chunk of results from an {@link IDataService}.
     */
    @Override
    protected ResultSet getResultSet(byte[] _fromKey, byte[] toKey, int capacity,
            int flags, ITupleFilter filter) {

        log.info("name=" + name + ", fromKey=" + BytesUtil.toString(_fromKey)
                + ", toKey=" + BytesUtil.toString(toKey)+", dataService="+dataService);

        try {
            
            return dataService.rangeIterator(tx, name, _fromKey, toKey,
                    capacity, flags, filter);
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }
        
    @Override
    protected void deleteBehind(int n, Iterator<byte[]> itr) {

        final byte[][] keys = new byte[n][];
        
        int i = 0;
        
        while(itr.hasNext()) {
            
            keys[i] = itr.next();
            
        }
        
        try {

            dataService.submit(tx, name, new BatchRemove(n, 0/* offset */,
                keys, false/* returnOldValues */));

        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }

    }

    @Override
    protected void deleteLast(byte[] key) {

        try {
            
            dataService.submit(tx, name, new BatchRemove(1, 0/* offset */,
                    new byte[][] { key }, false/* returnOldValues */));

        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    @Override
    protected IBlock readBlock(final int sourceIndex, long addr) {
        
        final IResourceMetadata resource = rset.getSources()[sourceIndex];
        
        try {
            
            return dataService.readBlock(resource, addr);
            
        } catch (IOException e) {

            throw new RuntimeException(e);
            
        }
        
    }
    
}
