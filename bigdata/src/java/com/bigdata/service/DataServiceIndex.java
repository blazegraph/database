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
 * Created on Jan 10, 2008
 */

package com.bigdata.service;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.bigdata.btree.ICounter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.AbstractIndexProcedureConstructor;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IResultHandler;
import com.bigdata.btree.ITupleFilter;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.RangeCountProcedure;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBitBuffer;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.BatchContains.BatchContainsConstructor;
import com.bigdata.btree.BatchInsert.BatchInsertConstructor;
import com.bigdata.btree.BatchLookup.BatchLookupConstructor;
import com.bigdata.btree.BatchRemove.BatchRemoveConstructor;
import com.bigdata.btree.IIndexProcedure.IKeyRangeIndexProcedure;
import com.bigdata.btree.IIndexProcedure.ISimpleIndexProcedure;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.util.InnerCause;

/**
 * A view onto an unpartitioned index living inside an embedded
 * {@link DataService}. Access to the index is moderated by the concurrency
 * control mechanisms of the data service.
 * <p>
 * Applications writing to this interface are directly portable to scale-out
 * partitioned indices and to embedded indices without concurrency control.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServiceIndex implements IIndex {

    /**
     * 
     */
    private static final String NON_BATCH_API = "Non-batch API";

    /**
     * Note: Invocations of the non-batch API are logged at the WARN level since
     * they result in an application that can not scale efficiently to
     * partitioned indices.
     */
    protected static final Logger log = Logger.getLogger(DataServiceIndex.class);
    
    private final String name;
    private final long timestamp;
    private final DataService dataService;

    /**
     * The transaction identifier -or- {@link ITx#UNISOLATED} iff the index
     * view is unisolated -or- <code>- timestamp</code> for a historical read
     * of the most recent committed state not later than <i>timestamp</i>.
     * 
     * @return The transaction identifier for the index view.
     */
    public long getTx() {
        
        return timestamp;
        
    }
    
    /**
     * The name of the scale-out index.
     */
    public String getName() {
        
        return name;
        
    }
    
    /**
     * The capacity for the range query iterator.
     * 
     * @todo should be a ctor option.
     */
    private final int capacity = 100000;

    /**
     * This may be used to disable the non-batch API, which is quite convenient
     * for location code that needs to be re-written to use
     * {@link IIndexProcedure}s.
     */
    private final boolean batchOnly = false;
    
    /**
     * Creates a view onto an unpartitioned index living on an embedded data
     * service.
     * 
     * @param name
     *            The index name.
     * @param timestamp
     *            The transaction identifier -or- {@link ITx#UNISOLATED} iff the
     *            index view is unisolated -or- <code>- timestamp</code> for a
     *            historical read of the most recent committed state not later
     *            than <i>timestamp</i>.
     * @param dataService
     *            The data service (a local object, not a proxy for a remote
     *            data service).
     * 
     * @throws NoSuchIndexException
     *             if the named index does not exist.
     */
    public DataServiceIndex(String name, long timestamp, DataService dataService) {

        this.name = name;
        
        this.timestamp = timestamp;
        
        this.dataService = dataService;
        
    }
    
    public String toString() {
        
        return name+" @ "+timestamp;
        
    }
    
    public IndexMetadata getIndexMetadata() {

        try {

            return dataService.getIndexMetadata(name,timestamp);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    public String getStatistics() {

        try {

            return dataService.getStatistics(name);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * Counters are local to a specific index partition and are only available
     * to unisolated procedures.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    public ICounter getCounter() {
        
        throw new UnsupportedOperationException();
        
    }
    
    public boolean contains(byte[] key) {
        
        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][] { key };
        
//        final BatchContains proc = new BatchContains(//
//                1, // n,
//                0, // offset
//                keys
//        );
//
//        final boolean[] ret = ((ResultBitBuffer) submit(key, proc)).getResult();
        
        final IResultHandler resultHandler = new IdentityHandler();
        
        submit(0/* fromIndex */, 1/* toIndex */, keys, null/* vals */,
                BatchContainsConstructor.INSTANCE, resultHandler);

        return ((ResultBitBuffer) resultHandler.getResult()).getResult()[0];
        
    }
    
    public byte[] insert(byte[] key, byte[] value) {

        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][] { key };
        final byte[][] vals = new byte[][] { value };
        
//        final BatchInsert proc = new BatchInsert(//
//                1, // n,
//                0, // offset
//                new byte[][] { key }, // keys
//                new byte[][] { value }, // vals
//                true // returnOldValues
//        );
//
//        final byte[][] ret = ((ResultBuffer) submit(key, proc)).getResult();
//
//        return ret[0];

        final IResultHandler resultHandler = new IdentityHandler();
        
        submit(0/* fromIndex */, 1/* toIndex */, keys, vals,
                BatchInsertConstructor.RETURN_OLD_VALUES, resultHandler);
     
        return ((ResultBuffer) resultHandler.getResult()).getResult(0);

    }

    public byte[] lookup(byte[] key) {

        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][]{key};
        
//        final BatchLookup proc = new BatchLookup(//
//                1, // n,
//                0, // offset
//                new byte[][] { key } // keys
//        );
//
//        final byte[][] ret = ((ResultBuffer)submit(key, proc)).getResult();
//
//        return ret[0];

        final IResultHandler resultHandler = new IdentityHandler();
        
        submit(0/* fromIndex */, 1/* toIndex */, keys, null/* vals */,
                BatchLookupConstructor.INSTANCE, resultHandler);
     
        return ((ResultBuffer) resultHandler.getResult()).getResult(0);

    }

    public byte[] remove(byte[] key) {

        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][]{key};
        
//        final BatchRemove proc = new BatchRemove(//
//                1, // n,
//                0, // offset
//                new byte[][] { key }, // keys
//                true // returnOldValues
//        );
//
//        final byte[][] ret = ((ResultBuffer) submit(key, proc)).getResult();
//
//        return ret[0];

        final IResultHandler resultHandler = new IdentityHandler();
        
        submit(0/* fromIndex */, 1/* toIndex */, keys, null/* vals */,
                BatchRemoveConstructor.RETURN_OLD_VALUES, resultHandler);
     
        return ((ResultBuffer) resultHandler.getResult()).getResult(0);

    }

    public long rangeCount(byte[] fromKey, byte[] toKey) {

        // final LongAggregator handler = new LongAggregator();

        final RangeCountProcedure proc = new RangeCountProcedure(fromKey, toKey);

        final long rangeCount;

        try {

            rangeCount = (Long) dataService.submit(timestamp, name, proc);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return rangeCount;

    }
  
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        return rangeIterator(fromKey, toKey, capacity, IRangeQuery.KEYS
                | IRangeQuery.VALS, null/* filter */);

    }

    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey, int capacity, int flags, ITupleFilter filter) {

        // @todo make this a ctor argument or settable property?
        final boolean readConsistent = (timestamp == ITx.UNISOLATED ? false
                : true);
        
        return new RawDataServiceRangeIterator(dataService, name, timestamp,
                readConsistent, fromKey, toKey, capacity, flags, filter);

    }

    public Object submit(byte[] key, ISimpleIndexProcedure proc) {

        try {

            return dataService.submit(timestamp, name, proc);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    @SuppressWarnings("unchecked")
    public void submit(byte[] fromKey, byte[] toKey,
            final IKeyRangeIndexProcedure proc, final IResultHandler handler) {

        if (proc == null)
            throw new IllegalArgumentException();

        try {
            
            Object result = dataService.submit(timestamp, name, proc);

            if(handler!=null) {
                
                handler.aggregate(result, new Split(null,0,0));
                
            }

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }
        
    }

    @SuppressWarnings("unchecked")
    public void submit(int fromIndex, int toIndex, byte[][] keys, byte[][] vals,
            AbstractIndexProcedureConstructor ctor, IResultHandler aggregator) {

        try {

            Object result = dataService.submit(timestamp, name, ctor
                    .newInstance(this,fromIndex, toIndex, keys, vals));
            
            if(aggregator != null) {

                aggregator.aggregate(result,
                        new Split(null, fromIndex, toIndex));
                
            }

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * The resources associated with the cached {@link IndexMetadata}.
     */
    public IResourceMetadata[] getResourceMetadata() {

        LocalPartitionMetadata pmd = getIndexMetadata()
                .getPartitionMetadata();

        return pmd.getResources();

    }

}
