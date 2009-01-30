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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.ICounter;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.AbstractIndexProcedureConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.btree.proc.RangeCountProcedure;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBitBuffer;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.proc.BatchContains.BatchContainsConstructor;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.btree.proc.BatchLookup.BatchLookupConstructor;
import com.bigdata.btree.proc.BatchRemove.BatchRemoveConstructor;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;

/**
 * A view onto an unpartitioned index living inside an embedded
 * {@link DataService}. Access to the index is moderated by the concurrency
 * control mechanisms of the data service.
 * <p>
 * Applications writing to this interface are directly portable to scale-out
 * partitioned indices and to embedded indices without concurrency control.
 * <p>
 * Note: Since the index is NOT partitioned we do not have to handle stale
 * locators. That makes this implementation MUCH simpler than the
 * {@link ClientIndexView}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServiceIndex implements IClientIndex {

    /**
     * 
     */
    private static final String NON_BATCH_API = "Non-batch API";

    /**
     * Note: Invocations of the non-batch API are logged at the WARN level since
     * they result in an application that can not scale efficiently to
     * partitioned indices.
     */
    protected static final Logger log = Logger
            .getLogger(DataServiceIndex.class);

    final protected static boolean WARN = log.getEffectiveLevel().toInt() <= Level.WARN
            .toInt();

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
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
    public long getTimestamp() {
        
        return timestamp;
        
    }
    
    public String getName() {
        
        return name;
        
    }
    
    public String toString() {
        
        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getSimpleName());

        sb.append("{ ");

        sb.append("name=" + name);

        sb.append(", timestamp=" + timestamp);

        sb.append("}");
        
        return sb.toString();
        
    }
   
    /**
     * The capacity for the range query iterator.
     */
    private final int capacity;

    /**
     * This may be used to disable the non-batch API, which is quite convenient
     * for location code that needs to be re-written to use
     * {@link IIndexProcedure}s.
     */
    private final boolean batchOnly;
    
    /**
     * We cache some stuff for historical reads.
     */
    private final boolean historicalRead;
    
    /**
     * Eagerly materialized and cached iff {@link #historicalRead}.
     */
    private IndexMetadata indexMetadata = null;
    
    /**
     * Creates a view onto an unpartitioned index living on an embedded data
     * service.
     * 
     * @param fed
     *            The {@link LocalDataServiceFederation}.
     * @param name
     *            The index name.
     * @param timestamp
     *            The transaction identifier -or- {@link ITx#UNISOLATED} iff the
     *            index view is unisolated -or- <code>- timestamp</code> for a
     *            historical read of the most recent committed state not later
     *            than <i>timestamp</i>.
     * 
     * @throws NoSuchIndexException
     *             if the named index does not exist.
     */
    public DataServiceIndex(LocalDataServiceFederation fed, String name, long timestamp) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (name == null)
            throw new IllegalArgumentException();
        
        this.name = name;
        
        this.timestamp = timestamp;
        
        this.dataService = fed.getDataService();

        this.capacity = fed.getClient().getDefaultRangeQueryCapacity();

        this.batchOnly = fed.getClient().getBatchApiOnly();
        
        this.historicalRead = TimestampUtility.isReadOnly(timestamp);
        
        if(historicalRead) {
            
            try {

                indexMetadata = dataService.getIndexMetadata(name,timestamp);

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }
        
    }
    
    public IndexMetadata getIndexMetadata() {

        if (historicalRead) {

            // cached.
            
            assert indexMetadata != null;
            
            return indexMetadata;
            
        }

        try {

            return dataService.getIndexMetadata(name, timestamp);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * FIXME populate with counters concerning the client's access to the index
     * rather than the index's counters.  This should be basically the same for
     * the {@link ClientIndexView} and the {@link DataServiceIndex}.
     */
    synchronized public ICounterSet getCounters() {

        if (counterSet == null) {

            counterSet = new CounterSet();
            
            counterSet.addCounter("name", new OneShotInstrument<String>(name));

            counterSet.addCounter("timestamp", new OneShotInstrument<Long>(timestamp));

        }
        
        return counterSet;
        
    }
    private CounterSet counterSet;

    public ICounter getCounter() {
        
        throw new UnsupportedOperationException();
        
    }
    
    private volatile ITupleSerializer tupleSer = null;

    protected ITupleSerializer getTupleSerializer() {
        if(tupleSer==null) {
            synchronized(this) {
                if(tupleSer==null) {
                    tupleSer = getIndexMetadata().getTupleSerializer();
                }
            }
        }
        return tupleSer;
    }
    
    public boolean contains(Object key) {

        key = getTupleSerializer().serializeKey(key);
        
        return contains((byte[])key);
        
    }
    
    public boolean contains(byte[] key) {
        
        if (batchOnly)
            log.error(NON_BATCH_API,new RuntimeException());
        else
            if(WARN) log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][] { key };
        
        final IResultHandler resultHandler = new IdentityHandler();
        
        submit(0/* fromIndex */, 1/* toIndex */, keys, null/* vals */,
                BatchContainsConstructor.INSTANCE, resultHandler);

        return ((ResultBitBuffer) resultHandler.getResult()).getResult()[0];
        
    }
    
    public Object insert(Object key,Object val) {
        
        final ITupleSerializer tupleSer = getTupleSerializer();
        
        key = tupleSer.serializeKey(key);
        
        val = tupleSer.serializeKey(val);
        
        final byte[] oldval = insert((byte[])key, (byte[])val);
        
        // FIXME decode tuple to old value.
        throw new UnsupportedOperationException();
        
    }
    
    public byte[] insert(byte[] key, byte[] value) {

        if (batchOnly)
            log.error(NON_BATCH_API,new RuntimeException());
        else
            if(WARN) log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][] { key };
        final byte[][] vals = new byte[][] { value };
        
        final IResultHandler resultHandler = new IdentityHandler();
        
        submit(0/* fromIndex */, 1/* toIndex */, keys, vals,
                BatchInsertConstructor.RETURN_OLD_VALUES, resultHandler);
     
        return ((ResultBuffer) resultHandler.getResult()).getResult(0);

    }

    public Object lookup(Object key) {
        
        key = getTupleSerializer().serializeKey(key);

        final byte[] val = lookup((byte[])key);
        
        // FIXME decode tuple to old value.
        throw new UnsupportedOperationException();
        
    }

    public byte[] lookup(byte[] key) {

        if (batchOnly)
            log.error(NON_BATCH_API,new RuntimeException());
        else
            if(WARN) log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][]{key};
        
        final IResultHandler resultHandler = new IdentityHandler();
        
        submit(0/* fromIndex */, 1/* toIndex */, keys, null/* vals */,
                BatchLookupConstructor.INSTANCE, resultHandler);
     
        return ((ResultBuffer) resultHandler.getResult()).getResult(0);

    }

    public Object remove(Object key) {
        
        key = getTupleSerializer().serializeKey(key);
        
        final byte[] oldval = remove((byte[])key);
        
        // FIXME decode tuple to old value.
        throw new UnsupportedOperationException();

    }
    
    public byte[] remove(byte[] key) {

        if (batchOnly)
            log.error(NON_BATCH_API,new RuntimeException());
        else
            if(WARN) log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][]{key};
        
        final IResultHandler resultHandler = new IdentityHandler();
        
        submit(0/* fromIndex */, 1/* toIndex */, keys, null/* vals */,
                BatchRemoveConstructor.RETURN_OLD_VALUES, resultHandler);
     
        return ((ResultBuffer) resultHandler.getResult()).getResult(0);

    }

    public long rangeCount() {
        
        return rangeCount(null, null);
        
    }
    
    public long rangeCount(byte[] fromKey, byte[] toKey) {

        final RangeCountProcedure proc = new RangeCountProcedure(
                false/* exact */, fromKey, toKey);

        final long rangeCount;

        try {

            rangeCount = (Long) dataService.submit(timestamp, name, proc);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return rangeCount;

    }

    public long rangeCountExact(byte[] fromKey, byte[] toKey) {

        final RangeCountProcedure proc = new RangeCountProcedure(
                true/* exact */, fromKey, toKey);

        final long rangeCount;

        try {

            rangeCount = (Long) dataService.submit(timestamp, name, proc);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return rangeCount;

    }

    final public ITupleIterator rangeIterator() {

        return rangeIterator(null, null);

    }
    
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        return rangeIterator(fromKey, toKey, capacity, IRangeQuery.DEFAULT,
                null/* filter */);

    }

    public ITupleIterator rangeIterator(final byte[] fromKey,
            final byte[] toKey, final int capacity, final int flags,
            final IFilterConstructor filter) {

        /*
         * @todo make this a ctor argument or settable property?
         * 
         * Note: this parameter is NOT sufficient to achieve read-consistent
         * semantics across a partitioned index!
         */
        final boolean readConsistent = (timestamp == ITx.UNISOLATED ? false
                : true);

        final boolean readOnly = ((flags & IRangeQuery.READONLY) != 0)
        || (filter == null &&
           ((flags & IRangeQuery.CURSOR) == 0) &&
           ((flags & IRangeQuery.REMOVEALL) == 0)
           );
        
        long timestamp = getTimestamp();

        if (timestamp == ITx.UNISOLATED && readOnly) {
            
            timestamp = ITx.READ_COMMITTED;
            
        }
        
//        if (timestamp == ITx.READ_COMMITTED && readConsistent) {
//        
//            timestamp = getFederation().getLastCommitTime();
//            
//        }
        
        return new RawDataServiceTupleIterator(dataService, name, timestamp,
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
