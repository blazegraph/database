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
 * Created on Apr 22, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.AbstractIndexProcedureConstructor;
import com.bigdata.btree.proc.AbstractKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.btree.proc.LongAggregator;
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
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.IBigdataClient.Options;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.TaskCounters;

/**
 * <p>
 * A client-side view of a scale-out index as of some <i>timestamp</i>.
 * </p>
 * <p>
 * This view automatically handles the split, join, or move of index partitions
 * within the federation. The {@link IDataService} throws back a (sometimes
 * wrapped) {@link StaleLocatorException} when it does not have a registered
 * index as of some timestamp. If this exception is observed when the client
 * makes a request using a cached {@link PartitionLocator} record then the
 * locator record is stale. The client automatically fetches the locator
 * record(s) covering the same key range as the stale locator record and the
 * re-issues the request against the index partitions identified in those
 * locator record(s). This behavior correctly handles index partition split,
 * merge, and move scenarios. The implementation of this policy is limited to
 * exactly three places in the code: {@link AbstractDataServiceProcedureTask},
 * {@link PartitionedTupleIterator}, and {@link DataServiceTupleIterator}.
 * </p>
 * <p>
 * Note that only {@link ITx#UNISOLATED} and {@link ITx#READ_COMMITTED}
 * operations are subject to stale locators since they are not based on a
 * historical committed state of the database. Historical read and
 * fully-isolated operations both read from historical committed states and the
 * locators are never updated for historical states (only the current state of
 * an index partition is split, joined, or moved - the historical states always
 * remain behind).
 * </p>
 * 
 * @todo If the index was dropped then that should cause the operation to abort
 *       (only possible for read committed or unisolated operations).
 *       <p>
 *       Likewise, if a transaction is aborted, then then index should refuse
 *       further operations.
 * 
 * @todo detect data service failure and coordinate cutover to the failover data
 *       services. ideally you can read on a failover data service at any time
 *       but it should not accept write operations unless it is the primary data
 *       service in the failover chain.
 *       <p>
 *       Offer policies for handling index partitions that are unavailable at
 *       the time of the request (continued operation during partial failure).
 * 
 * @todo We should be able to transparently use either a hash mod N approach to
 *       distributed index partitions or a dynamic approach based on overflow.
 *       This could even be decided on a per-index basis. The different
 *       approaches would be hidden by appropriate implementations of this
 *       class.
 *       <p>
 *       A hash partitioned index will need to enforce optional read-consistent
 *       semantics. This can be done by choosing a recent broadcast commitTime
 *       for the read or by re-issuing queries that come in with a different
 *       commitTime.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClientIndexView implements IClientIndex {

    /**
     * Note: Invocations of the non-batch API are logged at the WARN level since
     * they result in an application that can not scale-out efficiently.
     */
    protected static final transient Logger log = Logger
            .getLogger(ClientIndexView.class);
    
    /**
     * True iff the {@link #log} level is WARN or less.
     */
    final protected static boolean WARN = log.getEffectiveLevel().toInt() <= Level.WARN
            .toInt();

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * Error message used if we were unable to start a new transaction in order
     * to provide read-consistent semantics for an {@link ITx#READ_COMMITTED}
     * view or for a read-only operation on an {@link ITx#UNISOLATED} view.
     */
    static protected final transient String ERR_NEW_TX = "Could not start transaction";
    
    /**
     * Error message used if we were unable to abort a transaction that we
     * started in order to provide read-consistent semantics for an
     * {@link ITx#READ_COMMITTED} view or for a read-only operation on an
     * {@link ITx#UNISOLATED} view.
     */
    static protected final transient String ERR_ABORT_TX = "Could not abort transaction";
    
    private final AbstractScaleOutFederation fed;

    public AbstractScaleOutFederation getFederation() {
        
        return fed;
        
    }
    
    /**
     * The thread pool exposed by {@link IBigdataFederation#getExecutorService()}
     */
    protected ThreadPoolExecutor getThreadPool() {

        return (ThreadPoolExecutor) fed.getExecutorService();

    }

    /**
     * The timeout in milliseconds for tasks run on an {@link IDataService}.
     * 
     * @see Options#CLIENT_TASK_TIMEOUT
     */
    private final long taskTimeout;
    
    /**
     * 
     */
    protected static final String NON_BATCH_API = "Non-batch API";

    /**
     * This may be used to disable the non-batch API, which is quite convenient
     * for locating code that needs to be re-written to use
     * {@link IIndexProcedure}s.
     */
    private final boolean batchOnly;

    /**
     * The default capacity for the {@link #rangeIterator(byte[], byte[])}
     */
    private final int capacity;

    /**
     * The timestamp from the ctor.
     */
    private final long timestamp;

    final public long getTimestamp() {
        
        return timestamp;
        
    }

    /**
     * The name of the scale-out index (from the ctor).
     */
    private final String name;
    
    final public String getName() {
        
        return name;
        
    }

    /**
     * The {@link IMetadataIndex} for this scale-out index.
     */
    private final IMetadataIndex metadataIndex;
    
    /**
     * The {@link IndexMetadata} for the {@link MetadataIndex} that manages the
     * scale-out index. The metadata template for the managed scale-out index is
     * available as a field on this object.
     */
    private final MetadataIndexMetadata metadataIndexMetadata;
    
    /**
     * Obtain the proxy for a metadata service. if this instance fails, then we
     * can always ask for a new instance for the same federation (failover).
     */
    final protected IMetadataService getMetadataService() {
        
        return fed.getMetadataService();
        
    }
    
    /**
     * Return a view of the metadata index for the scale-out index as of the
     * timestamp associated with this index view.
     */
    final protected IMetadataIndex getMetadataIndex() {
        
        return metadataIndex;
        
    }
    
    /**
     * 
     * @see #getRecursionDepth()
     */
    private ThreadLocal<AtomicInteger> recursionDepth = new ThreadLocal<AtomicInteger>() {
   
        protected synchronized AtomicInteger initialValue() {
        
            return new AtomicInteger();
            
        }
        
    };

    /**
     * Return a {@link ThreadLocal} {@link AtomicInteger} whose value is the
     * recursion depth of the current {@link Thread}. This is initially zero
     * when the task is submitted by the application. The value incremented when
     * a task results in a {@link StaleLocatorException} and is decremented when
     * returning from the recursive handling of the
     * {@link StaleLocatorException}.
     * <p>
     * The recursion depth is used:
     * <ol>
     * <li>to limit the #of retries due to {@link StaleLocatorException}s for
     * a split of a task submitted by the application</li>
     * <li> to force execution of retried tasks in the caller's thread.</li>
     * </ol>
     * The latter point is critical - if the retry tasks are run in the client
     * {@link #getThreadPool() thread pool} then all threads in the pool can
     * rapidly become busy awaiting retry tasks with the result that the client
     * is essentially deadlocked.
     * 
     * @return The recursion depth.
     */
    protected AtomicInteger getRecursionDepth() {

        return recursionDepth.get();
        
    }
    
    /**
     * <code>true</code> iff globally consistent read operations are desired
     * for iterators or index procedures mapped across more than one index
     * partition. When <code>true</code> and the index is
     * {@link ITx#READ_COMMITTED} or (if the index is {@link ITx#UNISOLATED} and
     * the operation is read-only), {@link IIndexStore#getLastCommitTime()} is
     * queried at the start of the operation and used as the timestamp for all
     * requests made in support of that operation.
     * <p>
     * Note that {@link StaleLocatorException}s can not arise for
     * read-consistent operations. Such operations use a read-consistent view of
     * the {@link IMetadataIndex} and the locators therefore will not change
     * during the operation.
     * 
     * @todo make this a ctor argument or settable property?
     * 
     * FIXME (I've done the submit methods.) Read-consistent semantics for a
     * read-committed or unisolated view demand the transparent use of a
     * read-historical transaction starting from the lastCommitTime of the
     * federation.
     * <p>
     * However, if we open a transaction then we MUST abort() it or the
     * read-lock will remain in places for resources as of that timestamp.
     * <p>
     * This effects
     * {@link #submit(byte[], byte[], IKeyRangeIndexProcedure, IResultHandler)} ,
     * {@link #submit(int, int, byte[][], byte[][], AbstractIndexProcedureConstructor, IResultHandler)},
     * {@link #locatorScan(long, byte[], byte[], boolean)}, and
     * {@link #rangeIterator(byte[], byte[], int, int, IFilterConstructor)}
     * since all of those methods can span more than a single index partition.
     * <p>
     * Note: we only flag read-only vs read-write in the tx identifier itself,
     * not whether it is a tx vs a light-weight historical read!
     * <p>
     * Fixing this will require that the methods listed above create a
     * read-historical transaction (when the necessary semantics are
     * read-consistent from the global lastCommitTIme) and <code>finally</code>
     * abort() that transaction. This is easy for the submit methods but it will
     * require extensions to the {@link PartitionedTupleIterator} and whatever
     * is supporting the {@link #locatorScan(long, byte[], byte[], boolean)} to
     * make sure that we essentially "close" the iterator.
     */
    final private boolean readConsistent = true;

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
     * Create a view on a scale-out index.
     * 
     * @param fed
     *            The federation containing the index.
     * @param name
     *            The index name.
     * @param timestamp
     *            A transaction identifier, {@link ITx#UNISOLATED} for the
     *            unisolated index view, {@link ITx#READ_COMMITTED}, or
     *            <code>timestamp</code> for a historical view no later than
     *            the specified timestamp.
     * @param metadataIndex
     *            The {@link IMetadataIndex} for the named scale-out index as of
     *            that timestamp. Note that the {@link IndexMetadata} on this
     *            object contains the template {@link IndexMetadata} for the
     *            scale-out index partitions.
     */
    public ClientIndexView(final AbstractScaleOutFederation fed,
            final String name, final long timestamp,
            final IMetadataIndex metadataIndex) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (name == null)
            throw new IllegalArgumentException();
        
        if (metadataIndex == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;

        this.name = name;

        this.timestamp = timestamp;
        
        this.metadataIndex = metadataIndex;
        
        this.metadataIndexMetadata = metadataIndex.getIndexMetadata();
        
        this.capacity = fed.getClient().getDefaultRangeQueryCapacity();
        
        this.batchOnly = fed.getClient().getBatchApiOnly();

        this.taskTimeout = fed.getClient().getTaskTimeout();

    }

    /**
     * Metadata for the {@link MetadataIndex} that manages the scale-out index
     * (cached).
     */
    public MetadataIndexMetadata getMetadataIndexMetadata() {
     
        return metadataIndexMetadata;
        
    }
    
    /**
     * The metadata for the managed scale-out index. Among other things, this
     * gets used to determine how we serialize keys and values for
     * {@link IKeyArrayIndexProcedure}s when we serialize a procedure to be
     * sent to a remote {@link IDataService}.
     */
    public IndexMetadata getIndexMetadata() {

        return metadataIndexMetadata.getManagedIndexMetadata();

    }

    /**
     * @todo Add counters and report to the load balancer (a version for
     *       clients). average responseTime, average queueLength, latency due to
     *       RMI (by also obtaining the response time of the data service
     *       itself), #of procedures run and the execution time for those
     *       procedures; #of splits; #of tuples in each split; total #of tuples.
     * 
     * @todo Report more informatiom, but since scale-out indices can be very
     *       large, this method should report only on aspects of the clients
     *       access to the scale-out index rather than attempting to aggregate
     *       the data from the various index partitions.
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

    /*
     * All of these methods need to divide up the operation across index
     * partitions.
     */

    public long rangeCount() {
        
        return rangeCount(null, null);
        
    }
    
    /**
     * Returns the sum of the range count for each index partition spanned by
     * the key range.
     */
    public long rangeCount(final byte[] fromKey, final byte[] toKey) {

        final LongAggregator handler = new LongAggregator();
        
        final RangeCountProcedure proc = new RangeCountProcedure(
                false/* exact */, fromKey, toKey);

        submit(fromKey, toKey, proc, handler);

        return handler.getResult();
        
    }

    /**
     * The exact range count is obtained by mapping a key-range scan over the
     * index partitions. The operation is parallelized.
     * 
     * @todo watch for overflow of {@link Long#MAX_VALUE}
     */
    final public long rangeCountExact(final byte[] fromKey, final byte[] toKey) {

        final LongAggregator handler = new LongAggregator();
        
        final RangeCountProcedure proc = new RangeCountProcedure(
                true/* exact */, fromKey, toKey);

        submit(fromKey, toKey, proc, handler);

        return handler.getResult();
    
    }
    
    final public ITupleIterator rangeIterator() {

        return rangeIterator(null, null);

    }
    
    /**
     * An {@link ITupleIterator} that kinds the use of a series of
     * {@link ResultSet}s to cover all index partitions spanned by the key
     * range.
     */
    public ITupleIterator rangeIterator(final byte[] fromKey, final byte[] toKey) {
        
        return rangeIterator(fromKey, toKey, capacity,
                IRangeQuery.DEFAULT /* flags */, null/* filter */);
        
    }

    /**
     * Identifies the index partition(s) that are spanned by the key range query
     * and maps an iterator across each index partition. The iterator buffers
     * responses up to the specified capacity and a follow up iterator request
     * is automatically issued if the iterator has not exhausted the key range
     * on a given index partition. Once the iterator is exhausted on a given
     * index partition it is then applied to the next index partition spanned by
     * the key range.
     * 
     * @todo If the return iterator implements {@link ITupleCursor} then this
     *       will need be modified to defer request of the initial result set
     *       until the caller uses first(), last(), seek(), hasNext(), or
     *       hasPrior().
     */
    public ITupleIterator rangeIterator(final byte[] fromKey, final byte[] toKey,
            int capacity, int flags, final IFilterConstructor filter ) {

        if (capacity == 0) {

            capacity = this.capacity;

        }

        /*
         * Does the iterator declare that it will not write back on the index?
         */
        final boolean readOnly = ((flags & READONLY) != 0);

        if (readOnly && ((flags & REMOVEALL) != 0)) {

            throw new IllegalArgumentException();

        }

        long timestamp = getTimestamp();

        if (timestamp == ITx.UNISOLATED && readOnly) {

            // run as read-committed.
            timestamp = ITx.READ_COMMITTED;

        }
        
        if (timestamp == ITx.READ_COMMITTED && readConsistent) {
        
            // run as globally consistent read.
            timestamp = TimestampUtility.asHistoricalRead(fed.getLastCommitTime());
            
        }
        
        return new PartitionedTupleIterator(this, timestamp, fromKey,
                toKey, capacity, flags, filter);
        
    }

    public Object submit(final byte[] key, final ISimpleIndexProcedure proc) {

        // Find the index partition spanning that key.
        final PartitionLocator locator = getMetadataIndex().find(key);

        /*
         * Submit procedure to that data service.
         */
        try {

            if (INFO) {

                log.info("Submitting " + proc.getClass() + " to partition"
                        + locator);

            }

            // required to get the result back from the procedure.
            final IResultHandler resultHandler = new IdentityHandler();

            // procedure is not mapped, so timestamp is always the index timestamp.
            final SimpleDataServiceProcedureTask task = new SimpleDataServiceProcedureTask(
                    key, getTimestamp(), new Split(locator, 0, 0), proc, resultHandler);

            // submit procedure and await completion.
            getThreadPool().submit(task).get(taskTimeout, TimeUnit.MILLISECONDS);

            // the singleton result.
            Object result = resultHandler.getResult();

            return result;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }
    
    /**
     * Returns an iterator that will visit the {@link PartitionLocator}s for
     * the specified scale-out index key range.
     * 
     * @see AbstractScaleOutFederation#locatorScan(String, long, byte[], byte[],
     *      boolean)
     * 
     * @param timestamp
     *            The timestamp that will be used to visit the locators.
     * @param fromKey
     *            The scale-out index first key that will be visited
     *            (inclusive). When <code>null</code> there is no lower bound.
     * @param toKey
     *            The first scale-out index key that will NOT be visited
     *            (exclusive). When <code>null</code> there is no upper bound.
     * @param reverseScan
     *            <code>true</code> if you need to visit the index partitions
     *            in reverse key order (this is done when the partitioned
     *            iterator is scanning backwards).
     * 
     * @return The iterator. The value returned by {@link ITuple#getValue()}
     *         will be a serialized {@link PartitionLocator} object.
     */
    @SuppressWarnings("unchecked")
    public Iterator<PartitionLocator> locatorScan(final long timestamp,
            final byte[] fromKey, final byte[] toKey, final boolean reverseScan) {
        
        return fed.locatorScan(name, timestamp, fromKey, toKey, reverseScan);
        
//        if (INFO)
//            log.info("Querying metadata index: name=" + name + ", timestamp="
//                    + timestamp + ", reverseScan=" + reverseScan + ", fromKey="
//                    + BytesUtil.toString(fromKey) + ", toKey="
//                    + BytesUtil.toString(toKey) + ", capacity=" + capacity);
//        
//        /*
//         * The iterator uses a read-consistent view of the MDI as of the
//         * caller specified timestamp.
//         */
//        final IMetadataIndex mdi = timestamp == getTimestamp() ? getMetadataIndex()
//                : fed.getMetadataIndex(name, timestamp);
//        
//        final ITupleIterator<PartitionLocator> itr;
//
//        // the values are the locators (keys are not required).
//        final int flags = IRangeQuery.VALS;
//        
//        if (reverseScan) {
//         
//            /*
//             * Reverse locator scan.
//             * 
//             * The first locator visited will be the first index partition whose
//             * leftSeparator is LT the optional toKey. (If the toKey falls on an
//             * index partition boundary then we use the prior index partition).
//             */
//
//            itr = mdi.rangeIterator(//
//                    fromKey,//
//                    toKey, //
//                    0, // capacity
//                    flags | IRangeQuery.REVERSE,
//                    null // filter
//                    );
//
//        } else {
//            
//            /*
//             * Forward locator scan.
//             * 
//             * Note: The scan on the metadata index needs to start at the index
//             * partition in which the fromKey would be located. Therefore, when
//             * the fromKey is specified, we replace it with the leftSeparator of
//             * the index partition which would contain that fromKey.
//             */
//
//            final byte[] _fromKey = fromKey == null //
//                ? null //
//                : mdi.find(fromKey).getLeftSeparatorKey()//
//                ;
//
//            itr = mdi.rangeIterator(//
//                    _fromKey,//
//                    toKey, //
//                    0, // capacity
//                    flags,//
//                    null // filter
//                    );
//
//        }
//
//        return itr;
        
    }
    
    /**
     * Maps an {@link IIndexProcedure} across a key range by breaking it down
     * into one task per index partition spanned by that key range.
     * <p>
     * Note: In order to avoid growing the task execution queue without bound,
     * an upper bound of {@link Options#CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST}
     * tasks will be placed onto the queue at a time. More tasks will be
     * submitted once those tasks finish until all tasks have been executed.
     * When the task is not parallelizable the tasks will be submitted to the
     * corresponding index partitions at a time and in key order.
     */
    public void submit(final byte[] fromKey, final byte[] toKey,
            final IKeyRangeIndexProcedure proc, final IResultHandler resultHandler) {

        if (proc == null)
            throw new IllegalArgumentException();

        if (readConsistent && proc.isReadOnly()
                && TimestampUtility.isReadCommittedOrUnisolated(getTimestamp())) {
            /*
             * Use globally consistent reads for the mapped procedure.
             */

            final long tx;
            try {

                tx = fed.getTransactionService().newTx(ITx.READ_COMMITTED);

            } catch (IOException ex) {

                throw new RuntimeException(ERR_NEW_TX, ex);

            }

            try {

                mapProcedure(tx, fromKey, toKey, proc, resultHandler);

            } finally {

                try {

                    fed.getTransactionService().abort(tx);

                } catch (IOException ex) {

                    // log error and ignore since the operation is complete.
                    log.error("Could not abort tx: tx=" + tx, ex);

                }

            }
            
        } else {

            /*
             * Timestamp is either a tx already or the caller is risking errors
             * with lightweight historical reads.
             */
            
            mapProcedure(timestamp, fromKey, toKey, proc, resultHandler);
            
        }

    }

    /**
     * Inner method uses the timestamp choosen above.
     * 
     * @param tx
     * @param fromKey
     * @param toKey
     * @param proc
     * @param resultHandler
     */
    private void mapProcedure(final long tx, final byte[] fromKey,
            final byte[] toKey, final IKeyRangeIndexProcedure proc,
            final IResultHandler resultHandler) {

        // true iff the procedure is known to be parallelizable.
        final boolean parallel = proc instanceof IParallelizableIndexProcedure;

        if (INFO)
            log.info("Procedure " + proc.getClass().getName()
                    + " will be mapped across index partitions in "
                    + (parallel ? "parallel" : "sequence"));

        final int poolSize = ((ThreadPoolExecutor) getThreadPool())
                .getCorePoolSize();

        final int maxTasksPerRequest = fed.getClient()
                .getMaxParallelTasksPerRequest();

        // max #of tasks to queue at once.
        final int maxTasks = poolSize == 0 ? maxTasksPerRequest : Math.min(
                poolSize, maxTasksPerRequest);

        // verify positive or the loop below will fail to progress.
        assert maxTasks > 0 : "maxTasks=" + maxTasks + ", poolSize=" + poolSize
                + ", maxTasksPerRequest=" + maxTasksPerRequest;

        // scan visits index partition locators in key order.
        final Iterator<PartitionLocator> itr = locatorScan(tx, fromKey, toKey,
                false/* reverseScan */);

        long nparts = 0;

        while (itr.hasNext()) {

            /*
             * Process the remaining locators a "chunk" at a time. The chunk
             * size is choosen to be the configured size of the client thread
             * pool. This lets us avoid overwhelming the thread pool queue when
             * mapping a procedure across a very large #of index partitions.
             * 
             * The result is an ordered list of the tasks to be executed. The
             * order of the tasks is determined by the natural order of the
             * index partitions - that is, we submit the tasks in key order so
             * that a non-parallelizable procedure will be mapped in the correct
             * sequence.
             */

            final ArrayList<Callable<Void>> tasks = new ArrayList<Callable<Void>>(
                    maxTasks);

            for (int i = 0; i < maxTasks && itr.hasNext(); i++) {

                final PartitionLocator locator = itr.next();

                final Split split = new Split(locator, 0/* fromIndex */, 0/* toIndex */);

                tasks.add(new KeyRangeDataServiceProcedureTask(fromKey, toKey,
                        tx, split, proc, resultHandler));

                nparts++;

            }

            runTasks(parallel, tasks);

            // next index partition(s)

        }

        if (INFO)
            log.info("Procedure " + proc.getClass().getName()
                    + " mapped across " + nparts + " index partitions in "
                    + (parallel ? "parallel" : "sequence"));

    }

    /**
     * The procedure will be transparently broken down and executed against each
     * index partitions spanned by its keys. If the <i>ctor</i> creates
     * instances of {@link IParallelizableIndexProcedure} then the procedure
     * will be mapped in parallel against the relevant index partitions.
     * <p>
     * Note: Unlike mapping an index procedure across a key range, this method
     * is unable to introduce a truely enourmous burden on the client's task
     * queue since the #of tasks arising is equal to the #of splits and bounded
     * by <code>n := toIndex - fromIndex</code>.
     * 
     * @return The aggregated result of applying the procedure to the relevant
     *         index partitions.
     */
    public void submit(final int fromIndex, final int toIndex,
            final byte[][] keys, final byte[][] vals,
            final AbstractIndexProcedureConstructor ctor,
            final IResultHandler aggregator) {

        if (ctor == null) {

            throw new IllegalArgumentException();
        
        }
        
//        if (aggregator == null) {
//
//            throw new IllegalArgumentException();
//            
//        }
        
        /*
         * Break down the data into a series of "splits", each of which will be
         * applied to a different index partition.
         */

        final LinkedList<Split> splits = splitKeys(fromIndex, toIndex, keys);

        final int nsplits = splits.size();

        if (nsplits == 0)
            return;

        /*
         * Examine the first split in order to figure out how we are going to
         * run this operation.
         */
        
        // iff procedure can be executed in parallel.
        final boolean parallel;
        // iff procedure is read-only.
        final boolean readOnly;
        // iff we created a read-historical tx in this method.
        final boolean isTx;
        // the timestamp that will be used for the operation.
        final long ts;
        {

            final Split split = splits.getFirst();

            final IKeyArrayIndexProcedure proc = ctor.newInstance(this,
                    split.fromIndex, split.toIndex, keys, vals);

            parallel = (proc instanceof IParallelizableIndexProcedure);

            readOnly = proc.isReadOnly();

            if (readConsistent
                    && proc.isReadOnly()
                    && TimestampUtility
                            .isReadCommittedOrUnisolated(getTimestamp())) {

                /*
                 * Create a read-historical transaction from the last commit
                 * point of the federation in order to provide consistent
                 * reads for the mapped procedure.
                 */

                isTx = true;
                
                try {
                
                    ts = fed.getTransactionService().newTx(ITx.READ_COMMITTED);
                    
                } catch (IOException e) {
                    
                    throw new RuntimeException(ERR_NEW_TX,e);
                    
                }

            } else {
            
                // might be a tx, but not one that we created here.
                isTx = false;
                
                ts = getTimestamp();
            
            }
            
        }
        
        try {

            /*
             * Create the instances of the procedure for each split.
             */

            final ArrayList<Callable<Void>> tasks = new ArrayList<Callable<Void>>(
                    nsplits);

            final Iterator<Split> itr = splits.iterator();

            while (itr.hasNext()) {

                final Split split = itr.next();

                final IKeyArrayIndexProcedure proc = ctor.newInstance(this,
                        split.fromIndex, split.toIndex, keys, vals);

                tasks.add(new KeyArrayDataServiceProcedureTask(keys, vals, ts,
                        split, proc, aggregator, ctor));

            }

            if (INFO)
                log.info("Procedures created by " + ctor.getClass().getName()
                        + " will run on " + nsplits + " index partitions in "
                        + (parallel ? "parallel" : "sequence"));

            runTasks(parallel, tasks);

        } finally {

            if (isTx) {

                try {

                    fed.getTransactionService().abort(ts);
                    
                } catch (IOException e) {
                    
                    /*
                     * log error but do not rethrow since operation is over
                     * anyway.
                     */
                    
                    log.error(ERR_ABORT_TX + ": " + ts, e);
                    
                }
        
            }
            
        }
        
    }

    /**
     * Runs a set of tasks.
     * <p>
     * Note: If {@link #getRecursionDepth()} evaluates to a value larger than
     * zero then the task(s) will be forced to execute in the caller's thread.
     * <p>
     * {@link StaleLocatorException}s are handled by the recursive application
     * of <code>submit()</code>. These recursively submitted tasks are forced
     * to run in the caller's thread by incrementing the
     * {@link #getRecursionDepth()} counter. This is done to prevent the thread
     * pool from becoming deadlocked as threads wait on threads handling stale
     * locator retries. The deadlock situation arises as soon as all threads in
     * the thread pool are waiting on stale locator retries as there are no
     * threads remaining to process those retries.
     * 
     * @param parallel
     *            <code>true</code> iff the tasks MAY be run in parallel.
     * @param tasks
     *            The tasks to be executed.
     */
    protected void runTasks(final boolean parallel,
            final ArrayList<Callable<Void>> tasks) {

        if(tasks.isEmpty()) {
            
            log.warn("No tasks to run?",new RuntimeException("No tasks to run?"));
            
            return;
            
        }
        
        if (getRecursionDepth().get() > 0) {

            /*
             * Force sequential execution of the tasks in the caller's thread.
             */

            runInCallersThread(tasks);

        } else if(tasks.size()==1) {

            runOne(tasks.get(0));
            
        } else if (parallel) {

            /*
             * Map procedure across the index partitions in parallel.
             */

            runParallel(tasks);

        } else {

            /*
             * sequential execution against of each split in turn.
             */

            runSequence(tasks);

        }

    }
    
    /**
     * Maps a set of {@link DataServiceProcedureTask} tasks across the index
     * partitions in strict sequence. The tasks are run on the
     * {@link #getThreadPool()} so that sequential tasks never increase the
     * total burden placed by the client above the size of that thread pool.
     * 
     * @param tasks
     *            The tasks.
     */
    protected void runOne(Callable<Void> task) {

        if (INFO)
            log.info("Running one task (#active="
                    + getThreadPool().getActiveCount() + ", queueSize="
                    + getThreadPool().getQueue().size() + ") : "
                    + task.toString());

        try {

            final Future<Void> f = getThreadPool().submit(task);

            // await completion of the task.
            f.get(taskTimeout, TimeUnit.MILLISECONDS);

        } catch (Exception e) {

            if (INFO)
                log.info("Execution failed: task=" + task, e);

            throw new ClientException("Execution failed: " + task,e);

        }

    }
    
    /**
     * Maps a set of {@link DataServiceProcedureTask} tasks across the index
     * partitions in parallel.
     * 
     * @param tasks
     *            The tasks.
     */
    protected void runParallel(ArrayList<Callable<Void>> tasks) {

        final long begin = System.currentTimeMillis();
        
        if(INFO)
        log.info("Running " + tasks.size() + " tasks in parallel (#active="
                + getThreadPool().getActiveCount() + ", queueSize="
                + getThreadPool().getQueue().size() + ") : "
                + tasks.get(0).toString());
        
        int nfailed = 0;
        
        final LinkedList<Throwable> causes = new LinkedList<Throwable>();
        
        try {

            final List<Future<Void>> futures = getThreadPool().invokeAll(tasks,
                    taskTimeout, TimeUnit.MILLISECONDS);
            
            final Iterator<Future<Void>> itr = futures.iterator();
           
            int i = 0;
            
            while(itr.hasNext()) {
                
                final Future<Void> f = itr.next();
                
                try {
                    
                    f.get();
                    
                } catch (ExecutionException e) {
                    
                    final AbstractDataServiceProcedureTask task = (AbstractDataServiceProcedureTask) tasks
                            .get(i);

                    // log w/ stack trace so that we can see where this came
                    // from.
                    log.error("Execution failed: task=" + task, e);

                    if (task.causes != null) {

                        causes.addAll(task.causes);

                    } else {

                        causes.add(e);

                    }

                    nfailed++;
                    
                }
                
            }
            
        } catch (InterruptedException e) {

            throw new RuntimeException("Interrupted: "+e);

        }
        
        if (nfailed > 0) {
            
            throw new ClientException("Execution failed: ntasks="
                    + tasks.size() + ", nfailed=" + nfailed, causes);
            
        }

        if (INFO)
            log.info("Ran " + tasks.size() + " tasks in parallel: elapsed="
                + (System.currentTimeMillis() - begin));

    }

    /**
     * Maps a set of {@link DataServiceProcedureTask} tasks across the index
     * partitions in strict sequence. The tasks are run on the
     * {@link #getThreadPool()} so that sequential tasks never increase the
     * total burden placed by the client above the size of that thread pool.
     * 
     * @param tasks
     *            The tasks.
     */
    protected void runSequence(ArrayList<Callable<Void>> tasks) {

        if (INFO)
            log.info("Running " + tasks.size() + " tasks in sequence (#active="
                    + getThreadPool().getActiveCount() + ", queueSize="
                    + getThreadPool().getQueue().size() + ") : "
                    + tasks.get(0).toString());

        final Iterator<Callable<Void>> itr = tasks.iterator();

        while (itr.hasNext()) {

            final AbstractDataServiceProcedureTask task = (AbstractDataServiceProcedureTask) itr
                    .next();

            try {

                final Future<Void> f = getThreadPool().submit(task);
                
                // await completion of the task.
                f.get(taskTimeout, TimeUnit.MILLISECONDS);

            } catch (Exception e) {
        
                if(INFO) log.info("Execution failed: task=" + task, e);

                throw new ClientException("Execution failed: " + task, e, task.causes);

            }

        }

    }
    
    /**
     * Executes the tasks in the caller's thread.
     * 
     * @param tasks
     *            The tasks.
     */
    protected void runInCallersThread(final ArrayList<Callable<Void>> tasks) {
        
        final int ntasks = tasks.size();
        
        if (WARN && ntasks > 1)
            log.warn("Running " + ntasks
                + " tasks in caller's thread: recursionDepth="
                + getRecursionDepth().get() + "(#active="
                + getThreadPool().getActiveCount() + ", queueSize="
                + getThreadPool().getQueue().size() + ") : "
                + tasks.get(0).toString());

        final Iterator<Callable<Void>> itr = tasks.iterator();

        while (itr.hasNext()) {

            final AbstractDataServiceProcedureTask task = (AbstractDataServiceProcedureTask) itr
                    .next();

            try {

                task.call();
                
            } catch (Exception e) {

                if(INFO) log.info("Execution failed: task=" + task, e);

                throw new ClientException("Execution failed: " + task, e, task.causes);
                
            }
            
        }

    }
    
    /**
     * Helper class for submitting an {@link IIndexProcedure} to run on an
     * {@link IDataService}. The class traps {@link StaleLocatorException}s and
     * handles the redirection of requests to the appropriate
     * {@link IDataService}. When necessary, the data for an
     * {@link IKeyArrayIndexProcedure} will be re-split in order to distribute
     * the requests to the new index partitions following a split of the target
     * index partition.
     * <p>
     * Note: If an index partition is moved then the key range is unchanged.
     * <p>
     * Note: If an index partition is split, then the key range for each new
     * index partition is a sub-range of the original key range and the total
     * key range for the new index partitions is exactly the original key range.
     * <p>
     * Note: If an index partition is joined with another index partition then
     * the key range of the new index partition is increased. However, we will
     * wind up submitting one request to the new index partition for each index
     * partition that we knew about at the time that the procedure was first
     * mapped across the index partitions.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected abstract class AbstractDataServiceProcedureTask implements Callable<Void> {

        /**
         * The timestamp for the operation. This will be the timestamp for the
         * index view unless the operation is read-only, in which case a
         * different timestamp may be choosen either to improve concurrency or
         * to provide globally read-consistent operations.
         */
        protected final long timestamp;
        protected final Split split;
        protected final IIndexProcedure proc;
        protected final IResultHandler resultHandler;
        private final TaskCounters taskCounters;
//        protected final TaskCounters taskCountersByProc;
//        protected final TaskCounters taskCountersByIndex;
        
        private long nanoTime_submitTask;
        private long nanoTime_beginWork;
        private long nanoTime_finishedWork;
        
        /**
         * If the task fails then this will be populated with an ordered list of
         * the exceptions. There will be one exception per-retry of the task.
         * For some kinds of failure this list MAY remain unbound.
         */
        protected List<Throwable> causes = null;
        
        /**
         * A human friendly representation.
         */
        public String toString() {
            
            return "Index=" + ClientIndexView.this.getName() + ", Procedure "
                    + proc.getClass().getName() + " : " + split;
            
        }

        /**
         * Variant used for procedures that are NOT instances of
         * {@link IKeyArrayIndexProcedure}.
         * 
         * @param timestamp
         * @param split
         * @param proc
         * @param resultHandler
         */
        public AbstractDataServiceProcedureTask(long timestamp, Split split,
                IIndexProcedure proc, IResultHandler resultHandler
        ) {
            
            if (split.pmd == null)
                throw new IllegalArgumentException();
            
            if(!(split.pmd instanceof PartitionLocator)) {
                
                throw new IllegalArgumentException("Split does not have a locator");
                
            }

            if (proc == null)
                throw new IllegalArgumentException();

            this.timestamp = timestamp;
            
            this.split = split;
            
            this.proc = proc;

            this.resultHandler = resultHandler;

            this.taskCounters = fed.getTaskCounters();
            
//            this.taskCountersByProc = fed.getTaskCounters(proc);
//            
//            this.taskCountersByIndex = fed.getTaskCounters(ClientIndexView.this);

            nanoTime_submitTask = System.nanoTime();
            
        }
        
        final public Void call() throws Exception {

            // the index partition locator.
            final PartitionLocator locator = (PartitionLocator) split.pmd;

            taskCounters.taskSubmitCount.incrementAndGet();
//            taskCountersByProc.taskSubmitCount.incrementAndGet();
//            taskCountersByIndex.taskSubmitCount.incrementAndGet();

            nanoTime_beginWork = System.nanoTime();
            final long queueWaitingTime = nanoTime_beginWork - nanoTime_submitTask;
            taskCounters.queueWaitingTime.addAndGet(queueWaitingTime);
//            taskCountersByProc.queueWaitingTime.addAndGet(queueWaitingTime);
//            taskCountersByIndex.queueWaitingTime.addAndGet(queueWaitingTime);
            
            try {

                submit(locator);
                
                taskCounters.taskSuccessCount.incrementAndGet();
//                taskCountersByProc.taskSuccessCount.incrementAndGet();
//                taskCountersByIndex.taskSuccessCount.incrementAndGet();

            } catch(Exception ex) {

                taskCounters.taskFailCount.incrementAndGet();
//                taskCountersByProc.taskFailCount.incrementAndGet();
//                taskCountersByIndex.taskFailCount.incrementAndGet();

                throw ex;
                
            } finally {

                nanoTime_finishedWork = System.nanoTime();

                taskCounters.taskCompleteCount.incrementAndGet();
//                taskCountersByProc.taskCompleteCount.incrementAndGet();
//                taskCountersByIndex.taskCompleteCount.incrementAndGet();

                // increment by the amount of time that the task was executing.
                final long serviceNanoTime = nanoTime_finishedWork - nanoTime_beginWork;
                taskCounters.serviceNanoTime.addAndGet(serviceNanoTime);
//                taskCountersByProc.serviceNanoTime.addAndGet(serviceNanoTime);
//                taskCountersByIndex.serviceNanoTime.addAndGet(serviceNanoTime);

                // increment by the total time from submit to completion.
                final long queuingNanoTime = nanoTime_finishedWork - nanoTime_submitTask;
                taskCounters.queuingNanoTime.addAndGet(queuingNanoTime);
//                taskCountersByProc.queuingNanoTime.addAndGet(queuingNanoTime);
//                taskCountersByIndex.queuingNanoTime.addAndGet(queuingNanoTime);

            }
            
            return null;

        }

        /**
         * Submit the procedure to the {@link IDataService} identified by the
         * locator.
         * 
         * @param locator
         *            An index partition locator.
         *            
         * @throws Exception
         */
        final protected void submit(PartitionLocator locator) throws Exception {
            
            if (locator == null)
                throw new IllegalArgumentException();
            
            if (Thread.interrupted())
                throw new InterruptedException();
            
            // resolve service UUID to data service.
            final IDataService dataService = getDataService(locator);

            if (dataService == null)
                throw new RuntimeException("DataService not found: " + locator);
            
            // the name of the index partition.
            final String name = DataService.getIndexPartitionName(//
                    ClientIndexView.this.name, // the name of the scale-out index.
                    split.pmd.getPartitionId() // the index partition identifier.
                    );

            if (INFO)
                log.info("Submitting task=" + this + " on " + dataService);

            try {

                submit(dataService, name);

            } catch(Exception ex) {

                if (causes == null)
                    causes = new LinkedList<Throwable>();
                
                causes.add(ex);
                
                final StaleLocatorException cause = (StaleLocatorException) InnerCause
                        .getInnerCause(ex, StaleLocatorException.class);
                
                if(cause != null) {

                    // notify the client so that it can refresh its cache.
                    staleLocator(locator, cause);
                    
                    // retry the operation.
                    retry();
                    
                } else {
                    
                    throw ex;
                    
                }
                
            }
            
        }
        
        /**
         * Submit the procedure to the {@link IDataService} and aggregate the
         * result with the caller's {@link IResultHandler} (if specified).
         * 
         * @param dataService
         *            The data service on which the procedure will be executed.
         * @param name
         *            The name of the index partition on that data service.
         * 
         * @todo do not require aggregator for {@link ISimpleIndexProcedure} so
         *       make this abstract in the base class or just override for
         *       {@link ISimpleIndexProcedure}. This would also mean that
         *       {@link #call()} would return the value for
         *       {@link ISimpleIndexProcedure}.
         */
        @SuppressWarnings("unchecked")
        final private void submit(IDataService dataService, String name) throws Exception {

            /*
             * Note: The timestamp here is the one specified for the task. This
             * allows us to realize read-consistent procedures by choosing the
             * lastCommitTime of the federation for the procedure.
             */
            final Object result = dataService.submit(timestamp, name, proc);

            if (resultHandler != null) {

                resultHandler.aggregate(result, split);

            }

        }
        
        /**
         * Invoked when {@link StaleLocatorException} was thrown. Since the
         * procedure was being run against an index partition of some scale-out
         * index this exception indicates that the index partition locator was
         * stale. We re-cache the locator(s) for the same key range as the index
         * partition which we thought we were addressing and then re-map the
         * operation against those updated locator(s). Note that a split will go
         * from one locator to N locators for a key range while a join will go
         * from N locators for a key range to 1. A move does not change the #of
         * locators for the key range, just where that index partition is living
         * at this time.
         * 
         * @throws Exception
         */
        abstract protected void retry() throws Exception;

    }

    /**
     * Class handles stale locators by finding the current locator for the
     * <i>key</i> and redirecting the request to execute the procedure on the
     * {@link IDataService} identified by that locator.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class SimpleDataServiceProcedureTask extends AbstractDataServiceProcedureTask {

        protected final byte[] key;
        
        private int ntries = 1;
        
        /**
         * @param key
         * @param split
         * @param proc
         * @param resultHandler
         */
        public SimpleDataServiceProcedureTask(byte[] key, long timestamp,
                Split split, ISimpleIndexProcedure proc,
                IResultHandler resultHandler) {

            super(timestamp, split, proc, resultHandler);
            
            if (key == null)
                throw new IllegalArgumentException();
        
            this.key = key;
            
        }

        /**
         * The locator is stale. We locate the index partition that spans the
         * {@link #key} and re-submit the request.
         */
        @Override
        protected void retry() throws Exception {
            
            if (ntries++ > fed.getClient().getMaxStaleLocatorRetries()) {

                throw new RuntimeException("Retry count exceeded: ntries="
                        + ntries);

            }

            final PartitionLocator locator = getMetadataIndex().find(key);

            if(INFO)
            log.info("Retrying: proc=" + proc.getClass().getName()
                    + ", locator=" + locator + ", ntries=" + ntries);

            /*
             * Note: In this case we do not recursively submit to the outer
             * interface on the client since all we need to do is fetch the
             * current locator for the key and re-submit the request to the data
             * service identified by that locator.
             */

            submit(locator);
            
        }
        
    }

    /**
     * Handles stale locators for {@link IKeyRangeIndexProcedure}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class KeyRangeDataServiceProcedureTask extends AbstractDataServiceProcedureTask {

        final byte[] fromKey;
        final byte[] toKey;
        
        /**
         * @param fromKey
         * @param toKey
         * @param split
         * @param proc
         * @param resultHandler
         */
        public KeyRangeDataServiceProcedureTask(byte[] fromKey, byte[] toKey,
                long timestamp, Split split, IKeyRangeIndexProcedure proc,
                IResultHandler resultHandler) {

            super(timestamp, split, proc, resultHandler);
            
            /*
             * Constrain the range to the index partition. This constraint will
             * be used if we discover that the locator data was stale in order
             * to discover the new locator(s).
             */
            
            this.fromKey = AbstractKeyRangeIndexProcedure.constrainFromKey(
                    fromKey, split.pmd);

            this.toKey = AbstractKeyRangeIndexProcedure.constrainFromKey(toKey,
                    split.pmd);
            
        }

        /**
         * The {@link IKeyRangeIndexProcedure} is re-mapped for the constrained
         * key range of the stale locator using
         * {@link ClientIndexView#submit(byte[], byte[], IKeyRangeIndexProcedure, IResultHandler)}.
         */
        @Override
        protected void retry() throws Exception {

            /*
             * Note: recursive retries MUST run in the same thread in order to
             * avoid deadlock of the client's thread pool. The recursive depth
             * is used to enforce this constrain.
             */

            final int depth = getRecursionDepth().incrementAndGet();

            try {
            
                if (depth > fed.getClient().getMaxStaleLocatorRetries()) {

                    throw new RuntimeException("Retry count exceeded: ntries="
                            + depth);
                    
                }
                
                ClientIndexView.this.submit(fromKey, toKey,
                    (IKeyRangeIndexProcedure) proc, resultHandler);
            
            } finally {
                
                final int tmp = getRecursionDepth().decrementAndGet();
                
                assert tmp >= 0 : "depth="+depth+", tmp="+tmp;
                
            }
            
        }
        
    }

    /**
     * Handles stale locators for {@link IKeyArrayIndexProcedure}s. When
     * necessary the procedure will be re-split.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class KeyArrayDataServiceProcedureTask extends AbstractDataServiceProcedureTask {

        protected final byte[][] keys;
        protected final byte[][] vals;
        protected final AbstractIndexProcedureConstructor ctor;
        
        /**
         * Variant used for {@link IKeyArrayIndexProcedure}s.
         * 
         * @param keys
         *            The original keys[][].
         * @param vals
         *            The original vals[][].
         * @param split
         *            The split identifies the subset of keys and values to be
         *            applied by this procedure.
         * @param proc
         *            The procedure instance.
         * @param resultHandler
         *            The result aggregator.
         * @param ctor
         *            The object used to create instances of the <i>proc</i>.
         *            This is used to re-split the data if necessary in response
         *            to stale locator information.
         */
        public KeyArrayDataServiceProcedureTask(byte[][] keys, byte[][] vals,
                long timestamp, Split split, IKeyArrayIndexProcedure proc,
                IResultHandler resultHandler, AbstractIndexProcedureConstructor ctor
        ) {
            
            super( timestamp, split, proc, resultHandler );
            
            if (ctor == null)
                throw new IllegalArgumentException();

            this.ctor = ctor;
            
            this.keys = keys;
            
            this.vals = vals;
            
        }

        /**
         * Submit using
         * {@link ClientIndexView#submit(int, int, byte[][], byte[][], AbstractIndexProcedureConstructor, IResultHandler)}.
         * This will recompute the split points and re-map the procedure across
         * the newly determined split points.
         */
        @Override
        protected void retry() throws Exception {

            /*
             * Note: recursive retries MUST run in the same thread in order to
             * avoid deadlock of the client's thread pool. The recursive depth
             * is used to enforce this constrain.
             */

            final int depth = getRecursionDepth().incrementAndGet();

            try {
            
                if (depth > fed.getClient().getMaxStaleLocatorRetries()) {

                    throw new RuntimeException("Retry count exceeded: ntries="
                            + depth);
                    
                }
                
                ClientIndexView.this.submit(split.fromIndex, split.toIndex, keys,
                        vals, ctor, resultHandler);
                
            } finally {
                
                final int tmp = getRecursionDepth().decrementAndGet();
                
                assert tmp >= 0 : "depth="+depth+", tmp="+tmp;
                
            }
            
        }
        
    }
    
    /**
     * Utility method to split a set of ordered keys into partitions based the
     * index partitions defined for a scale-out index.
     * <p>
     * Find the partition for the first key. Check the last key, if it is in the
     * same partition then then this is the simplest case and we can just send
     * the data along.
     * <p>
     * Otherwise, perform a binary search on the remaining keys looking for the
     * index of the first key GTE the right separator key for that partition.
     * The batch for this partition is formed from all keys from the first key
     * for that partition up to but excluding the index position identified by
     * the binary search (if there is a match; if there is a miss, then the
     * binary search result needs to be converted into a key index and that will
     * be the last key for the current partition).
     * <p>
     * Examine the next key and repeat the process until all keys have been
     * allocated to index partitions.
     * <p>
     * Note: Split points MUST respect the "row" identity for a sparse row
     * store, but we get that constraint by maintaining the index partition
     * boundaries in agreement with the split point constraints for the index.
     * 
     * @param fromIndex
     *            The index of the first key in <i>keys</i> to be processed
     *            (inclusive).
     * @param toIndex
     *            The index of the last key in <i>keys</i> to be processed.
     * @param keys
     *            An array of keys. Each key is an interpreted as an unsigned
     *            byte[]. All keys must be non-null. The keys must be in sorted
     *            order.
     * 
     * @return The {@link Split}s that you can use to form requests based on
     *         the identified first/last key and partition identified by this
     *         process.
     * 
     * @see Arrays#sort(Object[], int, int, java.util.Comparator)
     * 
     * @see BytesUtil#compareBytes(byte[], byte[])
     * 
     * @todo Caching? This procedure performs the minimum #of lookups using
     *       {@link IMetadataIndex#find(byte[])} since that operation will be an
     *       RMI in a distributed federation. The find(byte[] key) operation is
     *       difficult to cache since it locates the index partition that would
     *       span the key and many, many different keys could fit into that same
     *       index partition. The only effective cache technique may be an LRU
     *       that scans ~10 caches locators to see if any of them is a match
     *       before reaching out to the remote {@link IMetadataService}. Or
     *       perhaps the locators can be cached in a local BTree and a miss
     *       there would result in a read through to the remote
     *       {@link IMetadataService} but then we have the problem of figuring
     *       out when to release locators if the client is long-lived.
     */
    public LinkedList<Split> splitKeys(final int fromIndex, final int toIndex,
            final byte[][] keys) {

        assert keys != null;
        
        assert fromIndex >= 0;
        assert fromIndex < toIndex;

        assert toIndex <= keys.length;
        
        final LinkedList<Split> splits = new LinkedList<Split>();
        
        // start w/ the first key.
        int currentIndex = fromIndex;
        
        while (currentIndex < toIndex) {
                
            // partition spanning the current key (RMI)
            final PartitionLocator locator = getMetadataIndex().find(keys[currentIndex]);

            if(locator==null) throw new RuntimeException("No index partitions?: name="+name);
            
            final byte[] rightSeparatorKey = locator.getRightSeparatorKey();

            if (rightSeparatorKey == null) {

                /*
                 * The last index partition does not have an upper bound and
                 * will absorb any keys that order GTE to its left separator
                 * key.
                 */

                assert validSplit( locator, currentIndex, toIndex, keys );
                
                splits.add(new Split(locator, currentIndex, toIndex));

                // done.
                currentIndex = toIndex;

            } else {

                /*
                 * Otherwise this partition has an upper bound, so figure out
                 * the index of the last key that would go into this partition.
                 * 
                 * We do this by searching for the rightSeparator of the index
                 * partition itself.
                 */
                
                int pos = BytesUtil.binarySearch(keys, currentIndex, toIndex
                        - currentIndex, rightSeparatorKey);

                if (pos >= 0) {

                    /*
                     * There is a hit on the rightSeparator key. The index
                     * returned by the binarySearch is the exclusive upper bound
                     * for the split. The key at that index is excluded from the
                     * split - it will be the first key in the next split.
                     * 
                     * Note: There is a special case when the keys[] includes
                     * duplicates of the key that corresponds to the
                     * rightSeparator. This causes a problem where the
                     * binarySearch returns the index of ONE of the keys that is
                     * equal to the rightSeparator key and we need to back up
                     * until we have found the FIRST ONE.
                     * 
                     * Note: The behavior of the binarySearch is effectively
                     * under-defined here and sometimes it will return the index
                     * of the first key EQ to the rightSeparator while at other
                     * times it will return the index of the second or greater
                     * key that is EQ to the rightSeparatoer.
                     */
                    
                    while (pos > currentIndex) {
                        
                        if (BytesUtil.bytesEqual(keys[pos - 1],
                                rightSeparatorKey)) {

                            // keep backing up.
                            pos--;

                            continue;

                        }
                        
                        break;
                        
                    }

                    if(DEBUG) log.debug("Exact match on rightSeparator: pos=" + pos
                            + ", key=" + BytesUtil.toString(keys[pos]));

                } else if (pos < 0) {

                    /*
                     * There is a miss on the rightSeparator key (it is not
                     * present in the keys that are being split). In this case
                     * the binary search returns the insertion point. We then
                     * compute the exclusive upper bound from the insertion
                     * point.
                     */
                    
                    pos = -pos - 1;

                    assert pos > currentIndex && pos <= toIndex : "Expected pos in ["
                        + currentIndex + ":" + toIndex + ") but pos=" + pos;

                }

                /*
                 * Note: this test can be enabled if you are having problems
                 * with KeyAfterPartition or KeyBeforePartition. It will go
                 * through more effort to validate the constraints on the split.
                 * However, due to the additional byte[] comparisons, this
                 * SHOULD be disabled except when tracking a bug.
                 */
//                assert validSplit( locator, currentIndex, pos, keys );

                splits.add(new Split(locator, currentIndex, pos));

                currentIndex = pos;

            }

        }

        return splits;

    }

    /**
     * Paranoia testing for generated splits.
     * 
     * @param locator
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @return
     */
    private boolean validSplit(PartitionLocator locator, int fromIndex,
            int toIndex, byte[][] keys) {

        assert fromIndex <= toIndex : "fromIndex=" + fromIndex + ", toIndex="
                + toIndex;

        assert fromIndex >= 0 : "fromIndex=" + fromIndex;

        assert toIndex <= keys.length : "toIndex=" + toIndex + ", keys.length="
                + keys.length;

        // begin with the left separator on the index partition.
        byte[] lastKey = locator.getLeftSeparatorKey();
        
        assert lastKey != null;

        for (int i = fromIndex; i < toIndex; i++) {

            final byte[] key = keys[i];

            assert key != null;

            if (lastKey != null) {

                final int ret = BytesUtil.compareBytes(lastKey, key);

                assert ret <= 0 : "keys out of order: i=" + i + ", lastKey="
                        + BytesUtil.toString(lastKey) + ", key="
                        + BytesUtil.toString(key)+", keys="+BytesUtil.toString(keys);
                
            }
            
            lastKey = key;
            
        }

        // Note: Must be strictly LT the rightSeparator key (when present).
        {

            final byte[] key = locator.getRightSeparatorKey();

            if (key != null) {

                int ret = BytesUtil.compareBytes(lastKey, key);

                assert ret < 0 : "keys out of order: lastKey="
                        + BytesUtil.toString(lastKey) + ", rightSeparator="
                        + BytesUtil.toString(key)+", keys="+BytesUtil.toString(keys);

            }
            
        }
        
        return true;
        
    }
    
    /**
     * Resolve the data service to which the index partition is mapped.
     * 
     * @param pmd
     *            The index partition locator.
     * 
     * @return The data service and never <code>null</code>.
     * 
     * @throws RuntimeException
     *             if none of the data services identified in the index
     *             partition locator record could be discovered.
     */
    public IDataService getDataService(PartitionLocator pmd) {

        final UUID serviceUUID = pmd.getDataServiceUUID();

        final IDataService dataService;

        try {

            dataService = fed.getDataService(serviceUUID);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return dataService;

    }

    /**
     * This operation is not supported - the resource description of a scale-out
     * index would include all "live" resources in the corresponding
     * {@link MetadataIndex}.
     */
    public IResourceMetadata[] getResourceMetadata() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Notifies the client that a {@link StaleLocatorException} was received.
     * The client will use this information to refresh the
     * {@link IMetadataIndex}.
     * 
     * @param locator
     *            The locator that was stale.
     * 
     * @throws RuntimeException
     *             unless the index view is {@link ITx#UNISOLATED} or
     *             {@link ITx#READ_COMMITTED} since stale locators do not occur
     *             for other views.
     */
    protected void staleLocator(PartitionLocator locator,
            StaleLocatorException cause) {
        
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

        // notify the metadata index view that it has a stale locator.
        metadataIndex.staleLocator(locator);

    }

}
