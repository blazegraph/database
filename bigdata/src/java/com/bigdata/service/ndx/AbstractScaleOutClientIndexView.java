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
 * Created on Mar 31, 2009
 */

package com.bigdata.service.ndx;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AsynchronousIndexWriteConfiguration;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
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
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.Split;
import com.bigdata.service.IBigdataClient.Options;
import com.bigdata.service.ndx.pipeline.IDuplicateRemover;
import com.bigdata.service.ndx.pipeline.IndexAsyncWriteStats;
import com.bigdata.service.ndx.pipeline.IndexWriteTask;

/**
 * Abstract base class for the {@link IScaleOutClientIndex} implementation(s).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
 */
abstract public class AbstractScaleOutClientIndexView implements IScaleOutClientIndex {

    /**
     * Note: Invocations of the non-batch API are logged at the WARN level since
     * they result in an application that can not scale-out efficiently.
     */
    protected static final transient Logger log = Logger
            .getLogger(AbstractScaleOutClientIndexView.class);
    
    /**
     * True iff the {@link #log} level is WARN or less.
     */
    final protected boolean WARN = log.getEffectiveLevel().toInt() <= Level.WARN
            .toInt();

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
    static protected final transient String ERR_ABORT_TX = "Could not abort transaction: tx=";
    
    protected final AbstractScaleOutFederation fed;

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
    protected final long taskTimeout;
    
    /**
     * 
     */
    protected static final String NON_BATCH_API = "Non-batch API";

    /**
     * This may be used to disable the non-batch API, which is quite convenient
     * for locating code that needs to be re-written to use
     * {@link IIndexProcedure}s.
     */
    protected final boolean batchOnly;

    /**
     * The default capacity for the {@link #rangeIterator(byte[], byte[])}
     */
    private final int capacity;

    /**
     * The timestamp from the ctor.
     */
    protected final long timestamp;

    final public long getTimestamp() {
        
        return timestamp;
        
    }

    /**
     * The name of the scale-out index (from the ctor).
     */
    protected final String name;
    
    final public String getName() {
        
        return name;
        
    }

    /**
     * The {@link IMetadataIndex} for this scale-out index.
     * 
     * @todo This is a bit dangerous since most of the time when you want the
     *       metadata index you may have a timestamp in effect which is
     *       different from the timestamp of the view (e.g., a read-consistent
     *       transaction).
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
     * 
     * @todo This is a bit dangerous since most of the time when you want the
     *       metadata index you may have a timestamp in effect which is
     *       different from the timestamp of the view (e.g., a read-consistent
     *       transaction).
     * 
     * @see IBigdataFederation#getMetadataIndex(String, long)
     */
    final protected IMetadataIndex getMetadataIndex() {
        
        return metadataIndex;
        
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
     */
    final protected boolean readConsistent = true;

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getSimpleName());

        sb.append("{ name=" + name);

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
    public AbstractScaleOutClientIndexView(final AbstractScaleOutFederation fed,
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

    public IDataService getDataService(final PartitionLocator pmd) {

        return fed.getDataService(pmd.getDataServiceUUID());

    }

    @SuppressWarnings("unchecked")
    public Iterator<PartitionLocator> locatorScan(final long ts,
            final byte[] fromKey, final byte[] toKey, final boolean reverseScan) {

        return fed.locatorScan(name, ts, fromKey, toKey, reverseScan);

    }
    
    /**
     * This operation is not supported - the resource description of a scale-out
     * index would include all "live" resources in the corresponding
     * {@link MetadataIndex}.
     */
    public IResourceMetadata[] getResourceMetadata() {
        
        throw new UnsupportedOperationException();
        
    }
    
    public ICounter getCounter() {
        
        throw new UnsupportedOperationException();
        
    }

    private volatile ITupleSerializer tupleSer = null;

    protected ITupleSerializer getTupleSerializer() {

        if (tupleSer == null) {

            synchronized (this) {

                if (tupleSer == null) {

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
    
    public byte[] remove(final byte[] key) {

        if (batchOnly)
            log.error(NON_BATCH_API,new RuntimeException());
        else
            if(WARN) log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][]{key};
        
        final IResultHandler resultHandler = new IdentityHandler();

        submit(0/* fromIndex */, 1/* toIndex */, keys, null/* vals */,
                BatchRemoveConstructor.RETURN_OLD_VALUES, resultHandler);

        return ((ResultBuffer) resultHandler.getResult()).getValues().get(0);

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
                false/* exact */, false/* deleted */, fromKey, toKey);

        submit(fromKey, toKey, proc, handler);

        return handler.getResult();
        
    }

    /**
     * The exact range count is obtained by mapping a key-range scan over the
     * index partitions. The operation is parallelized.
     */
    final public long rangeCountExact(final byte[] fromKey, final byte[] toKey) {

        final LongAggregator handler = new LongAggregator();
        
        final RangeCountProcedure proc = new RangeCountProcedure(
                true/* exact */, false/*deleted*/, fromKey, toKey);

        submit(fromKey, toKey, proc, handler);

        return handler.getResult();
    
    }
    
    /**
     * The exact range count of deleted and undeleted tuples is obtained by
     * mapping a key-range scan over the index partitions. The operation is
     * parallelized.
     */
    final public long rangeCountExactWithDeleted(final byte[] fromKey,
            final byte[] toKey) {

        final LongAggregator handler = new LongAggregator();

        final RangeCountProcedure proc = new RangeCountProcedure(
                true/* exact */, true/* deleted */, fromKey, toKey);

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
    public ITupleIterator rangeIterator(final byte[] fromKey,
            final byte[] toKey, int capacity, final int flags,
            final IFilterConstructor filter) {

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

        final boolean isReadConsistentTx;
        final long ts;
        if ((timestamp == ITx.UNISOLATED && readOnly)
                || (timestamp == ITx.READ_COMMITTED && readConsistent)) {

            try {

                // run as globally consistent read.
                ts = fed.getTransactionService().newTx(ITx.READ_COMMITTED);

            } catch (IOException ex) {

                throw new RuntimeException(ERR_NEW_TX, ex);

            }

            isReadConsistentTx = true;

        } else {

            ts = timestamp;

            isReadConsistentTx = false;

        }

        return new PartitionedTupleIterator(this, ts, isReadConsistentTx,
                fromKey, toKey, capacity, flags, filter);
        
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
     * @param ts
     *            The timestamp for the {@link IMetadataIndex} view that will be
     *            applied to choose the {@link Split}s.
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
    public LinkedList<Split> splitKeys(final long ts, final int fromIndex,
            final int toIndex, final byte[][] keys) {

        assert keys != null;
        
        assert fromIndex >= 0;
        assert fromIndex < toIndex;

        assert toIndex <= keys.length;
        
        final LinkedList<Split> splits = new LinkedList<Split>();
        
        // start w/ the first key.
        int currentIndex = fromIndex;
        
        while (currentIndex < toIndex) {
                
            /*
             * This is partition spanning the current key (RMI)
             * 
             * Note: Using the caller's timestamp here!
             */
            final PartitionLocator locator = fed.getMetadataIndex(name, ts)
                    .find(keys[currentIndex]);

            if (locator == null)
                throw new RuntimeException("No index partitions?: name=" + name);
            
            final byte[] rightSeparatorKey = locator.getRightSeparatorKey();

            if (rightSeparatorKey == null) {

                /*
                 * The last index partition does not have an upper bound and
                 * will absorb any keys that order GTE to its left separator
                 * key.
                 */

                assert isValidSplit( locator, currentIndex, toIndex, keys );
                
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

                    if (log.isDebugEnabled())
                        log.debug("Exact match on rightSeparator: pos=" + pos
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

    public LinkedList<Split> splitKeys(final long ts, final int fromIndex,
            final int toIndex, final KVO[] a) {

        /*
         * Change the shape of the data so that we can split it.
         */

        final byte[][] keys = new byte[a.length][];

        for (int i = 0; i < a.length; i++) {

            keys[i] = a[i].key;

        }

        return splitKeys(ts, fromIndex, toIndex, keys);

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
    private boolean isValidSplit(final PartitionLocator locator,
            final int fromIndex, final int toIndex, final byte[][] keys) {

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
    
    public void staleLocator(final long ts, final PartitionLocator locator,
            final StaleLocatorException cause) {

        if (locator == null)
            throw new IllegalArgumentException();

        if (ts != ITx.UNISOLATED && ts != ITx.READ_COMMITTED) {

            /*
             * Stale locator exceptions should not be thrown for these views.
             */

            throw new RuntimeException(
                    "Stale locator, but views should be consistent? timestamp="
                            + TimestampUtility.toString(ts));

        }

        // notify the metadata index view that it has a stale locator.
        fed.getMetadataIndex(name, timestamp).staleLocator(locator);

    }


    public Object submit(final byte[] key, final ISimpleIndexProcedure proc) {

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

                return submit(tx, key, proc);

            } finally {

                try {

                    fed.getTransactionService().abort(tx);

                } catch (IOException ex) {

                    // log error and ignore since the operation is complete.
                    log.error(ERR_ABORT_TX + tx, ex);

                }

            }
            
        } else {

            /*
             * Timestamp is either a tx already or the caller is risking errors
             * with lightweight historical reads.
             */
            
            return submit(timestamp, key, proc);

        }
        
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

                submit(tx, fromKey, toKey, proc, resultHandler);

            } finally {

                try {

                    fed.getTransactionService().abort(tx);

                } catch (IOException ex) {

                    // log error and ignore since the operation is complete.
                    log.error(ERR_ABORT_TX + tx, ex);
                    
                }

            }
            
        } else {

            /*
             * Timestamp is either a tx already or the caller is risking errors
             * with lightweight historical reads.
             */
            
            submit(timestamp, fromKey, toKey, proc, resultHandler);
            
        }

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
            final AbstractKeyArrayIndexProcedureConstructor ctor,
            final IResultHandler aggregator) {

        if (ctor == null) {

            throw new IllegalArgumentException();
        
        }
        
        // iff we created a read-historical tx in this method.
        final boolean isTx;
        // the timestamp that will be used for the operation.
        final long ts;
        {

            /*
             * Instantiate the procedure on all the data so we can figure out if
             * it is read-only and whether or not we need to create a read-only
             * transaction to run it.
             * 
             * @todo This assumes that people write procedures that are
             * flyweight in how they encode the data in their ctor. If the don't
             * then there will be an overhead for this.
             */
            final IKeyArrayIndexProcedure proc = ctor.newInstance(this,
                    fromIndex, toIndex, keys, vals);

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

                    throw new RuntimeException(ERR_NEW_TX, e);

                }

            } else {
            
                // might be a tx, but not one that we created here.
                isTx = false;
                
                ts = getTimestamp();
            
            }

        }

        try {

            submit(ts, fromIndex, toIndex, keys, vals, ctor, aggregator);

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
     * Variant uses the caller's timestamp.
     * 
     * @param ts
     * @param key
     * @param proc
     * @return
     */
    abstract protected Object submit(final long ts, final byte[] key,
            final ISimpleIndexProcedure proc);
    
    /**
     * Variant uses the caller's timestamp.
     * 
     * @param ts
     * @param fromKey
     * @param toKey
     * @param proc
     * @param resultHandler
     */
    abstract protected void submit(final long ts, final byte[] fromKey,
            final byte[] toKey, final IKeyRangeIndexProcedure proc,
            final IResultHandler resultHandler);

    /**
     * Variant uses the caller's timestamp.
     * 
     * @param ts
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @param vals
     * @param ctor
     * @param aggregator
     */
    abstract protected void submit(final long ts, final int fromIndex, final int toIndex,
            final byte[][] keys, final byte[][] vals,
            final AbstractKeyArrayIndexProcedureConstructor ctor,
            final IResultHandler aggregator);
    
    public <T extends IKeyArrayIndexProcedure, O, R, A> BlockingBuffer<KVO<O>[]> newWriteBuffer(
            final IResultHandler<R, A> resultHandler,
            final IDuplicateRemover<O> duplicateRemover,
            final AbstractKeyArrayIndexProcedureConstructor<T> ctor) {

        final AsynchronousIndexWriteConfiguration conf = getIndexMetadata()
                .getAsynchronousIndexWriteConfiguration();

        final BlockingBuffer<KVO<O>[]> writeBuffer = new BlockingBuffer<KVO<O>[]>(
                // @todo array vs linked w/ capacity and fair vs unfair.
                new ArrayBlockingQueue<KVO<O>[]>(conf.getMasterQueueCapacity()),
                conf.getMasterChunkSize(),//
                conf.getMasterChunkTimeoutNanos(),// 
                TimeUnit.NANOSECONDS,//
                true// ordered
        );
        
        final IndexWriteTask.M<T, O, R, A> task = new IndexWriteTask.M<T, O, R, A>(
                this, //
                conf.getSinkIdleTimeoutNanos(),//
                conf.getSinkPollTimeoutNanos(),//
                conf.getSinkQueueCapacity(), //
                conf.getSinkChunkSize(), //
                conf.getSinkChunkTimeoutNanos(),//
                duplicateRemover,//
                ctor,//
                resultHandler,//
                fed.getIndexCounters(name).asynchronousStats,
                writeBuffer//
                );

        final Future<? extends IndexAsyncWriteStats> future = fed
                .getExecutorService().submit(task);

        writeBuffer.setFuture(future);

        return task.getBuffer();

    }

    /**
     * Return a new {@link CounterSet} backed by the {@link ScaleOutIndexCounters}
     * for this scale-out index.
     */
    public ICounterSet getCounters() {

        return getFederation().getIndexCounters(name).getCounters();

    }

}
