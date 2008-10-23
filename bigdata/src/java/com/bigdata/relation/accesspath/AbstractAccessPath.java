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
 * Created on Jun 19, 2008
 */

package com.bigdata.relation.accesspath;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.filter.FilterConstructor;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.filter.ITupleFilter;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.service.IDataService;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.EmptyChunkedIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract base class for type-specific {@link IAccessPath} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param R
 *            The generic type of the [R]elation elements of the
 *            {@link IRelation}.
 * 
 * @todo This needs to be more generalized so that you can use a index that is
 *       best without being optimal by specifying a low-level filter to be
 *       applied to the index. That requires a means to dynamically filter out
 *       the elements we do not want from the key-range scan - the filtering
 *       should of course be done at the {@link IDataService}.
 */
abstract public class AbstractAccessPath<R> implements IAccessPath<R> {

    static final protected Logger log = Logger.getLogger(IAccessPath.class);
    
    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();

    /** Access to the index, resource locator, executor service, etc. */
    protected final IIndexManager indexManager;

    /** Timestamp of the view. */
    protected final long timestamp;

    /** Predicate (the resource name on the predicate is the relation namespace). */
    protected final IPredicate<R> predicate;

    /**
     * Index order (the relation namespace plus the index order and the option
     * partitionId constraint on the predicate identify the index).
     */
    protected final IKeyOrder<R> keyOrder;

    /** The index. */
    protected final IIndex ndx;

    /** Iterator flags. */
    protected final int flags;
    private final int chunkOfChunksCapacity;
    private final int chunkCapacity;
    private final int fullyBufferedReadThreshold;
    
    /**
     * The maximum <em>limit</em> that is allowed for a fully-buffered read.
     * The {@link #asynchronousIterator(Iterator)} will always be used above
     * this limit.
     */
    protected static final int MAX_FULLY_BUFFERED_READ_LIMIT = 250000;
    
    /**
     * We cache some stuff for historical reads.
     * <p>
     * Note: We cache results on a per-{@link IAccessPath} basis rather than a
     * per-{@link IIndex} basis since range counts and range iterators are both
     * constrained to a specific key range of interest for an
     * {@link IAccessPath} while they would span the entire {@link IIndex}
     * otherwise.
     * 
     * @todo cache the {@link IAccessPath}s themselves so that we benefit from
     *       reuse of the cached data.
     * 
     * @todo we could also cache small iterator result sets.
     */
    private final boolean historicalRead;
    
    /**
     * For {@link #historicalRead}s only, the range count is cached once it is
     * computed. It is also set if we discover using {@link #isEmpty()} or
     * {@link #iterator(long, long, int)} that the {@link IAccessPath} is empty.
     * Likewise, those methods test this flag to see if we have proven the
     * {@link IAccessPath} to be empty.
     */
    private long rangeCount = -1L;
    
    /**
     * The filter derived from the {@link IElementFilter}.
     */
    final protected FilterConstructor<R> filter;

    /**
     * Used to detect failure to call {@link #init()}.
     */
    private boolean didInit = false;

    private byte[] fromKey;
    
    private byte[] toKey;

    /**
     * The key corresponding to the inclusive lower bound for the
     * {@link IAccessPath} <code>null</code> if there is no lower bound.
     * <p>
     * <strong>This MUST be set by the concrete subclass using
     * {@link #setFromKey(byte[])} BEFORE calling
     * {@link AbstractAccessPath#init()} - it MAY be set to a <code>null</code>
     * value</strong>.
     */
    public byte[] getFromKey() {

        return fromKey;

    }

    /**
     * The key corresponding to the exclusive upper bound for the
     * {@link IAccessPath} -or- <code>null</code> if there is no upper bound.
     * <p>
     * <strong>This MUST be set by the concrete subclass using
     * {@link #setFromKey(byte[])} BEFORE calling
     * {@link AbstractAccessPath#init()} - it MAY be set to a <code>null</code>
     * value.</strong>
     */
    public byte[] getToKey() {
        
        return toKey;
        
    }
    
    protected void setFromKey(byte[] fromKey) {
        
        assertNotInitialized();
        
        this.fromKey = fromKey;
        
    }
    
    protected void setToKey(byte[] toKey) {
        
        assertNotInitialized();
        
        this.toKey = toKey;
        
    }
    
    public IKeyOrder<R> getKeyOrder() {
        
        return keyOrder;
        
    }
    
    /**
     * @param indexManager
     *            Access to the indices, resource locators, executor service,
     *            etc.
     * @param timestamp
     *            The timestamp of the index view.
     * @param predicate
     *            The constraints on the access path.
     * @param keyOrder
     *            The order in which the elements would be visited for this
     *            access path.
     * @param ndx
     *            The index on which the access path is reading.
     * @param flags
     *            The default {@link IRangeQuery} flags.
     * @param chunkOfChunksCapacity
     *            The #of chunks that can be held by an {@link IBuffer} that is
     *            the target or one or more producers. This is generally a small
     *            number on the order of the #of parallel producers that might
     *            be writing on the {@link IBuffer} since the capacity of the
     *            {@link UnsynchronizedArrayBuffer}s is already quite large
     *            (10k or better elements, defining a single "chunk" from a
     *            single producer).
     * @param chunkCapacity
     *            The maximum size for a single chunk (generally 10k or better).
     * @param fullyBufferedReadThreshold
     *            If the estimated remaining rangeCount for an
     *            {@link #iterator(long, long, int)} is LTE this threshold then
     *            we will do a fully buffered (synchronous) read. Otherwise we
     *            will do an asynchronous read.
     */
    protected AbstractAccessPath(//
            final IIndexManager indexManager,  //
            final long timestamp,//
            final IPredicate<R> predicate,//
            final IKeyOrder<R> keyOrder,  //
            final IIndex ndx,//
            final int flags, //
            final int chunkOfChunksCapacity,
            final int chunkCapacity,
            final int fullyBufferedReadThreshold
            ) {

        if (indexManager == null)
            throw new IllegalArgumentException();
        
        if (predicate == null)
            throw new IllegalArgumentException();

        if (keyOrder == null)
            throw new IllegalArgumentException();

        if (ndx == null)
            throw new IllegalArgumentException();

        final int partitionId = predicate.getPartitionId();
        
        if (partitionId != -1) {
            
            /*
             * An index partition constraint was specified, so verify that we
             * were given a local index object and that the index object is for
             * the correct index partition.
             */
            
            final LocalPartitionMetadata pmd = ndx.getIndexMetadata().getPartitionMetadata();

            if (pmd == null)
                throw new IllegalArgumentException("Not an index partition");
        
            if (pmd.getPartitionId() != partitionId) {
                
                throw new IllegalArgumentException("Expecting partitionId="
                        + partitionId + ", but have " + pmd.getPartitionId());
                
            }
            
        }

        this.indexManager = indexManager;
        
        this.timestamp = timestamp;
        
        this.predicate = predicate;

        this.keyOrder = keyOrder;

        this.ndx = ndx;

        this.flags = flags;

        this.chunkOfChunksCapacity = chunkOfChunksCapacity;

        this.chunkCapacity = chunkCapacity;

        this.fullyBufferedReadThreshold = fullyBufferedReadThreshold;
        
        this.historicalRead = TimestampUtility.isHistoricalRead(timestamp);
        
        final IElementFilter<R> constraint = predicate.getConstraint();

        if (constraint == null) {

            this.filter = null;

        } else {

            this.filter = new FilterConstructor<R>();
            
            this.filter.addFilter(new ElementFilter<R>(constraint));

        }
        
    }

    /**
     * Align the predicate's {@link IElementFilter} constraint with
     * {@link ITupleFilter} so that the {@link IElementFilter} can be evaluated
     * close to the data by an {@link ITupleIterator}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <R>
     *            The generic type of the elements presented to the filter.
     */
    public static class ElementFilter<R> extends TupleFilter<R> {
        
        private static final long serialVersionUID = 1L;

        private final IElementFilter<R> constraint;
        
        public ElementFilter(final IElementFilter<R> constraint) {
            
            if (constraint == null)
                throw new IllegalArgumentException();
            
            this.constraint = constraint;
            
        }
        
        @SuppressWarnings("unchecked")
        public boolean isValid(ITuple<R> tuple) {
            
            final R obj = (R) tuple.getObject();
        
            return constraint.accept( obj );
            
        }

    }

    public String toString() {

        return getClass().getName() + "{predicate=" + predicate + ", keyOrder="
                + keyOrder + ", flags=" + flags + ", fromKey="
                + (fromKey == null ? "n/a" : BytesUtil.toString(fromKey))
                + ", toKey="
                + (toKey == null ? "n/a" : BytesUtil.toString(toKey) + "}");

    }
    
    /**
     * @throws IllegalStateException
     *             unless {@link #init()} has been invoked.
     */
    final private void assertNotInitialized() {

        if (didInit)
            throw new IllegalStateException();
        
    }
    
    /**
     * @throws IllegalStateException
     *             unless {@link #init()} has been invoked.
     */
    final protected void assertInitialized() {

        if (!didInit)
            throw new IllegalStateException();
        
    }
    
    /**
     * Required post-ctor initialization.
     * 
     * @return <i>this</i>
     */
    public AbstractAccessPath<R> init() {
        
        if (didInit)
            throw new IllegalStateException();

        didInit = true;
        
        if(DEBUG) {
            
            log.debug(toString());
            
        }
        
        return this;
        
    }
    
    public IIndexManager getIndexManager() {
        
        return indexManager;
        
    }

    public long getTimestamp() {
        
        return timestamp;
        
    }
    
    public IPredicate<R> getPredicate() {
        
//        assertInitialized();
        
        return predicate;
        
    }

    public IIndex getIndex() {
        
        return ndx;
        
    }

    public boolean isEmpty() {

        assertInitialized();
        
        if (historicalRead && rangeCount != -1) {

            /*
             * Optimization for a historical read in which we have already
             * proven that the access path is empty.
             */
            
            return rangeCount == 0L;
            
        }
        
        if(DEBUG) {
            
            log.debug(toString());
            
        }
        
        final IChunkedIterator<R> itr = iterator(1,1);
        
        try {
            
            final boolean empty = ! itr.hasNext();
            
            if (empty && historicalRead) {

                // the access path is known to be empty.
                
                rangeCount = 0L;
                
            }
            
            return empty;
            
        } finally {
            
            itr.close();
            
        }
        
    }

    final public IChunkedOrderedIterator<R> iterator() {
        
        return iterator(0L/* offset */, 0L/* limit */, 0);
        
    }

    final public IChunkedOrderedIterator<R> iterator(int limit, int capacity) {
    
        return iterator(0L/* offset */, limit, capacity);
        
    }
    
    /**
     * FIXME Support both offset and limit for asynchronous iterators. right now
     * this will force the use of the
     * {@link #synchronousIterator(long, long, Iterator)} when the offset or
     * limit are non-zero, but that is only permitted up to a limit of
     * {@link #MAX_FULLY_BUFFERED_READ_LIMIT}.
     * 
     * FIXME in order to support large limits we need to verify that the
     * asynchonous iterator can correctly handle REMOVEALL and that incremental
     * materialization up to the [limit] will not effect the semantics for
     * REMOVEALL or the other iterator flags (per above). (In fact, the
     * asynchronous iterator does not support either [offset] or [limit] at this
     * time).
     * 
     * FIXME write unit tests for slice handling by this method and modify the
     * SAIL integration to use it for SLICE on an {@link IAccessPath} scan. Note
     * that there are several {@link IAccessPath} implementations and they all
     * need to be tested with SLICE.
     * 
     * Those tests should be located in
     * {@link com.bigdata.rdf.store.TestAccessPath}.
     */
    @SuppressWarnings("unchecked")
    final public IChunkedOrderedIterator<R> iterator(long offset, long limit,
            int capacity) {

        if (offset < 0)
            throw new IllegalArgumentException();
        
        if (limit < 0)
            throw new IllegalArgumentException();
        
        if (limit == Long.MAX_VALUE) {
            
            // treat MAX_VALUE as meaning NO limit.
            limit = 0L;
            
        }
        
        if (limit > MAX_FULLY_BUFFERED_READ_LIMIT) {
            
            // Note: remove constraint when async itr supports SLICE.
            throw new UnsupportedOperationException("limit=" + limit
                    + " exceeds maximum fully buffered read limit: "
                    + MAX_FULLY_BUFFERED_READ_LIMIT);
            
        }
        
        if (historicalRead && rangeCount >= 0L && ((rangeCount - offset) <= 0L)) {

            /*
             * The access path has already been proven to be empty.
             */
            
            return new EmptyChunkedIterator<R>(keyOrder);
            
        }
        
        if (DEBUG)
            log.debug("offset=" + offset + ", limit=" + limit + ", capacity="
                    + capacity + ", accessPath=" + this);
        
        final boolean fullyBufferedRead;
        
        if(predicate.isFullyBound()) {

            if (DEBUG)
                log.debug("Predicate is fully bound.");
            
            /*
             * If the predicate is fully bound then there can be at most one
             * element matched so we constrain the limit and capacity
             * accordingly.
             */
            
            if (offset > 0L) {

                // the iterator will be empty if the offset is GT zero.
                return new EmptyChunkedIterator<R>(keyOrder);
                
            }
            
            capacity = 1;

            limit = 1L;
            
            fullyBufferedRead = true;
            
        } else if (limit > 0L) {

            /*
             * A [limit] was specified.
             * 
             * NOTE: When the [limit] is (GT ZERO) we MUST NOT let the
             * DataService layer iterator read more than [limit] elements at a
             * time.
             * 
             * This is part of the contract for REMOVEALL - when you set the
             * [limit] and specify REMOVEALL you are only removing the 1st
             * [limit] elements in the traversal order.
             * 
             * This is also part of the atomic queue operations contract - the
             * head and tail queue operations function by specifying [limit :=
             * 1] (tail also specifies the REVERSE traversal option).
             * 
             * Note: When the [limit] is specified we always do a fully buffered
             * (aka synchronous) read. This simplifies the behavior of the
             * iterator and limits are generally quite small.
             */
            
            capacity = (int) limit;

            fullyBufferedRead = true;
                
        } else {

            /*
             * No limit was specified.
             * 
             * Range count the access path and use a synchronous read if the
             * rangeCount is LTE the threshold.
             * 
             * Note: the range count is corrected by the offset so that it gives
             * the effective remaining range count. When the effective remaining
             * range count is zero we know that the iterator will not visit
             * anything.
             * 
             * @todo this kind of rangeCount might be replaced by an estimated
             * range count basic on historical data and NOT requiring RMI.
             */
            
            final long rangeCountRemaining = rangeCount(false/* exact */)
                    - offset;

            if (DEBUG)
                log.debug("offset=" + offset + ", limit=" + limit
                        + ", rangeCountRemaining=" + rangeCountRemaining
                        + ", fullyBufferedReadThreashold="
                        + fullyBufferedReadThreshold);
                
            if(rangeCountRemaining <= 0) {
                
                /*
                 * Since the range count is an upper bound we KNOW that the
                 * iterator would not visit anything.
                 */

                if (DEBUG)
                    log.debug("No elements based on range count.");
                
                return new EmptyChunkedIterator<R>(keyOrder);
                
            }
            
            if(rangeCountRemaining < fullyBufferedReadThreshold) {
            
                // adjust limit to no more than the #of remaining elements.
                if (limit == 0L) {
                    limit = rangeCountRemaining;
                } else {
                    limit = Math.min(limit, rangeCountRemaining);
                }

                // adjust capacity to no more than the maximum capacity.
                capacity = (int) Math.min(MAX_FULLY_BUFFERED_READ_LIMIT, limit);
                
                fullyBufferedRead = true;
                
            } else {
                
                fullyBufferedRead = false;
                
            }

        }
        
        /*
         * Note: The [capacity] gets passed through to the DataService layer.
         * 
         * Note: The ElementFilter on the IPredicate (if any) is encapsulated
         * within [filter] and is passed through to the DataService layer. It
         * MUST be Serializable and it will be executed right up against the
         * data.
         * 
         * FIXME pass the offset and limit into the source iterator
         * (IRangeQuery, ITupleIterator). This will require a lot of changes to
         * the code as that gets used everywhere.
         */
        final Iterator<R> src = new Striterator(rangeIterator(capacity, flags,
                filter)).addFilter(new TupleObjectResolver());

        if (fullyBufferedRead) {

            /*
             * Synchronous fully buffered read of no more than [limit] elements.
             */

            return synchronousIterator(offset, limit, src);

        } else {

            /*
             * Asynchronous read (does not support either offset or limit for
             * now).
             */

            assert offset == 0L : "offset=" + limit;

            assert limit == 0L : "limit=" + limit;
            
            return asynchronousIterator(src);

        }

    }

    /**
     * Fully buffers all elements that would be visited by the
     * {@link IAccessPath} iterator.
     * 
     * @param accessPath
     *            The access path (including the triple pattern).
     * 
     * @param offset
     *            The first element that will be materialized (non-negative).
     * @param limit
     *            The maximum #of elements that will be materialized (must be
     *            positive, so use a range count before calling this method if
     *            there was no limit specified by the caller).
     * 
     * FIXME pass the offset and limit into the source iterator and remove them
     * from this method's signature. This will require a change to the
     * {@link IRangeQuery} API and {@link ITupleIterator} impls.
     */
    @SuppressWarnings("unchecked")
    final protected IChunkedOrderedIterator<R> synchronousIterator(
            final long offset, final long limit, final Iterator<R> src) {

        if (offset < 0)
            throw new IllegalArgumentException();
        
        if (limit <= 0)
            throw new IllegalArgumentException();

        assert limit < MAX_FULLY_BUFFERED_READ_LIMIT : "limit=" + limit
                + ", max=" + MAX_FULLY_BUFFERED_READ_LIMIT;
        
        if (DEBUG) {

            log.debug("offset=" + offset + ", limit=" + limit);

        }
        
        int nread = 0;
        int nused = 0;

        R[] buffer = null;

        // skip past the offset elements.
        while (nread < offset && src.hasNext()) {

            src.next();
            
            nread++;
            
        }
        
        // read up to the limit elements.
        while (src.hasNext() && nused < limit) {

            final R e = src.next();

            if (buffer == null) {

                buffer = (R[]) java.lang.reflect.Array.newInstance(
                        e.getClass(), (int)limit);

            }

            buffer[nused] = e;
            
            nused++;
            nread++;

        }

        if(DEBUG) {
            
            log.debug("Fully buffered: read=" + nread + ", used=" + nused
                    + ", offset=" + offset + ", limit=" + limit);

        }
        
        if (nread == 0) {

            return new EmptyChunkedIterator<R>(keyOrder);
            
        }
        
        return new ChunkedArrayIterator<R>(nused, buffer, keyOrder);

    }
    
    /**
     * Asynchronous read using a {@link BlockingBuffer}.
     * 
     * @param src The source iterator.
     * 
     * @return
     */
    final protected IChunkedOrderedIterator<R> asynchronousIterator(
            final Iterator<R> src) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        if (DEBUG)
            log.debug("");
        
        /*
         * Note: The filter is applied by the ITupleIterator so that it gets
         * evaluated close to the data, not here where it would be evaluated
         * once the elements were materialized on the client.
         */
        final BlockingBuffer<R[]> buffer = new BlockingBuffer<R[]>(
                chunkOfChunksCapacity);
        
        final Future<Void> future = indexManager.getExecutorService().submit(new Callable<Void>(){
        
            public Void call() {
                
                /*
                 * Chunked iterator reading from the ITupleIterator. The filter
                 * was already applied by the ITupleIterator so we do not use it
                 * here.
                 */
                final IChunkedOrderedIterator<R> itr = new ChunkedWrappedIterator<R>(
                                src, chunkCapacity, keyOrder, null/*filter*/);
                
                try {
                    
                    while(src.hasNext()) {
                     
                        /*
                         * Note: The chunk size is determined by the source
                         * iterator.
                         */
                        final R[] chunk = itr.nextChunk();
                        
                        buffer.add( chunk );
                        
                    }
                    
                } finally {
        
                    if (DEBUG)
                        log.debug("Closing buffer: " + AbstractAccessPath.this);
                    
                    buffer.close();
                    
                    itr.close();
                    
                }
                
                return null;
                
            }
            
        });

        buffer.setFuture(future);
        
        return new ChunkConsumerIterator<R>(buffer.iterator(), keyOrder);
            
    }

    final public long rangeCount(boolean exact) {

        assertInitialized();

        final long n;
        
        if(exact) {
        
            n = ndx.rangeCountExact(fromKey, toKey);
            
        } else {

            if (historicalRead) {

                // cachable.
                n = historicalRangeCount(fromKey,toKey);
                
            } else {
                
                // not cachable.
                n = ndx.rangeCount(fromKey, toKey);
                
            }
            
        }

        if (DEBUG) {

            log.debug("exact=" + exact + ", n=" + n + " : " + toString());

        }

        return n;
        
    }

    /**
     * Note: the range count is cached for a historical read to reduce round
     * trips to the DataService.
     */
    final private long historicalRangeCount(byte[] fromKey, byte[] toKey) {
        
        if (rangeCount == -1L) {
    
            // do query and cache the result.
            return rangeCount = ndx.rangeCount(fromKey, toKey);

        } else {
            
            // cached value.
            return rangeCount;
            
        }

    }
    
    final public ITupleIterator<R> rangeIterator() {

        return rangeIterator(0/* capacity */, flags, filter);
        
    }
    
    @SuppressWarnings( { "unchecked" })
    protected ITupleIterator<R> rangeIterator(int capacity, int flags,
            IFilterConstructor<R> filter) {

        assertInitialized();
        
        if (DEBUG) {

            log.debug(this+" : capacity="+capacity+", flags="+flags+", filter="+filter);
            
        }

        return ndx.rangeIterator(fromKey, toKey, capacity, flags, filter);
        
    }

    /**
     * This implementation removes all tuples that would be visited by the
     * access path from the backing index.
     * <p>
     * Note: If you are maintaining multiple indices then you MUST override this
     * method to remove the data from each of those indices.
     */
    public long removeAll() {

        assertInitialized();

        if (DEBUG) {

            log.debug(this.toString());
            
        }

        /*
         * Remove everything in the key range. Do not materialize keys or
         * values.
         */
        final ITupleIterator itr = rangeIterator(0/* capacity */,
                IRangeQuery.REMOVEALL, filter);

        long n = 0;

        while (itr.hasNext()) {

            itr.next();

            n++;

        }

        return n;

    }

}
