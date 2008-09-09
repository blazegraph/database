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
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.log4j.Level;
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
import com.bigdata.journal.TimestampUtility;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.striterator.ChunkedArrayIterator;
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
 */
abstract public class AbstractAccessPath<R> implements IAccessPath<R> {

    protected static final Logger log = Logger.getLogger(IAccessPath.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    protected final IRelation<R> relation;
    protected final IPredicate<R> predicate;
    protected final IKeyOrder<R> keyOrder;
    protected final IIndex ndx;
    protected final int flags;
    private final int queryBufferCapacity;
    private final int fullyBufferedReadThreshold;
    
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
     * The range count is cached once it is computed.
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
     * 
     * @param relation
     * @param predicate
     *            The constraints on the access path.
     * @param keyOrder
     *            The order in which the elements would be visited for this
     *            access path.
     * @param ndx
     *            The index on which the access path is reading.
     * @param flags
     *            The default {@link IRangeQuery} flags.
     * @param queryBufferCapacity
     *            The capacity for the internal {@link Queue} for the
     *            {@link BlockingBuffer} used by the
     *            {@link #asynchronousIterator(Iterator)}.
     * @param fullyBufferedReadThreshold
     *            If the estimated rangeCount for an {@link #iterator(int, int)}
     *            is LTE this threshold then we will do a fully buffered
     *            (synchronous) read. Otherwise we will do an asynchronous read.
     * 
     * @todo This needs to be more generalized so that you can use a index that
     *       is best without being optimal by specifying a low-level filter to
     *       be applied to the index.
     */
    protected AbstractAccessPath(//
            final IRelation<R> relation,  // 
            final IPredicate<R> predicate,//
            final IKeyOrder<R> keyOrder,  //
            final IIndex ndx,//
            final int flags, //
            final int queryBufferCapacity,
            final int fullyBufferedReadThreshold
            ) {

        if (relation == null)
            throw new IllegalArgumentException();

        if (predicate == null)
            throw new IllegalArgumentException();

        if (keyOrder == null)
            throw new IllegalArgumentException();

        if (ndx == null)
            throw new IllegalArgumentException();

        this.relation = relation;
        
        this.predicate = predicate;

        this.keyOrder = keyOrder;

        this.ndx = ndx;

        this.flags = flags;

        this.queryBufferCapacity = queryBufferCapacity;

        this.fullyBufferedReadThreshold = fullyBufferedReadThreshold;
        
        this.historicalRead = TimestampUtility.isHistoricalRead(relation.getTimestamp());
        
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
        
        if(log.isDebugEnabled()) {
            
            log.debug(toString());
            
        }
        
        return this;
        
    }
    
    /** 
     * The return type SHOULD be strengthened by impls.
     */
    public IRelation<R> getRelation() {
        
        return relation;
        
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
        
        if(log.isDebugEnabled()) {
            
            log.debug(toString());
            
        }
        
        final IChunkedIterator<R> itr = iterator(1,1);
        
        try {
            
            return ! itr.hasNext();
            
        } finally {
            
            itr.close();
            
        }
        
    }

    final public IChunkedOrderedIterator<R> iterator() {
        
        return iterator(0,0);
        
    }

    @SuppressWarnings("unchecked")
    final public IChunkedOrderedIterator<R> iterator(int limit, int capacity) {

        if (DEBUG)
            log.debug("limit=" + limit + ", capacity=" + capacity
                    + ", accessPath=" + this);
            
        final boolean fullyBufferedRead;
        
        if(predicate.isFullyBound()) {

            /*
             * If the predicate is fully bound then there can be at most one
             * element matched so we constrain the limit and capacity
             * accordingly.
             */
            
            capacity = limit = 1;
            
            fullyBufferedRead = true;
            
            if (DEBUG)
                log.debug("Predicate is fully bound.");
            
        } else if (limit > 0) {

            /*
             * A [limit] was specified.
             * 
             * NOTE: When the [limit] is specified (GT ZERO) we MUST NOT let the
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
             */
            
            capacity = limit;

            /*
             * Note: When the [limit] is specified we always do a fully buffered
             * (aka synchronous) read. This simplifies the behavior of the
             * iterator and limits are generally quite small.
             */
            
            fullyBufferedRead = true;
                
        } else {

            /*
             * No limit was specified.
             * 
             * Range count the access path and use a synchronous read if the
             * rangeCount is LTE the threshold.
             * 
             * @todo this kind of rangeCount might be replaced by an estimated
             * range count basic on historical data and NOT requiring RMI.
             */
            
            final long rangeCount = rangeCount(false/* exact */);
            
            if (DEBUG)
                log.debug("rangeCount=" + rangeCount
                        + ", fullyBufferedReadThreashold="
                        + fullyBufferedReadThreshold);
                
            if(rangeCount == 0) {
                
                /*
                 * Since the range count is an upper bound we KNOW that the
                 * iterator would not visit anything.
                 */

                if (DEBUG)
                    log.debug("No elements based on range count.");
                
                return new EmptyChunkedIterator<R>(keyOrder);
                
            }
            
            if(rangeCount < fullyBufferedReadThreshold) {
            
                limit = capacity = (int)rangeCount;
                
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
         */
        final Iterator<R> src = new Striterator(rangeIterator(capacity, flags,
                filter)).addFilter(new TupleObjectResolver());

        if (fullyBufferedRead) {

            /*
             * Synchronous fully buffered read of no more than [limit] elements.
             */

            return synchronousIterator(limit, src);

        } else {

            /*
             * Asynchronous read (no limit).
             */

            assert limit == 0;
            
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
     * @param limit
     *            The maximum #of elements that will be read -or- ZERO (0) if
     *            all elements should be read.
     */
    protected IChunkedOrderedIterator<R> synchronousIterator(final int limit,
            final Iterator<R> src) {

        if (limit <= 0)
            throw new IllegalArgumentException();

        if (DEBUG) {

            log.debug("limit=" + limit);

        }
        
        int nread = 0;

        R[] buffer = null;

        while (src.hasNext() && nread < limit) {

            final R e = src.next();

            if (buffer == null) {

                buffer = (R[]) java.lang.reflect.Array.newInstance(
                        e.getClass(), limit);

            }

            buffer[nread++] = e;

        }

        if(log.isDebugEnabled()) {
            
            log.debug("Fully buffered: read " + nread + " elements, limit="
                            + limit);

        }
        
        if (nread == 0) {

            return new EmptyChunkedIterator<R>(keyOrder);
            
        }
        
        return new ChunkedArrayIterator<R>(nread, buffer, keyOrder);

    }
    
    /**
     * Asynchronous read using a {@link BlockingBuffer}.
     * 
     * @param src The source iterator.
     * 
     * @return
     */
    protected IChunkedOrderedIterator<R> asynchronousIterator(
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
        final BlockingBuffer<R> buffer = new BlockingBuffer<R>(
                queryBufferCapacity, // 
                keyOrder, //
                null // filter
                );
        
        final Future<Void> future = getRelation().getExecutorService().submit(new Callable<Void>(){
        
            public Void call() {
                
                try {
                    
                    while(src.hasNext()) {
                     
                        final R e = src.next();
                        
                        buffer.add( e );
                        
                    }
                    
                } finally {
        
                    if (DEBUG)
                        log.debug("Closing buffer: " + AbstractAccessPath.this);
                    
                    buffer.close();
                    
                }
                
                return null;
                
            }
            
        });

        buffer.setFuture(future);
        
        return buffer.iterator();
            
    }

    final public long rangeCount(boolean exact) {

        assertInitialized();

        final long n;
        
        if(exact) {
        
            n = ndx.rangeCountExact(fromKey, toKey);
            
        } else {

            if (historicalRead) {
                
                /*
                 * Note: the range count is cached for a historical read to
                 * reduce round trips to the DataService.
                 */
                
                if (rangeCount == -1L) {
            
                    synchronized(this) {

                        // do query and cache the result.
                        n = rangeCount = ndx.rangeCount(fromKey, toKey);
                    
                    }
                    
                } else {
                    
                    // cached value.
                    n = rangeCount;
                    
                }
                
            } else {
                
                // not cached.
                n = ndx.rangeCount(fromKey, toKey);
                
            }
            
        }

        if (DEBUG) {

            log.debug("exact=" + exact + ", n=" + n + " : " + toString());

        }

        return n;
        
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

        if (log.isDebugEnabled()) {

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
