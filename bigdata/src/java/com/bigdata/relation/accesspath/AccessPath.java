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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.NV;
import com.bigdata.bop.ap.filter.BOpFilterBase;
import com.bigdata.bop.ap.filter.BOpTupleFilter;
import com.bigdata.bop.ap.filter.SameVariableConstraint;
import com.bigdata.bop.ap.filter.SameVariableConstraintFilter;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IBloomFilter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.Tuple;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.IRelation;
import com.bigdata.service.IDataService;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.EmptyChunkedIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.IFilter;
import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract base class for type-specific {@link IAccessPath} implementations.
 *<p>
 * Note: Filters should be specified when the {@link IAccessPath} is constructed
 * so that they will be evaluated on the data service rather than materializing
 * the elements and then filtering them. This can be accomplished by adding the
 * filter as a constraint on the predicate when specifying the access path.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param R
 *            The generic type of the elements of the {@link IRelation}.
 * 
 * @todo This needs to be more generalized so that you can use a index that is
 *       best without being optimal by specifying a low-level filter to be
 *       applied to the index. That requires a means to dynamically filter out
 *       the elements we do not want from the key-range scan - the filtering
 *       should of course be done at the {@link IDataService}.
 */
public class AccessPath<R> implements IAccessPath<R> {

    static final protected Logger log = Logger.getLogger(IAccessPath.class);
    
    /** Relation (resolved lazily if not specified to the ctor). */
    private final AtomicReference<IRelation<R>> relation;

    /** Access to the index, resource locator, executor service, etc. */
    protected final IIndexManager indexManager;

    /** Timestamp of the view. */
    protected final long timestamp;

    /** Predicate (the resource name on the predicate is the relation namespace). */
    protected final IPredicate<R> predicate;

    /**
     * The description of the index partition iff the {@link #predicate} is
     * constrained to an index partition and <code>null</code> otherwise.
     */
    final LocalPartitionMetadata pmd;
    
    /**
     * Index order (the relation namespace plus the index order and the option
     * partitionId constraint on the predicate identify the index).
     */
    protected final IKeyOrder<R> keyOrder;

    /** The index. */
    protected final IIndex ndx;

    /** Iterator flags. */
    protected final int flags;
    protected final int chunkOfChunksCapacity;
    protected final int chunkCapacity;
    protected final int fullyBufferedReadThreshold;

    private final boolean isFullyBoundForKey;
    
    /**
     * <code>true</code> iff all elements in the predicate which are required
     * to generate the key are bound to constants.
     */
    public boolean isFullyBoundForKey() {
        
        return isFullyBoundForKey;
        
    }
    
    /**
     * @see AbstractResource#getChunkCapacity()
     */
    public int getChunkCapacity() {
        
        return chunkCapacity;
        
    }

    /**
     * @see AbstractResource#getChunkOfChunksCapacity()
     */
    public int getChunkOfChunksCapacity() {
        
        return chunkOfChunksCapacity;
        
    }
    
    /**
     * The maximum <em>limit</em> that is allowed for a fully-buffered read.
     * The {@link #asynchronousIterator(Iterator)} will always be used above
     * this limit.
     * 
     * @todo This should probably be close to the branching factor or chunk
     * capacity.
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
     * The filter derived from optional
     * {@link IPredicate.Annotations#INDEX_LOCAL_FILTER}. If there are shared
     * variables in the {@link IPredicate} then a {@link SameVariableConstraint}
     * is added regardless of whether the {@link IPredicate} specified a filter
     * or not.
     */
    final protected BOpFilterBase indexLocalFilter;

    /**
     * The filter derived from optional
     * {@link IPredicate.Annotations#ACCESS_PATH_FILTER}.
     */
    final protected BOpFilterBase accessPathFilter;
    
    /**
     * Used to detect failure to call {@link #init()}.
     */
    private boolean didInit = false;

    private final byte[] fromKey;
    
    private final byte[] toKey;

    /**
     * The key corresponding to the inclusive lower bound for the
     * {@link IAccessPath} <code>null</code> if there is no lower bound.
     */
    final public byte[] getFromKey() {

        return fromKey;

    }

    /**
     * The key corresponding to the exclusive upper bound for the
     * {@link IAccessPath} -or- <code>null</code> if there is no upper bound.
     */
    final public byte[] getToKey() {
        
        return toKey;
        
    }
    
    final public IKeyOrder<R> getKeyOrder() {
        
        return keyOrder;
        
    }

    /**
     * @param relation
     *            The relation for the access path (optional). The
     *            <i>relation</> is not specified when requested an
     *            {@link IAccessPath} for a specific index partition in order to
     *            avoid forcing the materialization of the {@link IRelation}.
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
     *            {@link UnsynchronizedArrayBuffer}s is already quite large (10k
     *            or better elements, defining a single "chunk" from a single
     *            producer).
     * @param chunkCapacity
     *            The maximum size for a single chunk (generally 10k or better).
     * @param fullyBufferedReadThreshold
     *            If the estimated remaining rangeCount for an
     *            {@link #iterator(long, long, int)} is LTE this threshold then
     *            we will do a fully buffered (synchronous) read. Otherwise we
     *            will do an asynchronous read.
     */
    public AccessPath(//
            final IRelation<R> relation,//
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
            
            pmd = ndx.getIndexMetadata().getPartitionMetadata();

            if (pmd == null)
                throw new IllegalArgumentException("Not an index partition");
        
            if (pmd.getPartitionId() != partitionId) {
                
                throw new IllegalArgumentException("Expecting partitionId="
                        + partitionId + ", but have " + pmd.getPartitionId());
                
            }
            
        } else {
            
            // The predicate is not constrained to an index partition.
            
            pmd = null;
            
        }

        // Note: the caller's [relation] MAY be null.
        this.relation = new AtomicReference<IRelation<R>>(relation);
        
        this.indexManager = indexManager;
        
        this.timestamp = timestamp;
        
        this.predicate = predicate;

        this.keyOrder = keyOrder;

        this.ndx = ndx;

        this.flags = flags;

        this.chunkOfChunksCapacity = chunkOfChunksCapacity;

        this.chunkCapacity = chunkCapacity;

        this.fullyBufferedReadThreshold = fullyBufferedReadThreshold;
        
        this.historicalRead = TimestampUtility.isReadOnly(timestamp);

        this.isFullyBoundForKey = predicate.isFullyBound(keyOrder);

        {

            /*
             * The filter to be evaluated at the index (optional).
             * 
             * Note: This MUST be an implementation which is "aware" of the
             * reuse of tuples within tuple iterators. That is why it is being
             * cast to a BOpTupleIterator.
             */
            final BOpTupleFilter indexLocalFilter = (BOpTupleFilter<?>) predicate
                    .getProperty(IPredicate.Annotations.INDEX_LOCAL_FILTER);

            /*
             * Optional constraint enforces the "same variable" constraint. The
             * constraint will be null unless at least one variable appears in
             * more than one position in the predicate.
             */
            final SameVariableConstraint<R> sameVarConstraint = SameVariableConstraint
                    .newInstance(predicate);

            if (sameVarConstraint != null) {

                /*
                 * Stack filters.
                 */
                this.indexLocalFilter = new SameVariableConstraintFilter(
                        (indexLocalFilter == null ? new BOp[0]
                                : new BOp[] { indexLocalFilter }),
                                NV.asMap(new NV[] { new NV(
                                        SameVariableConstraintFilter.Annotations.FILTER,
                                        sameVarConstraint) }));

            } else {
                
                this.indexLocalFilter = indexLocalFilter;
                
            }
            
        }

        // optional filter to be evaluated by the AccessPath.
        this.accessPathFilter = (BOpFilterBase) predicate
                .getProperty(IPredicate.Annotations.ACCESS_PATH_FILTER);

        final IKeyBuilder keyBuilder = ndx.getIndexMetadata()
                .getTupleSerializer().getKeyBuilder();

        fromKey = keyOrder.getFromKey(keyBuilder, predicate);

        toKey = keyOrder.getToKey(keyBuilder, predicate);
        
    }

//    /**
//     * Align the predicate's {@link IElementFilter} constraint with
//     * {@link ITupleFilter} so that the {@link IElementFilter} can be evaluated
//     * close to the data by an {@link ITupleIterator}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     * @param <R>
//     *            The generic type of the elements presented to the filter.
//     */
//    public static class ElementFilter<R> extends TupleFilter<R> {
//        
//        private static final long serialVersionUID = 1L;
//
//        private final IElementFilter<R> constraint;
//        
//        public ElementFilter(final IElementFilter<R> constraint) {
//            
//            if (constraint == null)
//                throw new IllegalArgumentException();
//            
//            this.constraint = constraint;
//            
//        }
//        
//        public boolean isValid(final ITuple<R> tuple) {
//            
//            final R obj = (R) tuple.getObject();
//        
//            return constraint.accept( obj );
//            
//        }
//
//    }

    public String toString() {

        return getClass().getName()
                + "{predicate="
                + predicate
                + ", keyOrder="
                + keyOrder
                + ", flags="
                + Tuple.flagString(flags)
                + ", fromKey="
                + (fromKey == null ? "n/a" : BytesUtil.toString(fromKey))
                + ", toKey="
                + (toKey == null ? "n/a" : BytesUtil.toString(toKey)
                        + ", indexLocalFilter="
                        + (indexLocalFilter == null ? "n/a" : indexLocalFilter)
                        + ", accessPathFilter="
                        + (accessPathFilter == null ? "n/a" : accessPathFilter)
                        + "}");

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
    public AccessPath<R> init() {
        
        if (didInit)
            throw new IllegalStateException();

        didInit = true;
        
        if(log.isDebugEnabled()) {
            
            if (fromKey != null && toKey != null) {
                
                if (BytesUtil.compareBytes(fromKey, toKey) >= 0) {

                    throw new AssertionError("keys are out of order: " + toString());

                }
                
            }

            log.debug(toString());
            
        }
        
        return this;
        
    }
    
    /**
     * Resolved lazily if not specified to the ctor.
     */
    @SuppressWarnings("unchecked")
    public IRelation<R> getRelation() {

        IRelation<R> tmp = relation.get();
        
        if (tmp == null) {

            tmp = (IRelation<R>) indexManager.getResourceLocator().locate(
                    predicate.getOnlyRelationName(), timestamp);

            relation.compareAndSet(null/*expect*/, tmp/*update*/);
            
        }

        return relation.get();

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

    /**
     * @todo for scale-out, it may be better to implement {@link #isEmpty()}
     *       without specifying a capacity of ONE (1) and then caching the
     *       returned iterator. This could avoid an expensive RMI test if we
     *       invoke {@link #iterator()} shortly after {@link #isEmpty()} returns
     *       <code>false</code>.
     */
    public boolean isEmpty() {

        assertInitialized();
        
        if (historicalRead && rangeCount != -1) {

            /*
             * Optimization for a historical read in which we have already
             * proven that the access path is empty.
             */
            
            return rangeCount == 0L;
            
        }
        
        if(log.isDebugEnabled()) {
            
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

    final public IChunkedOrderedIterator<R> iterator(final int limit,
            final int capacity) {

        return iterator(0L/* offset */, limit, capacity);

    }

    /**
     * @throws RejectedExecutionException
     *             if the iterator is run asynchronously and the
     *             {@link ExecutorService} is shutdown or has a maximum capacity
     *             and is saturated.
     * 
     *             FIXME Support both offset and limit for asynchronous
     *             iterators. right now this will force the use of the
     *             {@link #synchronousIterator(long, long, Iterator)} when the
     *             offset or limit are non-zero, but that is only permitted up
     *             to a limit of {@link #MAX_FULLY_BUFFERED_READ_LIMIT}.
     * 
     *             FIXME in order to support large limits we need to verify that
     *             the asynchronous iterator can correctly handle REMOVEALL and
     *             that incremental materialization up to the [limit] will not
     *             effect the semantics for REMOVEALL or the other iterator
     *             flags (per above). (In fact, the asynchronous iterator does
     *             not support either [offset] or [limit] at this time).
     * 
     *             FIXME write unit tests for slice handling by this method and
     *             modify the SAIL integration to use it for SLICE on an
     *             {@link IAccessPath} scan. Note that there are several
     *             {@link IAccessPath} implementations and they all need to be
     *             tested with SLICE.
     * 
     *             Those tests should be located in
     *             {@link com.bigdata.rdf.spo.TestSPOAccessPath}.
     * 
     *             FIXME The offset and limit should probably be rolled into the
     *             predicate and removed from the {@link IAccessPath}. This way
     *             they will be correctly applied when {@link #isEmpty()} is
     *             implemented using the {@link #iterator()} to determine if any
     */
    @SuppressWarnings("unchecked")
    final public IChunkedOrderedIterator<R> iterator(final long offset,
            long limit, int capacity) {

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

            if (log.isDebugEnabled())
                log.debug("Proven empty by historical range count");

            return new EmptyChunkedIterator<R>(keyOrder);
            
        }
        
        if (log.isDebugEnabled())
            log.debug("offset=" + offset + ", limit=" + limit + ", capacity="
                    + capacity + ", accessPath=" + this);
        
        final boolean fullyBufferedRead;

        // true iff a point test is a hit on the bloom filter.
        boolean bloomHit = false;
        
        if(isFullyBoundForKey) {

            if (log.isDebugEnabled())
                log.debug("Predicate is fully bound for the key.");
            
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
            
            /*
             * Note: Since this is a point test, we apply the bloom filter for
             * fast rejection. However, we can only apply the bloom filter if
             * (a) you are using the local index object (either a BTree or a
             * FusedView); and (b) the bloom filter exists (and is enabled).
             * 
             * Note: The scale-out case is dealt with by pipelining the
             * intermediate binding sets to the data service on which the index
             * partition resides, at which point we again can apply the local
             * bloom filter efficiently.
             */
            
            if(ndx instanceof ILocalBTreeView) {
                
                final IBloomFilter filter = ((ILocalBTreeView)ndx).getBloomFilter();
                
                if (filter != null) {
                    
                    if(!filter.contains(fromKey)) {

                        // proven to not exist.
                        return new EmptyChunkedIterator<R>(keyOrder);
                        
                    }
                    
                    bloomHit = true;
                    
                    // fall through
                    
                }
                
                // fall through
                
            }
            
            // fall through
            
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

            if (log.isDebugEnabled())
                log.debug("offset=" + offset + ", limit=" + limit
                        + ", rangeCountRemaining=" + rangeCountRemaining
                        + ", fullyBufferedReadThreashold="
                        + fullyBufferedReadThreshold);
                
            if(rangeCountRemaining <= 0) {
                
                /*
                 * Since the range count is an upper bound we KNOW that the
                 * iterator would not visit anything.
                 */

                if (log.isDebugEnabled())
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
        
        // The raw tuple iterator: the impl depends on the IIndex impl (BTree,
        // IndexSegment, ClientIndexView, or DataServiceIndexView).
        final ITupleIterator<R> tupleItr = rangeIterator(capacity, flags,
                indexLocalFilter);
        
        // Wrap raw tuple iterator with resolver that materializes the elements
        // from the visited tuples.
        final Iterator<R> src = new Striterator(tupleItr)
                .addFilter(new TupleObjectResolver());
        
        if (accessPathFilter != null) {
            /*
             * Chain in the optional access path filter stack.
             */
            ((Striterator) src).addFilter(accessPathFilter);
        }

        if (fullyBufferedRead) {

            /*
             * Synchronous fully buffered read of no more than [limit] elements.
             */

            final IChunkedOrderedIterator<R> tmp = synchronousIterator(offset,
                    limit, src);
            
            if(bloomHit) {
                
                if(!tmp.hasNext()) {

                    // notify filter of a false positive.
                    ((ILocalBTreeView)ndx).getBloomFilter().falsePos();
                    
                }
                
            }
            
            return tmp;

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
        
        if (log.isDebugEnabled()) {

            log.debug("offset=" + offset + ", limit=" + limit);

        }
        
        int nread = 0;
        int nused = 0;

        // skip past the offset elements.
        while (nread < offset && src.hasNext()) {

            src.next();

            nread++;

        }

        // read up to [limit] elements into the buffer.
        R[] buffer = null;
        while (nused < limit && src.hasNext()) {

            final R e = src.next();

            if (buffer == null) {

                buffer = (R[]) java.lang.reflect.Array.newInstance(
                        e.getClass(), (int) limit);

            }

            buffer[nused] = e;

            nused++;
            nread++;

        }

        if(log.isDebugEnabled()) {
            
            log.debug("Fully buffered: read=" + nread + ", used=" + nused
                    + ", offset=" + offset + ", limit=" + limit);

        }

//        if (limit == 1)
//            System.err.println("Fully buffered: used=" + nused + ", limit=" + limit);

        if (nread == 0) {

            return new EmptyChunkedIterator<R>(keyOrder);
            
        }
        
        return new ChunkedArrayIterator<R>(nused, buffer, keyOrder);

    }
    
    /**
     * Asynchronous read using a {@link BlockingBuffer}.
     * 
     * @param src
     *            The source iterator.
     * 
     * @return
     * 
     * @throws RejectedExecutionException
     *             if the {@link ExecutorService} is shutdown or has a maximum
     *             capacity and is saturated.
     */
    final protected IChunkedOrderedIterator<R> asynchronousIterator(
            final Iterator<R> src) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        if (log.isDebugEnabled())
            log.debug("");
        
        /*
         * Note: The filter is applied by the ITupleIterator so that it gets
         * evaluated close to the data, not here where it would be evaluated
         * once the elements were materialized on the client.
         */
        final BlockingBuffer<R[]> buffer = new BlockingBuffer<R[]>(
                chunkOfChunksCapacity);

        final ExecutorService executorService = indexManager
                .getExecutorService();
        
        final Future<Void> future = executorService
                .submit(new ChunkConsumerTask<R>(this, src, buffer));

        buffer.setFuture(future);
        
        return new ChunkConsumerIterator<R>(buffer.iterator(), keyOrder);
            
    }
    
    /**
     * Consumes elements from the source iterator, converting them into chunks
     * on a {@link BlockingBuffer}. The consumer will drain the chunks from the
     * buffer.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private class ChunkConsumerTask<R> implements Callable<Void> {

        static protected final Logger log = Logger.getLogger(ChunkConsumerTask.class);
        
        private final AccessPath<R> accessPath;

        private final Iterator<R> src;
        
        private final BlockingBuffer<R[]> buffer;
        
        /**
         * 
         * @param src
         *            The source iterator visiting elements read from the
         *            relation.
         * @param buffer
         *            The buffer onto which chunks of those elements will be
         *            written.
         */
        public ChunkConsumerTask(final AccessPath<R> accessPath,
                final Iterator<R> src, final BlockingBuffer<R[]> buffer) {

            if (accessPath == null)
                throw new IllegalArgumentException();
            
            if (src == null)
                throw new IllegalArgumentException();
            
            if (buffer == null)
                throw new IllegalArgumentException();
            
            this.accessPath = accessPath;
            
            this.src = src;
            
            this.buffer = buffer;

        }
            
        public Void call() throws Exception {

            /*
             * Chunked iterator reading from the ITupleIterator. The filter was
             * already applied by the ITupleIterator so we do not use it here.
             * 
             * Note: The chunk size is determined [chunkCapacity].
             * 
             * Note: The BlockingBuffer can combine multiple chunks together
             * dynamically to provide a larger effective chunk size as long as
             * those chunks are available with little or no added latency.
             */
            final IChunkedOrderedIterator<R> itr = new ChunkedWrappedIterator<R>(
                    src, accessPath.chunkCapacity, accessPath.keyOrder, null/* filter */);

            long nchunks = 0;
            long nelements = 0;
            
            try {

                while (src.hasNext()) {

                    final R[] chunk = itr.nextChunk();

                    nchunks++;
                    nelements += chunk.length;

                    if (log.isDebugEnabled())
                        log.debug("#chunks=" + nchunks + ", chunkSize="
                            + chunk.length + ", nelements=" + nelements);

                    buffer.add(chunk);

                }

            } finally {

                if (log.isInfoEnabled())
                    log.info("Closing buffer: #chunks=" + nchunks
                            + ", #elements=" + nelements + ", accessPath="
                            + accessPath);

                buffer.close();
            
                itr.close();

            }

            return null;

        }

    }

    /**
     * Note: When there is a {@link IPredicate#getConstraint()} on the
     * {@link IPredicate} the exact range count will apply that constraint as a
     * filter during traversal. However, the constraint is ignored for the fast
     * range count.
     */
    final public long rangeCount(final boolean exact) {

        assertInitialized();

        long n = 0L;

        final boolean hasFilter = (indexLocalFilter != null || accessPathFilter != null);
        if (exact) {

            /*
             * @todo we can cache exact range counts also, but we can not return
             * a cached estimated range count when an exact range count is
             * requested.
             */

            if (hasFilter) {

                /*
                 * If there is a filter, then we need to visit the elements and
                 * apply the filter to those elements.
                 * 
                 * FIXME If the filter is properly driven through to the indices
                 * then the index should be able to enable the (KEYS,VALS) flags
                 * locally and we can avoid sending back the full tuple when
                 * just doing a range count. This could be done using a
                 * rangeCount(exact,filter) method on IIndex.
                 */
                
                final IChunkedOrderedIterator<R> itr = iterator();

                while (itr.hasNext()) {

                    itr.next();

                    n++;

                }

            } else {
            
                n = ndx.rangeCountExact(fromKey, toKey);
            
            }

        } else {

            if (historicalRead) {

                // cachable.
                n = historicalRangeCount(fromKey, toKey);

            } else {

                // not cachable.
                n = ndx.rangeCount(fromKey, toKey);
                
            }
            
        }

        if (log.isDebugEnabled()) {

            log.debug("exact=" + exact + ", filter=" + hasFilter + ", n=" + n
                    + " : " + toString());

        }

        return n;
        
    }

    /**
     * Note: the range count is cached for a historical read to reduce round
     * trips to the DataService.
     */
    final private long historicalRangeCount(final byte[] fromKey,
            final byte[] toKey) {
        
        if (rangeCount == -1L) {
    
            // do query and cache the result.
            return rangeCount = ndx.rangeCount(fromKey, toKey);

        } else {
            
            // cached value.
            return rangeCount;
            
        }

    }
    
    final public ITupleIterator<R> rangeIterator() {

        return rangeIterator(0/* capacity */, flags, indexLocalFilter);

    }

    @SuppressWarnings( { "unchecked" })
    protected ITupleIterator<R> rangeIterator(final int capacity,
            final int flags, final IFilter filter) {

        assertInitialized();

        if (log.isDebugEnabled()) {

            log.debug(this + " : capacity=" + capacity + ", flags=" + flags
                    + ", filter=" + filter);

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
         * Remove everything in the key range which satisfies the filter. Do
         * not materialize keys or values.
         * 
         * @todo if offset and limit are rolled into the access path then
         * they would also belong here.
         */
        final ITupleIterator<?> itr = rangeIterator(0/* capacity */,
                IRangeQuery.REMOVEALL, indexLocalFilter);

        long n = 0;

        while (itr.hasNext()) {

            itr.next();

            n++;

        }

        return n;

    }

}
