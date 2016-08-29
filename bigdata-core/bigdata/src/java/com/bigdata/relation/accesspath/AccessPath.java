/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.ap.filter.SameVariableConstraint;
import com.bigdata.bop.cost.BTreeCostModel;
import com.bigdata.bop.cost.DiskCostModel;
import com.bigdata.bop.cost.IndexSegmentCostModel;
import com.bigdata.bop.cost.ScanCostReport;
import com.bigdata.bop.join.BaseJoinStats;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IBTreeStatistics;
import com.bigdata.btree.IBloomFilter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.Tuple;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.btree.isolation.IsolatedFusedView;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.btree.view.FusedView;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.IRelation;
import com.bigdata.service.AbstractClient;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.service.ndx.IScaleOutClientIndex;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.EmptyChunkedIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;

import cutthecrap.utils.striterators.FilterBase;
import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.IFilter;
import cutthecrap.utils.striterators.NOPFilter;
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
 * @param R
 *            The generic type of the elements of the {@link IRelation}.
 * 
 * @todo Add support for non-perfect access paths. This class should layer on an
 *       index local {@link IFilter} which rejects tuples which do not satisfy
 *       the {@link IPredicate}'s bindings. This will give the effect of a SCAN
 *       with an implied filter. The javadoc on
 *       {@link IRelation#getKeyOrder(IPredicate)} should also be updated to
 *       reflect the allowance for non-perfect access paths.
 */
public class AccessPath<R> implements IAccessPath<R>, IBindingSetAccessPath<R> {

    static final protected Logger log = Logger.getLogger(IAccessPath.class);
    
    private static final boolean DEBUG = log.isDebugEnabled();
    
    /** Relation (resolved lazily if not specified to the ctor). */
    private final IRelation<R> relation;

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

    /**
     * <code>true</code> iff the {@link IPredicate}is fully bound.
     */
    private final boolean isFullyBoundForKey;

    /**
     * <code>true</code> iff there is a filter for the access path (either local
     * or remote).
     */
    private final boolean hasFilter;
    
    /**
     * <code>true</code> iff there is a filter for the access path (either local
     * or remote).
     */
    public final boolean hasFilter() {
    	
    	return hasFilter;
    	
    }
    
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
     * The maximum <em>limit</em> that is allowed for a fully-buffered read. The
     * {@link #asynchronousIterator(Iterator)} will always be used above this
     * limit.
     * 
     * FIXME Array limits in truth maintenance code. This should probably be
     * close to the branching factor or chunk capacity. It has been temporarily
     * raised to a very large value in order to support truth maintenance where
     * the code assumes access to the fully buffered result. That change needs
     * to be examined for an impact on query performance. It is effectively
     * forcing all access path reads to be fully buffered rather than using an
     * asynchronous iterator pattern.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/606">
     *      Array limits in truth maintenance code. </a>
     */
    protected static final int MAX_FULLY_BUFFERED_READ_LIMIT = 10000000;
    
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
    final protected IFilter indexLocalFilter;

    /**
     * The filter derived from optional
     * {@link IPredicate.Annotations#ACCESS_PATH_FILTER}.
     */
    final protected IFilter accessPathFilter;
    
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
    
    @Override
    final public IKeyOrder<R> getKeyOrder() {
        
        return keyOrder;
        
    }

    /**
     * @param relation
     *            The relation for the access path (optional). The
     *            <i>relation</> is not specified when requested an
     *            {@link IAccessPath} for a specific index partition in order to
     *            avoid forcing the materialization of the {@link IRelation}.
     * @param localIndexManager
     *            Access to the indices, resource locators, executor service,
     *            etc.
     * @param predicate
     *            The constraints on the access path.
     * @param keyOrder
     *            The order in which the elements would be visited for this
     *            access path.
     */
    public AccessPath(//
            final IRelation<R> relation,//
            final IIndexManager localIndexManager,  //
            final IPredicate<R> predicate,//
            final IKeyOrder<R> keyOrder  //
            ) {

        if (relation == null)
            throw new IllegalArgumentException();
        
        if (predicate == null)
            throw new IllegalArgumentException();

        if (keyOrder == null)
            throw new IllegalArgumentException();

        this.relation = relation;

        final int partitionId = predicate.getPartitionId();

        /*
         * If the predicate is addressing a specific shard, then the default is
         * to assume that it will not be using a remote access path. However, if
         * a remote access path was explicitly request and the partitionId was
         * specified, then it will be an error (which is trapped below).
         */
        final boolean remoteAccessPath = predicate
                .getProperty(
                        IPredicate.Annotations.REMOTE_ACCESS_PATH,
                        partitionId == -1 ? IPredicate.Annotations.DEFAULT_REMOTE_ACCESS_PATH
                                : false);

        /*
         * Chose the right index manger. If relation.getIndexManager() is not
         * federation, then always use that index manager. Otherwise, if AP is
         * REMOTE use the relation's index manager. Otherwise, the
         * localIndexManager MUST NOT be null and we will use it.
         */
        if (!(relation.getIndexManager() instanceof IBigdataFederation<?>)) {
            this.indexManager = relation.getIndexManager();
        } else if (remoteAccessPath) {
            this.indexManager = relation.getIndexManager();
        } else {
            if (localIndexManager == null) {
                throw new RuntimeException("Local index manager not given but"
                        + " access path specifies local index: pred="+predicate);
            }
            this.indexManager = localIndexManager;
        }
        
        this.predicate = predicate;

        this.keyOrder = keyOrder;

        final int flags = predicate.getProperty(
                IPredicate.Annotations.FLAGS,
                IPredicate.Annotations.DEFAULT_FLAGS);
        
        this.flags = flags;

		/*
		 * Choose the timestamp of the view. If the request is for the
		 * unisolated index but the predicate was flagged as READONLY then
		 * automatically choose READ_COMMITTED instead.
		 */
		{

			long timestamp = relation.getTimestamp();

			timestamp = (timestamp == ITx.UNISOLATED
					&& (flags & IRangeQuery.READONLY) != 0 ? ITx.READ_COMMITTED
					: timestamp);
			
			this.timestamp = timestamp;
			
		}
        
        this.historicalRead = TimestampUtility.isReadOnly(timestamp);
        
//        final int partitionId = predicate.getPartitionId();
        
        final IIndex ndx;
        if (partitionId != -1) {
            
            if (remoteAccessPath) {
                /*
                 * A request for a specific shard is not compatible with a
                 * request for a remote access path.
                 */
                throw new RuntimeException("Annotations are not compatible: "
                        + IPredicate.Annotations.REMOTE_ACCESS_PATH + "="
                        + remoteAccessPath + ", but "
                        + IPredicate.Annotations.PARTITION_ID + "="
                        + partitionId + " for "+predicate
                        );
            }
            
            final String namespace = relation.getNamespace();

            // The name of the desired index partition.
            final String name = DataService.getIndexPartitionName(namespace
                    + "." + keyOrder.getIndexName(), partitionId);

            try {
                // MUST be a local index view.
                ndx = (ILocalBTreeView) indexManager.getIndex(name, timestamp);
            } catch (Throwable t) {
                throw new RuntimeException(predicate.toString(), t);
            }

            if (ndx == null) {

                /*
                 * Some possible root causes for failing to find a shard on a DS
                 * are listed below. You should verify that the addressed shard
                 * was actually present on the addressed data service as of the
                 * effect read time of the request.
                 * 
                 * 
                 * - The as-bound predicate was mapped onto the wrong shard.
                 * Some subtle problems have been tracked back to this. See
                 * https://sourceforge.net/apps/trac/bigdata/ticket/457. There
                 * was also a problem where as were failing to use the as-bound
                 * predicate when mapping the predicate onto a shard.
                 * 
                 * - A failure in IndexManager to locate the shard. This could
                 * include concurrency holes in the indexCache, the access to
                 * the journal for the appropriate commit time, a
                 * read-historical request without a read-lock (application
                 * error), etc.
                 * 
                 * - The shard was moved (but this will be a
                 * StaleLocatorException and can only occur with the unisolated
                 * index view, at least until we implement shard caching as part
                 * of the hybrid shared disk / shared nothing architecture).
                 */
                
//            	// For debugging only - comment this out.
//				dumpMDI((AbstractScaleOutFederation<?>) relation
//						.getIndexManager(), relation.getNamespace(), timestamp,
//						keyOrder);

                throw new RuntimeException("No such index: relation="
                        + relation.getNamespace() + ", timestamp=" + timestamp
                        + ", keyOrder=" + keyOrder + ", pred=" + predicate
                        + ", indexManager=" + indexManager);

            }

            /*
             * An index partition constraint was specified, so verify that we
             * were given a local index object and that the index object is for
             * the correct index partition.
             */

            pmd = ndx.getIndexMetadata().getPartitionMetadata();

            if (pmd == null)
                throw new RuntimeException("Not an index partition");

            if (pmd.getPartitionId() != partitionId) {

                throw new RuntimeException("Expecting partitionId="
                        + partitionId + ", but have " + pmd.getPartitionId());

            }

        } else {

            // The predicate is not constrained to an index partition.
            pmd = null;

            /*
             * Obtain the index.
             * 
             * FIXME The getIndex(IKeyOrder) code path is optimized by
             * SPORelation and LexiconRelation. However, we should have
             * automatic caching of the index references to avoid the
             * significant penalty of going down to the commitRecordIndex and
             * Name2Addr each time we need to resolve an index. (Scale-out has
             * separate caching for this in IndexManager.)
             */
            ndx = relation.getIndex(keyOrder);
//            final String fqn = AbstractRelation.getFQN(relation, keyOrder);
//
//            ndx = AbstractRelation.getIndex(indexManager, fqn, timestamp);
            
            if (ndx == null) {

                throw new RuntimeException("No such index: relation="
                        + relation.getNamespace() + ", timestamp=" + timestamp
                        + ", keyOrder=" + keyOrder + ", pred=" + predicate
                        + ", indexManager=" + indexManager);

            }

        }

        this.ndx = ndx;

        /**
         * See AST2BOpUtility.toPredicate(). It is responsible for copying these
         * annotations from the StatementPatternNode onto the Predicate so they
         * can influence the behavior of the AccessPath.
         * 
         * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/791" >
         *      Clean up query hints </a>
         */
        final int chunkOfChunksCapacity = predicate.getProperty(
                BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY,
                BufferAnnotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);

        final int chunkCapacity = predicate.getProperty(
                BufferAnnotations.CHUNK_CAPACITY,
                BufferAnnotations.DEFAULT_CHUNK_CAPACITY);

        final int fullyBufferedReadThreshold = predicate.getProperty(
                IPredicate.Annotations.FULLY_BUFFERED_READ_THRESHOLD,
                IPredicate.Annotations.DEFAULT_FULLY_BUFFERED_READ_THRESHOLD);

        this.chunkOfChunksCapacity = chunkOfChunksCapacity;

        this.chunkCapacity = chunkCapacity;

        this.fullyBufferedReadThreshold = fullyBufferedReadThreshold;
        
        this.isFullyBoundForKey = predicate.isFullyBound(keyOrder);

        {

            /*
             * The filter to be evaluated at the index (optional).
             * 
             * Note: This MUST be an implementation which is "aware" of the
             * reuse of tuples within tuple iterators. That is why it is being
             * cast to a BOpTupleIterator.
             * 
             * @todo if not a perfect index then impose additional filter first
             * to skip over tuples which do not satisfy the concrete asBound
             * predicate. This allows us to use the "best" index, not just a
             * "perfect" index.
             */
            final IFilter indexLocalFilter = predicate.getIndexLocalFilter();

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
                final FilterBase tmp = new NOPFilter();

                if (indexLocalFilter != null)
                    tmp.addFilter(indexLocalFilter);
                
                tmp.addFilter(new SameVariableConstraintTupleFilter<R>(
                        sameVarConstraint));

                this.indexLocalFilter = tmp;
                
            } else {
                
                this.indexLocalFilter = indexLocalFilter;
                
            }
            
        }
        
        // optional filter to be evaluated by the AccessPath.
        this.accessPathFilter = predicate.getAccessPathFilter();

        // true iff there is a filter (either local or remote).
        this.hasFilter = (indexLocalFilter != null || accessPathFilter != null);

        final IKeyBuilder keyBuilder = ndx.getIndexMetadata()
                .getTupleSerializer().getKeyBuilder();

        fromKey = keyOrder.getFromKey(keyBuilder, predicate);

        toKey = keyOrder.getToKey(keyBuilder, predicate);
        
    }
    
    @Override
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
                + (toKey == null ? "n/a" : BytesUtil.toString(toKey))
				+ ", hasFilter=" + hasFilter
                + ", indexLocalFilter="
                + (indexLocalFilter == null ? "n/a" : indexLocalFilter)
                + ", accessPathFilter="
                + (accessPathFilter == null ? "n/a" : accessPathFilter)
                + ", indexManager="+indexManager
                + "}";

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
        
        if(DEBUG) {
            
            if (fromKey != null && toKey != null) {
                
                if (BytesUtil.compareBytes(fromKey, toKey) >= 0) {

                    throw new AssertionError("keys are out of order: " + toString());

                }
                
            }

            log.debug(toString());
            
        }
        
        return this;
        
    }
    
    public IRelation<R> getRelation() {

        return relation;
        
    }

    public IIndexManager getIndexManager() {
        
        return indexManager;
        
    }

    public long getTimestamp() {
        
        return timestamp;
        
    }
    
    @Override
    public IPredicate<R> getPredicate() {
        
        return predicate;
        
    }

    @Override
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
    @Override
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
        
        final IChunkedIterator<R> itr = iterator(0L/* offset */, 1L/* limit */,
                1/* capacity */);
        
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

//    /**
//     * {@inheritDoc}
//     * 
//     * @see https://sourceforge.net/apps/trac/bigdata/ticket/209 (Access path
//     *      should visit solutions for high level query).
//     */
//    public ICloseableIterator<IBindingSet> solutions(final BaseJoinStats stats) {
//
////        final IVariable<?>[] vars = BOpUtility
////                .getDistinctArgumentVariables(predicate);
//
//        return BOpContext.solutions(iterator(), predicate, /*vars,*/ stats);
//
//    }
    
    /**
     * {@inheritDoc}
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/209 (Access path
     *      should visit solutions for high level query).
     */
    @Override
    public ICloseableIterator<IBindingSet[]> solutions(final BOpContext context, final long limit,
    		final BaseJoinStats stats) {

//        final IVariable<?>[] vars = BOpUtility
//                .getDistinctArgumentVariables(predicate);

        return context.solutions(
                iterator(0L/* offset */, limit, 0/* capacity */), predicate,
                stats);

    }
    
    @Override
    final public IChunkedOrderedIterator<R> iterator() {
        
        return iterator(0L/* offset */, 0L/* limit */, 0);
        
    }

//    final public IChunkedOrderedIterator<R> iterator(final int limit,
//            final int capacity) {
//
//        return iterator(0L/* offset */, limit, capacity);
//
//    }

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
    @Override
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

            if (DEBUG)
                log.debug("Proven empty by historical range count");

            return new EmptyChunkedIterator<R>(keyOrder);
            
        }
        
        if (DEBUG)
            log.debug("offset=" + offset + ", limit=" + limit + ", capacity="
                    + capacity + ", accessPath=" + this);
        
        final boolean fullyBufferedRead;

        // true iff a point test is a hit on the bloom filter.
        boolean bloomHit = false;
        
        if(isFullyBoundForKey) {

            if (DEBUG)
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
        
        if (DEBUG) {

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

        if(DEBUG) {
            
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
        
        if (DEBUG)
            log.debug("");
        
        /*
         * Note: The filter is applied by the ITupleIterator so that it gets
         * evaluated close to the data, not here where it would be evaluated
         * once the elements were materialized on the client.
         */
        final BlockingBuffer<R[]> buffer = new BlockingBuffer<R[]>(
                chunkOfChunksCapacity);

        /**
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/707">
         *      BlockingBuffer.close() does not unblock threads </a>
         */
        
        // Wrap computation as FutureTask.
        final FutureTask<Void> ft = new FutureTask<Void>(
                new ChunkConsumerTask<R>(this, src, buffer));

        // Set Future on BlockingBuffer *before* starting computation.
        buffer.setFuture(ft);
        
        // Start computation.
        indexManager.getExecutorService().submit(ft);

        return new ChunkConsumerIterator<R>(buffer.iterator(), keyOrder);
            
    }
    
    /**
     * Consumes elements from the source iterator, converting them into chunks
     * on a {@link BlockingBuffer}. The consumer will drain the chunks from the
     * buffer.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
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

        @Override
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

                    if (DEBUG)
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

    @Override
    final public long rangeCount(final boolean exact) {

        assertInitialized();

        long n = 0L;

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

        if (DEBUG) {

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
    
//    @Override
//    final public ITupleIterator<R> rangeIterator() {
//
//        return rangeIterator(0/* capacity */, flags, indexLocalFilter);
//
//    }

    @SuppressWarnings( { "unchecked" })
    protected ITupleIterator<R> rangeIterator(final int capacity,
            final int flags, final IFilter filter) {

        assertInitialized();

        if (DEBUG) {

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
    @Override
    public long removeAll() {

        assertInitialized();

        if (DEBUG) {

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

    /**
     * Return an estimate of the cost of a scan on the predicate.
     * 
     * @param pred
     *            The predicate.
     * 
     * @return The estimated cost of a scan on that predicate.
     */
    public ScanCostReport estimateCost() {

        if(ndx instanceof UnisolatedReadWriteIndex) {
        
            return ((UnisolatedReadWriteIndex) ndx).estimateCost(diskCostModel,
                    rangeCount(false/* exact */));
            
        }
        
        if (ndx instanceof BTree) {

            /*
             * Fast path for a local BTree.
             */
            
            // fast range count (may be cached by the access path).
            final long rangeCount = rangeCount(false/*exact*/);

            return estimateCost(diskCostModel, (BTree) ndx, rangeCount);

        }

        if (ndx instanceof ILocalBTreeView) {

            /*
             * A local view. This path is for both transactions and local
             * shards.
             */
            
            // fast range count (may be cached by the access path).
            final long rangeCount = rangeCount(false/* exact */);
            
            return estimateCost((ILocalBTreeView) ndx, rangeCount, fromKey,
                    toKey);

        }

        if (ndx instanceof IScaleOutClientIndex) {
            
            /*
             * A scale-out index is being addressed.
             */
            return estimateCost((IScaleOutClientIndex) ndx);

        }

        throw new UnsupportedOperationException("index=" + ndx);
        
    }

    /**
     * Return the estimated cost of an index scan on a local {@link BTree}.
     * 
     * @param btree
     *            The {@link BTree}.
     *            
     * @return The estimated cost of the scan.
     */
    private ScanCostReport estimateCost(final DiskCostModel diskCostModel,
            final BTree btree, final long rangeCount) {

        // BTree is its own statistics view.
        final IBTreeStatistics stats = (BTree) btree;
        
        // Estimate cost based on random seek per node/leaf.
        final double cost = new BTreeCostModel(diskCostModel).rangeScan(
                rangeCount, stats.getBranchingFactor(), stats.getHeight(),
                stats.getUtilization().getLeafUtilization());

        return new ScanCostReport(rangeCount, cost);

    }

    /**
     * Return the estimated cost of a key-range scan for a local B+Tree view.
     * This handles both {@link IsolatedFusedView} (transactions) and
     * {@link FusedView} (shards).
     * 
     * @param view
     *            The view.
     * 
     * @return The estimated cost.
     */
    static private ScanCostReport estimateCost(final ILocalBTreeView view,
            final long rangeCount, final byte[] fromKey, final byte[] toKey) {
        
        double cost = 0d;

        final AbstractBTree[] sources = view.getSources();

        for (AbstractBTree source : sources) {

            final IBTreeStatistics stats = source.getStatistics();

            // fast range count on that source.
            final long sourceRangeCount = source.rangeCount(fromKey, toKey);

            if (source instanceof IndexSegment) {

                // Cost for an index segment based on multi-block IO.
                final IndexSegment seg = (IndexSegment) source;

                final long extentLeaves = seg.getStore().getCheckpoint().extentLeaves;

                final long leafCount = stats.getLeafCount();

                // Note: bytesPerLeaf is never more than an int32 value!
				final int bytesPerLeaf = (int) Math
						.ceil(((double) extentLeaves) / leafCount);

                cost += new IndexSegmentCostModel(diskCostModel).rangeScan(
                        (int) sourceRangeCount, stats.getBranchingFactor(),
                        bytesPerLeaf, DirectBufferPool.INSTANCE
                                .getBufferCapacity());

            } else {

                // Cost for a B+Tree based on random seek per node/leaf.
                cost += new BTreeCostModel(diskCostModel).rangeScan(
                        sourceRangeCount, stats.getBranchingFactor(), stats
                                .getHeight(), stats.getUtilization()
                                .getLeafUtilization());

            }

        }

        // @todo pass details per source back in the cost report.
        return new ScanCostReport(rangeCount, cost);


    }

    /**
     * Return the estimated cost of a key-range scan on a remote view of a
     * scale-out index.
     * 
     * @param ndx
     *            The scale-out index.
     * 
     * @return
     * 
     * @todo Remote scans can be parallelized. If flags includes PARALLEL then
     *       the cost can be as little as the cost of scanning one shard.
     *       However, the {@link IClientIndex} has a configuration value which
     *       specifies the maximum parallelism of any given operation (this is
     *       self-reported if we cast to the implementation class). Further,
     *       even if we assume that the shards are evenly distributed over the
     *       nodes, when the #of shards is significantly larger than the #of
     *       nodes then the scan can interfere with itself. Finally, this should
     *       include an estimate of the RMI overhead.
     */
    private ScanCostReport estimateCost(final IScaleOutClientIndex ndx) {
        
        final String name = ndx.getIndexMetadata().getName();

        final AbstractClient<?> client = ndx.getFederation().getClient();
        
        // maximum parallelization by the client : @todo not used yet.
        final int maxParallel = client.getMaxParallelTasksPerRequest();

        // the metadata index for that scale-out index.
        final IMetadataIndex mdi = ndx.getFederation().getMetadataIndex(name,
                timestamp);

        if (mdi == null)
            throw new NoSuchIndexException("name=" + name + "@"
                    + TimestampUtility.toString(timestamp));

        // #of index partitions to be scanned.
        final long partitionCount = mdi.rangeCount(fromKey, toKey);

        if (partitionCount == 0) {

            /*
             * SWAG in case zero partition count is reported (I am not sure that
             * this code path is possible).
             * 
             * @todo This is proven possible. Now figure out why. Maybe this is
             * fromKey==toKey, in which case we can optimize that out.
             */
            return new ScanCostReport(0L/* rangeCount */, partitionCount, 100/* millis */);
//        	/*
//        	 * Should never be "zero" partition count.
//        	 */
//        	throw new AssertionError();

        }

        // fast range count (may be cached by the access path).
        final long rangeCount = rangeCount(false/* exact */);

        if (partitionCount == 1) {

            /*
             * Delegate the operation to the remote shard.
             */

            return (ScanCostReport) ndx.submit(
                    fromKey == null ? BytesUtil.EMPTY : fromKey,
                    new EstimateShardScanCost(rangeCount, fromKey, toKey));

        }

        /*
         * Assume a statistical model. Each partition is comprised of 1 journal
         * with 50k tuples plus two index segments of 100M each.
         */

        // one journal per shard.
        final int njournals = 1;
        // two segments per shard.
        final int nsegments = 2;

        final long rangeCountOnJournal = rangeCount
                / (partitionCount * (njournals + nsegments));

        final double costPerJournal = new BTreeCostModel(diskCostModel)
                .rangeScan(rangeCountOnJournal, //
                        mdi.getIndexMetadata().getBranchingFactor(), //
                        5,// height (SWAG)
                        70// leafUtilization (percent, SWAG).
                );

        final double costPerSegment = diskCostModel.seekTime + Bytes.megabyte
                * 100;

        final double costPerShard = costPerJournal + 2 * costPerSegment;

        // @todo ignores potential parallelism.
        final double cost = costPerShard * partitionCount;

        return new ScanCostReport(rangeCount, partitionCount, cost);

    }

    /**
     * Procedure to estimate the cost of an index range scan on a remote shard.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static final class EstimateShardScanCost implements
            ISimpleIndexProcedure<ScanCostReport> {

        private static final long serialVersionUID = 1L;

        private final long rangeCount;

        private final byte[] fromKey;

        private final byte[] toKey;

        public EstimateShardScanCost(final long rangeCount,
                final byte[] fromKey, final byte[] toKey) {
            this.rangeCount = rangeCount;
            this.fromKey = fromKey;
            this.toKey = toKey;
        }

        @Override
        public ScanCostReport apply(final IIndex ndx) {
            
            final ScanCostReport scanCostReport = AccessPath.estimateCost(
                    ((ILocalBTreeView) ndx), rangeCount, fromKey, toKey);
            
            return scanCostReport;
            
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }
    }

    /*
     * Cost models.
     */

    /**
     * The cost model associated with the disk on which the indices are stored.
     * For a {@link Journal}, this is just the cost model of the backing disk.
     * For the federation, this should be an average cost model.
     * 
     * @todo This is not parameterized. A simple cost model is always assumed.
     *       The correct cost model is necessary in order to get the tradeoff
     *       point right for SCAN+FILTER versus SUBQUERY on SSD or RAID arrays
     *       with lots of spindles versus normal disk.
     * 
     * @todo In a shared disk deployment, we might introduce one cost model for
     *       local SSD used to cache journals, one for local non-SSD disks used
     *       to cache index segments, and one for remote storage used to
     *       materialize historical journals and index segments for query.
     * 
     * @todo In a federation, this should be reported out as metadata for the
     *       federation. Perhaps as a Jini attribute. Or we could self-publish
     *       this using a System property whose value was either the name of the
     *       desired cost model enum or a representation of the cost model which
     *       we could then parse.
     */
    private static final DiskCostModel diskCostModel = DiskCostModel.DEFAULT;

//	/**
//	 * Dumps the locators for an index of a relation.
//	 * 
//	 * @param fed
//	 * @param namespace
//	 *            The relation namespace.
//	 * @param timestamp
//	 *            The timestamp of the view.
//	 * @param keyOrder
//	 *            The index.
//	 */
//	private static void dumpMDI(AbstractScaleOutFederation<?> fed,
//			final String namespace, final long timestamp,
//			final IKeyOrder<?> keyOrder) {
//
//		final String name = namespace + "." + keyOrder.getIndexName();
//
//		final Iterator<PartitionLocator> itr = fed
//				.locatorScan(name, timestamp, new byte[] {}/* fromKey */,
//						null/* toKey */, false/* reverseScan */);
//
//		System.err.println("name=" + name + " @ "
//				+ TimestampUtility.toString(timestamp));
//		while (itr.hasNext()) {
//			System.err.println(itr.next());
//		}
//
//	}

}
