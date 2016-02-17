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
 * Created on Aug 26, 2010
 */
package com.bigdata.bop;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.eclipse.jetty.client.HttpClient;

import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.join.BaseJoinStats;
import com.bigdata.bop.join.IHashJoinUtility;
import com.bigdata.btree.ISimpleIndexAccess;
import com.bigdata.journal.IBTreeManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.ssets.ISolutionSetManager;
import com.bigdata.rdf.sparql.ast.ssets.SolutionSetManager;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.ChunkedFilter;
import com.bigdata.striterator.Chunkerator;
import com.bigdata.striterator.CloseableChunkedIteratorWrapperConverter;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedStriterator;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * The evaluation context for the operator (NOT serializable).
 * 
 * @param <E>
 *            The generic type of the objects processed by the operator.
 */
public class BOpContext<E> extends BOpContextBase {

//    static private final transient Logger log = Logger.getLogger(BOpContext.class);

    private final IRunningQuery runningQuery;
    
    private final int partitionId;

    private final BOpStats stats;

//    private final IMultiSourceAsynchronousIterator<E[]> source;
    private final ICloseableIterator<E[]> source;

    private final IBlockingBuffer<E[]> sink;

    private final IBlockingBuffer<E[]> sink2;

    /**
     * The operator that is being executed.
     */
    private final PipelineOp op;
    
    private final boolean lastInvocation;

	/**
	 * <code>true</code> iff this is the last invocation of the operator. The
	 * property is only set to <code>true</code> for operators which:
	 * <ol>
     * <li>{@link PipelineOp.Annotations#LAST_PASS} is <code>true</code></li>
	 * <li>{@link PipelineOp.Annotations#PIPELINED} is <code>true</code></li>
     * <li>{@link PipelineOp.Annotations#MAX_PARALLEL} is <code>1</code></li>
	 * </ol>
	 * Under these circumstances, it is possible for the {@link IQueryClient} to
	 * atomically decide that a specific invocation of the operator task for the
	 * query will be the last invocation for that task. This is not possible if
	 * the operator allows concurrent evaluation tasks. Sharded operators are
	 * intrinsically concurrent since they can evaluate at each shard in
	 * parallel. This is why the evaluation context is locked to the query
	 * controller. In addition, the operator must declare that it is NOT thread
	 * safe in order for the query engine to serialize its evaluation tasks.
	 */
    public boolean isLastInvocation() {
        return lastInvocation;
    }

    /**
     * The interface for a running query.
     * <p>
     * Note: In scale-out each node will have a distinct {@link IRunningQuery}
     * object and the query controller will have access to additional state,
     * such as the aggregation of the {@link BOpStats} for the query on all
     * nodes.
     */
    public IRunningQuery getRunningQuery() {
        return runningQuery;
    }

    /**
     * The index partition identifier -or- <code>-1</code> if the index is not
     * sharded.
     */
    public final int getPartitionId() {
        return partitionId;
    }

    /**
     * The object used to collect statistics about the evaluation of this
     * operator.
     */
    public final BOpStats getStats() {
        return stats;
    }

    /**
     * Return the operator that is being executed.
     */
    public PipelineOp getOperator() {
        return op;
    }

    /**
     * Where to read the data to be consumed by the operator.
     */
    public final ICloseableIterator<E[]> getSource() {
        return source;
    }

    /**
     * Where to write the output of the operator.
     * 
     * @see PipelineOp.Annotations#SINK_REF
     */
    public final IBlockingBuffer<E[]> getSink() {
        return sink;
    }

    /**
     * Optional alternative sink for the output of the operator. This is used by
     * things like SPARQL optional joins to route failed joins outside of the
     * join group.
     * 
     * @see PipelineOp.Annotations#ALT_SINK_REF
     * @see PipelineOp.Annotations#ALT_SINK_GROUP
     */
    public final IBlockingBuffer<E[]> getSink2() {
        return sink2;
    }

    /**
     * 
     * @param runningQuery
     *            The {@link IRunningQuery} (required).
     * @param partitionId
     *            The index partition identifier -or- <code>-1</code> if the
     *            index is not sharded.
     * @param stats
     *            The object used to collect statistics about the evaluation of
     *            this operator.
     * @param source
     *            Where to read the data to be consumed by the operator.
     * @param op
     *            The operator that is being executed.
     * @param lastInvocation
     *            <code>true</code> iff this is the last invocation pass for
     *            that operator.
     * @param sink
     *            Where to write the output of the operator.
     * @param sink2
     *            Alternative sink for the output of the operator (optional).
     *            This is used by things like SPARQL optional joins to route
     *            failed joins outside of the join group.
     * 
     * @throws IllegalArgumentException
     *             if the <i>stats</i> is <code>null</code>
     * @throws IllegalArgumentException
     *             if the <i>source</i> is <code>null</code> (use an empty
     *             source if the source will be ignored).
     * @throws IllegalArgumentException
     *             if the <i>sink</i> is <code>null</code>
     * 
     * @todo Modify to accept {@link IChunkMessage} or an interface available
     *       from getChunk() on {@link IChunkMessage} which provides us with
     *       flexible mechanisms for accessing the chunk data.
     *       <p>
     *       When doing that, modify to automatically track the {@link BOpStats}
     *       as the <i>source</i> is consumed.
     *       <p>
     *       Note: The only call to this method outside of the test suite is
     *       from ChunkedRunningQuery. It always has a fully materialized chunk
     *       on hand and ready to be processed.
     */
    public BOpContext(//
            final IRunningQuery runningQuery,//
            final int partitionId,//
            final BOpStats stats, //
            final PipelineOp op,//
            final boolean lastInvocation,//
            final ICloseableIterator<E[]> source,//
            final IBlockingBuffer<E[]> sink, //
            final IBlockingBuffer<E[]> sink2//
            ) {
        
        this(runningQuery, runningQuery.getFederation(), runningQuery
                .getLocalIndexManager(), partitionId, stats, op,
                lastInvocation, source, sink, sink2);
        
    }

    /**
     * Variant used by some test cases that need to mock up a {@link BOpContext}
     * .
     * 
     * @param runningQuery
     *            The {@link IRunningQuery} (not checked).
     * @param fed
     *            The federation iff running in scale-out.
     * @param localIndexManager
     *            The <strong>local</strong> index manager (required).
     * @param runningQuery
     *            The {@link IRunningQuery} (required).
     * @param partitionId
     *            The index partition identifier -or- <code>-1</code> if the
     *            index is not sharded.
     * @param stats
     *            The object used to collect statistics about the evaluation of
     *            this operator.
     * @param source
     *            Where to read the data to be consumed by the operator.
     * @param op
     *            The operator that is being executed.
     * @param lastInvocation
     *            <code>true</code> iff this is the last invocation pass for
     *            that operator.
     * @param sink
     *            Where to write the output of the operator.
     * @param sink2
     *            Alternative sink for the output of the operator (optional).
     *            This is used by things like SPARQL optional joins to route
     *            failed joins outside of the join group.
     * 
     * @throws IllegalArgumentException
     *             if the <i>stats</i> is <code>null</code>
     * @throws IllegalArgumentException
     *             if the <i>source</i> is <code>null</code> (use an empty
     *             source if the source will be ignored).
     * @throws IllegalArgumentException
     *             if the <i>sink</i> is <code>null</code>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    BOpContext(//
                final IRunningQuery runningQuery,//
                final IBigdataFederation<?> fed,//
                final IIndexManager localIndexManager,//
                final int partitionId,//
                final BOpStats stats, //
                final PipelineOp op,//
                final boolean lastInvocation,//
                final ICloseableIterator<E[]> source,//
                final IBlockingBuffer<E[]> sink, //
                final IBlockingBuffer<E[]> sink2//
                ) {
            
        super(fed, localIndexManager);
            
        if (stats == null)
            throw new IllegalArgumentException();

        if (op == null)
            throw new IllegalArgumentException();

        if (source == null)
            throw new IllegalArgumentException();
        
        if (sink == null)
            throw new IllegalArgumentException();

        this.runningQuery = runningQuery;
        this.partitionId = partitionId;
        this.stats = stats;
        this.op = op;
        this.lastInvocation = lastInvocation;
        /*
         * Wrap each IBindingSet to provide access to the BOpContext.
         * 
         * @see <a
         * href="https://sourceforge.net/apps/trac/bigdata/ticket/513"> Expose
         * the LexiconConfiguration to function BOPs </a>
         */
        this.source = (ICloseableIterator) new SetContextIterator(this,
                (ICloseableIterator) source);
//        this.source = source;
        this.sink = sink;
        this.sink2 = sink2; // may be null
    }

    /**
     * Test suite helper.
     */
    public static <E> BOpContext<E> newMock(//
            final IRunningQuery runningQuery,//
            final IBigdataFederation<?> fed,//
            final IIndexManager localIndexManager,//
            final int partitionId,//
            final BOpStats stats, //
            final PipelineOp op,//
            final boolean lastInvocation,//
            final ICloseableIterator<E[]> source,//
            final IBlockingBuffer<E[]> sink, //
            final IBlockingBuffer<E[]> sink2//
    ) {

        return new BOpContext<>(runningQuery, fed, localIndexManager,
                partitionId, stats, op, lastInvocation, source, sink, sink2);

    }
    
    /**
     * Wraps each {@link IBindingSet} to provide access to the
     * {@link BOpContext}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     *         
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/513">
     *      Expose the LexiconConfiguration to function BOPs </a>
     */
    private static class SetContextIterator implements
            ICloseableIterator<IBindingSet[]> {

        private final BOpContext<?> context;
        
        private final ICloseableIterator<IBindingSet[]> src;

        private IBindingSet[] cur = null;

        private boolean open = true;

        public SetContextIterator(final BOpContext<?> context,
                final ICloseableIterator<IBindingSet[]> src) {

            this.src = src;
            
            this.context = context;

        }

        @Override
        public void close() {

            if (open) {
                
                src.close();
                
                open = false;
                
            }
            
        }
        
        @Override
        public boolean hasNext() {

            if (!open)
                return false;
            
            if (cur != null)
                return true;
            
            if (!src.hasNext()) {
            
                close();
                
                return false;
                
            }

            final IBindingSet[] nxt = src.next();
            
            /*
             * Note: We need an IBindingSet[] rather than a ListBindingSet or
             * other concrete type in order to avoid array store errors when we
             * wrap the IBindingSet instances below. Therefore, if the component
             * type is wrong, we have to allocate a new array.
             */
            this.cur = nxt.getClass().getComponentType() == IBindingSet.class ? nxt
                    : new IBindingSet[nxt.length];
            
//            try {
            
            for (int i = 0; i < nxt.length; i++) {

                final IBindingSet bset = nxt[i];

                // Wrap the binding set.
                cur[i] = bset instanceof ContextBindingSet ? bset
                        : new ContextBindingSet(context, bset);

            }

            return true;
                
//            } catch (ArrayStoreException ex) {
//
//            /*
//             * Note: This could be used to locate array store exceptions arising
//             * from a ListBindingSet[] or other concrete array type. Remove once
//             * I track down the sources of a non-IBindingSet[]. The problem can
//             * of course be worked around by allocating a new IBindingSet[] into
//             * which the ContextBindingSets will be copied.
//             * 
//             * Likely causes are users of java.lang.reflect.Array.newInstance().
//             * Whenever possible, code should either an explicit component type
//             * for dynamically allocated arrays or use the component type of the
//             * source array (when there is one).
//             */
//                throw new RuntimeException("cur[" + nxt.length + "]=" + nxt
//                        + ", src=" + src, ex);
//
//            }
            
        }

        @Override
        public IBindingSet[] next() {
            
            if (!hasNext())
                throw new NoSuchElementException();
            
            final IBindingSet[] ret = cur;
            
            cur = null;
            
            return ret;
            
        }

        @Override
        public void remove() {

            throw new UnsupportedOperationException();
            
        }

    }

    /**
     * Return the {@link IRunningQuery} associated with the specified queryId.
     * 
     * @param queryId
     *            The {@link UUID} of some {@link IRunningQuery}.
     * 
     * @return The {@link IRunningQuery}.
     * 
     * @throws RuntimeException
     *             if the {@link IRunningQuery} has halted.
     * @throws RuntimeException
     *             if the {@link IRunningQuery} is not found.
     */
    public IRunningQuery getRunningQuery(final UUID queryId) {

        // Lookup the query by its UUID.
        final IRunningQuery runningQuery;
        try {
            if (queryId.equals(this.runningQuery.getQueryId())) {
                runningQuery = this.runningQuery;
            } else {
                runningQuery = getRunningQuery().getQueryEngine()
                        .getRunningQuery(queryId);
            }

        } catch (RuntimeException ex) {
            throw new RuntimeException("Query halted? : " + ex, ex);
        }

        if (runningQuery == null) {
            // We could not locate the query.
            throw new RuntimeException("IRunningQuery not found.");
        }
        
        return runningQuery;
        
    }
    
    /**
	 * Return the {@link IQueryAttributes} associated with the specified query.
	 * 
	 * @param queryId
	 *            The {@link UUID} of some {@link IRunningQuery} -or- null to
	 *            use the {@link IQueryAttributes} of this query.
	 * 
	 * @return The {@link IQueryAttributes} for that {@link IRunningQuery}.
	 * 
	 * @throws RuntimeException
	 *             if the {@link IRunningQuery} has halted.
	 * @throws RuntimeException
	 *             if the {@link IRunningQuery} is not found.
	 */
    public IQueryAttributes getQueryAttributes(final UUID queryId) {

		if (queryId == null) // See BLZG-1493
			return getRunningQuery().getAttributes();
    	
        return getRunningQuery(queryId).getAttributes();

    }

    /**
     * Return the {@link IQueryAttributes} associated with this query.
     * 
     * @return The {@link IQueryAttributes}.
     */
    public IQueryAttributes getQueryAttributes() {
        
        return getRunningQuery().getAttributes();
        
    }

//    /**
//     * Return an access path for a predicate that identifies a data structure
//     * which can be resolved to a reference attached to as a query attribute.
//     * <p>
//     * This method is used for data structures (including {@link Stream}s,
//     * {@link HTree}s, and {@link BTree} indices as well as non-durable data
//     * structures, such as JVM collection classes. When the data structure is
//     * pre-existing (such as a named solution set whose life cycle is broader
//     * than the query), then the data structure MUST be resolved during query
//     * optimization and attached to the {@link IRunningQuery} before operators
//     * with a dependency on those data structures can execute.
//     * 
//     * @param predicate
//     *            The predicate.
//     * 
//     * @return The access path.
//     * 
//     * @throws RuntimeException
//     *             if the access path could not be resolved.
//     * 
//     * @see #getQueryAttributes()
//     */
//    public IBindingSetAccessPath<?> getAccessPath(final IPredicate predicate) {
//
//        if (predicate == null)
//            throw new IllegalArgumentException();
//
//        /*
//         * TODO There are several cases here, one for each type of
//         * data structure we need to access and each means of identifying
//         * that data structure. The main case is Stream (for named solution sets).
//         * If we wind up always modeling a named solution set as a Stream, then
//         * that is the only case that we need to address.
//         */
//        if(predicate instanceof SolutionSetStream.SolutionSetStreamPredicate) {
//            
//            /*
//             * Resolve the name of the Stream against the query attributes for
//             * the running query, obtaining a reference to the Stream. Then
//             * request the access path from the Stream.
//             * 
//             * TODO We might need to also include the UUID of the top-level
//             * query since that is where any subquery will have to look to find
//             * the named solution set.
//             */
//            
//            @SuppressWarnings("unchecked")
//            final SolutionSetStreamPredicate<IBindingSet> p = (SolutionSetStreamPredicate<IBindingSet>) predicate;
//
//            final String attributeName = p.getOnlyRelationName();
//
//            final SolutionSetStream tmp = (SolutionSetStream) getQueryAttributes().get(
//                    attributeName);
//
//            if (tmp == null) {
//
//                /*
//                 * Likely causes include a failure to attach the solution set to
//                 * the query attributes when setting up the query or attaching
//                 * and/or resolving the solution set against the wrong running
//                 * query (especially when the queries are nested).
//                 */
//                throw new RuntimeException(
//                        "Could not resolve Stream: predicate=" + predicate);
//            
//            }
//            
//            return tmp.getAccessPath(predicate);
//
//        }
//        
//        throw new UnsupportedOperationException();
//        
//    }

    /**
     * Return an {@link ICloseableIterator} that can be used to read the
     * solutions to be indexed from a source other than the pipeline. The
     * returned iterator is intentionally aligned with the type returned by
     * {@link BOpContext#getSource()}.
     * 
     * @return An iterator visiting the solutions to be indexed and never
     *         <code>null</code>.
     * 
     * @throws RuntimeException
     *             if the source can not be resolved.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/531">
     *      SPARQL UPDATE for SOLUTION SETS </a>
     */
    @SuppressWarnings("unchecked")
    public ICloseableIterator<IBindingSet[]> getAlternateSource(
            final INamedSolutionSetRef namedSetRef) {

        // Iterator visiting the solution set.
        final ICloseableIterator<IBindingSet> src;

        // The local (application) name of the solution set.
        final String localName = namedSetRef.getLocalName();

        /*
         * When non-null, this identifies the IRunningQuery that we need to look
         * at to find the named solution set.
         */
        final UUID queryId = namedSetRef.getQueryId();
        
        if (queryId != null) {

            /*
             * Lookup the attributes for the query that will be used to resolve
             * the named solution set.
             */
            final IQueryAttributes queryAttributes = getQueryAttributes(queryId);

            // Resolve the named solution set.
            final Object tmp = queryAttributes.get(namedSetRef);

            if (tmp == null) {

                throw new RuntimeException("Not found: name=" + localName
                            + ", namedSetRef=" + namedSetRef);

            }

            if (tmp instanceof IHashJoinUtility) {

                /*
                 * Reading solutions from an existing hash index.
                 */

                final IHashJoinUtility state = (IHashJoinUtility) tmp;

                src = state.indexScan();

            } else if (tmp instanceof ISimpleIndexAccess) {

                /*
                 * Reading solutions from a raw BTree, HTree, or Stream.
                 */

                src = (ICloseableIterator<IBindingSet>) ((ISimpleIndexAccess) tmp)
                        .scan();

            } else {

                /*
                 * We found something, but we do not know how to turn it
                 * into an iterator visiting solutions.
                 */

                throw new UnsupportedOperationException("namedSetRef="
                        + namedSetRef + ", class=" + tmp.getClass());

            }

            return new Chunkerator<IBindingSet>(src, op.getChunkCapacity(),
                    IBindingSet.class);
            
        } else {

        	/**
			 * queryID is null.
			 * 
			 * Note: This is *not* the case addressed by BLZG-1493. That ticket
			 * allows a null queryID to be interpreted as indicating the current
			 * query. However this code path is pre-existing and is used to
			 * locate an alternative source based on a NamedSolutionSetRef. When
			 * the queryID is non-null (above) we look at the specified query.
			 * When it is null (here) we look at the index manager for a durable
			 * NamedSolutionSet.
			 * 
			 * BLZG-1493 might need to be applied here at some point. Right now,
			 * this code path provides access to a durable named solution set.
			 * If we need per-query scoped named solution sets that for cases
			 * with multiple executions of the same sub-query (this is the case
			 * that motivates BLZG-1493 - multiple sub-query evaluation for the
			 * same property path) then we would need to refactor the
			 * NamedSolutionSetRef to de-conflict these various use cases.
			 * 
			 * @see <a href="https://jira.blazegraph.com/browse/BLZG-1493" > NPE
			 *      in nested star property paths </a>
			 */
        	
            // Resolve the object which will give us access to the named
            // solution set.
//            final ICacheConnection cacheConn = CacheConnectionFactory
//                    .getExistingCacheConnection(getRunningQuery()
//                            .getQueryEngine());

            final String namespace = namedSetRef.getNamespace();

            final long timestamp = namedSetRef.getTimestamp();

            // TODO ClassCastException is possible?
            final IBTreeManager localIndexManager = (IBTreeManager) getIndexManager();

            final ISolutionSetManager sparqlCache = new SolutionSetManager(
                    localIndexManager, namespace, timestamp);

            return NamedSolutionSetRefUtility.getSolutionSet(//
                    sparqlCache,//
                    localIndexManager,// 
                    namespace,//
                    timestamp,//
                    localName,//
                    namedSetRef.getJoinVars(),//
                    op.getChunkCapacity()//
                    );

        }

    }
    
    /**
	 * Return the {@link IMemoryManager} associated with the specified query.
	 * 
	 * @param queryId
	 *            The {@link UUID} of some {@link IRunningQuery} -or- null to
	 *            use the {@link IMemoryManager} of this query.
	 * 
	 * @return The {@link IMemoryManager} for that {@link IRunningQuery}.
	 * 
	 * @throws RuntimeException
	 *             if the {@link IRunningQuery} has halted.
	 * @throws RuntimeException
	 *             if the {@link IRunningQuery} is not found.
	 */
    public IMemoryManager getMemoryManager(final UUID queryId) {

		if (queryId == null) // See BLZG-1493
			return getRunningQuery().getMemoryManager();

		return getRunningQuery(queryId).getMemoryManager();

    }

    /**
     * Return the {@link HttpClient} used to make remote SERVICE
     * call requests.
     */
    public HttpClient getClientConnectionManager() {
        
        return getRunningQuery().getQueryEngine().getClientConnectionManager();
        
    }
    
    /**
     * Binds variables from a visited element.
     * <p>
     * Note: The bindings are propagated before the constraints are verified so
     * this method will have a side-effect on the bindings even if the
     * constraints were not satisfied. Therefore you should clone the bindings
     * before calling this method.
     * 
     * @param pred
     *            The {@link IPredicate} from which the element was read.
     * @param constraint
     *            A constraint which must be satisfied (optional).
     * @param e
     *            An element materialized by the {@link IAccessPath} for that
     *            {@link IPredicate}.
     * @param bindingSet
     *            the bindings to which new bindings from the element will be
     *            applied.
     * 
     * @return <code>true</code> unless the new bindings would violate any of
     *         the optional {@link IConstraint}.
     * 
     * @throws NullPointerException
     *             if an argument is <code>null</code>.
     */ @Deprecated// with PipelineJoin.JoinTask.AccessPathTask.handleJoin()
    final static public boolean bind(final IPredicate<?> pred,
            final IConstraint[] constraints, final Object e,
            final IBindingSet bindings) {

        // propagate bindings from the visited object into the binding set.
        copyValues((IElement) e, pred, bindings);

        if (constraints != null) {

            // verify constraint.
            return BOpUtility.isConsistent(constraints, bindings);

        }

        // no constraint.
        return true;        
    }

    /**
     * Copy the values for variables in the predicate from the element, applying
     * them to the caller's {@link IBindingSet}.
     * <p>
     * Note: A variable which is bound outside of the query to a constant gets
     * turned into a {@link Constant} with that variable as its annotation. This
     * method causes the binding to be created for the variable and the constant
     * when the constant is JOINed.
     * 
     * @param e
     *            The element.
     * @param pred
     *            The predicate.
     * @param bindingSet
     *            The binding set, which is modified as a side-effect.
     * 
     *            TODO Make this method package private once we convert to using
     *            an inline access path.
     */
    @SuppressWarnings("unchecked")
    static public void copyValues(final IElement e, final IPredicate<?> pred,
            final IBindingSet bindingSet) {

    		final int arity = pred.arity();
        
        for (int i = 0; i < arity; i++) {

            final IVariableOrConstant<?> t = pred.get(i);

            if (t.isVar()) {

                final IVariable<?> var = (IVariable<?>) t;

                final Object val = e.get(i);
                
                if (val != null) {

                    bindingSet.set(var, new Constant(val));

                }

            } else {

                /*
                 * Note: A variable which is bound outside of the query to a
                 * constant gets turned into a Constant with that variable as
                 * its annotation. This code path causes the binding to be
                 * created for the variable and the constant when the constant
                 * is JOINed.
                 */
                
                final IVariable<?> var = (IVariable<?>) t
                        .getProperty(Constant.Annotations.VAR);

                if (var != null) {

                    final Object val = e.get(i);

                    if (val != null) {

                        bindingSet.set(var, new Constant(val));

                    }

                }

            }

        }

		if (QueryHints.DEFAULT_REIFICATION_DONE_RIGHT
				&& pred instanceof SPOPredicate) {

			final SPOPredicate tmp = (SPOPredicate) pred;

			final IVariable<?> sidVar = tmp.sid();

			if (sidVar != null) {

				/*
				 * Build a SidIV for the (s,p,o) and binding it on the sid
				 * variable.
				 * 
				 * @see <a
				 * href="https://sourceforge.net/apps/trac/bigdata/ticket/526">
				 * Reification Done Right</a>
				 * 
				 * TODO This is RDF specific code. It would be nice if we
				 * did not have to put it into BOpContext.
				 */

				final IV s = (IV) e.get(0);
				final IV p = (IV) e.get(1);
				final IV o = (IV) e.get(2);
				final ISPO spo = new SPO(s, p, o);
				final SidIV sidIV = new SidIV<BigdataBNode>(spo);

				bindingSet.set(sidVar, new Constant(sidIV));

			}
			
        }

    }

//    /**
//     * Copy the as-bound values for the named variables out of the
//     * {@link IElement} and into the caller's array.
//     * 
//     * @return The caller's array. If a variable was resolved to a bound value,
//     *         then it was set on the corresponding index of the array. If not,
//     *         then that index of the array was cleared to <code>null</code>.
//     * 
//     * @param e
//     *            The element.
//     * @param pred
//     *            The predicate.
//     * @param vars
//     *            The variables whose values are desired. They are located
//     *            within the element by examining the arguments of the
//     *            predicate.
//     * @param out
//     *            The array into which the values are copied.
//     * 
//     * @deprecated This fails to propagate the binding for a variable which was
//     *             replaced by Constant/2 from the predicate. Use the variant
//     *             method which copies things into an {@link IBindingSet}
//     *             instead.
//     */
//    @SuppressWarnings({ "rawtypes", "unchecked" })
//    static public void copyValues(final IElement e, final IPredicate<?> pred,
//            final IVariable<?>[] vars, final IConstant<?>[] out) {
//
//        final int arity = pred.arity();
//
//        for (int i = 0; i < vars.length; i++) {
//            
//            out[i] = null; // clear old value (if any).
//
//            boolean found = false;
//            
//            for (int j = 0; j < arity && !found; j++) {
//
//                final IVariableOrConstant<?> t = pred.get(j);
//
//                if (t.isVar()) {
//
//                    final IVariable<?> var = (IVariable<?>) t;
//
//                    if (var.equals(vars[i])) {
//
//                        // the as-bound value of the predicate given that
//                        // element.
//                        final Object val = e.get(j);
//
//                        if (val != null) {
//
//                            out[i] = new Constant(val);
//
//                            found = true;
//
//                        }
//
//                    }
//
//                }
//
//            }
//
//        }
//
//    }

    /**
     * Copy the values for variables from the source {@link IBindingSet} to the
     * destination {@link IBindingSet}. It is an error if a binding already
     * exists in the destination {@link IBindingSet} which is not consistent
     * with a binding in the source {@link IBindingSet}.
     * 
     * @param left
     *            The left binding set (target).
     * @param right
     *            The right binding set (source).
     * @param constraints
     *            An array of constraints (optional). When given, destination
     *            {@link IBindingSet} will be validated <em>after</em> mutation.
     * @param varsToKeep
     *            An array of variables whose bindings will be retained. The
     *            bindings are not stripped out until after the constraint(s)
     *            (if any) have been tested.
     * 
     * @return The solution with the combined bindings and <code>null</code> if
     *         the bindings were not consistent, if a constraint was violated,
     *         etc. Note that either <code>left</code> or <code>right</code> MAY
     *         be returned if the other solution set is empty (optimization).
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static public IBindingSet bind(//
            final IBindingSet left,//
            final IBindingSet right,//
            final IConstraint[] constraints, //
            final IVariable[] varsToKeep//
            ) {
 
        if (constraints == null && varsToKeep == null) {
            
            /*
             * Optimize the case when left is an empty binding set, there are no
             * constraints, and we are keeping all variables. This corresponds
             * to a named subquery include.
             */

            if (left.isEmpty())
                return right;
            
            if (right.isEmpty())
                return left;

        }
        
//        /*
//         * Note: The binding sets from the query pipeline are always chosen as
//         * the destination into which we will copy the bindings. This allows us
//         * to preserve any state attached to those solutions (this is not
//         * something that we do right now).
//         * 
//         * Note: We clone the destination binding set in order to avoid a side
//         * effect on that binding set if the join fails.
//         */
//        final IBindingSet src = leftIsPipeline ? right : left;
//        final IBindingSet dst = leftIsPipeline ? left.clone() : right.clone();
        final IBindingSet src = right;
        final IBindingSet dst = left.clone();

//        log.error("LEFT :" + left); 
//        log.error("RIGHT:" + right);

        // Propagate bindings from src => dst
        {

            final Iterator<Map.Entry<IVariable, IConstant>> sitr = src
                    .iterator();

            while (sitr.hasNext()) {

                final Map.Entry<IVariable, IConstant> e = sitr.next();

                // A variable in the source solution.
                final IVariable<?> var = (IVariable<?>) e.getKey();

                // The binding for that variable in the source solution.
                final IConstant<?> sval = e.getValue();

                if (sval != null) {

                    // The binding for that variable in the destination solution.
                    final IConstant<?> dval = dst.get(var);

                    if (dval != null) {

                        if (!sval.equals(dval)) {

                            // Bindings are not consistent.
//                            log.error("FAIL : " + var + " have " + sval + " and " + dval); 
                            return null;

                        } else if (sval.get() instanceof IV<?, ?>) { 
                            /*
                             * Already bound to the same value; Check cached
                             * Value on the IVs.
                             */
                            final IV siv = (IV) sval.get();
                            final IV div = (IV) dval.get();
                            if (siv.hasValue() && !div.hasValue()) {
                                // Propagate the cached Value to the dst.
                                div.setValue(siv.getValue());
                            }
                        }

                    } else {

                        dst.set(var, sval);

                    }

                }

            }

        }

        // Test constraint(s)
        if (constraints != null && !BOpUtility.isConsistent(constraints, dst)) {

//            log.error("FAIL : CONSTRAINTS : " + constraints);
            return null;

        }

        /*
         * Strip off unnecessary variables.
         * 
         * Note: We can't strip of variables until after we have verified that
         * the solutions may join since a conflict in a variable to be stripped
         * out should still cause the join to fail.
         */
        if (varsToKeep != null && varsToKeep.length > 0) {

            final Iterator<Map.Entry<IVariable, IConstant>> itr = dst
                    .iterator();

            while (itr.hasNext()) {

                final Map.Entry<IVariable, IConstant> e = itr.next();

                final IVariable<?> var = (IVariable<?>) e.getKey();

                boolean found = false;
                for (int i = 0; i < varsToKeep.length; i++) {

                    if (var == varsToKeep[i]) {
                        
                        found = true;
                        
                        break;
                        
                    }

                }

                if (!found) {

//                    // strip out this binding.
//                    dst.clear(var);
                    itr.remove();
                    
                }

            }

        }

        /* 
         * Bindings are consistent. Constraints (if any) were not violated.
         */

//        log.error("JOIN :" + dst);

        return dst;

    }

    /**
     * Convert an {@link IAccessPath#iterator()} into a stream of chunks of
     * {@link IBindingSet}.
     * 
     * @param src
     *            The iterator draining the {@link IAccessPath}. This will visit
     *            {@link IElement}s.
     * @param pred
     *            The predicate for that {@link IAccessPath}
     * @param stats
     *            Statistics to be updated as elements and chunks are consumed
     *            (optional).
     * 
     * @return An iterator visiting chunks of solutions. The order of the
     *         original {@link IElement}s is preserved.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/209 (AccessPath
     *      should visit binding sets rather than elements when used for high
     *      level query.)
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/233 (Inline access
     *      path).
     * 
     *      TODO Move to {@link IAccessPath}? {@link AccessPath}?
     */
//    * @param vars
//    *            The array of distinct variables (no duplicates) to be
//    *            extracted from the visited {@link IElement}s.
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public ICloseableIterator<IBindingSet[]> solutions(
            final IChunkedIterator<?> src, //
            final IPredicate<?> pred,//
//            final IVariable<?>[] varsx, 
            final BaseJoinStats stats//
            ) {

        //return new CloseableIteratorWrapper(
        final IChunkedStriterator itr1 =
                new com.bigdata.striterator.ChunkedStriterator(src).addFilter(
//                        new ChunkedFilter() {
                        new ChunkedFilter<IChunkedIterator<Object>, Object, Object>() {

                            private static final long serialVersionUID = 1L;

                            /**
                             * Count AP chunks and units consumed.
                             */
                            @Override
                            protected Object[] filterChunk(final Object[] chunk) {

                                stats.accessPathChunksIn.increment();

                                stats.accessPathUnitsIn.add(chunk.length);

                                return chunk;

                            }

                        }).addFilter(new com.bigdata.striterator.Resolver() {

                    private static final long serialVersionUID = 1L;

                    /**
                     * Resolve IElements to IBindingSets.
                     */
                    @Override
                    protected Object resolve(final Object obj) {

                        final IElement e = (IElement) obj;

                        final IBindingSet bset = new ContextBindingSet(BOpContext.this, new ListBindingSet());

                        /*
                         * Propagate bindings from the element to the binding
                         * set.
                         * 
                         * Note: This is responsible for handling the semantics
                         * of Constant/2 (when a predicate has a Constant which
                         * binds a variable).
                         */
                        copyValues(e, pred, bset);
                        return bset;

                    }

                });
        //) {
//
//            /**
//             * Close the real source if the caller closes the returned iterator.
//             */
//            @Override
//            public void close() {
//                super.close();
//                src.close();
//            }
//        };

        /*
         * Convert from IChunkedIterator<IBindingSet> to
         * ICloseableIterator<IBindingSet[]>. This is a fly weight conversion.
         */
        final ICloseableIterator<IBindingSet[]> itr2 = new CloseableChunkedIteratorWrapperConverter<IBindingSet>(
                itr1);

        return itr2;

    }

/*
 * I've replaced this with AbstractSplitter for the moment.
 */
//    /**
//     * Return an iterator visiting the {@link PartitionLocator} for the index
//     * partitions from which an {@link IAccessPath} must read in order to
//     * materialize all elements which would be visited for that predicate.
//     * 
//     * @param predicate
//     *            The predicate on which the next stage in the pipeline must
//     *            read, with whatever bindings already applied. This is used to
//     *            discover the shard(s) which span the key range against which
//     *            the access path must read.
//     * 
//     * @return The iterator.
//     */
//    public Iterator<PartitionLocator> locatorScan(final IPredicate<?> predicate) {
//
//        final long timestamp = getReadTimestamp();
//
//        // Note: assumes that we are NOT using a view of two relations.
//        final IRelation<?> relation = (IRelation<?>) fed.getResourceLocator()
//                .locate(predicate.getOnlyRelationName(), timestamp);
//
//        /*
//         * Find the best access path for the predicate for that relation.
//         * 
//         * Note: All we really want is the [fromKey] and [toKey] for that
//         * predicate and index. This MUST NOT layer on expanders since the
//         * layering also hides the [fromKey] and [toKey].
//         */
//        @SuppressWarnings("unchecked")
//        final AccessPath<?> accessPath = (AccessPath<?>) relation
//                .getAccessPath((IPredicate) predicate);
//
//        // Note: assumes scale-out (EDS or JDS).
//        final IClientIndex ndx = (IClientIndex) accessPath.getIndex();
//
//        /*
//         * Note: could also be formed from relationName + "." +
//         * keyOrder.getIndexName(), which is cheaper unless the index metadata
//         * is cached.
//         */
//        final String name = ndx.getIndexMetadata().getName();
//
//        return ((AbstractScaleOutFederation<?>) fed).locatorScan(name,
//                timestamp, accessPath.getFromKey(), accessPath.getToKey(),
//                false/* reverse */);
//
//    }

}
