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
 * Created on Aug 26, 2010
 */
package com.bigdata.bop;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.BaseJoinStats;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.striterator.ChunkedFilter;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * The evaluation context for the operator (NOT serializable).
 * 
 * @param <E>
 *            The generic type of the objects processed by the operator.
 */
public class BOpContext<E> extends BOpContextBase {

    static private final transient Logger log = Logger.getLogger(BOpContext.class);

    private final IRunningQuery runningQuery;
    
    private final int partitionId;

    private final BOpStats stats;

//    private final IMultiSourceAsynchronousIterator<E[]> source;
    private final IAsynchronousIterator<E[]> source;

    private final IBlockingBuffer<E[]> sink;

    private final IBlockingBuffer<E[]> sink2;

    private final AtomicBoolean lastInvocation = new AtomicBoolean(false);

	/**
	 * Set by the {@link QueryEngine} when the criteria specified by
	 * {@link #isLastInvocation()} are satisfied.
	 */
    public void setLastInvocation() {
    	lastInvocation.set(true);
    }

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
	 * 
	 * @todo This should be a ctor parameter.  We just have to update the test
	 * suites for the changed method signature.
	 */
//    * <li>{@link BOp.Annotations#EVALUATION_CONTEXT} is
//    * {@link BOpEvaluationContext#CONTROLLER}</li>
    public boolean isLastInvocation() {
    	return lastInvocation.get();
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
     * Where to read the data to be consumed by the operator.
     */
    public final IAsynchronousIterator<E[]> getSource() {
        return source;
    }

//    /**
//     * Attach another source. The decision to attach the source is mutex with
//     * respect to the decision that the source reported by {@link #getSource()}
//     * is exhausted.
//     * 
//     * @param source
//     *            The source.
//     *            
//     * @return <code>true</code> iff the source was attached.
//     */
//    public boolean addSource(IAsynchronousIterator<E[]> source) {
//
//        if (source == null)
//            throw new IllegalArgumentException();
//        
//        return this.source.add(source);
//        
//    }

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
	 *            The {@link IRunningQuery}.
	 * @param partitionId
	 *            The index partition identifier -or- <code>-1</code> if the
	 *            index is not sharded.
	 * @param stats
	 *            The object used to collect statistics about the evaluation of
	 *            this operator.
	 * @param source
	 *            Where to read the data to be consumed by the operator.
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
	 * @todo modify to accept {@link IChunkMessage} or an interface available
	 *       from getChunk() on {@link IChunkMessage} which provides us with
	 *       flexible mechanisms for accessing the chunk data.
	 *       <p>
	 *       When doing that, modify to automatically track the {@link BOpStats}
	 *       as the <i>source</i> is consumed.
	 */
    public BOpContext(final IRunningQuery runningQuery,final int partitionId,
            final BOpStats stats, final IAsynchronousIterator<E[]> source,
            final IBlockingBuffer<E[]> sink, final IBlockingBuffer<E[]> sink2) {
        
        super(runningQuery.getFederation(), runningQuery.getIndexManager());
        
        if (stats == null)
            throw new IllegalArgumentException();
        
        if (source == null)
            throw new IllegalArgumentException();
        
        if (sink == null)
            throw new IllegalArgumentException();

        this.runningQuery = runningQuery;
        this.partitionId = partitionId;
        this.stats = stats;
        this.source = source;
//        this.source = new MultiSourceSequentialAsynchronousIterator<E[]>(source);
        this.sink = sink;
        this.sink2 = sink2; // may be null
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
     */
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
     */
    @SuppressWarnings("unchecked")
    static public void copyValues(final IElement e, final IPredicate<?> pred,
            final IBindingSet bindingSet) {

        for (int i = 0; i < pred.arity(); i++) {

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
//     *            TODO Unit tests.
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
     *            The left binding set.
     * @param right
     *            The right binding set.
     * @param leftIsPipeline
     *            <code>true</code> iff <i>left</i> is a solution from upstream
     *            in the query pipeline. Otherwise, <i>right</i> is the upstream
     *            solution. The upstream solution may be associated with a
     *            symbol table stack used to manage the visibility of variables
     *            in SPARQL 1.1 subqueries. Therefore, this is the solution
     *            which must be clone and into which the bindings must be
     *            propagated.
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
     *         etc.
     */
    @SuppressWarnings("rawtypes")
    static public IBindingSet bind(final IBindingSet left,
            final IBindingSet right, final boolean leftIsPipeline,
            final IConstraint[] constraints, final IVariable[] varsToKeep) {

        /*
         * Note: The binding sets from the query pipeline are always chosen as
         * the destination into which we will copy the bindings. This allows us
         * to preserve any state attached to those solutions (this is not
         * something that we do right now).
         * 
         * Note: We clone the destination binding set in order to avoid a side
         * effect on that binding set if the join fails.
         */
        final IBindingSet src = leftIsPipeline ? right : left;
        final IBindingSet dst = leftIsPipeline ? left.clone() : right.clone();

        // Propagate bindings from src => dst
        {

            final Iterator<Map.Entry<IVariable, IConstant>> itr = src
                    .iterator();

            while (itr.hasNext()) {

                final Map.Entry<IVariable, IConstant> e = itr.next();

                final IVariable<?> var = (IVariable<?>) e.getKey();

                final IConstant<?> val = e.getValue();

                if (val != null) {

                    final IConstant<?> oval = dst.get(var);

                    if (oval != null) {

                        if (!val.equals(oval)) {

                            // Bindings are not consistent.
                            return null;

                        } // else already bound to the same value.

                    } else {

                        dst.set(var, val);

                    }

                }

            }

        }

        // Test constraint(s)
        if (constraints != null && !BOpUtility.isConsistent(constraints, dst)) {

            return null;

        }

        // strip off unnecessary variables.
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

                    // strip out this binding.
                    dst.clear(var);
                    
                }

            }

        }

        // Bindings are consistent. Constraints (if any) were not violated.
        return dst;

    }

    /**
     * Convert an {@link IAccessPath#iterator()} into a stream of
     * {@link IBindingSet}s.
     * 
     * @param src
     *            The iterator draining the {@link IAccessPath}. This will visit
     *            {@link IElement}s.
     * @param pred
     *            The predicate for that {@link IAccessPath}
     * @param vars
     *            The array of distinct variables (no duplicates) to be
     *            extracted from the visited {@link IElement}s.
     * @param stats
     *            Statistics to be updated as elements and chunks are consumed
     *            (optional).
     * 
     * @return The dechunked iterator visiting the solutions. The order of the
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
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static public ICloseableIterator<IBindingSet> solutions(
            final IChunkedIterator<?> src, final IPredicate<?> pred,
            final IVariable<?>[] vars, final BaseJoinStats stats) {

        return new CloseableIteratorWrapper(
                new com.bigdata.striterator.ChunkedStriterator(src).addFilter(
                        new ChunkedFilter() {

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

                        final IBindingSet bset = new ListBindingSet();

                        // propagate bindings from the element to the bset.
                        copyValues(e, pred, bset);

                        return bset;

                    }

                })) {

            /**
             * Close the real source if the caller closes the returned iterator.
             */
            @Override
            public void close() {
                super.close();
                src.close();
            }
        };

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
