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

import org.apache.log4j.Logger;

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.service.IBigdataFederation;
import com.ibm.icu.impl.ByteBuffer;

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

    private final IAsynchronousIterator<E[]> source;

    private final IBlockingBuffer<E[]> sink;

    private final IBlockingBuffer<E[]> sink2;

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
     * 
     * @todo Since joins now run from locally materialized data in all cases the
     *       API could be simplified somewhat given that we know that there will
     *       be a single "source" chunk of binding sets. Also, the reason for
     *       the {@link IAsynchronousIterator} here is that a downstream join
     *       could error (or satisfy a slice) and halt the upstream joins. That
     *       is being coordinated through the {@link RunningQuery} now.
     *       <p>
     *       It is not yet clear what the right API is for the source. The
     *       iterator model might be just fine, but might not need to be
     *       asynchronous and does not need to be closeable.
     *       <p>
     *       Perhaps the right thing is to expose an object with a richer API
     *       for obtaining various kinds of iterators or even access to the
     *       direct {@link ByteBuffer}s backing the data (for high volume joins,
     *       external merge sorts, etc).
     */
    public final IAsynchronousIterator<E[]> getSource() {
        return source;
    }

    /**
     * Where to write the output of the operator.
     */
    public final IBlockingBuffer<E[]> getSink() {
        return sink;
    }

    /**
     * Optional alternative sink for the output of the operator. This is used by
     * things like SPARQL optional joins to route failed joins outside of the
     * join group.
     */
    public final IBlockingBuffer<E[]> getSink2() {
        return sink2;
    }

    /**
     * 
     * @param fed
     *            The {@link IBigdataFederation} IFF the operator is being
     *            evaluated on an {@link IBigdataFederation}. When evaluating
     *            operations against an {@link IBigdataFederation}, this
     *            reference provides access to the scale-out view of the indices
     *            and to other bigdata services.
     * @param indexManager
     *            The <strong>local</strong> {@link IIndexManager}. Query
     *            evaluation occurs against the local indices. In scale-out,
     *            query evaluation proceeds shard wise and this
     *            {@link IIndexManager} MUST be able to read on the
     *            {@link ILocalBTreeView}.
     * @param readTimestamp
     *            The timestamp or transaction identifier against which the
     *            query is reading.
     * @param writeTimestamp
     *            The timestamp or transaction identifier against which the
     *            query is writing.
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
//    * @throws IllegalArgumentException
//    *             if the <i>indexManager</i> is <code>null</code>
//    * @throws IllegalArgumentException
//    *             if the <i>indexManager</i> is is not a <em>local</em> index
//    *             manager.
//    * @throws IllegalArgumentException
//    *             if the <i>readTimestamp</i> is {@link ITx#UNISOLATED}
//    *             (queries may not read on the unisolated indices).
//    * @throws IllegalArgumentException
//    *             if the <i>writeTimestamp</i> is neither
//    *             {@link ITx#UNISOLATED} nor a read-write transaction
//    *             identifier.
    public BOpContext(final IRunningQuery runningQuery,final int partitionId,
            final BOpStats stats, final IAsynchronousIterator<E[]> source,
            final IBlockingBuffer<E[]> sink, final IBlockingBuffer<E[]> sink2) {
        
        super(null);
        
        this.runningQuery = runningQuery;
//        if (indexManager == null)
//            throw new IllegalArgumentException();
//        if (indexManager instanceof IBigdataFederation<?>) {
//            /*
//             * This is disallowed because predicates always read on local index
//             * objects, even in scale-out.
//             */
//            throw new IllegalArgumentException(
//                    "Expecting a local index manager, not: "
//                            + indexManager.getClass().toString());
//        }
//        if (readTimestamp == ITx.UNISOLATED)
//            throw new IllegalArgumentException();
//        if (TimestampUtility.isReadOnly(writeTimestamp))
//            throw new IllegalArgumentException();
        if (stats == null)
            throw new IllegalArgumentException();
        if (source == null)
            throw new IllegalArgumentException();
        if (sink == null)
            throw new IllegalArgumentException();
//        this.fed = fed; // may be null
//        this.indexManager = indexManager;
//        this.readTimestamp = readTimestamp;
//        this.writeTimestamp = writeTimestamp;
        this.partitionId = partitionId;
        this.stats = stats;
        this.source = source;
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
    final public boolean bind(final IPredicate<?> pred,
            final IConstraint[] constraints, final Object e,
            final IBindingSet bindings) {

        // propagate bindings from the visited object into the binding set.
        copyValues((IElement) e, pred, bindings);

        if (constraints != null) {

            // verify constraint.
            return isConsistent(constraints, bindings);
        
        }
        
        // no constraint.
        return true;
        
    }

    /**
     * Copy the values for variables in the predicate from the element, applying
     * them to the caller's {@link IBindingSet}.
     * 
     * @param e
     *            The element.
     * @param pred
     *            The predicate.
     * @param bindingSet
     *            The binding set, which is modified as a side-effect.
     */
    @SuppressWarnings("unchecked")
    final private void copyValues(final IElement e, final IPredicate<?> pred,
            final IBindingSet bindingSet) {

        for (int i = 0; i < pred.arity(); i++) {

            final IVariableOrConstant<?> t = pred.get(i);

            if (t.isVar()) {

                final IVariable<?> var = (IVariable<?>) t;

                final Constant<?> newval = new Constant(e.get(i));

                bindingSet.set(var, newval);

            }

        }

    }

    /**
     * Copy the bound values from the element into a binding set using the
     * caller's variable names.
     * 
     * @param vars
     *            The ordered list of variables.
     * @param e
     *            The element.
     * @param bindingSet
     *            The binding set, which is modified as a side-effect.
     */
    final public void bind(final IVariable<?>[] vars, final IElement e,
            final IBindingSet bindingSet) {

        for (int i = 0; i < vars.length; i++) {

            final IVariable<?> var = vars[i];

            @SuppressWarnings("unchecked")
            final Constant<?> newval = new Constant(e.get(i));

            bindingSet.set(var, newval);

        }

    }

    /**
     * Check constraints.
     * 
     * @param constraints
     * @param bindingSet
     * 
     * @return <code>true</code> iff the constraints are satisfied.
     */
    public boolean isConsistent(final IConstraint[] constraints,
            final IBindingSet bindingSet) {

        for (int i = 0; i < constraints.length; i++) {

            final IConstraint constraint = constraints[i];

            if (!constraint.accept(bindingSet)) {

                if (log.isDebugEnabled()) {

                    log.debug("Rejected by "
                            + constraint.getClass().getSimpleName() + " : "
                            + bindingSet);

                }

                return false;

            }

            if (log.isTraceEnabled()) {

                log.debug("Accepted by "
                        + constraint.getClass().getSimpleName() + " : "
                        + bindingSet);

            }

        }

        return true;

    }

//    /**
//     * Cancel the running query (normal termination).
//     * <p>
//     * Note: This method provides a means for an operator to indicate that the
//     * query should halt immediately. It used used by {@link SliceOp}, which
//     * needs to terminate the entire query once the slice has been satisfied.
//     * (If {@link SliceOp} just jumped out of its own evaluation loop then the
//     * query would not produce more results, but it would continue to run and
//     * the over produced results would just be thrown away.)
//     * <p>
//     * Note: When an individual {@link BOp} evaluation throws an exception, the
//     * {@link QueryEngine} will catch that exception and halt query evaluation
//     * with that thrown cause.
//     * 
//     * @see IRunningQuery#halt()
//     */
//    public void halt() {
//
//        runningQuery.halt();
//        
//    }
    
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
