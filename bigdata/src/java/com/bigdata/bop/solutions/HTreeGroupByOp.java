/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 6, 2011
 */

package com.bigdata.bop.solutions;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IQueryContext;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableFactory;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.htree.HTree;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.util.InnerCause;

/**
 * A general aggregation operator which buffers its data on an {@link HTree} and
 * can scale to very large solution sets.
 * <p>
 * The operator accepts input solutions incrementally and buffers them onto an
 * {@link HTree} (if there is no GROUP BY clause then all solutions will be
 * buffered in a single group). The operator is NOT thread-safe. Each invocation
 * will accept all source solutions and buffer them on the {@link HTree}. Only
 * the last invocation will compute the aggregates for the group(s) and write
 * them on the sink.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HTreeGroupByOp extends GroupByOp implements IVariableFactory {

    private final static transient Logger log = Logger
            .getLogger(HTreeGroupByOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations,
            HTreeAnnotations, GroupByOp.Annotations {
   
    }

    /**
     * Required deep copy constructor.
     */
    public HTreeGroupByOp(final HTreeGroupByOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public HTreeGroupByOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        switch (getEvaluationContext()) {
        case CONTROLLER:
            break;
        default:
            throw new UnsupportedOperationException(
                    Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

        if (getMaxParallel() != 1)
            throw new UnsupportedOperationException(Annotations.MAX_PARALLEL
                    + "=" + getMaxParallel());

        // shared state is used to share the hash table.
        if (!isSharedState()) {
            throw new UnsupportedOperationException(Annotations.SHARED_STATE
                    + "=" + isSharedState());
        }

    }

    /**
     * @see Annotations#ADDRESS_BITS
     */
    public int getAddressBits() {

        return getProperty(Annotations.ADDRESS_BITS,
                Annotations.DEFAULT_ADDRESS_BITS);

    }

    /**
     * @see Annotations#RAW_RECORDS
     */
    public boolean getRawRecords() {

        return getProperty(Annotations.RAW_RECORDS,
                Annotations.DEFAULT_RAW_RECORDS);

    }
    
    /**
     * @see Annotations#MAX_RECLEN
     */
    public int getMaxRecLen() {

        return getProperty(Annotations.MAX_RECLEN,
                Annotations.DEFAULT_MAX_RECLEN);

    }

    /**
     * Return a new anonymous variable (this is overridden by some unit tests in
     * order to have predictable variable names).
     */
    public IVariable<?> var() {

        return Var.var();

    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns <code>false</code>. This is a generalized aggregation operator
     * and may be used to evaluate any aggregation request.
     */
    @Override
    final public boolean isPipelinedAggregationOp() {
        
        return false;
        
    }

    public BOpStats newStats(final IQueryContext queryContext) {
        
        return new HTreeGroupByStats(this,queryContext);
            
    }

    @Override
    public FutureTask<Void> eval(BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(this, context));

    }

    /**
     * Extends {@link BOpStats} to provide the shared state for the distinct
     * solution groups across multiple invocations of the aggregation operator.
     */
    private static class HTreeGroupByStats extends BOpStats {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        /**
         * A map whose keys are the bindings on the specified variables. The
         * values in the map are <code>null</code>s.
         * <p>
         * Note: The map is shared state and can not be discarded or cleared
         * until the last invocation!!!
         */
        private final HTree map;

        public HTreeGroupByStats(final HTreeGroupByOp op,
                final IQueryContext queryContext) {
            
            /*
             * TODO Annotations for key and value raba coders.
             */
            final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

            metadata.setAddressBits(op.getAddressBits());

            metadata.setRawRecords(op.getRawRecords());

            metadata.setMaxRecLen(op.getMaxRecLen());

            metadata.setKeyLen(Bytes.SIZEOF_INT); // int32 hash code keys.
            
            /*
             * TODO This sets up a tuple serializer for a presumed case of 4
             * byte keys (the buffer will be resized if necessary) and
             * explicitly chooses the SimpleRabaCoder as a workaround since the
             * keys IRaba for the HTree does not report true for isKeys(). Once
             * we work through an optimized bucket page design we can revisit
             * this as the FrontCodedRabaCoder should be a good choice, but it
             * currently requires isKeys() to return true.
             */
            final ITupleSerializer<?, ?> tupleSer = new DefaultTupleSerializer(
                    new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
                    // new FrontCodedRabaCoder(),// Note: reports true for
                    // isKeys()!
                    new SimpleRabaCoder(),// keys : TODO Optimize for int32!
                    new SimpleRabaCoder() // vals
            );

            metadata.setTupleSerializer(tupleSer);

            /*
             * This wraps an efficient raw store interface around a child memory
             * manager created from the IMemoryManager which is backing the
             * query.
             */
            final IRawStore store = new MemStore(queryContext
                    .getMemoryManager().createAllocationContext());

            // Will support incremental eviction and persistence.
            this.map = HTree.create(store, metadata);

        }
        
    }
    
    private static class ChunkTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final HTreeGroupByStats stats;

        /**
         * A map whose keys are the computed bindings on the GROUP_BY
         * expressions and whose values are the solution multisets which fall
         * into a given group.
         */
        private final HTree map;

        private final IGroupByState groupByState;

        private final IGroupByRewriteState rewrite;

        private final IValueExpression<?>[] groupBy;

        public ChunkTask(final HTreeGroupByOp op,
                final BOpContext<IBindingSet> context) {

            this.context = context;

            this.groupByState = new GroupByState(
                    //
                    (IValueExpression<?>[]) op.getRequiredProperty(GroupByOp.Annotations.SELECT), //
                    (IValueExpression<?>[]) op
                            .getProperty(GroupByOp.Annotations.GROUP_BY), //
                    (IConstraint[]) op
                            .getProperty(GroupByOp.Annotations.HAVING)//
            );

            this.rewrite = new GroupByRewriter(groupByState) {
                @Override
                public IVariable<?> var() {
                    return op.var();
                }
            };

            this.groupBy = groupByState.getGroupByClause();

            this.stats = (HTreeGroupByStats) context.getStats();
            
            // The map is shared state across invocations of this operator task.
            this.map = ((HTreeGroupByStats) context.getStats()).map;

        }

        public Void call() throws Exception {

            final IAsynchronousIterator<IBindingSet[]> itr = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                acceptSolutions(itr);

                if (context.isLastInvocation()) {

                    doAggregation(sink);

                    sink.flush();
                    
                }

            } finally {

                if (context.isLastInvocation()) {

                    /*
                     * Discard the map.
                     * 
                     * Note: The map can not be discarded (or cleared) until the
                     * last invocation.
                     */

                    final IRawStore store = map.getStore();
                    
                    map.close();
                    
                    store.close();

                }

                sink.close();

            }

            // Done.
            return null;

        }

        /**
         * Accept and group solutions.
         * 
         * @param itr
         *            The source solutions.
         */
        private void acceptSolutions(
                final IAsynchronousIterator<IBindingSet[]> itr) {

            try {

                /*
                 * Group solutions.
                 * 
                 * TODO We should process the solutions in chunks, form the keys
                 * for each group, order the solutions by their keys, and the
                 * batch them into the htree in key order. This will be MUCH
                 * more efficient.
                 */

                while (itr.hasNext()) {

                    final IBindingSet[] a = itr.next();

                    stats.chunksIn.increment();
                    stats.unitsIn.add(a.length);

                    final IBindingSet groupId = new ListBindingSet();

                    for (IBindingSet bset : a) {

                        final int hashCode;

                        if (groupBy == null) {

                            /*
                             * TODO This is projecting the GROUP BY expressions
                             * but it is NOT storing them. There is a tradeoff
                             * here. Life is simpler during aggregation if we
                             * just store the projected expressions by copying
                             * them onto [bset] before inserting [bset] into the
                             * HTree, but we can also just recompute the GROUP
                             * BY expressions during aggregation and then we do
                             * not need to store them.
                             */
                            projectGroupByClause(groupId, bset);
                            
                            hashCode = groupId.hashCode();
                            
                            groupId.clearAll();
                            
                        } else {
                            
                            hashCode = 0;
                            
                        }

                        map.insert(hashCode, SerializerUtil.serialize(bset));

                    }

                }

            } finally {
        
                itr.close();
                
            }
        
        }

        /**
         * 
         * Aggregate across each group, writing the projected aggregates on the
         * sink.
         * 
         * TODO The tree will be immutable so we could do this in parallel for
         * each group, but it is much simpler to use the values() (or tuples())
         * iterator and recognize when we change groups by the change computed
         * group key (which is materialized in the stored solutions).
         * 
         * TODO Unless there is a prodigious number of distinct groups we can
         * capture all output solutions in LinkedList and write them onto the
         * sink in one go.
         * 
         * FIXME This depends on the htree maintaining the keys in key order
         * within each bucket page. Until we do that it can put keys for small
         * groups on the same page and deliver the solutions for those groups in
         * an arbitrary order from that page.
         * 
         * TODO If we a maintained on htree per group, or just a list of linked
         * pages per group, then we would not need to use the htree and we would
         * not have to recompute the projection of the group by clause during
         * aggregation.
         */
        private void doAggregation(final IBlockingBuffer<IBindingSet[]> sink) {

            /*
             * Compute aggregates.
             */
            
            final List<IBindingSet> accepted = new LinkedList<IBindingSet>();

            aggregate(map.rangeIterator(), accepted);

            /*
             * Output the aggregated bindings for the accepted solutions.
             */

            if (!accepted.isEmpty()) {

                final IBindingSet[] b = accepted
                        .toArray(new IBindingSet[accepted.size()]);

                sink.add(b);

                // flush the output.
                sink.flush();

            }

        }
        
        /**
         * Compute the aggregate solution for a solution multiset (aka a group).
         * 
         * @param solutions
         *            An iterator over the solutions. Note that all solutions in
         *            the same group will be visited contiguously by the
         *            iterator since they will all have the same computed
         *            groupId and the same hash code key. Thus we watch for the
         *            change in the computed groupId and start a new group each
         *            time. The solutions in the current group (if any) are
         *            added to the <i>accepted</i> list each time the previous
         *            group ends and when the iterator is exhausted.
         * 
         * @return The aggregate solution -or- <code>null</code> if the solution
         *         for the group was dropped (type error or violated HAVING
         *         constraint).
         */
        public void aggregate(final ITupleIterator<IBindingSet> solutions,
                final List<IBindingSet> accepted) {

            if (!solutions.hasNext()) {
                // Drop empty group.
                return;
            }

            /*
             * FIXME if any aggregate uses DISTINCT then we need to compute the
             * distinct column projection or distinct solution set when we
             * compute that group. We can do this during a single incremental
             * pass over the group UNLESS [selectDependency] is also true.
             * 
             * If [selectDependency] is true, then we must run over the group
             * once per expression (unless we build a better model of the
             * dependencies in the select expressions). We can do this for the
             * HTree with lookupAll(hashCode) and then filtering on the groupId
             * (which is *extremely* likely to be correct for all visited
             * tuples).
             */
            final boolean selectDependency = groupByState.isNestedAggregates();

            IBindingSet aggregates = null;
            while (solutions.hasNext()) {
                final ITuple<IBindingSet> t = solutions.next();
                final IBindingSet aSolution = t.getObject();
                if (aggregates == null) {
                    // First group.
                    aggregates = newGroup(aSolution);
                } else if (groupBy != null) {
                    /*
                     * Check the projection of the GROUP BY clause to decide
                     * whether or not this is a new group. We have to do this
                     * every time since it is possible (widely unlikely, but
                     * possible) for the hash code to remain the same for a
                     * different groupId.
                     * 
                     * FIXME This suggests a model where we allocate and link
                     * "pages" of solutions for each group so we do not need to
                     * recompute the groupId projection.
                     * 
                     * FIXME We are already projecting the GROUP BY expressions
                     * onto the solutions when they are accepted, so we do not
                     * need to be doing that again here, right?
                     */
                    projectGroupByClause(aSolution, aSolution);
                    for (IVariable<?> v : groupByState.getGroupByVars()) {
                        if (!aggregates.get(v).equals(aSolution.get(v))) {
                            /*
                             * Test the constraints on the aggregate solution.
                             * If they are satisfied, then output the the
                             * projection of the aggregates for the old group.
                             */
                            endGroup(aggregates, accepted);
                            // Start a new group.
                            aggregates = newGroup(aSolution);
                            break;
                        }
                    }
                    /**
                     * Compute the aggregates.
                     * 
                     * TODO This can be further optimized by computing the
                     * column projections of the different value expressions
                     * exactly once and then applying the aggregation functions
                     * to those column projections. As long as we adhere to the
                     * dependency ordering among those aggregates, we can
                     * compute them all in a single pass over the column
                     * projections.
                     * 
                     * TODO DISTINCT projections of columns projections can be
                     * modeled in a bunch of different ways, but if we need the
                     * original column projection as well as the DISTINCT of
                     * that column projection then it makes sense to either form
                     * the DISTINCT projection while building the column
                     * projection or as an after action.
                     */
                    {

                        final Iterator<Map.Entry<IAggregate<?>, IVariable<?>>> itr = rewrite
                                .getAggExpr().entrySet().iterator();

                        final IBindingSet bset = aSolution;

                        while (itr.hasNext()) {

                            final Map.Entry<IAggregate<?>, IVariable<?>> e = itr
                                    .next();

                            final IAggregate<?> expr = e.getKey();

                            if (expr.isDistinct()) {
                                
                                /*
                                 * FIXME Must either build up distinct column
                                 * projection or distinct solution set for the
                                 * group here so we can compute the aggregate in
                                 * endGroup().
                                 */

                                throw new UnsupportedOperationException();
                                
                            } else {
                                
                                /*
                                 * Update aggregates which do not include the
                                 * DISTINCT keyword incrementally as we process
                                 * the solutions the group.
                                 */
                                
                                try {

                                    if (selectDependency)
                                        propagateAggregateBindings(aggregates,
                                                bset);

                                    expr.get(bset);

                                } catch (Throwable t2) {

                                    if (InnerCause.isInnerCause(t2,
                                            SparqlTypeErrorException.class)) {

                                        /*
                                         * Trap the type error. The error is
                                         * sticky and the aggregate expression
                                         * for will wind up not binding in
                                         * endGroup.
                                         */

                                        if (log.isInfoEnabled())
                                            log.info("will not bind aggregate: expr="
                                                    + expr + " : " + t2);

                                        return;

                                    }

                                    throw new RuntimeException(t2);

                                }
                            
                            }
                            
                        }

                        if (log.isTraceEnabled())
                            log.trace("aggregates: " + aggregates);

                    }
                } // next solution

                // End of the last group.
                endGroup(aggregates, accepted);

            }

        } // doAggregation()
        
        /**
         * Start a new group.
         * 
         * @param aSolution
         *            The first solution in the group (must exist since the
         *            group was observed).
         * 
         * @return An aggregate solution for that group which has been
         *         initialized with the computed groupBy value expressions.
         */
        private IBindingSet newGroup(final IBindingSet aSolution) {
            
            /**
             * The intermediate solution with all bindings produced when
             * evaluating this solution group. Evaluation begins by binding any
             * bare variables or BINDs in the GROUP_BY clause, followed by
             * evaluating all aggregates, and then finally evaluating the
             * (rewritten) SELECT expressions. The rewritten HAVING clause (if
             * any) may then be then be trivially evaluated. If the solution is
             * not dropped, then only the SELECTed variables are projected out.
             */
            final IBindingSet aggregates = new ListBindingSet();

            projectGroupByClause(aggregates, aSolution);

            /*
             * Reset the aggregate functions for the new group.
             * 
             * FIXME We should also allocate a child memory manager here if we
             * need to impose DISTINCT on a column projection or DISTINCT on the
             * solutions in the group. The child memory manager can be cleared
             * as soon as we are done with the group.
             */
            final Iterator<Map.Entry<IAggregate<?>, IVariable<?>>> itr = rewrite
                    .getAggExpr().entrySet().iterator();

            while (itr.hasNext()) {

                final Map.Entry<IAggregate<?>, IVariable<?>> e = itr.next();

                final IAggregate<?> a = e.getKey();

                a.reset();

            }
            
            return aggregates;

        }

        /**
         * Project the computed value expressions for the GROUP BY clause (if
         * any) onto <i>aggregates</i> for the given <i>aSolution</i>.
         * 
         * @param aggregates
         * @param aSolution
         */
        private void projectGroupByClause(final IBindingSet aggregates,
                final IBindingSet aSolution) {

            /**
             * Propagate GROUP_BY expression onto [aggregates]. 
             */
            if (groupBy != null) {

                for (IValueExpression<?> expr : groupBy) {

                    if (expr instanceof IVariable<?>) {

                        /**
                         * Propagate bare variable used in GROUP_BY clause to
                         * [aggregates].
                         * 
                         * <pre>
                         * GROUP BY ?x
                         * </pre>
                         */

                        final IVariable<?> var = (IVariable<?>) expr;

                        // Note: MUST be a binding for each groupBy var.
                        @SuppressWarnings({ "rawtypes", "unchecked" })
                        final Constant<?> val = new Constant(var.get(aSolution));

                        // Bind on [aggregates].
                        aggregates.set(var, val);

                    } else if (expr instanceof IBind<?>) {

                        /**
                         * Propagate BIND declared by GROUP_BY clause to
                         * [aggregates].
                         * 
                         * <pre>
                         * GROUP BY (2*?y as ?x)
                         * </pre>
                         */

                        final IBind<?> bindExpr = (IBind<?>) expr;

                        // Compute value expression.
                        // Note: MUST be valid since group exists.
                        @SuppressWarnings({ "rawtypes", "unchecked" })
                        final Constant<?> val = new Constant(
                                bindExpr.get(aSolution));

                        // Variable to be projected out by SELECT.
                        final IVariable<?> ovar = ((IBind<?>) expr).getVar();

                        // Bind on [aggregates].
                        aggregates.set(ovar, val);

                    }

                } // next GROUP_BY value expression

            } // if(groupBy != null)

        } // project GROUP_BY clause()
        
        /**
         * Verify optional constraint(s).
         * 
         * @return The projection of the SELECT expression -or-
         *         <code>null</code> if the optional constraints were not
         *         satisfied.
         * 
         *         TODO This could be done before fully computing the aggregates
         *         as we only need to have on hand those computed aggregates on
         *         which the HAVING clause depends.
         */
        private void endGroup(final IBindingSet aggregates,
                final List<IBindingSet> accepted) {

            /*
             * FIXME We need to bind the result of a.done() onto the appropriate
             * variable here as well as type errors arising during the
             * aggregation.
             * 
             * FIXME handle COUNT(DISTINCT foo) and COUNT(DISTINCT *) here (we
             * have to evaluate the aggregate over the appropriate HTree
             * instance having the distinct column projection or distinct
             * solutions in the group. we also need to discard those htree
             * instances (htree.close()), and clear the child memory memory
             * used to scope their allocations to the group.)
             */
            final Iterator<Map.Entry<IAggregate<?>, IVariable<?>>> itr = rewrite
                    .getAggExpr().entrySet().iterator();

            while (itr.hasNext()) {

                final Map.Entry<IAggregate<?>, IVariable<?>> e = itr
                        .next();

                final IAggregate<?> expr = e.getKey();

                try {
                expr.done();
//                FiXME BIND_ON_CONSTANT;
                } catch (Throwable t) {

                    if (InnerCause.isInnerCause(t, SparqlTypeErrorException.class)) {

                        // trap the type error and filter out the binding
                        if (log.isInfoEnabled())
                            log.info("will not bind aggregate: expr=" + expr + " : " + t);

                        return;
                        
                    }

                    throw new RuntimeException(t);

                }

                
            }
            
            // Evaluate SELECT expressions.
            for (IValueExpression<?> expr : rewrite.getSelect2()) {

                /*
                 * Note: This is a hack turning an IllegalArgumentException
                 * which we presume is coming out of new Constant(null) into an
                 * (implicit) SPARQL type error so we can drop the binding for
                 * this SELECT expression. (Note that we are not trying to drop
                 * the entire group!)
                 */
                try {
                    expr.get(aggregates);
                } catch (IllegalArgumentException ex) {
                    if (log.isInfoEnabled())
                        log.info("will not bind solution for aggregate due to error: expr="
                                + expr + ", cause=" + ex);
                    continue;
                }

            }

            /*
             * Test the optional constraints.
             */
            {

                final boolean drop;

                final IConstraint[] having2 = rewrite.getHaving2();

                if (having2 != null
                        && !BOpUtility.isConsistent(having2, aggregates)) {

                    // drop this solution.
                    drop = true;

                } else {

                    drop = false;

                }

                if (log.isInfoEnabled())
                    log.info((drop ? "drop" : "keep") + " : " + aggregates);

                if (drop) {

                    // Drop this solution.
                    return;

                }

            }

            // project out only selected variables.
            final IBindingSet out = aggregates.copy(groupByState
                    .getSelectVars().toArray(new IVariable[0]));

            // accept this solution.
            accepted.add(out);

        } // endGroup()

    } // class ChunkTask

    /**
     * Apply the value expression to each solution in the group.
     * 
     * @param expr
     *            The {@link IAggregate} to be evaluated.
     * @param var
     *            The variable on which computed value of the {@link IAggregate}
     *            will be bound.
     * @param selectDependency
     *            When <code>true</code>, some aggregates bind variables which
     *            are relied on both other aggregates. In this case, this method
     *            must ensure that those bindings become visible.
     * @param aggregates
     *            The binding set on which the results are being bound (by the
     *            caller).
     * @param solutions
     *            The input solutions for a solution group across which we will
     *            compute the aggregate.
     * 
     *            FIXME I am winding up with an inversion of control since the
     *            group change decision is made at the top. This means that we
     *            will need to move the reset()/done() logic out of this method.
     * 
     *            FIXME COUNT(DISTINCT x) and COUNT(DISTINCT *) need to be
     *            handled using an HTree backed by the same memory manager.
     *            Discard any such HTree instances used to support DISTINCT
     *            within an aggregate after each GROUP.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void doAggregate(//
            final IAggregate<?> expr,//
            final IVariable<?> var,//
            final boolean selectDependency,//
            final IBindingSet aggregates,//
            final Iterable<IBindingSet> solutions//
            ) {
        
        try {

            final IConstant<?> c;

            if (expr.isWildcard() && expr.isDistinct()) {
            
                /**
                 * For a wildcard we basically need to operate on solution
                 * multisets. For example, COUNT(*) is the size of the solution
                 * multiset (aka group).
                 * 
                 * Note: It is possible to optimize COUNT(*) and COUNT(DISTINCT
                 * *) as the cardinality of the solution multiset / solution set
                 * respectively. However, we can not undertake this optimization
                 * when COUNT() is parameterized by an {@link IValueExpression},
                 * even a simple {@link IVariable}, since then we need to count
                 * the solutions where the value expression is non-
                 * <code>null</code> and NOT bind the result of the COUNT() for
                 * the group if the evaluation of the value expression results
                 * in an error for any solution in that group.
                 */
                
                // Set used to impose DISTINCT on the solution multiset.
                final LinkedHashSet<IBindingSet> set = new LinkedHashSet<IBindingSet>();

                expr.reset();
                
                for (IBindingSet bset : solutions) {

                    if (set.add(bset)) {
                    
                        if (selectDependency)
                            propagateAggregateBindings(aggregates, bset);
                        
                        // aggregate iff this is a new result.
                        expr.get(bset);
                        
                    }

                }
                
                c = new Constant(expr.done());
                
            } else if (expr.isDistinct()) {
                
                /*
                 * Apply aggregate function only to the distinct values which
                 * it's inner value expression takes on.
                 */
                
                // Set used to impose "DISTINCT" on value expression results.
                final Set<Object> set = new LinkedHashSet<Object>();
                
                // The inner value expression.
                final IValueExpression<?> innerExpr = expr.getExpr();
                
                expr.reset();
                
                for (IBindingSet bset : solutions) {
                
                    final Object val = innerExpr.get(bset);
                    
                    if (set.add(val)) {
                        
                        if (selectDependency)
                            propagateAggregateBindings(aggregates, bset);

                        // aggregate iff this is a new result.
                        expr.get(bset);
                        
                    }

                }
                
                c = new Constant(expr.done());
            
            } else {
                
                /*
                 * Apply aggregate function to all solutions in the multiset.
                 */
                
                expr.reset();
                
                for (IBindingSet bset : solutions) {
                
                    if (selectDependency)
                        propagateAggregateBindings(aggregates, bset);

                    expr.get(bset);
                    
                }

                c = new Constant(expr.done());

            }

            if (c != null) {
                
                // bind the result.
                aggregates.set(var,c);
                
            }
            
        } catch (Throwable t) {

            if (InnerCause.isInnerCause(t, SparqlTypeErrorException.class)) {

                // trap the type error and filter out the binding
                if (log.isInfoEnabled())
                    log.info("will not bind aggregate: expr=" + expr + " : " + t);

                return;
                
            }

            throw new RuntimeException(t);

        }

    }

    /**
     * Propagate the bound values for any aggregates to the incoming solution in
     * order to make those bindings available when there is a dependency among
     * the aggregate expressions.
     * 
     * @param aggregates
     * @param bset
     */
    @SuppressWarnings("rawtypes")
    private static void propagateAggregateBindings(
            final IBindingSet aggregates, final IBindingSet bset) {

        final Iterator<Map.Entry<IVariable, IConstant>> itr = aggregates
                .iterator();

        while (itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> e = itr.next();

            bset.set(e.getKey(), e.getValue());
            
        }

    }

}
