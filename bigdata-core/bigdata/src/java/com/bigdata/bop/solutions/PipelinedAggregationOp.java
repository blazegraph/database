package com.bigdata.bop.solutions;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.ContextBindingSet;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.ISingleThreadedOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.util.InnerCause;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A pipelined aggregation operator based on an in memory hash table associating
 * with per-group state for each aggregate expression (it can also handle the
 * degenerate case where all solutions form a single implicit group). This
 * operator is highly efficient, but may only be used if (a) DISTINCT is NOT
 * specified for any aggregate and (b) aggregates do not embed other aggregates.
 * <p>
 * Note: This implementation is a pipelined operator which inspects each chunk
 * of solutions as they arrive. The state is shared across invocations of the
 * operator for each source chunk. The operator waits until the last chunk has
 * been consumed before writing the output solutions. In order to observe the
 * lastInvocation signal, the operator MUST be single threaded (
 * {@link PipelineOp.Annotations#MAX_PARALLEL}:=1) and running on the query
 * controller.
 * <p>
 * Note: Since this operator evaluates {@link IAggregate}s incrementally (one
 * input solution at a time), it relies on {@link IAggregate}'s contract for
 * "sticky" errors. See {@link IAggregate#get(IBindingSet)} and
 * {@link IAggregate#done()}.
 * <p>
 * Note: This this operator will be invoked multiple times, and potentially on
 * multiple nodes in a cluster, it is critical that the anonymous variables
 * assigned by the {@link GroupByRewriter} are stable across all invocations on
 * any node of the cluster (this caution also applies for a single node where
 * the operator can still be invoked multiple times).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class PipelinedAggregationOp extends GroupByOp implements
        ISingleThreadedOp {

	private final static transient Logger log = Logger
			.getLogger(PipelinedAggregationOp.class);
	
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations,
            HashMapAnnotations, GroupByOp.Annotations {

    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns <code>true</code>. This is a pipelined aggregation operator and
     * MAY NOT be used to evaluate aggregation requests which use DISTINCT or
     * which nest {@link IAggregate}s in other {@link IAggregate}s.
     */
    @Override
    public boolean isPipelinedAggregationOp() {

        return true;
        
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public PipelinedAggregationOp(final PipelinedAggregationOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public PipelinedAggregationOp(final BOp[] args,
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

        getRequiredProperty(Annotations.GROUP_BY_STATE);

        getRequiredProperty(Annotations.GROUP_BY_REWRITE);
        
		if (!isSharedState()) {
            /*
             * Note: shared state is used to share the hash table across
             * invocations.
             */
			throw new UnsupportedOperationException(Annotations.SHARED_STATE
					+ "=" + isSharedState());
		}
		
		if (!isLastPassRequested()) {
            /*
             * Note: A final evaluation pass is required to write out the
             * aggregates.
             */
            throw new UnsupportedOperationException(Annotations.LAST_PASS
                    + "=" + isLastPassRequested());
        }
        
        /*
         * Note: The operator MUST be single threaded in order to receive the
         * isLastInvocation notice.
         */
        assertMaxParallelOne();

    }

    /**
     * @see Annotations#INITIAL_CAPACITY
     */
    public int getInitialCapacity() {

        return getProperty(Annotations.INITIAL_CAPACITY,
                Annotations.DEFAULT_INITIAL_CAPACITY);

    }

    /**
     * @see Annotations#LOAD_FACTOR
     */
    public float getLoadFactor() {

        return getProperty(Annotations.LOAD_FACTOR,
                Annotations.DEFAULT_LOAD_FACTOR);

    }
    
    @Override
    public BOpStats newStats() {
    	
    	return new AggregateStats(this);
    	
    }

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(this, context));
        
    }

    /**
     * Wrapper used for the solution groups.
     */
    private static class SolutionGroup {

        /** The hash code for {@link #vals}. */
        private final int hash;

        /**
         * The computed values for the groupBy value expressions in the order in
         * which they were declared.
         */
        private final IConstant<?>[] vals;

        @Override
        public String toString() {
            return super.toString() + //
                    "{group=" + Arrays.toString(vals) + //
                    "}";
        }

        /**
         * Return a new {@link SolutionGroup} given the value expressions and
         * the binding set.
         * 
         * @param groupBy
         *            The value expressions to be computed.
         *            The binding set.
         * 
//         * @return The new {@link SolutionGroup} -or- <code>null</code> if any
//         *         of the value expressions evaluates to a <code>null</code>
//         *         -OR- throws a {@link SparqlTypeErrorException}.
         * @return The new {@link SolutionGroup} (non-null)
         */
        static SolutionGroup newInstance(final IValueExpression<?>[] groupBy,
                final IBindingSet bset, final BOpStats stats) {

            final IConstant<?>[] r = new IConstant<?>[groupBy.length];

            for (int i = 0; i < groupBy.length; i++) {

                final IValueExpression<?> expr = groupBy[i];
                
                Object exprValue;
                
                try {
                    /*
                     * Note: This has a side-effect on the solution and causes
                     * the evaluated GROUP_BY value expressions to become bound
                     * on the solution. This is necessary in order for us to
                     * compute the aggregates incrementally.
                     */
                    exprValue = expr.get(bset);
                } catch (SparqlTypeErrorException ex) {
                    exprValue = null; 
                    // Corresponds to the error value in the SPARQL 1.1 spec 
                }
                
                @SuppressWarnings({ "rawtypes", "unchecked" })
                final IConstant<?> x = 
                        (exprValue == null)?
                        Constant.errorValue()
                        :
                        new Constant(exprValue);
                
                r[i] = x;

            }

            return new SolutionGroup(r);
            
        }

        private SolutionGroup(final IConstant<?>[] vals) {
            this.vals = vals;
            this.hash = java.util.Arrays.hashCode(vals);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof SolutionGroup)) {
                return false;
            }
            final SolutionGroup t = (SolutionGroup) o;
            if (vals.length != t.vals.length)
                return false;
            for (int i = 0; i < vals.length; i++) {
                if (vals[i] == t.vals[i])
                    continue;
                if (vals[i] == null)
                    return false;
                if (!vals[i].equals(t.vals[i]))
                    return false;
            }
            return true;
        }

    } // SolutionGroup
    
    /**
     * State associated with each {@link SolutionGroup} (this is not used if all
     * solutions belong to a single implicit group).
     */
    private static class SolutionGroupState {
        
        /**
         * The aggregate expressions to be evaluated. The {@link IAggregate}s
         * MUST have been cloned to avoid side-effect across groups.
         */
        private final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr;

        /**
         * The intermediate solution with all bindings produced when evaluating
         * this solution group.  Any bare variables and any variables declared
         * by the GROUP_BY clause are projected onto {@link #aggregates} by 
         * the constructor.
         */
        private final IBindingSet aggregates;

        /**
         * 
         * @param groupBy
         *            The (rewritten) GROUP_BY clause.
         * @param aggExpr
         *            The aggregates to be computed for each group. The
         *            {@link IAggregate}s will be *cloned* in order to avoid
         *            side-effects across groups.
         * @param bset
         *            The first input solution encountered for the group (the
         *            one which led to the group becoming defined).
         */
        SolutionGroupState(final BOpContext context, final IValueExpression<?>[] groupBy,
                final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr,
                final IBindingSet bset) {

            this.aggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();

            for (Map.Entry<IAggregate<?>, IVariable<?>> e : aggExpr.entrySet()) {
               
                // Note: IAggregates MUST be cloned to avoid side-effects.
                this.aggExpr.put((IAggregate<?>) e.getKey().clone(),
                        e.getValue());

            }

            /**
             * Propagate GROUP_BY expression onto [aggregates].
             */

            this.aggregates = new ContextBindingSet(context, new ListBindingSet());

            final IBindingSet aSolution = bset;
            
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
                  
                    final Object varValue = var.get(aSolution);
                    final Constant<?> val;
                    
                    if (varValue == null) {
                    
                      val = Constant.errorValue();
                        
                    } else {
                      val = new Constant(varValue.getClass().cast(varValue));
                    };
                    
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
                    
                    final Constant<?> val;
                    final Object exprValue = bindExpr.get(aSolution);
                    
                    if (exprValue == null) {
                    
                      val = Constant.errorValue();
                        
                    } else {
                    
                    val = new Constant(exprValue.getClass().cast(exprValue));
                    }
                    
                    // Variable to be projected out by SELECT.
                    final IVariable<?> ovar = ((IBind<?>) expr).getVar();

                    // Bind on [aggregates].
                    aggregates.set(ovar, val);

                }

            } // next GROUP_BY value expression

        }
        
    } // class SolutionGroupState

    /**
     * Extends {@link BOpStats} to provide the shared state for the aggregation
     * operator across invocations for different source chunks.
     * <p>
     * Note: mutable fields on instances of this class are guarded by the
     * monitor for the instance.
     */
    private static class AggregateStats extends BOpStats {

        /**
		 * 
		 */
        private static final long serialVersionUID = 1L;

        /**
         * <code>true</code> until we initialize the shared start during the
         * first invocation of the {@link ChunkTask}.
         */
        private boolean first = true;

        private final IGroupByState groupByState;

        private final IGroupByRewriteState rewrite;

        public AggregateStats(final PipelinedAggregationOp op) {
            
            this.groupByState = (IGroupByState) op
                    .getRequiredProperty(Annotations.GROUP_BY_STATE);

            this.rewrite = (IGroupByRewriteState) op
                    .getRequiredProperty(Annotations.GROUP_BY_REWRITE);

            if (groupByState.isAnyDistinct()) {
                // Pipelined aggregation does not support DISTINCT.
                throw new UnsupportedOperationException(
                        "DISTINCT not allowed with pipelined aggregation.");
            }

            if (groupByState.isNestedAggregates()) {
                /*
                 * Pipelined aggregation does not support aggregates which embed
                 * other aggregates.
                 */
                throw new UnsupportedOperationException(
                        "Nested aggregates not allowed with pipelined aggregation.");
            }

        }

    }

    /**
     * Shared execution state for the {@link PipelinedAggregationOp}.
     */
    private static class SharedState {

        /**
         * A map whose keys are the bindings on the specified variables and
         * whose values are the per-group state.
         * <p>
         * Note: The map is shared state and can not be discarded or cleared
         * until the last invocation!!!
         * <p>
         * Note: This is only iff an explicit GROUP_BY clause is used.
         */
        private final LinkedHashMap<SolutionGroup, SolutionGroupState> map;

        /**
         * The aggregates to be computed (they have internal state).
         * <p>
         * Note: The map is shared state and can not be discarded or cleared
         * until the last invocation!!!
         * <p>
         * Note: This is bound iff all solutions will be collected within a
         * single implicit group.
         */
        private final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr;

        SharedState(final PipelinedAggregationOp op, final AggregateStats stats) {

            if (stats.groupByState.getGroupByClause() == null) {
            
                map = null;
                
                aggExpr = stats.rewrite.getAggExpr();
                
            } else {

                /*
                 * The map is only defined if a GROUP_BY clause was used.
                 */
                
                map = new LinkedHashMap<SolutionGroup, SolutionGroupState>(
                        op.getInitialCapacity(), op.getLoadFactor());
                
                aggExpr = null;
                
            }

        }
        
    }
    
    /**
     * Task executing on the node.
     */
    static private class ChunkTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * A map whose keys are the bindings on the specified variables and
         * whose values are the per-group state.
         * <p>
         * Note: The map is shared state and can not be discarded or cleared
         * until the last invocation!!!
         * <p>
         * Note: This is only iff an explicit GROUP_BY clause is used.
         */
        private final LinkedHashMap<SolutionGroup, SolutionGroupState> map;

        /**
         * The aggregates to be computed (they have internal state).
         * <p>
         * Note: The map is shared state and can not be discarded or cleared
         * until the last invocation!!!
         * <p>
         * Note: This is bound iff all solutions will be collected within a
         * single implicit group.
         */
        private final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr;
        
        private final IGroupByState groupByState;

        private final IGroupByRewriteState rewrite;
        
        private final IValueExpression<?>[] groupBy;

        private final Object sharedStateKey;
        
        private final BOpStats stats;

        ChunkTask(final PipelinedAggregationOp op,
                final BOpContext<IBindingSet> context) {

            this.context = context;

            this.sharedStateKey = op.getId();
            
            final AggregateStats stats = (AggregateStats) context.getStats();
            this.stats = stats;

            final SharedState sharedState;
            synchronized (stats) {
                if (stats.first) {
                    /*
                     * Setup the shared state.
                     */
                    stats.first = false;
                    sharedState = new SharedState(op, stats);
                    context.getRunningQuery().getAttributes()
                            .put(sharedStateKey, sharedState);
                } else {
                    sharedState = (SharedState) context.getRunningQuery()
                            .getAttributes().get(sharedStateKey);
                }
            } // synchronized(stats)
            /*
             * Initialize from the shared state.
             */
            this.map = sharedState.map;
            this.aggExpr = sharedState.aggExpr;
            this.groupByState = stats.groupByState;
            this.rewrite = stats.rewrite;
            this.groupBy = stats.groupByState.getGroupByClause();
        }

        /**
         * Discard the shared state (this can not be discarded until the last
         * invocation).
         */
        private void release() {

            context.getRunningQuery().getAttributes().remove(sharedStateKey);

        }

        /**
         * Update the state of the {@link IAggregate}s for the appropriate
         * group.
         * 
         * @param bset
         *            The solution.
         */
        private void accept(final IBindingSet bset) {

            if (groupBy == null || groupBy.length == 0)
                throw new IllegalArgumentException();

            if (bset == null)
                throw new IllegalArgumentException();
   
            final SolutionGroup s = SolutionGroup.newInstance(groupBy, bset,
                    stats);
            assert s != null;
            

            SolutionGroupState m = map.get(s);

            if (m == null) {

                map.put(s,
                        m = new SolutionGroupState(context, groupBy, rewrite
                                .getAggExpr(), bset));

            }

            // Accept the solution.
            if (log.isTraceEnabled())
                log.trace("Accepting solution: " + bset);

            // Update the aggregates.
            doAggregate(m.aggExpr, bset, stats);
            
        }

        @Override
        public Void call() throws Exception {

            final ICloseableIterator<IBindingSet[]> itr = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                while (itr.hasNext()) {
                    
                    final IBindingSet[] a = itr.next();

                    stats.chunksIn.increment();
                    stats.unitsIn.add(a.length);

                    for (IBindingSet bset : a) {

                        if (groupBy == null) {

                            /*
                             * A single implicit group.
                             */
                            doAggregate(aggExpr, bset, stats);

                        } else {

                            /*
                             * Explicit GROUP_BY.
                             */
                            accept(bset);

                        }
                        
                    }

                }

                if(context.isLastInvocation()) {

                    // The solutions to be written onto the sink.
                    final List<IBindingSet> outList = new LinkedList<IBindingSet>();
                    
                    if(groupBy == null) {

                        /*
                         * A single implicit group.
                         * 
                         * Output solution for the implicit group IFF the HAVING
                         * constraints are satisfied.
                         */

                        /**
                         * The intermediate solution with all bindings produced
                         * when evaluating this solution group.
                         * 
                         * Note: There is no GROUP_BY so we do not need to
                         * propagate any bindings declared by that clause.
                         * 
                         * This evaluates the (rewritten) SELECT expressions.
                         * The rewritten HAVING clause (if any) is then
                         * evaluated. If the solution is not dropped, then only
                         * the SELECTed variables are projected out.
                         */
                        final IBindingSet aggregates = new ContextBindingSet(context, new ListBindingSet());

                        // Finalize and bind on [aggregates].
                        finalizeAggregates(aggExpr, aggregates, stats);
                        
                        // Evaluate SELECT expressions.
                        for (IValueExpression<?> expr : rewrite.getSelect2()) {

                            try {
                                expr.get(aggregates);
                            } catch (SparqlTypeErrorException ex) {
                                TypeErrorLog.handleTypeError(ex, expr, stats);
                                continue;
                            } catch (IllegalArgumentException ex) {
                                /*
                                 * Note: This a hack turns an
                                 * IllegalArgumentException which we presume is
                                 * coming out of new Constant(null) into an
                                 * (implicit) SPARQL type error so we can drop
                                 * the binding for this SELECT expression. (Note
                                 * that we are not trying to drop the entire
                                 * group!)
                                 */
                                TypeErrorLog.handleTypeError(ex, expr, stats);
                                continue;
                            }

                        }

                        // Verify optional HAVING constraint(s)
                        final boolean drop;
                        final IConstraint[] having2 = rewrite.getHaving2();
                        if (having2 != null
                                && !BOpUtility
                                        .isConsistent(having2, aggregates)) {
                            // drop this solution.
                            drop = true;
                        } else {
                            drop = false;
                        }

                        if (log.isInfoEnabled())
                            log.info((drop ? "drop" : "keep") + " : "
                                    + aggregates);

                        if (!drop) {

                            assert !aggregates.containsErrorValues();
                            // Because this is imlicit grouping.
                            // The invariant implies that we don't have to use 
                            // the more expensive copyMinusErrors() below.
                            
                            // project out only selected variables.
                            final IBindingSet out = aggregates
                                    .copy(groupByState.getSelectVars().toArray(
                                            new IVariable[0]));

                            outList.add(out);
                            
                        }

                    } else {

                        /*
                         * Explicit GROUP_BY.
                         * 
                         * Output solutions for the observed groups which pass
                         * the optional HAVING constraint(s).
                         */
                        for (SolutionGroupState groupState : map.values()) {

                            final IBindingSet aggregates = groupState.aggregates;

                            // Finalize and bind on [aggregates].
                            finalizeAggregates(groupState.aggExpr, aggregates,
                                    stats);

                            // Evaluate SELECT expressions.
                            for (IValueExpression<?> expr : rewrite
                                    .getSelect2()) {

                                try {
                                    expr.get(aggregates);
                                } catch (SparqlTypeErrorException ex) {
                                    TypeErrorLog.handleTypeError(ex, expr, stats);
                                    continue;
                                } catch (IllegalArgumentException ex) {
                                    /*
                                     * Note: This hack turns an
                                     * IllegalArgumentException which we presume
                                     * is coming out of new Constant(null) into
                                     * an (implicit) SPARQL type error so we can
                                     * drop the binding for this SELECT
                                     * expression. (Note that we are not trying
                                     * to drop the entire group!)
                                     */
                                    TypeErrorLog.handleTypeError(ex, expr, stats);
                                    continue;
                                }

                            }

                            // Verify optional HAVING constraint(s)
                            final boolean drop;
                            final IConstraint[] having2 = rewrite.getHaving2();
                            if (having2 != null
                                    && !BOpUtility.isConsistent(having2,
                                            aggregates)) {
                                // drop this solution.
                                drop = true;
                            } else {
                                drop = false;
                            }

                            if (log.isInfoEnabled())
                                log.info((drop ? "drop" : "keep") + " : "
                                        + aggregates);

                            if (!drop) {

                                // project out only selected variables that
                                // are not assigned error values:
                                // "solutions containing error values are 
                                // removed at projection time"
                                // https://www.w3.org/TR/sparql11-query/#defn_algGroup
                                final IBindingSet out = aggregates
                                        .copyMinusErrors(groupByState.getSelectVars()
                                                .toArray(new IVariable[0]));

                                outList.add(out);

                            }

                        }

                    }
                
                    if (!outList.isEmpty()) {

                        // Write the solutions onto the sink.
                        sink.add(outList.toArray(new IBindingSet[0]));

                        sink.flush();

                    }

                    // Discard the shared state.
                    release();

                }
                
                // done.
                return null;
                
            } finally {

                sink.close();

            }

        }

    }

    /**
     * Update the {@link IAggregate}s for the given binding set.
     * <p>
     * Note: The {@link IAggregate} instances MUST be distinct within each group
     * to avoid side-effects across groups.
     * 
     * @param aggExpr
     *            The aggregate expressions to be evaluated.
     * @param bset
     *            The binding set.
     * @param stats
     *            Used to report type errors.
     */
    static private void doAggregate(
            final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr,
            final IBindingSet bset,
            final BOpStats stats) {

        for (IAggregate<?> a : aggExpr.keySet()) {

            try {

                a.get(bset);

            } catch (Throwable t) {

                if (InnerCause.isInnerCause(t, SparqlTypeErrorException.class)) {

                    /*
                     * Trap the type error. The group will be reported, but this
                     * aggregate will not bind a value for that group (the
                     * aggregate will track its error state internally.)
                     */
                    TypeErrorLog.handleTypeError(t, a, stats);
//                    if (log.isInfoEnabled())
//                        log.info("type error: expr=" + a + " : " + t);

                }

            }

        }

    }

    /**
     * Finalize the {@link IAggregate}s for a solution group (or for the
     * implicit group formed from all solutions when no GROUP_BY was given).
     * This invokes {@link IAggregate#done()} on each {@link IAggregate} in turn
     * and binds any non-<code>null</code> results onto <i>aggregates</i>.
     * 
     * @param aggExpr
     *            The aggregate expressions to be evaluated.
     * @param aggregates
     *            The binding set where the aggregates will become bound.
     */
    static private void finalizeAggregates(
            final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr,
            final IBindingSet aggregates,
            final BOpStats stats) {

        for (Map.Entry<IAggregate<?>, IVariable<?>> e : aggExpr.entrySet()) {

            final IAggregate<?> expr = e.getKey();

            final Object val;
            
            try {

                val = expr.done();
                
            } catch (Throwable t) {

                if (InnerCause.isInnerCause(t, SparqlTypeErrorException.class)) {

                    // trap the type error and filter out the solution
                    TypeErrorLog.handleTypeError(t, expr, stats);
//                    if (log.isInfoEnabled())
//                        log.info("aggregate will not bind due type error: expr="
//                                + expr + " : " + t);

                    // No binding.
                    continue;

                } else {
                    
                    throw new RuntimeException(t);
                    
                }
                
            }

            if (val != null) {
                
                // bind the result.
                aggregates.set(e.getValue(), new Constant(val));
                
            }

        }

    }

}
