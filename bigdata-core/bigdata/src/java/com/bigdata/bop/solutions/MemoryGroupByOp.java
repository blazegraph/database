/**

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
package com.bigdata.bop.solutions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.util.InnerCause;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * An in-memory at-once generalized aggregation operator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
public class MemoryGroupByOp extends GroupByOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

	private static final transient Logger log = Logger
			.getLogger(MemoryGroupByOp.class);
    
    public interface Annotations extends GroupByOp.Annotations,
            HashMapAnnotations {

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
    
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public MemoryGroupByOp(final MemoryGroupByOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public MemoryGroupByOp(final BOp[] args,
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

        assertAtOnceJavaHeapOp();

        getRequiredProperty(Annotations.GROUP_BY_STATE);
        
        getRequiredProperty(Annotations.GROUP_BY_REWRITE);
        
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
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new GroupByTask(this, context));
        
    }

    /**
     * Wrapper used for the solution groups in the {@link ConcurrentHashMap}.
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
         * @param bset
         *            The binding set.
         * 
         *         -OR- throws a {@link SparqlTypeErrorException}.
         */
        static SolutionGroup newInstance(final IValueExpression<?>[] groupBy,
                final IBindingSet bset, final BOpStats stats) {

            final IConstant<?>[] r = new IConstant<?>[groupBy.length];

            for (int i = 0; i < groupBy.length; i++) {

                final IValueExpression<?> expr = groupBy[i];
                                
                Object exprValue;
                
                try {
                    /*
                     * Note: This has a side-effect on the solution, which means
                     * that it needs to be mutable and we have to store the
                     * modified solution. However, it might be nicer to NOT have
                     * a side effect on the incoming solution. That means that
                     * it can continue to be buffered in a read-only encoding on
                     * the native heap and all we need to do here is associate
                     * it with the appropriate group. We can easily re-compute
                     * the GROUP_BY value expressions when we actually evaluate
                     * the aggregates over the solutions in a solution group. At
                     * that point, the solution is once again materialized in
                     * memory on the JVM. [These concerns are only relevant when
                     * developing a generalized aggregation operator backed by
                     * the HTree.]
                     */
                    exprValue = expr.get(bset);
                } catch (SparqlTypeErrorException ex) {
                    exprValue = null;
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
     * A multiset of solutions associated with a {@link SolutionGroup}.
     */
    private static class SolutionMultiSet {

        private List<IBindingSet> solutions = new LinkedList<IBindingSet>();

        public void add(final IBindingSet bset) {

            if(bset == null)
                throw new IllegalArgumentException();
            
            solutions.add(bset);
            
        }

    }
    
    /**
     * Task executing on the node.
     */
    static private class GroupByTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * A map whose keys are the computed bindings on the GROUP_BY
         * expressions and whose values are the solution multisets which fall
         * into a given group.
         */
        private final LinkedHashMap<SolutionGroup, SolutionMultiSet> map;

        private final IGroupByState groupByState;

        private final IGroupByRewriteState rewrite;
        
        private final IValueExpression<?>[] groupBy;

        private final BOpStats stats;

        GroupByTask(final MemoryGroupByOp op,
                final BOpContext<IBindingSet> context) {
        	
            this.context = context;

            this.stats = context.getStats();
            
            this.groupByState = (IGroupByState) op
                    .getRequiredProperty(Annotations.GROUP_BY_STATE);

            this.rewrite = (IGroupByRewriteState) op
                    .getRequiredProperty(Annotations.GROUP_BY_REWRITE);
            
            this.groupBy = groupByState.getGroupByClause();

            // The map is only defined if a GROUP_BY clause was used.
            this.map = groupBy == null ? null
                    : new LinkedHashMap<SolutionGroup, SolutionMultiSet>(
                            op.getInitialCapacity(), op.getLoadFactor());

        }

        /**
         * Add the solution to the multiset for the appropriate group. If we can
         * not compute the GROUP_BY value expressions for a solution, then the
         * solution is dropped.
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
            

            SolutionMultiSet m = map.get(s);

            if (m == null) {

                map.put(s, m = new SolutionMultiSet());

            }

            // Accept the solution.
            if (log.isTraceEnabled())
                log.trace("Accepting solution: " + bset);

            m.add(bset);

        }

        @Override
        public Void call() throws Exception {

			final ICloseableIterator<IBindingSet[]> itr = context
					.getSource();

			final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

			try {

                final List<IBindingSet> accepted = new LinkedList<IBindingSet>();

                int naccepted = 0;

                if (groupBy == null) {

                    /*
                     * Combine all solutions into a single multiset.
                     */
                    final SolutionMultiSet m = new SolutionMultiSet();
                    
                    while (itr.hasNext()) {

                        final IBindingSet[] a = itr.next();

                        stats.chunksIn.increment();
                        stats.unitsIn.add(a.length);

                        for (IBindingSet bset : a) {

                            m.add(bset);

                        }                    
                        
                    }
                    
                    // Compute the aggregate for that group.
                    final IBindingSet bset = aggregate(m.solutions);

                    if (bset != null) {

                        if (log.isDebugEnabled())
                            log.debug("output: solution=" + bset);

                        accepted.add(bset);

                        naccepted++;

                    } else {

                        if (log.isDebugEnabled())
                            log.debug("output : no solution.");
                        
                    }

                } else {

                    /*
                     * Group the solutions.
                     */
                    
                    while (itr.hasNext()) {

                        final IBindingSet[] a = itr.next();

                        stats.chunksIn.increment();
                        stats.unitsIn.add(a.length);

                        for (IBindingSet bset : a) {

                            accept(bset);

                        }

                    }

                    for (Map.Entry<SolutionGroup, SolutionMultiSet> e : map
                            .entrySet()) {

                        final SolutionMultiSet m = e.getValue();

                        // Compute the aggregate for that group.
                        final IBindingSet bset = aggregate(m.solutions);

                        if (bset != null) {
                            
                            if (log.isDebugEnabled())
                                log.debug("output: groupBy=" + e.getKey()
                                        + ", solution=" + bset);

                            accepted.add(bset);

                            naccepted++;
                            
                        } else {
                            
                            if (log.isDebugEnabled())
                                log.debug("output: groupBy=" + e.getKey()
                                        + " : dropped.");
                            
                        }

                    }

                    // discard the map.
                    map.clear();

                }

                /*
                 * Output the aggregated bindings for the accepted solutions.
                 */
                if (naccepted > 0) {

                    final IBindingSet[] b = accepted
                            .toArray(new IBindingSet[naccepted]);

                    sink.add(b);

                    // flush the output.
                    sink.flush();

                }

                // done.
                return null;

            } finally {

                sink.close();

            }

        } // call()

        /**
         * Compute the aggregate solution for a solution multiset (aka a group).
         * 
         * @return The aggregate solution -or- <code>null</code> if the solution
         *         for the group was dropped (type error or violated HAVING
         *         constraint).
         */
        private IBindingSet aggregate(final Iterable<IBindingSet> solutions) {
            /**
             * The intermediate solution with all bindings produced when
             * evaluating this solution group. Evaluation begins by binding any
             * bare variables or BINDs in the GROUP_BY clause, followed by
             * evaluating all aggregates, and then finally evaluating the
             * (rewritten) SELECT expressions. The rewritten HAVING clause (if
             * any) may then be then be trivially evaluated. If the solution is
             * not dropped, then only the SELECTed variables are projected out.
             */
            final IBindingSet aggregates = new ContextBindingSet(context,new ListBindingSet());

            /**
             * Propagate GROUP_BY expression onto [aggregates]. 
             */
            if (groupBy != null) {

                // The first solution in the group (must exist since the
                // group was observed).
                final IBindingSet aSolution = solutions.iterator().next();

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

            } // if(groupBy != null)

            /**
             * Compute the aggregates.
             * 
             * TODO This can be further optimized by computing the column
             * projections of the different value expressions exactly once and
             * then applying the aggregation functions to those column
             * projections. As long as we adhere to the dependency ordering
             * among those aggregates, we can compute them all in a single pass
             * over the column projections.
             * 
             * TODO DISTINCT projections of columns projections can be modeled
             * in a bunch of different ways, but if we need the original column
             * projection as well as the DISTINCT of that column projection then
             * it makes sense to either form the DISTINCT projection while
             * building the column projection or as an after action.
             */
            {

                final boolean nestedAggregates = groupByState.isNestedAggregates();
                
                final Iterator<Map.Entry<IAggregate<?>, IVariable<?>>> itr = rewrite
                        .getAggExpr().entrySet().iterator();

                while (itr.hasNext()) {
                
                    final Map.Entry<IAggregate<?>, IVariable<?>> e = itr.next();
                    
                    // Aggregate.
                    doAggregate(e.getKey(), e.getValue(), nestedAggregates,
                            aggregates, solutions, stats);
                    
                }
                
                if (log.isTraceEnabled())
                    log.trace("aggregates: " + aggregates);
                
            }

            // Evaluate SELECT expressions.
            for (IValueExpression<?> expr : rewrite.getSelect2()) {

                try {
                    expr.get(aggregates);
                } catch (SparqlTypeErrorException ex) {
                    TypeErrorLog.handleTypeError(ex, expr, stats);
                    continue;
                } catch (IllegalArgumentException ex) {
                    /*
                     * Note: This is a hack turning an IllegalArgumentException
                     * which we presume is coming out of new Constant(null) into
                     * an (implicit) SPARQL type error so we can drop the
                     * binding for this SELECT expression. (Note that we are not
                     * trying to drop the entire group!)
                     */
                    TypeErrorLog.handleTypeError(ex, expr, stats);
                    continue;
                }

            }

            /*
             * Verify optional constraint(s).
             * 
             * TODO This could be done before fully computing the aggregates as
             * we only need to have on hand those computed aggregates on which
             * the HAVING clause depends.
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
                    return null;

                }
            }

            // project out only selected variables  that
            // are not assigned error values:
            // "solutions containing error values are 
            // removed at projection time"
            // https://www.w3.org/TR/sparql11-query/#defn_algGroup
            
            final IBindingSet out;
            
            if (groupBy == null) { // implicit group

                assert !aggregates.containsErrorValues();
                out = aggregates.copy(groupByState
                        .getSelectVars().toArray(new IVariable[0]));

            } else { // explicit group

                out = aggregates.copyMinusErrors(groupByState
                        .getSelectVars().toArray(new IVariable[0]));
            }
            
            return out;

        }

    } // GroupByTask

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
     *            are relied on by other aggregates. In this case, this method
     *            must ensure that those bindings become visible.
     * @param aggregates
     *            The binding set on which the results are being bound (by the
     *            caller).
     * @param solutions
     *            The input solutions for a solution group across which we will
     *            compute the aggregate.
     * @param stats
     *            Used to report type errors.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void doAggregate(//
            final IAggregate<?> expr,//
            final IVariable<?> var,//
            final boolean selectDependency,//
            final IBindingSet aggregates,//
            final Iterable<IBindingSet> solutions,//
            final BOpStats stats//
            ) {
        
        try {

            IConstant<?> c=null;

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

                final Object result = expr.done();
                if (result != null) {
                    c = new Constant(result);
                }
                
            } else if (expr.isDistinct()) {
                
                /*
                 * Apply aggregate function only to the distinct values which
                 * it's inner value expression takes on.
                 */
                
                // Set used to impose "DISTINCT" on value expression results.
                final Set<Solution> set = new LinkedHashSet<Solution>();
                
                expr.reset();
                
                for (IBindingSet bset : solutions) {
                    
                    final Object constants[] = new Object[expr.arity()];

                    for (int i=0;i<expr.arity();i++){

                        constants[i] = ((IValueExpression)expr.get(i)).get(bset);

                    }
                
                    final Solution sol=new Solution(constants);
                    
                    if (set.add(sol)) {
                        
                        if (selectDependency)
                            propagateAggregateBindings(aggregates, bset);

                        // aggregate iff this is a new result.
                        expr.get(bset);
                        
                    }

                }
                
                final Object result = expr.done();
                if (result != null) {
                    c = new Constant(result);
                }
            
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
                final Object result = expr.done();
                if (result != null) {
                    c = new Constant(result);
                }

            }

            if (c != null) {
                
                // bind the result.
                aggregates.set(var,c);
                
            }
            
        } catch (Throwable t) {

            if (InnerCause.isInnerCause(t, SparqlTypeErrorException.class)) {

                // trap the type error and filter out the binding
                TypeErrorLog.handleTypeError(t, expr, stats);
//                if (log.isInfoEnabled())
//                    log.info("will not bind aggregate: expr=" + expr + " : " + t);

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
    private static void propagateAggregateBindings(
            final IBindingSet aggregates, final IBindingSet bset) {

        @SuppressWarnings("rawtypes")
        final Iterator<Map.Entry<IVariable, IConstant>> itr = aggregates
                .iterator();

        while (itr.hasNext()) {

            @SuppressWarnings("rawtypes")
            final Map.Entry<IVariable, IConstant> e = itr.next();

            bset.set(e.getKey(), e.getValue());
            
        }

    }
    /**
     * Wrapper used for as bound solutions in the {@link HTree}.
     * <p>
     * Note: A similar class appears in different operators which use the
     * {@link HTree}. However, these classes differ in what bindings are
     * conceptually part of the key in the {@link HTree}, in how they compute
     * the hash code under which the solution will be indexed, and in how they
     * compare the solutions for equality.
     * <p>
     * This implementation relies on an ordered {@link IVariable}[] which
     * defines the bindings that are part of the key and permits a simpler
     * {@link #equals(Object)}s method that would be used if an entire
     * {@link IBindingSet} was being tested for equality (binding sets do not
     * consider order of the bindings when testing for equality).
     */
    private static class Solution implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int hash;

        private final Object[] vals;

        /**
         * Solution whose hash code is the hash code of the {@link IConstant}[].
         *
         * @param vals
         *            The values.
         */
        public Solution(final Object[] vals) {

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
            if (!(o instanceof Solution)) {
                return false;
            }
            final Solution t = (Solution) o;
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

    } // class Solution
}
