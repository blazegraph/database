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
package com.bigdata.bop.solutions;

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
import com.bigdata.bop.ConcurrentHashMapAnnotations;
import com.bigdata.bop.Constant;
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
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * An in-memory GROUP_BY for binding sets.
 * <p>
 * Note: This implementation is a pipelined operator which aggregates each chunk
 * of solutions as they arrive and outputs empty messages (containing no
 * solutions) until the last chunk is consumed. This operator relies on
 * {@link BOpContext#isLastInvocation()} in order to decide when to write its
 * output solutions, which requires the operator to (a) be evaluated on the
 * controller and (b) declare itself as NOT thread-safe. In addition, the
 * operator must be marked as SHARED_STATE := true such that the hash table
 * associated with the {@link BOpStats} is shared across multiple invocations of
 * this operator for a given query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 * 
 * @todo GROUP_BY implementation which depends on an ORDER_BY operator to setup
 *       the correct order and then performs the aggregations in a single pass
 *       over the ordered data.
 * 
 * @todo In scale-out, we can hash partition the GROUP_BY operator over the
 *       nodes as long as all of the aggregation functions can be combined from
 *       the partitions. If AVG is used, then it needs to be replaced by SUM and
 *       COUNT in the GROUP_BY operator and the use of the AVG in the SELECT
 *       needs to be rewritten as (SUM(v)/COUNT(v)).
 * 
 * @todo As a special twist, there can also be memory burdens, even with a small
 *       #of groups, when the aggregated solution data is very large and a
 *       GROUP_CONCAT function is specified such that it combines a large #of
 *       input solution bindings into a big string.
 * 
 *       FIXME How should we handle nulls (unbound variables) and type errors
 *       during aggregation? (LeeF suggests that they cause type errors which
 *       are propagated such that the aggregated value winds up unbound but I
 *       can not reconcile this with the language in the W3C draft which would
 *       appear to suggest that detail records are ignored if they result in
 *       type errors when computing the aggregate).
 * 
 *       FIXME All of the {@link IAggregate} operators have a side-effect. In
 *       order for them to have isolated side-effects for distinct groups, they
 *       would have to either internalize a value map for the group or each
 *       group would have to use a distinct instance. If the latter, then
 *       provide for this on the operator, e.g., newInstance(), and document
 *       why.
 * 
 *       FIXME Review all syntax/semantic:
 * 
 *       <pre>
 * [17]  	SolutionModifier  ::=  	GroupClause? HavingClause? OrderClause? LimitOffsetClauses?
 * [18]  	GroupClause	      ::=  	'GROUP' 'BY' GroupCondition+
 * [19]  	GroupCondition	  ::=  	( BuiltInCall | FunctionCall | '(' Expression ( 'AS' Var )? ')' | Var )
 * [20]  	HavingClause	  ::=  	'HAVING' HavingCondition+
 * [21]  	HavingCondition	  ::=  	Constraint
 * [61]  	FunctionCall	  ::=  	IRIref ArgList
 * [62]  	ArgList	          ::=  	( NIL | '(' 'DISTINCT'? Expression ( ',' Expression )* ')' )
 * [106]  	BuiltInCall	      ::=  	'STR' '(' Expression ')' ....
 * </pre>
 * 
 *       FIXME The aggregate functions can have the optional keyword DISTINCT.
 *       When present, the aggregation function needs to operate over the
 *       distinct computed values for its value expression within each solution
 *       group. Thus, the DISTINCT keyword within the aggregate function makes
 *       it impossible to undertake certain optimizations where the aggregate
 *       can be computed without first materializing the computed values for its
 *       value expressions.
 *       <p>
 *       The other exception is COUNT(DISTINCT *), where the DISTINCT is applied
 *       to the solutions in the group rather than to the column (this is not
 *       supported by MySQL and might not be valid SQL).
 *       <p>
 *       So, if we always materialize the grouped value expressions, we can then
 *       run the aggregate functions afterwards. In fact, the approach could be
 *       broken down into distinct stages which: (a) compute value expressions;
 *       (b) group solutions; (c) compute aggregates. This staging also works
 *       when the GROUPs are themselves computed expressions. (This might even
 *       be inherently more parallelizable, e.g., on a GPU. Also, a column
 *       projection would be a natural fit when computing the aggregates.)
 *       <p>
 *       It is possible to roll all of these stages together and do less work.
 *       The savings from computing all stages at once would be one pass over
 *       the solutions, directly generating the aggregated grouped solutions
 *       versus three passes. We can still do this if the keyword DISTINCT is
 *       used by any of the aggregate functions if we use a pipelined hash table
 *       as a distinct filter on the computed value expressions. So, this
 *       suggests two implementations: a three-stage operator and a single stage
 *       operator which optionally uses embedded pipelined DISTINCT filters.
 */
public class MemoryGroupByOp extends GroupByOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

	private static final transient Logger log = Logger
			.getLogger(MemoryGroupByOp.class);
    
	public interface Annotations extends GroupByOp.Annotations,
			ConcurrentHashMapAnnotations {
	
	}

    /**
     * Required deep copy constructor.
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

		// shared state is used to share the hash table.
		if (isSharedState()) {
			throw new UnsupportedOperationException(Annotations.SHARED_STATE
					+ "=" + isSharedState());
		}

		// single threaded required for pipelining w/ isLastInvocation() hook.
		if (getMaxParallel() != 1) {
			throw new UnsupportedOperationException(Annotations.MAX_PARALLEL
					+ "=" + getMaxParallel());
		}

		// operator is pipelined, but relies on isLastEvaluation() hook.
		if (!isPipelined()) {
			throw new UnsupportedOperationException(Annotations.PIPELINED + "="
					+ isPipelined());
		}

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

    /**
     * @see Annotations#CONCURRENCY_LEVEL
     */
    public int getConcurrencyLevel() {

        return getProperty(Annotations.CONCURRENCY_LEVEL,
                Annotations.DEFAULT_CONCURRENCY_LEVEL);

    }
    
    public BOpStats newStats() {
    	
    	return new GroupByStats(this);
    	
    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new GroupByTask(this, context));
        
    }

    /**
     * Wrapper used for the solution groups in the {@link ConcurrentHashMap}.
     */
    private static class SolutionGroup {

		/** The precomputed hash code for {@link #vals}. */
		private final int hash;

		/** The values for the groupBy variables which define a distinct group. */
		private final IConstant<?>[] vals;

		/**
		 * The values for the variables which are being computed by the
		 * aggregation. The binding set is when the {@link SolutionGroup} is
		 * first constructed.
		 * <p>
		 * Note: Updates to this binding set MUST be protected by synchronizing
		 * on {@link SolutionGroup}.
		 */
		private final IBindingSet aggregatedBSet;

		public String toString() {
			return super.toString() + //
					"{group=" + Arrays.toString(vals) + //
					",solution=" + aggregatedBSet + //
					"}";
		}
		
        public SolutionGroup(final IConstant<?>[] vals) {
            this.vals = vals;
            this.hash = java.util.Arrays.hashCode(vals);
            this.aggregatedBSet = new ListBindingSet();
        }

        public int hashCode() {
            return hash;
        }

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
                // @todo verify that this allows for nulls with a unit test.
                if (vals[i] == t.vals[i])
                    continue;
                if (vals[i] == null)
                    return false;
                if (!vals[i].equals(t.vals[i]))
                    return false;
            }
            return true;
        }

        /**
         * Apply the {@link IValueExpression}s to compute the updated variable
         * bindings in the {@link SolutionGroup}.
         * 
         * @param bset
         *            An input solution.
         * @param select
         *            The ordered array of {@link IValueExpression}s to be
         *            projected out of the query.
         */
        public void aggregate(final IBindingSet bset,
                final IValueExpression<?>[] select) {

            /*
             * FIXME The aggregate functions have side-effects so we need to use
             * a distinct instance of each function for each group.
             */

            // synchronize for visibility.
            synchronized (this) {
                for (IValueExpression<?> expr : select) {
                    final Object result = expr.get(bset);
                    if (log.isTraceEnabled())
                        log.trace("expr: " + expr + "=>" + result);
                }
			}

        }
        
    } // SolutionGroup

	/**
	 * Extends {@link BOpStats} to provide the shared state for the solution
	 * groups across multiple invocations of the GROUP_BY operator.
	 */
    private static class GroupByStats extends BOpStats {

        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * A concurrent map whose keys are the bindings on the specified
		 * variables (the keys and the values are the same since the map
		 * implementation does not allow <code>null</code> values).
		 * <p>
		 * Note: The map is shared state and can not be discarded or cleared
		 * until the last invocation!!!
		 * 
		 * @todo The operator is single threaded so use a {@link LinkedHashMap}
		 *       and the {@link HashMapAnnotations} rather than the
		 *       {@link ConcurrentHashMapAnnotations}
		 */
        private /*final*/ ConcurrentHashMap<SolutionGroup, SolutionGroup> map;

    	public GroupByStats(final MemoryGroupByOp op) {
    		
            this.map = new ConcurrentHashMap<SolutionGroup, SolutionGroup>(
                    op.getInitialCapacity(), op.getLoadFactor(),
                    op.getConcurrencyLevel());

    	}
    	
    }
    
    /**
     * Task executing on the node.
     */
    static private class GroupByTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * A concurrent map whose keys are the bindings on the specified
         * variables (the keys and the values are the same since the map
         * implementation does not allow <code>null</code> values).
		 * <p>
		 * Note: The map is shared state and can not be discarded or cleared
		 * until the last invocation!!!
         */
        private final ConcurrentHashMap<SolutionGroup, SolutionGroup> map;

        /**
         * The value expressions to be projected out of the GROUP_BY operator.
         */
        private final IValueExpression<?>[] select;

        /**
         * The ordered array of variables which define the distinct groups to
         * be aggregated.
         */
        private final IValueExpression<?>[] groupBy;

        /**
         * Optional constraints applied to the aggregated solutions.
         */
        private final IConstraint[] having;

        /**
         * <code>true</code> iff any aggregate functions will be applied to the
         * DISTINCT values arising from their inner value expression.
         */
        private final boolean anyDistinct;
        
        /**
         * <code>true</code> iff any aggregate expression uses a reference to
         * another aggregate expression in the select clause.
         */
        private final boolean selectDependency;

        GroupByTask(final MemoryGroupByOp op,
                final BOpContext<IBindingSet> context) {
        	
            this.context = context;

            /*
             * Validate GROUP_BY value expressions.
             * 
             * Note: The GROUP BY clause may include bare variables such as
             * "?x", non-aggregate expressions such as "STR(?x)" and
             * declarations of variables for non-aggregate expressions such as
             * "STR(?x) as strX". However, only bare variables or variables
             * declared using "AS" may appear in the SELECT clause.  Those
             * variables are collected in [groupByVars].
             * 
             * Note: Aggregate functions MAY NOT appear in the GROUP_BY clause.
             */
            // The top-level variables in the GROUP_BY clause.
            final Set<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
            {
                // must be non-null, non-empty array w/o aggregate functions.
                this.groupBy = (IValueExpression<?>[]) op
                        .getRequiredProperty(GroupByOp.Annotations.GROUP_BY);

                if (groupBy == null)
                    throw new IllegalArgumentException();

                if (groupBy.length == 0)
                    throw new IllegalArgumentException();

                // Collect top-level variables from GROUP_BY value exprs.
                for(IValueExpression<?> expr : groupBy) {
                    if (expr instanceof IVariable<?>) {
                        groupByVars.add((IVariable<?>) expr);
                    } else if (expr instanceof IBind<?>) {
                        final IBind<?> bindExpr = (IBind<?>) expr;
                        if (bindExpr.getExpr() instanceof IAggregate<?>) {
                            throw new IllegalArgumentException(
                                    "Aggregate expression not allowed in GROUP_BY");
                        }
                        groupByVars.add(bindExpr.getVar());
                    }
                }
                
            }

            /*
             * Validate SELECT value expressions.
             * 
             * Note: SELECT value expressions must be either variables appearing
             * in the top-level of the GROUP BY value expressions -or- a IBind
             * wrapping an aggregate function.
             * 
             * Note: Certain optimizations are possible when none of the SELECT
             * value expressions use DISTINCT.
             * 
             * Note: Certain optimizations are possible when all of the SELECT
             * value expressions may be computed based on per-group counters.
             * 
             * TODO detect case with per-group counters and support
             * optimizations for that case.
             */
            // The top-level variables in the SELECT clause.
            final Set<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
            {
                // must be non-null, non-empty array.
                this.select = (IValueExpression[]) op
                        .getRequiredProperty(GroupByOp.Annotations.SELECT);

                if (/*select != null && */select.length == 0)
                    throw new IllegalArgumentException();

                // true iff any aggregate expression uses DISTINCT.
                boolean anyDistinct = false;
                // true iff any aggregate expression uses a reference to another
                // aggregate expression in the select clause.
                boolean selectDependency = false;
                for (IValueExpression<?> varOrBindExpr : select) {
                    /*
                     * Each SELECT value expression must be either a top-level
                     * IVariable in the GROUP BY clause or an IBind wrapping a
                     * value expression consisting solely of aggregates (which
                     * may of course wrap bare variables) and constants.
                     */
                    if (varOrBindExpr instanceof IVariable<?>) {
                        final IVariable<?> var = (IVariable<?>) varOrBindExpr;
                        if (!groupByVars.contains(var)) {
                            throw new IllegalArgumentException(
                                    "Bare variable not declared by GROUP_BY clause: "
                                            + var);
                        }
                        selectVars.add(var);
                    } else if (varOrBindExpr instanceof IBind<?>) {
                        /*
                         * Child of IBind must be a valid aggregate expression
                         * consisting solely of aggregates (which may wrap bare
                         * variables declared in the GROUP_BY clause) and
                         * constants.
                         * 
                         * Note: Top-level variables already declared in a
                         * SELECT clause MAY appear within other value
                         * expressions in the SELECT clause. [TODO is this
                         * restricted to forward references? If not, then we
                         * need to figure out the dependency graph for the
                         * SELECT expressions]. When such dependencies exist
                         * certain optimizations are not possible (we have to
                         * compute each select expression in turn for a given
                         * group rather than computing them in parallel).
                         * 
                         * Note: If any aggregate in the expression uses
                         * DISTINCT then we make a note of that as certain
                         * optimizations are not possible when DISTINCT is used
                         * within an aggregate expression.
                         */
                        final IBind<?> bindExpr = (IBind<?>) varOrBindExpr;
                        final IValueExpression<?> aggExpr = (IValueExpression<?>) bindExpr
                                .getExpr();
                        final Iterator<BOp> itr = BOpUtility.preOrderIterator(aggExpr);
                        while(itr.hasNext()) {
                            final IValueExpression<?> t = (IValueExpression<?>) itr
                                    .next();
                            if (t instanceof IConstant)
                                continue;
                            if (t instanceof IVariable<?>) {
                                final IVariable<?> v = (IVariable<?>) t;
                                if (groupByVars.contains(v))
                                    continue;
                                if (selectVars.contains(v)) {
                                    selectDependency = true;
                                    continue;
                                }
                                throw new IllegalArgumentException(
                                        "Non-aggregate variable in select expression: "
                                                + v);
                            }
                            if (t instanceof IAggregate<?>) {
                                /*
                                 * FIXME Must use stateful visitation or
                                 * recursion pattern so we can accept the
                                 * presence of non-aggregate variables within
                                 * the scope of an IAggregate.
                                 * 
                                 * TODO Can an IAggregate have another
                                 * IAggregate as part of its value expression? I
                                 * think so, either as a dependency on a
                                 * previously computed variable in the select
                                 * clause or as an anonymous aggregate
                                 * expression such as SUM(?x+sum(?y)).
                                 */
                                final IAggregate<?> u = (IAggregate<?>) aggExpr;
                                if (u.isDistinct()) {
                                    anyDistinct = true;
                                }
                            }
                        }
                    } else {
                        throw new IllegalArgumentException(
                                "Top-level of SELECT expression must be IVariable or IBind: "
                                        + varOrBindExpr);
                    }
                }
                this.anyDistinct = anyDistinct;
                this.selectDependency = selectDependency;
            }

            /*
             * HAVING clause.
             * 
             * The having[] may be null or an empty[]. However, any value
             * expressions must be aggregates (as defined for SELECT
             * expressions) or variables declared in the groupBy or select
             * clauses.
             */
            this.having = (IConstraint[]) op
                    .getProperty(GroupByOp.Annotations.HAVING);
            
			// The map is shared state across invocations of this operator task.
			this.map = ((GroupByStats) context.getStats()).map;

        }

		/**
		 * Return the "row" for the groupBy variables.
		 * 
		 * @param bset
		 *            The binding set to be filtered.
		 * 
		 * @return The distinct as bound values -or- <code>null</code> if the
		 *         binding set duplicates a solution which was already accepted.
		 */
        private SolutionGroup accept(final IBindingSet bset) {

            final IConstant<?>[] r = new IConstant<?>[groupBy.length];

            for (int i = 0; i < groupBy.length; i++) {

                /*
                 * Note: This allows null's.
                 * 
                 * @todo write a unit test when some variables are not bound.
                 */
//                r[i] = bset.get(groupBy[i]);
                r[i] = new Constant(groupBy[i].get(bset));

            }

            final SolutionGroup s = new SolutionGroup(r);
            
            map.putIfAbsent(s, s);

			return s;

		}

		public Void call() throws Exception {

			final BOpStats stats = context.getStats();

			final boolean isLastInvocation = context.isLastInvocation();

			final IAsynchronousIterator<IBindingSet[]> itr = context
					.getSource();

			final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

			try {

				/*
				 * Present each source solution in turn, identifying the group
				 * into which it falls and then applying the value expressions
				 * to update the aggregated variable bindings for that group.
				 */
				while (itr.hasNext()) {

					final IBindingSet[] a = itr.next();

					stats.chunksIn.increment();
					stats.unitsIn.add(a.length);

					for (IBindingSet bset : a) {

						// identify the solution group.
						final SolutionGroup solutionGroup = accept(bset);

						// aggregate the bindings
						solutionGroup.aggregate(bset, select);

					}

				}

				if (isLastInvocation) {

					/*
					 * Write aggregated solutions on the sink, applying the
					 * [having] filter to remove any solutions which do not
					 * satisfy its constraints.
					 */

                  final List<IBindingSet> accepted = new LinkedList<IBindingSet>();
					
                  int naccepted = 0;

                  for(SolutionGroup solutionGroup: map.values()) {
						
						synchronized(solutionGroup) {

							IBindingSet bset = solutionGroup.aggregatedBSet;
							
							// verify optional constraint(s)
							if (having != null
									&& !BOpUtility.isConsistent(having, bset)) {

								// skip this group.
								continue;
								
							}

//							/*
//							 * We will accept this solution group, so filter out
//							 * any variables which are not being projected out
//							 * of this operator.
//							 */
							if (log.isDebugEnabled())
								log.debug("accepted: " + solutionGroup);
//
//							// optionally strip off unnecessary variables.
//							bset = select == null ? bset : bset
//									.copy(select);

                            accepted.add(bset);

                            naccepted++;

						}
						
					}
					
					/*
					 * Output the aggregated bindings for the accepted
					 * solutions.
					 */
					if (naccepted > 0) {

						final IBindingSet[] b = accepted
								.toArray(new IBindingSet[naccepted]);

						sink.add(b);

						// flush the output.
						sink.flush();

						// discard the map.
						map.clear();
						
					}

				}

				// done.
				return null;

			} finally {

				sink.close();

			}

		} // call()

	} // GroupByTask
    
}
