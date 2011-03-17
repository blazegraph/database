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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
 * @todo GROUP_BY implementation using an HTree suitable for use when the #of
 *       groups is very large. The HTree would be associated with the allocation
 *       context for the (queryId,bopId(,shardId))). (The shardId would be used
 *       iff the GROUP_BY operator was hash partitioned across the nodes.)
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
		 * @param compute
		 *            The ordered array of {@link IValueExpression}s which
		 *            define the aggregated variables.
		 */
		public void aggregate(final IBindingSet bset,
				final IValueExpression<?>[] compute) {

			/*
			 * FIXME The aggregate functions have side-effects so we need to use
			 * a distinct instance of each function for each group.
			 */

			// synchronize for visibility.
			synchronized(this) {
				for(IValueExpression<?> expr : compute) {
					System.err.println(expr.get(bset));
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
         * The ordered array of variables which define the distinct groups to
         * be aggregated.
         */
        private final IValueExpression<?>[] groupBy;

//		/**
//		 * The {@link IValueExpression}s used to compute each of the variables
//		 * in the aggregated solutions.
//		 */
//        private final IValueExpression<?>[] compute;
        
        /**
         * Optional constraints applied to the aggregated solutions.
         */
        private final IConstraint[] having;
        
		/**
		 * Optional set of variables to be projected out of the GROUP_BY
		 * operator. When <code>null</code>, all variables will be projected
		 * out.
		 */
		private final IVariable<?>[] select;
        
        GroupByTask(final MemoryGroupByOp op,
                final BOpContext<IBindingSet> context) {
        	
            this.context = context;

            // must be non-null, and non-empty array w/o dups.
			this.groupBy = (IValueExpression<?>[]) op
					.getRequiredProperty(GroupByOp.Annotations.GROUP_BY);

            if (groupBy == null)
                throw new IllegalArgumentException();

            if (groupBy.length == 0)
                throw new IllegalArgumentException();

//			/*
//			 * Must be non-null, and non-empty array. Any variables in the
//			 * source solutions may only appear within aggregation operators
//			 * such as SUM, COUNT, etc. Variables declared in [compute] may be
//			 * referenced inside the value expressions as long as they do not
//			 * appear within an aggregation function, but they they must be
//			 * defined earlier in the ordered compute[]. The value expressions
//			 * must include an assignment to the appropriate aggregate variable.
//			 * 
//			 * FIXME This must include a LET or BIND to assign the computed
//			 * value to the appropriate variable.
//			 * 
//			 * FIXME verify references to unaggregated and aggregated variables.
//			 */
//			this.compute = (IValueExpression<?>[]) op
//					.getRequiredProperty(GroupByOp.Annotations.COMPUTE);
//
//            if (compute == null)
//                throw new IllegalArgumentException();
//
//            if (compute.length == 0)
//                throw new IllegalArgumentException();

            // may be null or empty[].
            this.having = (IConstraint[]) op
					.getProperty(GroupByOp.Annotations.HAVING);

			/*
			 * The variables to project out of the GROUP_BY operator. This may
			 * be null, but not empty[].
			 * 
			 * TODO Variables may only appear once and must be distinct from the
			 * source variables.
			 */
			this.select = (IVariable[]) op
					.getRequiredProperty(GroupByOp.Annotations.SELECT);

			if (select != null && select.length == 0)
				throw new IllegalArgumentException();

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
						solutionGroup.aggregate(bset, null/*FIXME compute*/);

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

							/*
							 * We will accept this solution group, so filter out
							 * any variables which are not being projected out
							 * of this operator.
							 */
							if (log.isDebugEnabled())
								log.debug("accepted: " + solutionGroup);

							// optionally strip off unnecessary variables.
							bset = select == null ? bset : bset
									.copy(select);

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
