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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * A "operator-at-once" in-memory implementation of GROUP_BY. The processing
 * stages are:
 * <ol>
 * <li>Each value expressions named by a SELECT or HAVING clause is broken down
 * into: (a) a value expression which is computed over the detail records (the
 * incoming solutions); and (b) a value expression which includes an aggregate
 * function computed over the result of (a). For example,
 * <code>SUM(i+1)*2</code> is broken down into <code>foo := i+1</code>, which is
 * computed against the detail records; and SUM(<i>foo</i>)*2</code>, which will
 * be computed against each solution group. It is an error if any of the value
 * expressions in the SELECT or HAVING clause do not include an aggregate
 * function defined over variables in the detail records.</li>
 * <li>Apply both the value expressions (a) and the value expressions in the
 * GROUP BY clause to the incoming solutions to compute a new set of solutions.
 * Those solutions are grouped using a hash table which maps the GROUP BY value
 * expressions onto the distinct solution groups. Each solution is added to the
 * set of solutions for its solution group.</li>
 * <li>Compute the aggregated value expressions for each solution set, producing
 * a single solution set. When the aggregate function uses the DISTINCT option,
 * it is applied to the distinct values of the corresponding value expression</li>
 * </ol>
 * 
 * This implementation can not take advantage of optimizations when none of the
 * aggregate functions use the DISTINCT keyword.
 * 
 * @author thompsonbry
 */
public class GroupByStagedOp extends GroupByOp {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public interface Annotations extends GroupByOp.Annotations,
			HashMapAnnotations {

	}
	
	public GroupByStagedOp(GroupByOp op) {
		super(op);
	}

	public GroupByStagedOp(BOp[] args, Map<String, Object> annotations) {
		super(args, annotations);

		switch (getEvaluationContext()) {
		case CONTROLLER:
			break;
		default:
			throw new UnsupportedOperationException(
					Annotations.EVALUATION_CONTEXT + "="
							+ getEvaluationContext());
		}

		// operator is not pipelined.
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
    
	@Override
	public FutureTask<Void> eval(BOpContext<IBindingSet> context) {
        return new FutureTask<Void>(new GroupByTask(this, context));
	}

    /**
     * A group of solutions sharing some key described by an ordered array
     * of {@link IConstant}s.
     */
    private static class Group {

		/** The precomputed hash code for {@link #vals}. */
		private final int hash;

		/** The values for the groupBy variables which define a distinct group. */
		private final IConstant<?>[] vals;

		public String toString() {
			return super.toString() + //
					"{vals=" + Arrays.toString(vals) + //
					"}";
		}
		
        public Group(final IConstant<?>[] vals) {
            this.vals = vals;
            this.hash = java.util.Arrays.hashCode(vals);
        }

        public int hashCode() {
            return hash;
        }

        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Group)) {
                return false;
            }
            final Group t = (Group) o;
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

    } // Group

    /**
     * Task executing on the node.
     */
    static private class GroupByTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

		/**
		 * Optional set of variables to be projected out of the GROUP_BY
		 * operator. When <code>null</code>, all variables will be projected
		 * out.
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

//        /*
//         * derived data
//         */
//        
//		/**
//		 * An ordered array of {@link IValueExpression}s computed for each
//		 * incoming solution. This array is built up from the {@link #select},
//		 * {@link #groupBy}, and {@link #having} collections.  Only that
//		 * component of each {@link IValueExpression} which DOES NOT include
//		 * the aggregate function(s) is captured within {@link #compute}.
//		 */
//        private final IValueExpression<?>[] compute;
//
//		/**
//		 * An ordered array of {@link IVariable}s correlated with the
//		 * {@link #compute} array and used to assign the compute results into
//		 * the solution which will be inserted into a {@link Group} and
//		 * aggregated in a post-process step.
//		 */
//        private final IVariable<?>[] vars;
//
//        /**
//         * The subset of {@link #vars} which were specified by {@link #select}
//         * and will be projected out of the GROUP_BY operator.
//         */
//        private final IVariable<?>[] project;
        
		/**
		 * A map whose keys are the bindings on the specified variables (the
		 * keys and the values are the same since the map implementation does
		 * not allow <code>null</code> values).
		 */
        private final HashMap<Group, List<IBindingSet>> map;

        GroupByTask(final GroupByStagedOp op,
                final BOpContext<IBindingSet> context) {
        	
            this.context = context;

			/*
			 * The IValueExpressions to be SELECTed.
			 */
			this.select = (IVariable[]) op
					.getRequiredProperty(GroupByOp.Annotations.SELECT);

			if (select.length == 0)
				throw new IllegalArgumentException();

			/*
			 * The IValueExpressions defining the groups. This must be non-null,
			 * and non-empty array w/o dups.
			 */
			this.groupBy = (IValueExpression<?>[]) op
					.getRequiredProperty(GroupByOp.Annotations.GROUP_BY);

            if (groupBy.length == 0)
                throw new IllegalArgumentException();

            // may be null or empty[].
            this.having = (IConstraint[]) op
					.getProperty(GroupByOp.Annotations.HAVING);

			// The solution groups.
			this.map = new LinkedHashMap<Group, List<IBindingSet>>();

        }

		/**
		 * Add the solution to the appropriate group.
		 * 
		 * @param bset
		 *            The solution.
		 */
        private void accept(final IBindingSet bset) {

            final IConstant<?>[] a = new IConstant<?>[groupBy.length];

            for (int i = 0; i < groupBy.length; i++) {

                /*
                 * Note: This allows null's.
                 * 
                 * @todo write a unit test when some variables are not bound.
                 */
//                r[i] = bset.get(groupBy[i]);
                a[i] = new Constant(groupBy[i].get(bset));

            }

			final Group group = new Group(a);

			List<IBindingSet> solutions = map.get(group);

			if (solutions == null) {

				// New group.
				map.put(group, solutions = new LinkedList<IBindingSet>());
				
			}
			
			// add solution to the solutions in the group.
			solutions.add(bset);

		}

		public Void call() throws Exception {

			final BOpStats stats = context.getStats();

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

					// A chunk of binding sets from the source.
					final IBindingSet[] a = itr.next();

					stats.chunksIn.increment();
					stats.unitsIn.add(a.length);

					// For each given binding set in that chunk.
					for (IBindingSet bsetIn : a) {

						/*
						 * Compute each of the value expressions, creating a new
						 * binding set and then insert the new binding set into
						 * the appropriate group.
						 * 
						 * @todo How will we name variables which were not
						 * explicitly assigned names? This can happen even for
						 * variables which are being projected out of the
						 * GROUP_BY operator. We will need to wrap them with a
						 * Bind() to get a side-effect on the new solution.
						 */

						// The computed value expressions.
						final IBindingSet bsetComputed = new ListBindingSet();

//						// For each value expression to be computed/
//						for (int i = 0; i < compute.length; i++) {
//
//							/*
//							 * Compute the value expression.
//							 * 
//							 * FIXME Do not use Bind() for a side-effect since
//							 * the expression is computed based on original
//							 * binding set.
//							 * 
//							 * FIXME Except if expressions can reference earlier
//							 * expressions in the computed binding set, then we
//							 * need to operate on the union of the given and the
//							 * computed binding sets. [The easier way to handle
//						     * this is to clone the bindingSet and then just add
//						     * the new variables to the binding set.]
//							 */
//							final IValueExpression<?> e = compute[i];
//
//							final Object val = e.get(bsetIn);
//
//							// FIXME Set the computed value on the solution.
//							bsetComputed.set(var[i], new Constant(val));
//
//						}

						accept(bsetComputed);

					}

				}

				/*
				 * FIXME Now run the aggregate functions over the computed
				 * solutions within each group.
				 */
				
//				if (isLastInvocation) {
//
//					/*
//					 * Write aggregated solutions on the sink, applying the
//					 * [having] filter to remove any solutions which do not
//					 * satisfy its constraints.
//					 */
//
//                  final List<IBindingSet> accepted = new LinkedList<IBindingSet>();
//					
//                  int naccepted = 0;
//
//                  for(SolutionGroup solutionGroup: map.values()) {
//						
//						synchronized(solutionGroup) {
//
//							IBindingSet bset = solutionGroup.aggregatedBSet;
//							
//							// verify optional constraint(s)
//							if (having != null
//									&& !BOpUtility.isConsistent(having, bset)) {
//
//								// skip this group.
//								continue;
//								
//							}
//
//							/*
//							 * We will accept this solution group, so filter out
//							 * any variables which are not being projected out
//							 * of this operator.
//							 */
//							if (log.isDebugEnabled())
//								log.debug("accepted: " + solutionGroup);
//
//							// optionally strip off unnecessary variables.
//							bset = select == null ? bset : bset
//									.copy(select);
//
//                            accepted.add(bset);
//
//                            naccepted++;
//
//						}
//						
//					}
//					
//					/*
//					 * Output the aggregated bindings for the accepted
//					 * solutions.
//					 */
//					if (naccepted > 0) {
//
//						final IBindingSet[] b = accepted
//								.toArray(new IBindingSet[naccepted]);
//
//						sink.add(b);
//
//						// flush the output.
//						sink.flush();
//
//						// discard the map.
//						map.clear();
//						
//					}
//
//				}

				// done.
				return null;

			} finally {

				sink.close();

			}

		} // call()

	} // GroupByTask

}
