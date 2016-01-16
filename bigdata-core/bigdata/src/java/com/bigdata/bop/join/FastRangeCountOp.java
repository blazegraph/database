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
/*
 * Created on Aug 25, 2010
 */

package com.bigdata.bop.join;

import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

/**
 * This operator reports the fast-range count for an as-bound {@link IPredicate}
 * . The cost of this operator is two key probes. Unlike a normal access path,
 * this operator does not bind variables to data in tuples in the underlying
 * index. Instead it binds a pre-identified variable to the aggregate (COUNT) of
 * the tuple range spanned by the {@link IPredicate}.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1037" > Rewrite SELECT
 *      COUNT(...) (DISTINCT|REDUCED) {single-triple-pattern} as ESTCARD </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class FastRangeCountOp<E> extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends AccessPathJoinAnnotations {

		/**
		 * The name of the variable that will be bound to the fast-range count
		 * of the access path associated with the predicate.
		 */
		String COUNT_VAR = FastRangeCountOp.class.getName() + ".countVar";

    }

	/**
	 * Deep copy constructor.
	 * 
	 * @param op
	 */
	public FastRangeCountOp(final FastRangeCountOp<E> op) {
		
		super(op);
		
	}

	/**
	 * Shallow copy constructor.
	 * 
	 * @param args
	 * @param annotations
	 */
	public FastRangeCountOp(final BOp[] args,
			final Map<String, Object> annotations) {

		super(args, annotations);

		// MUST be given.
		getRequiredProperty(Annotations.COUNT_VAR);
		getRequiredProperty(Annotations.PREDICATE);

		if(isOptional()) {
			/*
			 * TODO OPTIONAL is not implemented for this operator.
			 * 
			 * Note: For this operator, an OPTIONAL join would be *nearly*
			 * identical to a normal join. This is because the OPTIONAL join
			 * succeeds where the normal join would fail due to no matched
			 * tuples. However, since this join is computing an aggregate
			 * (COUNT), the count is simply zero for the OPTIONAL join case.
			 * However, we would still produce a solution in the case where the
			 * countVar was bound on input to this operator and the range-count
			 * produced a different value for the countVar.
			 */
			throw new UnsupportedOperationException();
			
		}
		
	}

	public FastRangeCountOp(final BOp[] args, final NV... annotations) {

		this(args, NV.asMap(annotations));

	}

	/**
	 * @see Annotations#COUNT_VAR
	 */
	protected IVariable<?> getCountVar() {
		
		return (IVariable<?>) getRequiredProperty(Annotations.COUNT_VAR);
		
	}
	
    /**
     * @see Annotations#SELECT
     */
    protected IVariable<?>[] getSelect() {

        return getProperty(Annotations.SELECT, null/* defaultValue */);

    }

    /**
     * @see Annotations#CONSTRAINTS
     */
    protected IConstraint[] constraints() {

        return getProperty(Annotations.CONSTRAINTS, null/* defaultValue */);

    }

	@SuppressWarnings("unchecked")
	public IPredicate<E> getPredicate() {

		return (IPredicate<E>) getRequiredProperty(Annotations.PREDICATE);

	}

    /**
     * Return the value of {@link IPredicate#isOptional()} for the
     * {@link IPredicate} associated with this join.
     * 
     * @see IPredicate.Annotations#OPTIONAL
     */
	private boolean isOptional() {

//		return getProperty(Annotations.OPTIONAL, Annotations.DEFAULT_OPTIONAL);
        return getPredicate().isOptional();

	}

	@Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask<E>(this, context));

    }

    /**
     * Copy the source to the sink.
     */
    static protected class ChunkTask<E> implements Callable<Void> {

        protected final FastRangeCountOp<E> op;

        protected final BOpContext<IBindingSet> context;

        /**
         * The variable that gets bound to the fast range count.
         */
        protected final IVariable<?> countVar;
        
		/**
		 * The source for the elements to be joined.
		 */
        protected final IPredicate<E> predicate;
        
		/**
		 * The relation associated with the {@link #predicate} operand.
		 */
        protected final IRelation<E> relation;
        
        protected ChunkTask(final FastRangeCountOp<E> op,
                final BOpContext<IBindingSet> context) {

            this.op = op;

            this.context = context;

            this.countVar = op.getCountVar();
            
			this.predicate = op.getPredicate();
			
			this.relation = context.getRelation(predicate);

        }

        @Override
        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            // Convert source solutions to array (assumes low cardinality).
            final IBindingSet[] leftSolutions = BOpUtility.toArray(
                    context.getSource(), stats);

            // default sink
            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

			/*
			 * This is at most 1:1 operator. Each source solution produces one
			 * output solution (the range-count for the as-bound predicate on
			 * the corresponding access path). The only way in which we can get
			 * less than a 1:1 join hit ratio is if the countVar is bound on
			 * input and the actual range count does not unify with the incoming
			 * binding for that variable.
			 */
			final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
					leftSolutions.length/* capacity */, sink);

            final IVariable<?>[] selectVars = op.getSelect();

            final IConstraint[] constraints = op.constraints();

    		try {

				/*
				 * TODO If there are multiple left solutions (from the pipeline)
				 * then we could generate their fromKeys and order them to
				 * improve cache locality.  See PipelineJoin for an example of
				 * how this is done.
				 */
				
				// For each source solution.
				for (IBindingSet bindingSet : leftSolutions) {

					// constrain the predicate to the given bindings.
					IPredicate<E> asBound = predicate.asBound(bindingSet);

                    if (asBound == null) {

                        /*
                         * This can happen for a SIDS mode join if some of the
                         * (s,p,o,[c]) and SID are bound on entry and they can not
                         * be unified. For example, the s position might be
                         * inconsistent with the Subject that can be decoded from
                         * the SID binding.
                         * 
                         * @see #815 (RDR query does too much work)
                         */

                        continue;

                    }

//                    if (partitionId != -1) {
//
//						/*
//						 * Constrain the predicate to the desired index
//						 * partition.
//						 * 
//						 * Note: we do this for scale-out joins since the
//						 * access path will be evaluated by a JoinTask
//						 * dedicated to this index partition, which is part
//						 * of how we give the JoinTask to gain access to the
//						 * local index object for an index partition.
//						 */
//
//						asBound = asBound.setPartitionId(partitionId);
//
//					}

					final long rangeCount = determineRangeCount( asBound );

					// New binding set.
					final IBindingSet right = new ListBindingSet();

					/*
					 * Bind the countVar.
					 * 
					 * Note: per the spec, SPARQL expects an xsd:integer here.
					 */
					right.set(countVar, new Constant<XSDIntegerIV>(
							new XSDIntegerIV(BigInteger.valueOf(rangeCount))));

					// See if the solutions join.
					final IBindingSet outSolution = BOpContext.bind(//
							bindingSet,// left
							right,//
							constraints,//
							selectVars//
							);

					if (outSolution != null) {

						// Output the solution.
						unsyncBuffer.add(outSolution);

					}
					
				}
                
	            // flush the unsync buffer.
	            unsyncBuffer.flush();
                
                // flush the sink.
                sink.flush();

                // Done.
                return null;
                
            } finally {

                sink.close();
                
                context.getSource().close();
                
            }

        }

        protected long determineRangeCount( final IPredicate<E> pred )
        {
           /**
            * The {@link IAccessPath} corresponding to the asBound
            * {@link IPredicate} for this join dimension. The asBound
            * {@link IPredicate} is {@link IAccessPath#getPredicate()}.
            * 
            * Note: The exact range count using will be two key probes
            * unless the index supports delete markers or there is a
            * filter attached to the access path.
            * 
            * Note: This will throw an exception if either of those
            * conditions is true (the index supports delete markers or
            * there is a filter attached to the access path). The
            * exception is thrown since those conditions change the
            * cost of this operator from O(2) (the cost of TWO key
            * probes) to the O(fast-range-count) (the cost of a
            * key-range scan). Thus, generating this operator when
            * those conditions are violated leads to incorrect
            * reasoning about the cost of the operator.
            */
           final IAccessPath<E> accessPath = context.getAccessPath( relation,
                                                                    pred );

           if (accessPath.getPredicate().getIndexLocalFilter() != null) {
              // index has local filter. requires scan.
              throw new AssertionError();
           }

           if (accessPath.getPredicate().getAccessPathFilter() != null) {
              // access path filter exists. requires scan.
              throw new AssertionError();
           }

           /*
            * Request an exact range count.
            * 
            * Note: This will be 2 key probes since we have verified
            * that there are no filters imposed on the access path.
            */
           return accessPath.rangeCount(true/* exact */);
        }

    } // class ChunkTask

}
