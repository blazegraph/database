/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.controller;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

/**
 * Hash join with subquery.
 * <p>
 * All source solutions are fully materialized in a hash table. The keys of the
 * hash table are the as-bound join variable(s). The values in the hash table is
 * the list of solutions having a specific value for the as-bound join
 * variables. Once all solutions are materialized, the subquery is evaluated
 * once. For each solution materialized by the subquery, the operator probes the
 * hash table using the as-bound join variables for the subquery solution. If
 * there is a hit in the hash table, then operator then outputs the cross
 * product of the subquery solution with the solutions list found under that key
 * in the hash table, applying any optional CONSTRAINTS.
 * <p>
 * In order to support OPTIONAL semantics for the subquery, a bit flag must be
 * carried for each entry in the hash table. Once the subquery solutions have
 * been exhausted, if the bit was never set for some entry and the subquery is
 * optional, then the solutions associated with that entry are output, applying
 * any optional CONSTRAINTS.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class SubqueryHashJoinOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

    	/**
    	 * The join variables (required). This is an {@link IVariable}[] with
    	 * at least one variable. The order of the entries is used when forming
    	 * the as-bound keys for the hash table.  Duplicate elements and null
    	 * elements are not permitted.
    	 */
    	String JOIN_VARS = SubqueryHashJoinOp.class.getName() + ".subquery";
    	
		/**
		 * The subquery to be evaluated (required). This should be a
		 * {@link PipelineOp}.  (It is basically the equivalent of the
		 * {@link IPredicate} for a {@link PipelineJoin}).
		 */
        String SUBQUERY = SubqueryHashJoinOp.class.getName() + ".subquery";

		/**
		 * An optional {@link IVariable}[] identifying the variables to be
		 * retained in the {@link IBindingSet}s written out by the operator. All
		 * variables are retained unless this annotation is specified.
		 * 
		 * @todo This should be on {@link SubqueryOp} as well.
		 */
		String SELECT = SubqueryHashJoinOp.class.getName() + ".select";

        /**
         * An {@link IConstraint}[] which places restrictions on the legal
         * patterns in the variable bindings (optional).
         * 
         * @todo This should be on {@link SubqueryOp} as well.
         */
        String CONSTRAINTS = SubqueryHashJoinOp.class.getName() + ".constraints";
    	
        /**
         * When <code>true</code> the subquery has optional semantics (if the
         * subquery fails, the original binding set will be passed along to the
         * downstream sink anyway) (default {@value #DEFAULT_OPTIONAL}).
         */
        String OPTIONAL = SubqueryHashJoinOp.class.getName() + ".optional";

        boolean DEFAULT_OPTIONAL = false;
        
    }

//    /**
//     * @see Annotations#MAX_PARALLEL
//     */
//    public int getMaxParallel() {
//        return getProperty(Annotations.MAX_PARALLEL,
//                Annotations.DEFAULT_MAX_PARALLEL);
//    }
    
    /**
     * Deep copy constructor.
     */
    public SubqueryHashJoinOp(final SubqueryHashJoinOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public SubqueryHashJoinOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

//        if (!getEvaluationContext().equals(BOpEvaluationContext.CONTROLLER))
//            throw new IllegalArgumentException(Annotations.EVALUATION_CONTEXT
//                    + "=" + getEvaluationContext());

		final IVariable<?>[] joinVars = (IVariable[]) getRequiredProperty(Annotations.JOIN_VARS);

		if (joinVars.length == 0)
			throw new IllegalArgumentException(Annotations.JOIN_VARS);

		for (IVariable<?> var : joinVars) {

			if (var == null)
				throw new IllegalArgumentException(Annotations.JOIN_VARS);

		}

		getRequiredProperty(Annotations.SUBQUERY);

		assertAtOnceJavaHeapOp();

//        if (!getProperty(Annotations.CONTROLLER, Annotations.DEFAULT_CONTROLLER))
//            throw new IllegalArgumentException(Annotations.CONTROLLER);
        
//        // The id of this operator (if any).
//        final Integer thisId = (Integer)getProperty(Annotations.BOP_ID);
//        
//        for(BOp op : args) {
//
//            final Integer sinkId = (Integer) op
//                    .getRequiredProperty(Annotations.SINK_REF);
//            
//            if(sinkId.equals(thisId))
//                throw new RuntimeException("Operand may not target ") 
//            
//        }

    }

    public SubqueryHashJoinOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ControllerTask(this, context));
        
    }

    /**
     * Evaluates the arguments of the operator as subqueries. The arguments are
     * evaluated in order. An {@link Executor} with limited parallelism to
     * evaluate the arguments. If the controller operator is interrupted, then
     * the subqueries are cancelled. If a subquery fails, then all subqueries
     * are cancelled.
     */
    private static class ControllerTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;
        private final boolean optional;
        private final PipelineOp subquery;
        
        public ControllerTask(final SubqueryHashJoinOp controllerOp, final BOpContext<IBindingSet> context) {

            if (controllerOp == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.optional = controllerOp.getProperty(Annotations.OPTIONAL,
                    Annotations.DEFAULT_OPTIONAL);

            this.subquery = (PipelineOp) controllerOp
                    .getRequiredProperty(Annotations.SUBQUERY);
            
        }

        /**
         * Evaluate the subquery.
         * 
         * @todo Support limited parallelism for each binding set read from the
         *       source. We will need to keep track of the running subqueries in
         *       order to wait on them before returning from this method and in
         *       order to cancel them if something goes wrong.
         */
        public Void call() throws Exception {
            
            try {

                final IAsynchronousIterator<IBindingSet[]> sitr = context
                        .getSource();
                
                while(sitr.hasNext()) {
                    
                    final IBindingSet[] chunk = sitr.next();
                    
                    for(IBindingSet bset : chunk) {

						final IRunningQuery runningSubquery = new SubqueryTask(
								bset, subquery, context).call();

						if (!runningSubquery.isDone()) {

							throw new AssertionError("Future not done: "
									+ runningSubquery.toString());
							
						}

                    }
                    
                }
                
                // Now that we know the subqueries ran Ok, flush the sink.
                context.getSink().flush();
                
                // Done.
                return null;

            } finally {

                context.getSource().close();

                context.getSink().close();
                
                if (context.getSink2() != null)
                    context.getSink2().close();

            }
            
        }

        /**
         * Run a subquery.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         */
        private class SubqueryTask implements Callable<IRunningQuery> {

            /**
             * The evaluation context for the parent query.
             */
            private final BOpContext<IBindingSet> parentContext;

            /**
             * The source binding set. This will be copied to the output if
             * there are no solutions for the subquery (optional join
             * semantics).
             */
            private final IBindingSet bset;
            
            /**
             * The root operator for the subquery.
             */
            private final BOp subQueryOp;

            public SubqueryTask(final IBindingSet bset, final BOp subQuery,
                    final BOpContext<IBindingSet> parentContext) {

                this.bset = bset;
                
                this.subQueryOp = subQuery;

                this.parentContext = parentContext;

            }

            public IRunningQuery call() throws Exception {

            	// The subquery
                IRunningQuery runningSubquery = null;
            	// The iterator draining the subquery
                IAsynchronousIterator<IBindingSet[]> subquerySolutionItr = null;
                try {

                    final QueryEngine queryEngine = parentContext.getRunningQuery()
                            .getQueryEngine();

                    runningSubquery = queryEngine.eval((PipelineOp) subQueryOp,
                            bset);

					long ncopied = 0L;
					try {
						
						// Iterator visiting the subquery solutions.
						subquerySolutionItr = runningSubquery.iterator();

						// Copy solutions from the subquery to the query.
						ncopied = BOpUtility.copy(subquerySolutionItr,
								parentContext.getSink(), null/* sink2 */,
								null/* constraints */, null/* stats */);

						// wait for the subquery to halt / test for errors.
						runningSubquery.get();
						
					} catch (InterruptedException ex) {

						// this thread was interrupted, so cancel the subquery.
						runningSubquery
								.cancel(true/* mayInterruptIfRunning */);

						// rethrow the exception.
						throw ex;
						
					}
					
                    if (ncopied == 0L && optional) {

                        /*
                         * Since there were no solutions for the subquery, copy
                         * the original binding set to the default sink.
                         * 
                         * @todo If we add a CONSTRAINTS annotation to the
                         * SubqueryOp then we need to make sure that it is
                         * applied to all solutions copied out of the subquery.
                         */

                    	parentContext.getSink().add(new IBindingSet[]{bset});
                        
                    }
                    
                    // done.
                    return runningSubquery;
                    
                } catch (Throwable t) {

					if (runningSubquery == null
							|| runningSubquery.getCause() != null) {
						/*
						 * If things fail before we start the subquery, or if a
						 * subquery fails (due to abnormal termination), then
						 * propagate the error to the parent and rethrow the
						 * first cause error out of the subquery.
						 * 
						 * Note: IHaltable#getCause() considers exceptions
						 * triggered by an interrupt to be normal termination.
						 * Such exceptions are NOT propagated here and WILL NOT
						 * cause the parent query to terminate.
						 */
                        throw new RuntimeException(ControllerTask.this.context
                                .getRunningQuery().halt(
                                        runningSubquery == null ? t
                                                : runningSubquery.getCause()));
                    }
					
					return runningSubquery;
                    
                } finally {

					try {

						// ensure subquery is halted.
						if (runningSubquery != null)
							runningSubquery
									.cancel(true/* mayInterruptIfRunning */);
						
					} finally {

						// ensure the subquery solution iterator is closed.
						if (subquerySolutionItr != null)
							subquerySolutionItr.close();

					}
					
                }

            }

        } // SubqueryTask

    } // ControllerTask

}
