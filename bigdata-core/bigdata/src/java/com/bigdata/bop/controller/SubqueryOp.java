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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.controller;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.JoinAnnotations;
import com.bigdata.bop.join.JoinTypeEnum;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.relation.accesspath.IBlockingBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Pipelined join with subquery.
 * <p>
 * For each binding set presented, this operator executes a subquery. Any
 * solutions produced by the subquery are copied to the default sink. If no
 * solutions are produced and {@link Annotations#OPTIONAL} is <code>true</code>,
 * then the original binding set is copied to the default sink (optional join
 * semantics). Each subquery is run as a separate query but will be cancelled if
 * the parent query is cancelled.
 * <p>
 * This operator does not use internal parallelism, but it is thread-safe and
 * multiple instances of this operator may be run in parallel by the query
 * engine for parallel evaluation of different binding set chunks flowing
 * through the pipeline. However, there are much more efficient query plan
 * patterns for most use cases. E.g., (a) creating a hash index with all source
 * solutions, (b) flooding a sub-section of the query plan with the source
 * solutions from the hash index; and (c) hash joining the solutions from the
 * sub-section of the query plan back against the hash index to reunite the
 * solutions from the subquery with those in the parent context.
 * 
 * <h3>Usage Notes</h3>
 * 
 * If there are no shared variables which must already be bound in the caller,
 * then subquery join is (or may be if there are some "might" be bound
 * variables) the full cross product (constraints are still applied and optional
 * solutions must be reported if a constraint fails and the join is optional).
 * Such subqueries should be run as named subqueries instead so they run once,
 * rather than once per binding set.
 * <p>
 * If there are variables in scope in the parent query which are not projected
 * by the subquery but which appear in the subquery as well, then such variables
 * in the subquery are effectively distinct from those having the same name
 * which appear in the parent query. In order to have correct bottom-up
 * evaluation semantics under these conditions. This is handled by "projecting"
 * only those variables into the subquery which it will project out.
 * 
 * <h3>Efficiency</h3>
 * 
 * This non-vectored operator issues one sub-query per source solution flowing
 * into the operator. In general, it is MUCH more efficient to vector the
 * solutions into a sub-plan. The latter is accomplished by building a hash
 * index over the source solutions, flooding them into the sub-plan, and then
 * executing the appropriate hash join back against the source solutions after
 * the sub-plan.
 * <p>
 * There are a few cases where it may make sense to use the non-vectored
 * operator. For example, for EXISTS where LIMIT ONE can be imposed on the
 * subquery. However, there can still be cases where the vectored sub-plan is
 * more efficient.
 * 
 * @see AbstractSubqueryOp
 * @see JVMNamedSubqueryOp
 * @see HTreeNamedSubqueryOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/988"> bad performance for FILTER
 *      EXISTS </a>
 */
public class SubqueryOp extends PipelineOp {

    private static final Logger log = Logger.getLogger(SubqueryOp.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends SubqueryJoinAnnotations {

        /**
         * When non-<code>null</code>, the {@link IVariable} which will be bound
         * to <code>true</code> iff there is at least one solution for the
         * subquery. When specified, {@link #SELECT} SHOULD be <code>null</code>
         * in order to project all bindings from the parent's context into the
         * subquery. However, bindings in the subquery WILL NOT be projected
         * back into the parent.
         * <p>
         * Note: This supports EXISTS and NOT EXISTS semantics.
         */
        String ASK_VAR = Annotations.class.getName() + ".askVar";

        /**
         * The {@link IVariable}[] projected by the subquery (optional).
         * <p>
         * Rule: A variable within a subquery is distinct from the same name
         * variable outside of the subquery unless the variable is projected
         * from the subquery.
         * <p>
         * When this option is given, only the variables projected by the
         * subquery will be visible during the subquery (this models SPARQL 1.1
         * subquery variable scope semantics). A variable having the same name
         * in the parent context and the subquery which is not projected by the
         * subquery is a distinct variable. This constraint is enforced by
         * passing only the projected variables into the subquery and then
         * merging the unprojected variables from the source solution into each
         * result produced by the subquery for a given source solution.
         * <p>
         * When this option is not given, all variables are passed into the
         * subquery (basically, the semantics are those of <code>SELECT *</code>
         * ).
         * <p>
         * Note: This overrides the semantics of the same named annotation on
         * the {@link JoinAnnotations} interface.
         */
        String SELECT = SubqueryJoinAnnotations.SELECT;
        
        /**
         * Boolean annotation should be <code>true</code> if the subquery is an
         * aggregate (default {@value #DEFAULT_IS_AGGREGATE}).
         * <p>
         * Note: We need to have distinct {@link IAggregate} objects in each
         * subquery issued since they have internal state in order to prevent
         * inappropriate sharing of state across invocations of the subquery.
         */
        String IS_AGGREGATE = Annotations.class.getName() + ".isAggregate";

        boolean DEFAULT_IS_AGGREGATE = false;
        
    }

    /**
     * Deep copy constructor.
     */
    public SubqueryOp(final SubqueryOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public SubqueryOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        getRequiredProperty(Annotations.SUBQUERY);

        final IVariable<?>[] selectVars = (IVariable<?>[]) getProperty(Annotations.SELECT);

        if (selectVars != null && selectVars.length == 0) {

            /*
             * An empty array suggests that nothing is being projected. Either
             * we should reject that here or we should treat it instead as
             * "SELECT *".
             */
            
            throw new IllegalArgumentException(Annotations.SELECT
                    + " is optional, but may not be empty.");
            
        }

        final JoinTypeEnum joinType = (JoinTypeEnum) getRequiredProperty(Annotations.JOIN_TYPE);
        switch (joinType) {
        case Normal:
        case Optional:
            break;
        default:
            throw new UnsupportedOperationException(Annotations.JOIN_TYPE + "="
                    + joinType);
        }

    }

    public SubqueryOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ControllerTask(this, context));
        
    }
    
	/**
	 * Evaluates the subquery for each source binding set. If the controller
	 * operator is interrupted, then the subqueries are cancelled. If a subquery
	 * fails, then all subqueries are cancelled.
	 */
    private static class ControllerTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;
        /** The type of join. */
        private final JoinTypeEnum joinType;
        /** <code>true</code> if the subquery is an aggregate. */
        private final boolean aggregate;
        /** The subquery which is evaluated for each input binding set. */
        private final PipelineOp subquery;
        /** Bound to true or false depending on whether or not there are solutions for the subquery (optional). */
        private final IVariable<?> askVar;
        /** The projected variables (<code>select *</code>) if missing. */
        private final IVariable<?>[] selectVars;
        /** The optional constraints on the join. */
        private final IConstraint[] constraints;
        
        public ControllerTask(final SubqueryOp controllerOp,
                final BOpContext<IBindingSet> context) {

            if (controllerOp == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

//            this.optional = controllerOp.getProperty(
//                    Annotations.OPTIONAL,
//                    Annotations.DEFAULT_OPTIONAL);
            joinType = (JoinTypeEnum) controllerOp
                    .getRequiredProperty(Annotations.JOIN_TYPE);
            
            this.aggregate = controllerOp.getProperty(
                    Annotations.IS_AGGREGATE,
                    Annotations.DEFAULT_IS_AGGREGATE);

            this.subquery = (PipelineOp) controllerOp
                    .getRequiredProperty(Annotations.SUBQUERY);
            
            this.askVar = (IVariable<?>) controllerOp
                    .getProperty(Annotations.ASK_VAR);

            this.selectVars = (IVariable<?>[]) controllerOp
                    .getProperty(Annotations.SELECT);

            this.constraints = (IConstraint[]) controllerOp
                    .getProperty(Annotations.CONSTRAINTS);
            
        }

        /**
         * Evaluate the subquery.
         */
        @Override
        public Void call() throws Exception {
            
            try {

                final ICloseableIterator<IBindingSet[]> sitr = context
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
            private final IBindingSet parentSolutionIn;
            
            /**
             * The root operator for the subquery.
             */
            private final PipelineOp subQueryOp;

            public SubqueryTask(final IBindingSet bset, final PipelineOp subQuery,
                    final BOpContext<IBindingSet> parentContext) {

                this.parentSolutionIn = bset;
                
                if (aggregate) {
                    
                    /*
                     * Note: We need to have distinct IAggregates in subqueries
                     * since they have internal state. This makes a copy of the
                     * subquery in which each IAggregate function is a distinct
                     * instance. This prevents inappropriate sharing of state
                     * across invocations of the subquery.
                     */
                    
                    this.subQueryOp = BOpUtility
                            .makeAggregateDistinct(subQuery);
                    
                } else {

                    this.subQueryOp = subQuery;
                    
                }

                this.parentContext = parentContext;

            }

            public IRunningQuery call() throws Exception {

                /*
                 * Binding set in which only the projected variables are
                 * visible. (if selectVars is empty, then all variables remain
                 * visible.).
                 */
                final IBindingSet childSolutionIn = parentSolutionIn
                        .copy(selectVars);

            	// The subquery
                IRunningQuery runningSubquery = null;
            	// The iterator draining the subquery
                ICloseableIterator<IBindingSet[]> subquerySolutionItr = null;
                try {

                    final QueryEngine queryEngine = parentContext
                            .getRunningQuery().getQueryEngine();

                    if(log.isDebugEnabled())
                        log.debug("\nRunning subquery:" //
                            + "\n        selectVars: "
                            + Arrays.toString(selectVars) //
                            + "\nparentSolution(in): "
                            + parentSolutionIn //
                            + "\n childSolution(in): "
                            + childSolutionIn//
                    );
//                    System.out.println("Running subquery" //
//                            + ": selectVars: "
//                            + Arrays.toString(selectVars) //
//                            + ", parentSolution(in): "
//                            + parentSolutionIn //
//                            + ", childSolution(in): "
//                            + childSolutionIn//
//                    );
                    
                    runningSubquery = queryEngine.eval(subQueryOp,
                            childSolutionIn);

					long ncopied = 0L;
					try {

                        // Declare the child query to the parent.
                        ((AbstractRunningQuery) parentContext.getRunningQuery())
                                .addChild(runningSubquery);

						// Iterator visiting the subquery solutions.
						subquerySolutionItr = runningSubquery.iterator();

                        if (askVar != null) {
                            
                            /*
                             * For an ASK style subquery, we are only interested
                             * in whether or not at least one solution exists.
                             */
                            
                            final IV<BigdataLiteral, Boolean> success = XSDBooleanIV.valueOf(subquerySolutionItr.hasNext());
                            
//                            System.err
//                                    .println("in="
//                                            + childSolutionIn
//                                            + ", out="
//                                            + (subquerySolutionItr.hasNext() ? subquerySolutionItr
//                                                    .next() : "N/A")
//                                            + ", askVar=" + success);
//                            
                            parentSolutionIn.set(askVar,
                                    new Constant<IV<BigdataLiteral, Boolean>>(
                                            success));
                            
                            parentContext.getSink().add(
                                    new IBindingSet[] { parentSolutionIn });
                            
                            // halt the subquery.
                            runningSubquery.cancel(true/*mayInterruptIfRunning*/);
                            
                            ncopied = 1;

                        } else {

                            // Copy solutions from the subquery to the query.
                            ncopied = BOpUtility.copy(//
                                    subquerySolutionItr,// subquery solutions.
                                    parentContext.getSink(), //
                                    null, // sink2
                                    parentSolutionIn,// original bindings from
                                                     // parent query.
                                    selectVars, // variables projected by
                                                // subquery.
                                    constraints, //
                                    parentContext.getStats()//
                                    );
                        }

						// wait for the subquery to halt / test for errors.
						runningSubquery.get();
						
					} catch (InterruptedException ex) {

						// this thread was interrupted, so cancel the subquery.
						runningSubquery
								.cancel(true/* mayInterruptIfRunning */);

						// rethrow the exception.
						throw ex;
						
					}
					
                    if (ncopied == 0L && joinType.isOptional()) {

                        /*
                         * Since there were no solutions for the subquery, copy
                         * the original binding set to the appropriate sink and
                         * do NOT apply the constraints.
                         */

                        final IBlockingBuffer<IBindingSet[]> optionalSink = parentContext
                                .getSink2() != null ? parentContext.getSink2()
                                : parentContext.getSink();

                        optionalSink
                                .add(new IBindingSet[] { parentSolutionIn });

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
