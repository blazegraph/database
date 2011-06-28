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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bset.Tee;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.util.concurrent.LatchedExecutor;

/**
 * Executes each of the operands as a subquery. The operands are evaluated in
 * the order given and with the annotated parallelism. Each subquery is run as a
 * separate query but is linked to the parent query in the operator is being
 * evaluated. The subqueries receive bindings from the pipeline and may be
 * executed independently. By default, the subqueries are run with unlimited
 * parallelism. Since the #of subqueries is generally small (2), this means that
 * the subqueries run in parallel.
 * <p>
 * Note: This operator must execute on the query controller.
 * <p>
 * If you want to route intermediate results from other computations into
 * subqueries, then consider a {@link Tee} pattern instead.
 * <p>
 * For example:
 * 
 * <pre>
 * SLICE[1](
 *   UNION[2]([...],{subqueries=[a,b,c]})
 *   )
 * </pre>
 * 
 * Will run the subqueries <i>a</i>, <i>b</i>, and <i>c</i> in parallel. Each
 * subquery will be run once for each source {@link IBindingSet}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo There is relatively little difference between this class and SubqueryOp
 *       and we should consider converging them into a single concrete subquery
 *       operator with specializations for UNION and STEPS. The main difference
 *       is that the SubqueryOp can not run multiple subqueries.
 */
abstract public class AbstractSubqueryOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

		/**
		 * The ordered {@link BOp}[] of subqueries to be evaluated for each
		 * binding set presented (required).
		 */
		String SUBQUERIES = (AbstractSubqueryOp.class.getName() + ".subqueries")
				.intern();

		/**
		 * The maximum parallelism with which the subqueries will be evaluated
		 * (default is unlimited).
		 */
		String MAX_PARALLEL_SUBQUERIES = (AbstractSubqueryOp.class.getName() + ".maxParallelSubqueries")
				.intern();

		int DEFAULT_MAX_PARALLEL_SUBQUERIES = Integer.MAX_VALUE;

    }

    /**
     * @see Annotations#MAX_PARALLEL_SUBQUERIES
     */
    public int getMaxParallelSubqueries() {
        return getProperty(Annotations.MAX_PARALLEL_SUBQUERIES,
                Annotations.DEFAULT_MAX_PARALLEL_SUBQUERIES);
    }
    
    /**
     * Deep copy constructor.
     */
    public AbstractSubqueryOp(final AbstractSubqueryOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public AbstractSubqueryOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

//        if (!getEvaluationContext().equals(BOpEvaluationContext.CONTROLLER))
//            throw new IllegalArgumentException(Annotations.EVALUATION_CONTEXT
//                    + "=" + getEvaluationContext());

//        if (!getProperty(Annotations.CONTROLLER, Annotations.DEFAULT_CONTROLLER))
//            throw new IllegalArgumentException(Annotations.CONTROLLER);
 
        // verify required annotation.
        final BOp[] subqueries = (BOp[]) getRequiredProperty(Annotations.SUBQUERIES);

        if (subqueries.length == 0)
            throw new IllegalArgumentException(Annotations.SUBQUERIES);

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

        private final AbstractSubqueryOp controllerOp;
        private final BOp[] subqueries;
        private final BOpContext<IBindingSet> context;
        private final int nparallel;
        private final Executor executor;

        public ControllerTask(final AbstractSubqueryOp controllerOp,
                final BOpContext<IBindingSet> context) {

            if (controllerOp == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.controllerOp = controllerOp;
            
            this.context = context;

            this.subqueries = (BOp[]) controllerOp
                    .getRequiredProperty(Annotations.SUBQUERIES);

            this.nparallel = controllerOp.getProperty(Annotations.MAX_PARALLEL_SUBQUERIES,
                    Annotations.DEFAULT_MAX_PARALLEL_SUBQUERIES);

            this.executor = new LatchedExecutor(context.getIndexManager()
                    .getExecutorService(), nparallel);
            
        }

        /**
         * Evaluate the subqueries with limited parallelism.
         */
        public Void call() throws Exception {

            final IAsynchronousIterator<IBindingSet[]> source = context
                    .getSource();

            try {

                while (source.hasNext()) {

                    final IBindingSet[] chunk = source.next();
                    
                    for (IBindingSet bset : chunk) {

                        consumeBindingSet(bset);

                    }
 
                }

                // Now that we know the subqueries ran Ok, flush the sink.
                context.getSink().flush();
                
                // Done.
                return null;

            } finally {

                // Close the source.
                source.close();

                context.getSink().close();
                
                if (context.getSink2() != null)
                    context.getSink2().close();

            }
            
        }

        private void consumeBindingSet(final IBindingSet bset)
                throws InterruptedException, ExecutionException {

            final List<FutureTask<IRunningQuery>> tasks = new LinkedList<FutureTask<IRunningQuery>>();
            
            try {

                final CountDownLatch latch = new CountDownLatch(
                        subqueries.length);

                /*
                 * Create FutureTasks for each subquery. The futures are not
                 * submitted to the Executor yet. That happens in call(). By
                 * deferring the evaluation until call() we gain the ability to
                 * cancel all subqueries if any subquery fails.
                 */
                for (BOp op : subqueries) {

                    /*
                     * Task runs subquery and cancels all subqueries in [tasks]
                     * if it fails.
                     */
                    tasks.add(new FutureTask<IRunningQuery>(new SubqueryTask(
                            op, context, bset)) {
                        /*
                         * Hook future to count down the latch when the task is
                         * done.
                         */
                        public void run() {
                            try {
                                super.run();
                            } finally {
                                latch.countDown();
                            }
                        }
                    });

                }

                /*
                 * Run subqueries with limited parallelism.
                 */
                for (FutureTask<IRunningQuery> ft : tasks) {
                    executor.execute(ft);
                }

                /*
                 * Wait for all subqueries to complete.
                 */
                latch.await();

                /*
                 * Get the futures, throwing out any errors.
                 */
                for (FutureTask<IRunningQuery> ft : tasks)
                    ft.get();

            } finally {

                // Cancel any tasks which are still running.
                for (FutureTask<IRunningQuery> ft : tasks)
                    ft.cancel(true/* mayInterruptIfRunning */);

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
             * The root operator for the subquery.
             */
            private final BOp subQueryOp;

            /**
             * The input for this invocation of the subquery.
             */
            private final IBindingSet bset;
            
            public SubqueryTask(final BOp subQuery,
                    final BOpContext<IBindingSet> parentContext,
                    final IBindingSet bset) {

                this.subQueryOp = subQuery;

                this.parentContext = parentContext;
                
                this.bset = bset;

            }

            public IRunningQuery call() throws Exception {

            	IRunningQuery runningSubquery = null;
                IAsynchronousIterator<IBindingSet[]> subquerySolutionItr = null;
                try {

                    final QueryEngine queryEngine = parentContext.getRunningQuery()
                            .getQueryEngine();

					runningSubquery = queryEngine.eval(subQueryOp, bset);

                    // Iterator visiting the subquery solutions.
                    subquerySolutionItr = runningSubquery.iterator();

                    // Copy solutions from the subquery to the query.
                    BOpUtility.copy(subquerySolutionItr, parentContext
                            .getSink(), null/* sink2 */, null/* select */,
                            null/* constraints */, null/* stats */);
                    
                    // wait for the subquery.
                    runningSubquery.get();

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

                    if (subquerySolutionItr != null)
                        subquerySolutionItr.close();

                }

            }

        } // SubqueryTask

    } // ControllerTask

}
