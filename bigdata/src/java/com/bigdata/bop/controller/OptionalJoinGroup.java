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
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.LocalChunkMessage;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.util.concurrent.LatchedExecutor;

/**
 * For each binding set presented, this operator executes a subquery. Any
 * solutions produced by the subquery are copied to the default sink. If no
 * solutions are produced, then the original binding set is copied to the
 * default sink (optional join semantics). Each subquery is run as a separate
 * query but will be cancelled if the parent query is cancelled.
 * 
 * FIXME Parallel evaluation of subqueries is not implemented. What is the
 * appropriate parallelism for this operator? More parallelism should reduce
 * latency but could increase the memory burden. Review this decision once we
 * have the RWStore operating as a binding set buffer on the Java process heap.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class OptionalJoinGroup extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The subquery to be evaluated for each binding sets presented to the
         * {@link OptionalJoinGroup} (required). This should be a
         * {@link PipelineOp}.
         */
        String SUBQUERY = OptionalJoinGroup.class.getName() + ".subquery";

        /**
         * When <code>true</code> the subquery has optional semantics (if the
         * subquery fails, the original binding set will be passed along to the
         * downstream sink anyway).
         */
        String OPTIONAL = OptionalJoinGroup.class.getName() + ".optional";

        boolean DEFAULT_OPTIONAL = true;
        
        /**
         * The maximum parallelism with which the subqueries will be evaluated
         * (default {@value #DEFAULT_MAX_PARALLEL}). 
         */
        String MAX_PARALLEL = OptionalJoinGroup.class.getName()
                + ".maxParallel";

        int DEFAULT_MAX_PARALLEL = 1;

    }

    /**
     * @see Annotations#MAX_PARALLEL
     */
    public int getMaxParallel() {
        return getProperty(Annotations.MAX_PARALLEL,
                Annotations.DEFAULT_MAX_PARALLEL);
    }
    
    /**
     * Deep copy constructor.
     */
    public OptionalJoinGroup(final OptionalJoinGroup op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public OptionalJoinGroup(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

//        if (!getEvaluationContext().equals(BOpEvaluationContext.CONTROLLER))
//            throw new IllegalArgumentException(Annotations.EVALUATION_CONTEXT
//                    + "=" + getEvaluationContext());

        getRequiredProperty(Annotations.SUBQUERY);
        
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

    public OptionalJoinGroup(final BOp[] args, NV... annotations) {

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

        private final OptionalJoinGroup controllerOp;
        private final BOpContext<IBindingSet> context;
//        private final List<FutureTask<IRunningQuery>> tasks = new LinkedList<FutureTask<IRunningQuery>>();
//        private final CountDownLatch latch;
        private final boolean optional;
        private final int nparallel;
        private final PipelineOp subquery;
        private final Executor executor;
        
        public ControllerTask(final OptionalJoinGroup controllerOp, final BOpContext<IBindingSet> context) {

            if (controllerOp == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.controllerOp = controllerOp;
            
            this.context = context;

            this.optional = controllerOp.getProperty(Annotations.OPTIONAL,
                    Annotations.DEFAULT_OPTIONAL);

            this.nparallel = controllerOp.getProperty(Annotations.MAX_PARALLEL,
                    Annotations.DEFAULT_MAX_PARALLEL);

            this.subquery = (PipelineOp) controllerOp
                    .getRequiredProperty(Annotations.SUBQUERY);
            
            this.executor = new LatchedExecutor(context.getIndexManager()
                    .getExecutorService(), nparallel);
            
//            this.latch = new CountDownLatch(controllerOp.arity());

//            /*
//             * Create FutureTasks for each subquery. The futures are submitted
//             * to the Executor yet. That happens in call(). By deferring the
//             * evaluation until call() we gain the ability to cancel all
//             * subqueries if any subquery fails.
//             */
//            for (BOp op : controllerOp.args()) {
//
//                /*
//                 * Task runs subquery and cancels all subqueries in [tasks] if
//                 * it fails.
//                 */
//                tasks.add(new FutureTask<IRunningQuery>(new SubqueryTask(op,
//                        context)) {
//                    /*
//                     * Hook future to count down the latch when the task is
//                     * done.
//                     */
//                    public void run() {
//                        try {
//                            super.run();
//                        } finally {
//                            latch.countDown();
//                        }
//                    }
//                });
//
//            }

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

                        FutureTask<IRunningQuery> ft = new FutureTask<IRunningQuery>(
                                new SubqueryTask(bset, subquery, context));

                        // run the subquery.
                        executor.execute(ft);

                        try {

                            // wait for the outcome.
                            ft.get();

                        } finally {
                            
                            /*
                             * Ensure that the inner task is cancelled if the
                             * outer task is interrupted.
                             */
                            ft.cancel(true/* mayInterruptIfRunning */);
                            
                        }
                        
                    }
                    
                }
                
//                /*
//                 * Run subqueries with limited parallelism.
//                 */
//                for (FutureTask<IRunningQuery> ft : tasks) {
//                    executor.execute(ft);
//                }
//
//                /*
//                 * Wait for all subqueries to complete.
//                 */
//                latch.await();
//
//                /*
//                 * Get the futures, throwing out any errors.
//                 */
//                for (FutureTask<IRunningQuery> ft : tasks)
//                    ft.get();

                // Now that we know the subqueries ran Ok, flush the sink.
                context.getSink().flush();
                
                // Done.
                return null;

            } finally {

//                // Cancel any tasks which are still running.
//                for (FutureTask<IRunningQuery> ft : tasks)
//                    ft.cancel(true/* mayInterruptIfRunning */);
                
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

                IAsynchronousIterator<IBindingSet[]> subquerySolutionItr = null;
                try {

                    final QueryEngine queryEngine = parentContext.getRunningQuery()
                            .getQueryEngine();

//                    final IRunningQuery runningQuery = queryEngine
//                            .eval(subQueryOp);

                    final BOp startOp = BOpUtility.getPipelineStart(subQueryOp);

                    final int startId = startOp.getId();
                    
                    final UUID queryId = UUID.randomUUID();

                    // execute the subquery, passing in the source binding set.
                    final IRunningQuery runningQuery = queryEngine
                            .eval(
                                    queryId,
                                    (PipelineOp) subQueryOp,
                                    new LocalChunkMessage<IBindingSet>(
                                            queryEngine,
                                            queryId,
                                            startId,
                                            -1 /* partitionId */,
                                            new ThickAsynchronousIterator<IBindingSet[]>(
                                                    new IBindingSet[][] { new IBindingSet[] { bset } })));

                    // Iterator visiting the subquery solutions.
                    subquerySolutionItr = runningQuery.iterator();

                    // Copy solutions from the subquery to the query.
                    final long ncopied = BOpUtility.copy(subquerySolutionItr,
                            parentContext.getSink(), null/* sink2 */,
                            null/* constraints */, null/* stats */);
                    
                    // wait for the subquery.
                    runningQuery.get();

                    if (ncopied == 0L && optional) {

                        /*
                         * Since there were no solutions for the subquery, copy
                         * the original binding set to the default sink.
                         */
                        parentContext.getSink().add(new IBindingSet[]{bset});
                        
                    }
                    
                    // done.
                    return runningQuery;
                    
                } catch (Throwable t) {

                    /*
                     * If a subquery fails, then propagate the error to the
                     * parent and rethrow the first cause error out of the
                     * subquery.
                     */
                    throw new RuntimeException(ControllerTask.this.context
                            .getRunningQuery().halt(t));

                } finally {

                    if (subquerySolutionItr != null)
                        subquerySolutionItr.close();

                }

            }

        } // SubqueryTask

    } // ControllerTask

}
