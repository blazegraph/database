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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.ISingleThreadedOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.HTreeHashJoinAnnotations;
import com.bigdata.bop.join.HTreeHashJoinUtility;
import com.bigdata.bop.join.HTreeSolutionSetHashJoinOp;
import com.bigdata.bop.join.JoinTypeEnum;
import com.bigdata.bop.join.NamedSolutionSetStats;
import com.bigdata.relation.accesspath.IBlockingBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.SingleValueIterator;

/**
 * Evaluation of a subquery, producing a named result set. This operator passes
 * through any source binding sets without modification. The subquery is
 * evaluated exactly once, the first time this operator is invoked for a given
 * query plan. If some variables are known to be bound, then they should be
 * rewritten into constants or their bindings should be inserted into the
 * subquery using LET() operator.
 * <p>
 * This operator is NOT thread-safe. It relies on the query engine to provide
 * synchronization for the "run-once" contract of the subquery. The operator
 * MUST be run on the query controller.
 * 
 * @see HTreeSolutionSetHashJoinOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HTreeNamedSubqueryOp extends PipelineOp implements
        INamedSubqueryOp, ISingleThreadedOp {

    static private final transient Logger log = Logger
            .getLogger(HTreeNamedSubqueryOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends SubqueryAnnotations,
            HTreeHashJoinAnnotations, NamedSetAnnotations {

//        /**
//         * The name of {@link IQueryAttributes} attribute under which the
//         * subquery solution set is stored (a {@link HTreeHashJoinState}
//         * reference). The attribute name includes the query UUID. The query
//         * UUID must be extracted and used to lookup the {@link IRunningQuery}
//         * to which the solution set was attached.
//         * 
//         * @see NamedSolutionSetRef
//         */
//        final String NAMED_SET_REF = "namedSetRef";
        
    }

    /**
     * Deep copy constructor.
     */
    public HTreeNamedSubqueryOp(final HTreeNamedSubqueryOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public HTreeNamedSubqueryOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        if (getEvaluationContext() != BOpEvaluationContext.CONTROLLER) {
            throw new IllegalArgumentException(
                    BOp.Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

        assertMaxParallelOne();

        if (!isAtOnceEvaluation())
            throw new IllegalArgumentException();

        getRequiredProperty(Annotations.SUBQUERY);

        getRequiredProperty(Annotations.NAMED_SET_REF);

        // Join variables must be specified.
        final IVariable<?>[] joinVars = (IVariable[]) getRequiredProperty(Annotations.JOIN_VARS);

//        if (joinVars.length == 0)
//            throw new IllegalArgumentException(Annotations.JOIN_VARS);

        for (IVariable<?> var : joinVars) {

            if (var == null)
                throw new IllegalArgumentException(Annotations.JOIN_VARS);

        }

    }

    public HTreeNamedSubqueryOp(final BOp[] args, final NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }
    
    @Override
    public BOpStats newStats() {

        return new NamedSolutionSetStats();

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

        private final NamedSolutionSetStats stats;
        
        /** The subquery which is evaluated for each input binding set. */
        private final PipelineOp subquery;
        
        /** Metadata to identify the named solution set. */
        private final INamedSolutionSetRef namedSetRef;

        /**
         * The {@link IQueryAttributes} for the {@link IRunningQuery} off which
         * we will hang the named solution set.
         */
        private final IQueryAttributes attrs;
        
        /**
         * <code>true</code> iff this is the first time the task is being
         * invoked, in which case we will evaluate the subquery and save its
         * result set on {@link #solutions}.
         */
        private final boolean first;

        private final HTreeHashJoinUtility state;
        
        public ControllerTask(final HTreeNamedSubqueryOp op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.stats = ((NamedSolutionSetStats) context.getStats());
            
            this.subquery = (PipelineOp) op
                    .getRequiredProperty(Annotations.SUBQUERY);

            this.namedSetRef = (INamedSolutionSetRef) op
                    .getRequiredProperty(Annotations.NAMED_SET_REF);
            
            {

                /*
                 * First, see if the map already exists.
                 * 
                 * Note: Since the operator is not thread-safe, we do not need
                 * to use a putIfAbsent pattern here.
                 */
                
                /*
    			 * Lookup the attributes for the query on which we will hang the
    			 * solution set. See BLZG-1493 (if queryId is null, use the query
    			 * attributes for this running query).
    			 */
				attrs = context.getQueryAttributes(namedSetRef.getQueryId());

                HTreeHashJoinUtility state = (HTreeHashJoinUtility) attrs
                        .get(namedSetRef);

                if (state == null) {

                    /*
                     * Note: This operator does not support optional semantics.
                     */
                    state = new HTreeHashJoinUtility(
                            context.getMemoryManager(namedSetRef.getQueryId()),
                            op, JoinTypeEnum.Normal);

                    if (attrs.putIfAbsent(namedSetRef, state) != null)
                        throw new AssertionError();

                    this.first = true;
                    
                } else {
                    
                    this.first = false;
                    
                }

                this.state = state;

            }
            
        }

        /**
         * Evaluate.
         */
        @Override
        public Void call() throws Exception {
            
            try {

                final IBindingSet[] bindingSets = BOpUtility.toArray(
                        context.getSource(), stats);

                if(first) {

//                    final IBindingSet tmp;
//                    if(bindingSets.length != 1) {
//                        // Unbound if more than one source solution (should not happen).
//                        tmp = new ListBindingSet();
//                    } else {
//                        // Only one solution.
//                        tmp = bindingSets[0];
//                    }

                    // Generate the result set and write it on the HTree.
                    new SubqueryTask(bindingSets, subquery, context).call();

                }

                // source.
                final Iterator<IBindingSet[]> source = new SingleValueIterator<IBindingSet[]>(bindingSets);

                // default sink
                final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

                BOpUtility.copy(//
                        source, //
                        sink,//
                        null, // sink2
                        null, // mergeSolution (aka parent's source solution).
                        null, // selectVars (aka projection).
                        null, // constraints
                        null  // stats were updated above.
                        //context.getStats()//
                        );

                sink.flush();

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
         */
        private class SubqueryTask implements Callable<Void> {

            /**
             * The evaluation context for the parent query.
             */
            private final BOpContext<IBindingSet> parentContext;

            /**
             * The source binding sets.
             */
            private final IBindingSet[] bindingSets;

            /**
             * The root operator for the subquery.
             */
            private final BOp subQueryOp;

            public SubqueryTask(final IBindingSet[] bindingSets,
                    final BOp subQuery,
                    final BOpContext<IBindingSet> parentContext) {

                this.bindingSets = bindingSets;
                
                this.subQueryOp = subQuery;

                this.parentContext = parentContext;

            }

            @Override
            public Void call() throws Exception {

            	// The subquery
                IRunningQuery runningSubquery = null;
            	// The iterator draining the subquery
                ICloseableIterator<IBindingSet[]> subquerySolutionItr = null;
                try {

                    final QueryEngine queryEngine = parentContext.getRunningQuery()
                            .getQueryEngine();
                    
                    runningSubquery = queryEngine.eval((PipelineOp) subQueryOp,
                            bindingSets);

					try {

					    // Declare the child query to the parent.
                        ((AbstractRunningQuery) parentContext.getRunningQuery())
                                .addChild(runningSubquery);
					    
						// Iterator visiting the subquery solutions.
						subquerySolutionItr = runningSubquery.iterator();

                        // Buffer the solutions on the hash index.
                        final long ncopied = state.acceptSolutions(
                                subquerySolutionItr, stats);

						// Wait for the subquery to halt / test for errors.
						runningSubquery.get();

                        // Report the #of solutions in the named solution set.
                        stats.solutionSetSize.add(ncopied);

                        // Checkpoint the solution set.
                        state.saveSolutionSet();

                        if (log.isInfoEnabled())
                            log.info("Solution set " + namedSetRef + " has "
                                    + ncopied + " solutions.");
                        
					} catch (InterruptedException ex) {

						// this thread was interrupted, so cancel the subquery.
						runningSubquery
								.cancel(true/* mayInterruptIfRunning */);

						// rethrow the exception.
						throw ex;
						
					}
					
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

                // Done.
                return null;
                
            }

        } // SubqueryTask

    } // ControllerTask

}
