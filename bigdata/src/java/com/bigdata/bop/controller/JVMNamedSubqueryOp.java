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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.JVMHashJoinAnnotations;
import com.bigdata.bop.join.JVMHashJoinUtility;
import com.bigdata.bop.join.JVMSolutionSetHashJoinOp;
import com.bigdata.htree.HTree;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

import cutthecrap.utils.striterators.SingleValueIterator;

/**
 * Evaluation of a subquery, producing a named result set. This operator passes
 * through any source binding sets without modification. The subquery is
 * evaluated exactly once, the first time this operator is invoked for a given
 * query plan. No bindings are pushed into the subquery. If some variables are
 * known to be bound, then they should be rewritten into constants or their
 * bindings should be inserted into the subquery using LET() operator.
 * <p>
 * This operator is NOT thread-safe. It relies on the query engine to provide
 * synchronization for the "run-once" contract of the subquery. The operator
 * MUST be run on the query controller.
 * 
 * @see JVMSolutionSetHashJoinOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class JVMNamedSubqueryOp extends PipelineOp {

    static private final transient Logger log = Logger
            .getLogger(JVMNamedSubqueryOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends SubqueryAnnotations,
            JVMHashJoinAnnotations, NamedSetAnnotations {

    }

    /**
     * Deep copy constructor.
     */
    public JVMNamedSubqueryOp(final JVMNamedSubqueryOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public JVMNamedSubqueryOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        if (getEvaluationContext() != BOpEvaluationContext.CONTROLLER) {
            throw new IllegalArgumentException(
                    BOp.Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

        if (getMaxParallel() != 1) {
            throw new IllegalArgumentException(
                    PipelineOp.Annotations.MAX_PARALLEL + "="
                            + getMaxParallel());
        }

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

    public JVMNamedSubqueryOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }
    
    @Override
    public BOpStats newStats() {

        return new NamedSolutionSetStats();

    }

    /**
     * Adds reporting for the size of the named solution set.
     */
    public static class NamedSolutionSetStats extends BOpStats {
        
        private static final long serialVersionUID = 1L;
        
        final AtomicLong solutionSetSize = new AtomicLong();

        public void add(final BOpStats o) {

            super.add(o);

            if (o instanceof NamedSolutionSetStats) {

                final NamedSolutionSetStats t = (NamedSolutionSetStats) o;

                solutionSetSize.addAndGet(t.solutionSetSize.get());

            }

        }

        @Override
        protected void toString(final StringBuilder sb) {
            super.toString(sb);
            sb.append(",solutionSetSize=" + solutionSetSize.get());
        }

    }
    
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
        private final NamedSolutionSetRef namedSetRef;

        /**
         * The {@link IQueryAttributes} for the {@link IRunningQuery} off which
         * we will hang the named solution set.
         */
        private final IQueryAttributes attrs;
        
//        /**
//         * The join variables.
//         */
//        @SuppressWarnings("rawtypes")
//        private final IVariable[] joinVars;
        
        /**
         * <code>true</code> iff this is the first time the task is being
         * invoked, in which case we will evaluate the subquery and save its
         * result set on {@link #solutions}.
         */
        private final boolean first;
        
        private final JVMHashJoinUtility state;

        public ControllerTask(final JVMNamedSubqueryOp op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.stats = ((NamedSolutionSetStats) context.getStats());
            
            this.subquery = (PipelineOp) op
                    .getRequiredProperty(Annotations.SUBQUERY);

            this.namedSetRef = (NamedSolutionSetRef) op
                    .getRequiredProperty(Annotations.NAMED_SET_REF);
            
            {

                /*
                 * First, see if the map already exists.
                 * 
                 * Note: Since the operator is not thread-safe, we do not need
                 * to use a putIfAbsent pattern here.
                 */
                
                // Lookup the attributes for the query on which we will hang the
                // solution set.
                attrs = context.getQueryAttributes(namedSetRef.queryId);

                JVMHashJoinUtility state = (JVMHashJoinUtility) attrs
                        .get(namedSetRef);

                if (state == null) {

                    /*
                     * Note: This operator does not support optional semantics.
                     */
                    state = new JVMHashJoinUtility(op, false/* optional */,
                            false/* filter */);

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
        public Void call() throws Exception {
            
            try {
                
                final IBindingSet[] a = BOpUtility.toArray(context.getSource(),
                        stats);
                
                if(first) {

                    final IBindingSet tmp;
                    if(a.length != 1) {
                        // Unbound if more than one source solution (should not happen).
                        tmp = new ListBindingSet();
                    } else {
                        // Only one solution.
                        tmp = a[0];
                    }
                    
                    // Generate the result set and write it on the HTree.
                    new SubqueryTask(tmp, subquery, context).call();

                }

                // source.
//                final IAsynchronousIterator<IBindingSet[]> source = context
//                        .getSource();
                @SuppressWarnings("unchecked")
                final Iterator<IBindingSet[]> source = new SingleValueIterator(a);

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
//                        context.getStats()//
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
             * The source binding set.
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

            public Void call() throws Exception {

            	// The subquery
                IRunningQuery runningSubquery = null;
            	// The iterator draining the subquery
                IAsynchronousIterator<IBindingSet[]> subquerySolutionItr = null;
                try {

                    final QueryEngine queryEngine = parentContext.getRunningQuery()
                            .getQueryEngine();
                    
                    runningSubquery = queryEngine.eval((PipelineOp) subQueryOp,
                            bset);

					try {

                        // Declare the child query to the parent.
                        ((AbstractRunningQuery) parentContext.getRunningQuery())
                                .addChild(runningSubquery);

						// Iterator visiting the subquery solutions.
						subquerySolutionItr = runningSubquery.iterator();

						// Buffer the solutions on the hash index.
                        final long ncopied = state.acceptSolutions(
                                subquerySolutionItr, stats);
//                        final long ncopied = JVMHashJoinUtility.acceptSolutions(
//                                subquerySolutionItr, joinVars, stats,
//                                solutions, false/* optional */);

						// Wait for the subquery to halt / test for errors.
						runningSubquery.get();

                        // Report the #of solutions in the named solution set.
                        stats.solutionSetSize.addAndGet(ncopied);

//                        // Publish the solution set on the query context.
//                        saveSolutionSet();

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
