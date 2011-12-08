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
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.BaseJoinStats;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.bop.join.JoinTypeEnum;
import com.bigdata.bop.join.JVMHashJoinUtility;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.striterator.Dechunkerator;

/**
 * Hash join with subquery.
 * <p>
 * All source solutions are fully materialized in a hash table. The keys of the
 * hash table are the as-bound join variable(s). The values in the hash table is
 * the list of solutions having a specific value for the as-bound join
 * variables. Once all solutions are materialized, the subquery is evaluated
 * once.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: HTreeHashJoinOp.java 5033 2011-08-16 19:02:04Z thompsonbry $
 * 
 * @deprecated by a different pattern for handling subquery evaluation.
 */
public class SubqueryHashJoinOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static private final transient Logger log = Logger
            .getLogger(SubqueryHashJoinOp.class);

    public interface Annotations extends SubqueryJoinAnnotations,
            HashJoinAnnotations, HashMapAnnotations {
	
    }
    
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

        final IVariable<?>[] joinVars = (IVariable[]) getRequiredProperty(Annotations.JOIN_VARS);

//        if (joinVars.length == 0)
//            throw new IllegalArgumentException(Annotations.JOIN_VARS);

        for (IVariable<?> var : joinVars) {

            if (var == null)
                throw new IllegalArgumentException(Annotations.JOIN_VARS);

		}

		getRequiredProperty(Annotations.SUBQUERY);

		assertAtOnceJavaHeapOp();

    }

    public SubqueryHashJoinOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

//    /**
//     * @see HashMapAnnotations#INITIAL_CAPACITY
//     */
//    public int getInitialCapacity() {
//
//        return getProperty(HashMapAnnotations.INITIAL_CAPACITY,
//                HashMapAnnotations.DEFAULT_INITIAL_CAPACITY);
//
//    }
//
//    /**
//     * @see HashMapAnnotations#LOAD_FACTOR
//     */
//    public float getLoadFactor() {
//
//        return getProperty(HashMapAnnotations.LOAD_FACTOR,
//                HashMapAnnotations.DEFAULT_LOAD_FACTOR);
//
//    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ControllerTask(this, context));
        
    }

    @Override
    public BaseJoinStats newStats() {

        return new BaseJoinStats();

    }

    /**
     * Evaluation task.
     */
    private static class ControllerTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * The operator which is being evaluated.
         */
        private final SubqueryHashJoinOp joinOp;
        
//        /**
//         * The join variables.
//         * 
//         * @see SubqueryHashJoinOp.Annotations#JOIN_VARS
//         */
//        private final IVariable<?>[] joinVars;
//
//        /**
//         * The variables to be retained by the join operator. Variables not
//         * appearing in this list will be stripped before writing out the
//         * binding set onto the output sink(s).
//         * 
//         * @see SubqueryHashJoinOp.Annotations#SELECT
//         */
//        final private IVariable<?>[] selectVars;
//
//        /**
//         * An array of constraints to be applied to the generated solutions
//         * (optional).
//         * 
//         * @see SubqueryHashJoinOp.Annotations#CONSTRAINTS
//         */
//        final private IConstraint[] constraints;
        
        /**
         * The subquery to be evaluated.
         * 
         * @see SubqueryHashJoinOp.Annotations#SUBQUERY
         */
        private final PipelineOp subquery;
        
//        /**
//         * <code>true</code> iff the subquery has OPTIONAL semantics.
//         * 
//         * @see IPredicate.Annotations#OPTIONAL
//         */
//        private final boolean optional;

        private final JVMHashJoinUtility state;
        
        /**
         * Where the join results are written.
         * <p>
         * Solutions are written on a {@link UnsyncLocalOutputBuffer}, which
         * converts them into chunks. Those {@link UnsyncLocalOutputBuffer}
         * overflows onto the {@link #sink}.
         */
        final private IBlockingBuffer<IBindingSet[]> sink;

        /**
         * The alternative sink to use when the join is
         * {@link JoinTypeEnum#Optional} AND {@link BOpContext#getSink2()}
         * returns a distinct buffer for the alternative sink. The binding sets
         * from the source are copied onto the alternative sink for an optional
         * join if the join fails. Normally the {@link BOpContext#getSink()} can
         * be used for both the joins which succeed and those which fail. The
         * alternative sink is only necessary when the failed join needs to jump
         * out of a join group rather than routing directly to the ancestor in
         * the operator tree.
         */
        final private IBlockingBuffer<IBindingSet[]> sink2;

        public ControllerTask(final SubqueryHashJoinOp op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.joinOp = op;
            
//            this.joinVars = (IVariable<?>[]) joinOp
//                    .getRequiredProperty(Annotations.JOIN_VARS);
//
//            this.selectVars = (IVariable<?>[]) joinOp
//                    .getProperty(Annotations.SELECT);
//
//            this.constraints = joinOp.getProperty(
//                    Annotations.CONSTRAINTS, null/* defaultValue */);

            this.subquery = (PipelineOp) op
                    .getRequiredProperty(Annotations.SUBQUERY);

            final boolean optional = op.getProperty(Annotations.OPTIONAL,
                    Annotations.DEFAULT_OPTIONAL);

            this.state = new JVMHashJoinUtility(op,
                    optional ? JoinTypeEnum.Optional : JoinTypeEnum.Normal);

            this.sink = context.getSink();

            this.sink2 = context.getSink2();
            
        }

        public Void call() throws Exception {

//            if (log.isDebugEnabled())
//                log.debug("Evaluating subquery hash join: " + joinOp);

            final BaseJoinStats stats = (BaseJoinStats) context.getStats();

            final QueryEngine queryEngine = context.getRunningQuery()
                    .getQueryEngine();

            try {

//                /*
//                 * Materialize the binding sets and populate a hash map.
//                 */
//                final Map<Key, Bucket> map = new LinkedHashMap<Key, Bucket>(//
//                        joinOp.getInitialCapacity(),//
//                        joinOp.getLoadFactor()//
//                );

                state.acceptSolutions(context.getSource(), stats);
                
//                JVMHashJoinUtility.acceptSolutions(context.getSource(),
//                        joinVars, stats, map, optional);

                /*
                 * Run the subquery once.
                 * 
                 * TODO We may want to use hash-joins at a position other than
                 * the head of the query plan, in which case we would invoke the
                 * hash join once per input binding set and the input bindings
                 * would be passed into the subquery. [I do not believe that
                 * this can be reconciled with "at-once" evaluation]
                 */

                final IRunningQuery runningSubquery = queryEngine.eval(
                        (PipelineOp) subquery);

                try {

                    if (log.isDebugEnabled())
                        log.debug("Running subquery...");
                    
                    /*
                     * For each solution for the subquery, probe the hash map.
                     * If there is a hit, output the cross product of the
                     * solution with the solutions in the map having the same
                     * as-bound values for their join vars.
                     * 
                     * When outputting a solution, first test the constraints.
                     * If they are satisfied, then output the SELECTed
                     * variables.
                     */

                    final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                            joinOp.getChunkCapacity(), sink);

                    // Thread-local buffer iff optional sink is in use.
                    final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = sink2 == null ? null
                            : new UnsyncLocalOutputBuffer<IBindingSet>(
                                    joinOp.getChunkCapacity(), sink2);

                    // The iterator draining the subquery
                    final IAsynchronousIterator<IBindingSet[]> subquerySolutionItr = runningSubquery
                            .iterator();

                    state.hashJoin(new Dechunkerator<IBindingSet>(
                            subquerySolutionItr),// leftItr
                            unsyncBuffer// outputBuffer,
//                            true// leftIsPipeline
                    );

//                    JVMHashJoinUtility
//                            .hashJoin(new Dechunkerator<IBindingSet>(
//                                    subquerySolutionItr),
//                                    unsyncBuffer/* outputBuffer */, joinVars,
//                                    selectVars, constraints, map, optional,
//                                    true/* leftIsPipeline */);
                    
                    if (state.getJoinType().isOptional()) {

                        final IBuffer<IBindingSet> outputBuffer;
                        if (unsyncBuffer2 == null) {
                            // use the default sink.
                            outputBuffer = unsyncBuffer;
                        } else {
                            // use the alternative sink.
                            outputBuffer = unsyncBuffer2;
                        }

                        state.outputOptionals(outputBuffer);
                        
//                        JVMHashJoinUtility.outputOptionals(outputBuffer, map);

                        if (sink2 != null) {
                            unsyncBuffer2.flush();
                            sink2.flush();
                        }

                    } // if(optional)
                    
                    /*
                     * Flush the output.
                     */
                    unsyncBuffer.flush();
                    sink.flush();
                    
                } catch (Throwable t) {

                    if (runningSubquery.getCause() != null) {
                        /*
                         * If a subquery fails (due to abnormal termination),
                         * then propagate the error to the parent and rethrow
                         * the first cause error out of the subquery.
                         * 
                         * Note: IHaltable#getCause() considers exceptions
                         * triggered by an interrupt to be normal termination.
                         * Such exceptions are NOT propagated here and WILL NOT
                         * cause the parent query to terminate.
                         */

                        throw new RuntimeException(runningSubquery.getCause());

                    }

                } finally {

                    runningSubquery.cancel(true/* mayInterruptIfRunning */);

                }

                // done.
                return null;

            } finally {

                sink.close();
                if (sink2 != null)
                    sink2.close();

            }

        }
        
    } // ControllerTask

}
