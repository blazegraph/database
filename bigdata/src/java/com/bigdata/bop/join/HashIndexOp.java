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

package com.bigdata.bop.join;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.NamedSetAnnotations;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.htree.HTree;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

/**
 * Operator builds a hash index from the source solutions. Once all source
 * solutions have been indexed, the source solutions are output on the default
 * sink. The set of variables to be copied to the sink may be restricted by an
 * annotation.
 * <p>
 * The main use case for building a hash index is to execute a sub-group or
 * sub-select. In both cases, the {@link HashIndexOp} is generated before we
 * enter the sub-plan. All solutions from the hash index are then flowed into
 * the sub-plan. Solutions emitted by the sub-plan are then re-integrated into
 * the parent using a {@link SolutionSetHashJoinOp}.
 * <p>
 * There are two concrete implementations of this operator. One for the
 * {@link HTree} and one for the JVM {@link ConcurrentHashMap}. Both hash index
 * build operators have the same general logic, but differ in their specifics.
 * Those differences are mostly encapsulated by the {@link IHashJoinUtility}
 * interface. They also have somewhat different annotations, primarily because
 * the {@link HTree} version needs access to the lexicon to setup its ivCache.
 * <p>
 * This operator is NOT thread-safe. It relies on the query engine to provide
 * synchronization. The operator MUST be run on the query controller.
 * 
 * @see SolutionSetHashJoinOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class HashIndexOp extends PipelineOp {

//    static private final transient Logger log = Logger
//            .getLogger(HashIndexOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends HashJoinAnnotations, JoinAnnotations,
            NamedSetAnnotations {

    }

    /**
     * Deep copy constructor.
     */
    public HashIndexOp(final HashIndexOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public HashIndexOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        /*
         * The basic constraint is that the hash index needs to be available
         * where it will be consumed. Thus, ANY is not an appropriate evaluation
         * context since the hash index would be built at whatever node had a
         * given intermediate solution. However, any evaluation context which
         * establishes a predictable relationship between the join variables and
         * the hash index partition should work. The CONTROLLER always works,
         * but the index will be built and consumed on the controller.
         */
        switch (getEvaluationContext()) {
        case CONTROLLER:
        case SHARDED:
        case HASHED:
            break;
        default:
            throw new IllegalArgumentException(
                    BOp.Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }
//        if (getEvaluationContext() != BOpEvaluationContext.CONTROLLER) {
//            throw new IllegalArgumentException(
//                    BOp.Annotations.EVALUATION_CONTEXT + "="
//                            + getEvaluationContext());
//        }

        if (getMaxParallel() != 1) {
            /*
             * Parallel evaluation is not allowed. This operator writes on an
             * object that is not thread-safe for mutation.
             */
            throw new IllegalArgumentException(
                    PipelineOp.Annotations.MAX_PARALLEL + "="
                            + getMaxParallel());
        }

        if (!isLastPassRequested()) {
            /*
             * Last pass evaluation must be requested. This operator will not
             * produce any outputs until all source solutions have been
             * buffered.
             */
            throw new IllegalArgumentException(PipelineOp.Annotations.LAST_PASS
                    + "=" + isLastPassRequested());
        }

        getRequiredProperty(Annotations.NAMED_SET_REF);

        @SuppressWarnings("unused")
        final JoinTypeEnum joinType = (JoinTypeEnum) getRequiredProperty(Annotations.JOIN_TYPE);

        // Join variables must be specified.
        final IVariable<?>[] joinVars = (IVariable[]) getRequiredProperty(Annotations.JOIN_VARS);

//        if (joinVars.length == 0)
//            throw new IllegalArgumentException(Annotations.JOIN_VARS);

        for (IVariable<?> var : joinVars) {

            if (var == null)
                throw new IllegalArgumentException(Annotations.JOIN_VARS);

        }

    }

    public HashIndexOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    @Override
    public BOpStats newStats() {

        return new NamedSolutionSetStats();

    }

    /**
     * Return the instance of the {@link IHashJoinUtility} to be used by this
     * operator. This method is invoked once, the first time this operator is
     * evaluated. The returned {@link IHashJoinUtility} reference is attached to
     * the {@link IQueryAttributes} and accessed there on subsequent evaluation
     * passes for this operator.
     * 
     * @param context
     *            The {@link BOpEvaluationContext}
     * @param namedSetRef
     *            Metadata to identify the named solution set.
     * @param joinType
     *            The type of join.
     */
    abstract protected IHashJoinUtility newState(
            BOpContext<IBindingSet> context,
            final NamedSolutionSetRef namedSetRef, final JoinTypeEnum joinType);

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

        private final HashIndexOp op;
        
        private final NamedSolutionSetStats stats;
        
        private final IHashJoinUtility state;
        
        public ControllerTask(final HashIndexOp op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.op = op;
            
            this.stats = ((NamedSolutionSetStats) context.getStats());

            // Metadata to identify the named solution set.
            final NamedSolutionSetRef namedSetRef = (NamedSolutionSetRef) op
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
                final IQueryAttributes attrs = context
                        .getQueryAttributes(namedSetRef.queryId);

                IHashJoinUtility state = (IHashJoinUtility) attrs
                        .get(namedSetRef);

                if (state == null) {
                    
                    final JoinTypeEnum joinType = (JoinTypeEnum) op
                            .getRequiredProperty(Annotations.JOIN_TYPE);

                    state = op.newState(context, namedSetRef, joinType);

                    if (attrs.putIfAbsent(namedSetRef, state) != null)
                        throw new AssertionError();
                                        
                }
                
                this.state = state;

            }
            
        }
        
        /**
         * Evaluate.
         */
        public Void call() throws Exception {
            
            try {

                // Buffer all source solutions.
                acceptSolutions();
                
                if(context.isLastInvocation()) {

                    // Checkpoint the solution set.
                    checkpointSolutionSet();
                    
                    // Output the buffered solutions.
                    outputSolutions();
                    
                }
                
                // Done.
                return null;

            } finally {
                
                context.getSource().close();

                context.getSink().close();

            }
            
        }

        /**
         * Buffer intermediate resources.
         */
        private void acceptSolutions() {

            state.acceptSolutions(context.getSource(), stats);

        }

        /**
         * Checkpoint and save the solution set.
         * <p>
         * Note: We must checkpoint the solution set before we output anything.
         * Otherwise the chunks output by this operator could appear at the
         * {@link SolutionSetHashJoinOp} before this operator is done and it
         * would have the mutable view of the {@link HTree} rather than the
         * concurrent read-only view of the {@link HTree}.
         */
        private void checkpointSolutionSet() {
            
            state.saveSolutionSet();
            
        }
        
        /**
         * Output the buffered solutions.
         */
        private void outputSolutions() {

            // default sink
            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                    op.getChunkCapacity(), sink);

            state.outputSolutions(unsyncBuffer);
            
            unsyncBuffer.flush();

            sink.flush();

        }

    } // ControllerTask

}
