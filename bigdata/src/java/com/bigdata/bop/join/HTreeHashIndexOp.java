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

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.controller.INamedSolutionSetRef;

/**
 * Concrete implementation based on the {@link HTreeHashJoinUtility}.
 * 
 * @see HTreeSolutionSetHashJoinOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HTreeHashIndexOp extends HashIndexOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends HashIndexOp.Annotations,
            HTreeHashJoinAnnotations {

    }
    
    /**
     * Deep copy constructor.
     */
    public HTreeHashIndexOp(final HTreeHashIndexOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public HTreeHashIndexOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

//        if (getEvaluationContext() != BOpEvaluationContext.CONTROLLER) {
//            throw new IllegalArgumentException(
//                    BOp.Annotations.EVALUATION_CONTEXT + "="
//                            + getEvaluationContext());
//        }
//
//        if (getMaxParallel() != 1) {
//            /*
//             * Parallel evaluation is not allowed. This operator writes on an
//             * object that is not thread-safe for mutation.
//             */
//            throw new IllegalArgumentException(
//                    PipelineOp.Annotations.MAX_PARALLEL + "="
//                            + getMaxParallel());
//        }
//
//        if (!isLastPassRequested()) {
//            /*
//             * Last pass evaluation must be requested. This operator will not
//             * produce any outputs until all source solutions have been
//             * buffered.
//             */
//            throw new IllegalArgumentException(PipelineOp.Annotations.LAST_PASS
//                    + "=" + isLastPassRequested());
//        }
//
//        getRequiredProperty(Annotations.NAMED_SET_REF);
//
//        @SuppressWarnings("unused")
//        final JoinTypeEnum joinType = (JoinTypeEnum) getRequiredProperty(Annotations.JOIN_TYPE);
//
//        // Join variables must be specified.
//        final IVariable<?>[] joinVars = (IVariable[]) getRequiredProperty(Annotations.JOIN_VARS);
//
////        if (joinVars.length == 0)
////            throw new IllegalArgumentException(Annotations.JOIN_VARS);
//
//        for (IVariable<?> var : joinVars) {
//
//            if (var == null)
//                throw new IllegalArgumentException(Annotations.JOIN_VARS);
//
//        }

    }

    public HTreeHashIndexOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

//    @Override
//    public BOpStats newStats() {
//
//        return new NamedSolutionSetStats();
//
//    }
    
    @Override
    protected HTreeHashJoinUtility newState(
            final BOpContext<IBindingSet> context,
            final INamedSolutionSetRef namedSetRef, final JoinTypeEnum joinType) {

        return new HTreeHashJoinUtility(
                context.getMemoryManager(namedSetRef.getQueryId()), this, joinType);

    }
    
//    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {
//
//        return new FutureTask<Void>(new ControllerTask(this, context));
//        
//    }
    
//	/**
//	 * Evaluates the subquery for each source binding set. If the controller
//	 * operator is interrupted, then the subqueries are cancelled. If a subquery
//	 * fails, then all subqueries are cancelled.
//	 */
//    private static class ControllerTask implements Callable<Void> {
//
//        private final BOpContext<IBindingSet> context;
//
//        private final HTreeHashIndexOp op;
//        
//        private final NamedSolutionSetStats stats;
//        
//        private final IHashJoinUtility state;
//        
//        public ControllerTask(final HTreeHashIndexOp op,
//                final BOpContext<IBindingSet> context) {
//
//            if (op == null)
//                throw new IllegalArgumentException();
//
//            if (context == null)
//                throw new IllegalArgumentException();
//
//            this.context = context;
//
//            this.op = op;
//            
//            this.stats = ((NamedSolutionSetStats) context.getStats());
//
//            // Metadata to identify the named solution set.
//            final NamedSolutionSetRef namedSetRef = (NamedSolutionSetRef) op
//                    .getRequiredProperty(Annotations.NAMED_SET_REF);
//            
//            {
//
//                /*
//                 * First, see if the map already exists.
//                 * 
//                 * Note: Since the operator is not thread-safe, we do not need
//                 * to use a putIfAbsent pattern here.
//                 */
//                
//                // Lookup the attributes for the query on which we will hang the
//                // solution set.
//                final IQueryAttributes attrs = context
//                        .getQueryAttributes(namedSetRef.queryId);
//
//                HTreeHashJoinUtility state = (HTreeHashJoinUtility) attrs
//                        .get(namedSetRef);
//
//                if (state == null) {
//                    
//                    final JoinTypeEnum joinType = (JoinTypeEnum) op
//                            .getRequiredProperty(Annotations.JOIN_TYPE);
//
//                    state = new HTreeHashJoinUtility(
//                            context.getMemoryManager(namedSetRef.queryId), op,
//                            joinType);
//
//                    if (attrs.putIfAbsent(namedSetRef, state) != null)
//                        throw new AssertionError();
//                                        
//                }
//                
//                this.state = state;
//
//            }
//            
//        }
//
//        /**
//         * Evaluate.
//         */
//        public Void call() throws Exception {
//            
//            try {
//
//                // Buffer all source solutions.
//                acceptSolutions();
//                
//                if(context.isLastInvocation()) {
//
//                    // Checkpoint the solution set.
//                    checkpointSolutionSet();
//                    
//                    // Output the buffered solutions.
//                    outputSolutions();
//                    
//                }
//                
//                // Done.
//                return null;
//
//            } finally {
//                
//                context.getSource().close();
//
//                context.getSink().close();
//
//            }
//            
//        }
//
//        /**
//         * Buffer intermediate resources.
//         */
//        private void acceptSolutions() {
//
//            state.acceptSolutions(context.getSource(), stats);
//
//        }
//
//        /**
//         * Checkpoint and save the solution set.
//         */
//        private void checkpointSolutionSet() {
//            
//            state.saveSolutionSet();
//            
//        }
//        
//        /**
//         * Output the buffered solutions.
//         */
//        private void outputSolutions() {
//
//            // default sink
//            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();
//
//            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
//                    op.getChunkCapacity(), sink);
//
//            state.outputSolutions(unsyncBuffer);
//            
//            unsyncBuffer.flush();
//
//            sink.flush();
//
//        }
//
//    } // ControllerTask

}
