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
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.HTreeNamedSubqueryOp;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.htree.HTree;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

/**
 * Operator builds an {@link HTree} index from the source solutions. Once all
 * source solutions have been materialized on the {@link HTree}, the source
 * solutions are output on the default sink. The set of variables to be copied
 * to the sink may be restricted by an annotation.
 * <p>
 * There are two basic use cases for the {@link HTreeHashIndexOp}, both of which rely
 * on a {@link HTreeSolutionSetHashJoinOp} to re-integrate the results buffered on
 * the {@link HTree}.
 * <p>
 * The first use case is when we will run an OPTIONAL group. In this case, an
 * OPTIONAL hash join will be used and a buffered solution will be output if
 * there was no solution in the optional group for that buffered solution. All
 * known bound variables should be used as the join variables. All variables
 * should be selected.
 * <p>
 * The second use case is when we will run a sub-select. In this case, only the
 * variables which are projected by the subquery should be selected. Those will
 * also serve as the join variables. The hash join will integrate the solutions
 * from the subquery with the buffered solutions using those join variables. The
 * integrated solutions will be the net output of the hash join.
 * <p>
 * This operator is NOT thread-safe. It relies on the query engine to provide
 * synchronization. The operator MUST be run on the query controller.
 * 
 * @see HTreeSolutionSetHashJoinOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HTreeHashIndexOp extends PipelineOp {

//    static private final transient Logger log = Logger
//            .getLogger(HashIndexOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends HTreeHashJoinAnnotations,
            HashJoinAnnotations {

        /**
         * The name of {@link IQueryAttributes} attribute under which the
         * subquery solution set is stored (a {@link HTreeHashJoinState}
         * reference). The attribute name includes the query UUID. The query
         * UUID must be extracted and used to lookup the {@link IRunningQuery}
         * to which the solution set was attached.
         * 
         * @see NamedSolutionSetRef
         */
        final String NAMED_SET_REF = HTreeNamedSubqueryOp.Annotations.NAMED_SET_REF;
        
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

        if (getEvaluationContext() != BOpEvaluationContext.CONTROLLER) {
            throw new IllegalArgumentException(
                    BOp.Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

        if (getMaxParallel() != 1) {
            /*
             * Parallel evaluation is not allowed. This operator writes on an
             * HTree and that object is not thread-safe for mutation.
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

        // Join variables must be specified.
        final IVariable<?>[] joinVars = (IVariable[]) getRequiredProperty(Annotations.JOIN_VARS);

//        if (joinVars.length == 0)
//            throw new IllegalArgumentException(Annotations.JOIN_VARS);

        for (IVariable<?> var : joinVars) {

            if (var == null)
                throw new IllegalArgumentException(Annotations.JOIN_VARS);

        }

    }

    public HTreeHashIndexOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    /**
     * Return <code>true</code> iff the solutions on the hash index will be
     * re-integrated using an OPTIONAL join.
     * 
     * @see Annotations#OPTIONAL
     */
    public boolean isOptional() {
       
        return getProperty(Annotations.OPTIONAL, Annotations.DEFAULT_OPTIONAL);
        
    }
   
    @Override
    public BOpStats newStats() {

        return new NamedSolutionSetStats();

    }

    /**
     * Adds reporting for the size of the named solution set.
     */
    private static class NamedSolutionSetStats extends BOpStats {
        
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

        private final HTreeHashIndexOp op;
        
        private final NamedSolutionSetStats stats;
        
        /** Metadata to identify the named solution set. */
        private final NamedSolutionSetRef namedSetRef;

        /**
         * The {@link IQueryAttributes} for the {@link IRunningQuery} off which
         * we will hang the named solution set.
         */
        private final IQueryAttributes attrs;

        /**
         * The {@link IVariable}[]s to be projected.
         */
        @SuppressWarnings("rawtypes")
        private final IVariable[] selected; 
        
        /**
         * <code>true</code> iff this is the first time the task is being
         * invoked, in which case we allocate the {@link #solutions} map.
         */
        private final boolean first;
        
        private final HTreeHashJoinUtility state;
        
        public ControllerTask(final HTreeHashIndexOp op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.op = op;
            
            this.stats = ((NamedSolutionSetStats) context.getStats());

            this.selected = (IVariable[]) op.getProperty(Annotations.SELECT);
            
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

                HTreeHashJoinUtility state = (HTreeHashJoinUtility) attrs
                        .get(namedSetRef);

                if (state == null) {
                    
                    state = new HTreeHashJoinUtility(
                            context.getMemoryManager(namedSetRef.queryId), op,
                            op.isOptional());

                    if (attrs.putIfAbsent(namedSetRef, state) != null)
                        throw new AssertionError();

                    first = true;
                    
                } else {
                 
                    first = false;
                    
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
                state.acceptSolutions(context.getSource(), stats);

                if(context.isLastInvocation()) {

                    /*
                     * Note: The [HTree] object is already present on the
                     * IQueryAttributes. However, it is the mutable HTree
                     * object. We convert it to an immutable HTree object here
                     * by check pointing the HTree and updating the reference on
                     * the IQueryAttributes. That would allow the consumer of
                     * the HTree to be safe for concurrent readers.
                     * 
                     * Note: We must checkpoint the solution set before we
                     * output anything. Otherwise the chunks output by this
                     * operator could appear at the SolutionSetHashJoinOp before
                     * this operator is done and it would have the mutable view
                     * of the HTree rather than the concurrent read-only view of
                     * the HTree.
                     */
                    
                    // Checkpoint and save the solution set.
                    state.saveSolutionSet();
                    
                    // Output the buffered solutions.
                    outputSolutions();
                    
                }
                
                // Done.
                return null;

            } finally {
                
                context.getSource().close();

                context.getSink().close();
                
//                if (context.getSink2() != null)
//                    context.getSink2().close();

            }
            
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
