/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.ISingleThreadedOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.controller.NamedSetAnnotations;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.htree.HTree;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.SingleValueIterator;

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
abstract public class HashIndexOp extends PipelineOp implements ISingleThreadedOp {

//    static private final transient Logger log = Logger
//            .getLogger(HashIndexOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends HashJoinAnnotations, JoinAnnotations,
            NamedSetAnnotations {

        /**
         * An optional attribute specifying the <em>source</em> named solution
         * set for the index build operation. Normally, the hash index is built
         * from the solutions flowing through the pipeline. When this attribute
         * is specified, the hash index is instead built from the solutions in
         * the specified named solution set. Regardless, the solutions flowing
         * through the pipeline are copied to the sink once the hash index has
         * been built.
         */
        final String NAMED_SET_SOURCE_REF = "namedSetSourceRef";

        /**
         * An optional attribute specifying the <em>source</em> IBindingSet[]
         * for the index build operation. Normally, the hash index is built from
         * the solutions flowing through the pipeline. When this attribute is
         * specified, the hash index is instead built from the solutions in the
         * specified IBindingSet[]. Regardless, the solutions flowing through
         * the pipeline are copied to the sink once the hash index has been
         * built.
         */
        final String BINDING_SETS_SOURCE = "bindingSets";
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
    public HashIndexOp(final BOp[] args, final Map<String, Object> annotations) {

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

        /*
         * This operator writes on an object that is not thread-safe for
         * mutation.
         */
        assertMaxParallelOne();

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

        for (IVariable<?> var : joinVars) {

            if (var == null)
                throw new IllegalArgumentException(Annotations.JOIN_VARS);

        }

    }

    public HashIndexOp(final BOp[] args, final NV... annotations) {

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
    abstract protected IHashJoinUtility newState(//
            final BOpContext<IBindingSet> context,//
            final INamedSolutionSetRef namedSetRef, //
            final JoinTypeEnum joinType//
            );

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(this, context));
        
    }
    
    /**
     * Evaluates the subquery for each source binding set. If the controller
     * operator is interrupted, then the subqueries are cancelled. If a subquery
     * fails, then all subqueries are cancelled.
     */
    private static class ChunkTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final HashIndexOp op;
        
        private final NamedSolutionSetStats stats;
        
        private final IHashJoinUtility state;

        /**
         * <code>true</code> iff this is the first invocation of this operator.
         */
        private final boolean first;
        
        /**
         * <code>true</code> iff the hash index will be generated from the
         * intermediate solutions arriving from the pipeline. When
         * <code>false</code>, the
         * {@link HashIndexOp.Annotations#NAMED_SET_SOURCE_REF} identifies the
         * source from which the index will be built.
         */
        private final boolean sourceIsPipeline;
        
        public ChunkTask(final HashIndexOp op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.op = op;
            
            this.stats = ((NamedSolutionSetStats) context.getStats());

            // Metadata to identify the target named solution set.
            final INamedSolutionSetRef namedSetRef = (INamedSolutionSetRef) op
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
                        .getQueryAttributes(namedSetRef.getQueryId());

                IHashJoinUtility state = (IHashJoinUtility) attrs
                        .get(namedSetRef);

                if (state == null) {
                    
                    final JoinTypeEnum joinType = (JoinTypeEnum) op
                            .getRequiredProperty(Annotations.JOIN_TYPE);

                    state = op.newState(context, namedSetRef, joinType);

                    if (attrs.putIfAbsent(namedSetRef, state) != null)
                        throw new AssertionError();
                    
                    first = true;
                                        
                } else {
                    
                    first = false;

                }
                
                this.state = state;

            }
            
            // true iff we will build the index from the pipeline.
            this.sourceIsPipeline //
                = (op.getProperty(Annotations.NAMED_SET_SOURCE_REF) == null)
                && (op.getProperty(Annotations.BINDING_SETS_SOURCE) == null)
                ;

        }
        
        /**
         * Evaluate.
         */
        @Override
        public Void call() throws Exception {

            try {

                if (sourceIsPipeline) {

                    // Buffer all source solutions.
                    acceptSolutions();

                    if (context.isLastInvocation()) {

                        // Checkpoint the solution set.
                        checkpointSolutionSet();

                        // Output the buffered solutions.
                        outputSolutions();

                    }

                } else {
                    
                    if(first) {
                    
                        // Accept ALL solutions.
                        acceptSolutions();
                        
                        // Checkpoint the generated solution set index.
                        checkpointSolutionSet();
                        
                    }

                    // Copy all solutions from the pipeline to the sink.
                    BOpUtility.copy(context.getSource(), context.getSink(),
                            null/* sink2 */, null/* mergeSolution */,
                            null/* selectVars */, null/* constraints */, stats);

                    // Flush solutions to the sink.
                    context.getSink().flush();

                }

                // Done.
                return null;

            } finally {
                
                context.getSource().close();

                context.getSink().close();

            }
            
        }

        /**
         * Add solutions to the hash index. The solutions to be indexed will be
         * read either from the pipeline or from an "alternate" source
         * identified by an annotation.
         * 
         * @see HashIndexOp.Annotations#NAMED_SET_SOURCE_REF
         */
        private void acceptSolutions() {

            final ICloseableIterator<IBindingSet[]> src;

            if (sourceIsPipeline) {
            
                src = context.getSource();
                
            } else if (op.getProperty(Annotations.NAMED_SET_SOURCE_REF) != null) {
                
                /*
                 * Metadata to identify the optional *source* solution set. When
                 * <code>null</code>, the hash index is built from the solutions flowing
                 * through the pipeline. When non-<code>null</code>, the hash index is
                 * built from the solutions in the identifier solution set.
                 */
                final INamedSolutionSetRef namedSetSourceRef = (INamedSolutionSetRef) op
                        .getRequiredProperty(Annotations.NAMED_SET_SOURCE_REF);

                src = context.getAlternateSource(namedSetSourceRef);
                
            } else if (op.getProperty(Annotations.BINDING_SETS_SOURCE) != null) {

                /*
                 * The IBindingSet[] is directly given. Just wrap it up as an
                 * iterator. It will visit a single chunk of solutions.
                 */
                final IBindingSet[] bindingSets = (IBindingSet[]) op
                        .getProperty(Annotations.BINDING_SETS_SOURCE);

                src = new SingleValueIterator<IBindingSet[]>(bindingSets);
                
            } else {

                throw new UnsupportedOperationException(
                        "Source was not specified");
                
            }

            try {

                state.acceptSolutions(src, stats);

            } finally {

                src.close();

            }

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
