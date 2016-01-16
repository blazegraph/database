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
 * Created on Aug 30, 2011
 */

package com.bigdata.bop.join;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.HTreeNamedSubqueryOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.controller.INamedSubqueryOp;
import com.bigdata.bop.controller.JVMNamedSubqueryOp;
import com.bigdata.bop.controller.NamedSetAnnotations;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.htree.HTree;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

/**
 * Operator joins a solution set modeled as a hash index into the pipeline. The
 * solution set may be modeled by an {@link HTree} or a JVM {@link HashMap}.
 * While this JOIN requires the RHS hash index to be fully materialized,
 * evaluation of the LHS source solutions is pipelined.
 * <p>
 * Parallel evaluation of source chunks is permitted, but the RHS hash index
 * must have been checkpointed before this operator begins evaluation (the
 * read-only {@link HTree} is thread-safe for concurrent readers). The
 * checkpoint is a NOP for the thread-safe JVM {@link ConcurrentHashMap}
 * collection class.
 * <p>
 * A {@link HTree} solution set must be be constructed by a
 * {@link HTreeNamedSubqueryOp} or a {@link HTreeHashIndexOp}. A JVM solution
 * set must be constructed by a {@link JVMNamedSubqueryOp} or
 * {@link JVMHashIndexOp}.
 * 
 * <h2>Handling OPTIONAL, EXISTS, and NOT-EXISTS</h2>
 * 
 * {@link PipelineOp.Annotations#LAST_PASS} evaluation MUST be requested for any
 * other than a {@link JoinTypeEnum#Normal}. See
 * {@link ChunkTask#doLastPass(UnsyncLocalOutputBuffer)} for details.
 * 
 * TODO This class could be made concrete. There is no logic it in that is
 * specific to either the {@link HTree} or JVM hash join operation.
 * 
 * @see INamedSubqueryOp
 * @see HashIndexOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: NamedSubqueryIncludeOp.java 5178 2011-09-12 19:09:23Z
 *          thompsonbry $
 */
abstract public class SolutionSetHashJoinOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations,
            NamedSetAnnotations {

        /**
         * An {@link IConstraint}[] to be applied to solutions when they are
         * joined (optional).
         */
        final String CONSTRAINTS = JoinAnnotations.CONSTRAINTS;
        
        /**
         * When <code>true</code> the hash index identified by
         * {@link #NAMED_SET_REF} will be released when this operator is done
         * (default {@value #DEFAULT_RELEASE}).
         * <p>
         * Note: Whether or not the hash index can be released depends on
         * whether or not the hash index will be consumed by more than one
         * operator in the query plan. For example, a named solution set can be
         * consumed by more than one operator and thus must not be released
         * until all such operators are done.
         * 
         * TODO Alternatively, we could specify the #of different locations in
         * the query plan where the named solution set will be consumed. This
         * could be part of the {@link HTreeHashJoinUtility} state, in which
         * case it would only be set as an annotation on the operator which
         * generates the hash index.
         * <p>
         * Note: Any memory associated with the {@link IRunningQuery} will be
         * released no later than when the {@link IRunningQuery#isDone()}. This
         * only provides a means to release data as soon as it is known that the
         * data will not be referenced again during the query.
         */
        final String RELEASE = SolutionSetHashJoinOp.class + ".release";

        final boolean DEFAULT_RELEASE = true;
        
    }

    /**
     * Deep copy constructor.
     */
    public SolutionSetHashJoinOp(final SolutionSetHashJoinOp op) {

        super(op);
        
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public SolutionSetHashJoinOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        if (getProperty(Annotations.RELEASE, Annotations.DEFAULT_RELEASE)
                && !isLastPassRequested()) {
            /*
             * In order to release the hash index, this operator needs to be
             * notified when no more source solutions will become available.
             */
            throw new IllegalArgumentException(Annotations.RELEASE
                    + " requires " + Annotations.LAST_PASS);
        }
        
        // The RHS annotation must be specified.
        getRequiredProperty(Annotations.NAMED_SET_REF);
        
    }

    public SolutionSetHashJoinOp(final BOp[] args, final NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }
    
    @Override
    public BaseJoinStats newStats() {

        return new BaseJoinStats();

    }

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask<IBindingSet>(context, this));
        
    }

    /**
     * Task executing on the node.
     */
    private static class ChunkTask<E> implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final SolutionSetHashJoinOp op;

        private final IHashJoinUtility state;

        private final IConstraint[] constraints;
        
        private final boolean release;
        
        private final BaseJoinStats stats;

        private final IBlockingBuffer<IBindingSet[]> sink;
        
        private final IBlockingBuffer<IBindingSet[]> sink2;

        public ChunkTask(final BOpContext<IBindingSet> context,
                final SolutionSetHashJoinOp op) {

            this.context = context;

            this.stats = (BaseJoinStats) context.getStats();

            this.release = op.getProperty(Annotations.RELEASE,
                    Annotations.DEFAULT_RELEASE);

            this.sink = context.getSink();

            this.sink2 = context.getSink2();

            this.op = op;

            // The name of the attribute used to discover the solution set.
            final INamedSolutionSetRef namedSetRef = (INamedSolutionSetRef) op
                    .getRequiredProperty(Annotations.NAMED_SET_REF);

            /*
			 * Lookup the attributes for the query on which we will hang the
			 * solution set. See BLZG-1493 (if queryId is null, use the query
			 * attributes for this running query).
			 */
			final IQueryAttributes attrs = context.getQueryAttributes(namedSetRef.getQueryId());

            state = (IHashJoinUtility) attrs.get(namedSetRef);

            if (state == null) {
                
                // The solution set was not found!
                
                throw new RuntimeException("Not found: " + namedSetRef);
                
            }

            if (!state.getJoinType().isNormal() && !op.isLastPassRequested()) {

                /*
                 * Anything but a Normal join requires that we observe all solutions
                 * and then do some final reporting. This is necessary for Optional,
                 * Exists, and NotExists. 
                 */

                throw new UnsupportedOperationException(state.getJoinType()
                        + " requires " + Annotations.LAST_PASS);

            }

            /*
             * Combine the original constraints (if any) with those attached to
             * this operator (if any).
             * 
             * Note: The solution set hash join is used to join in a hash index
             * generated by some other part of the query plan. Since it is also
             * used for named subqueries, which can be included in more than one
             * location, it is necessary that we can override/expand on the join
             * constraints for this operator.
             */
            this.constraints = BOpUtility.concat(
                    (IConstraint[]) op.getProperty(Annotations.CONSTRAINTS),
                    state.getConstraints());

        }

        @Override
        public Void call() throws Exception {

            try {

                doHashJoin();
                
                // Done.
                return null;
                
            } finally {

                if (release && context.isLastInvocation()) {

                    /*
                     * Note: It is possible to INCLUDE the named temporary
                     * solution set multiple times within a query. If we want to
                     * release() the hash tree then we need to know how many
                     * times the temporary solution set is being included and
                     * decrement a counter each time. When the counter reaches
                     * zero, we can release the hash index.
                     */
                    state.release();

                }
                
                sink.close();

                if (sink2 != null)
                    sink2.close();
                
            }

        }
        
        /**
         * Do a hash join of the buffered solutions with the access path.
         */
        private void doHashJoin() {

            if (state.isEmpty())
                return;
            
            stats.accessPathCount.increment();

            stats.accessPathRangeCount.add(state.getRightSolutionCount());

            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                    op.getChunkCapacity(), sink);

            state.hashJoin2(context.getSource(), stats, unsyncBuffer,
                    constraints);

            if (context.isLastInvocation()) {

                doLastPass(unsyncBuffer);
            
            }
            
            unsyncBuffer.flush();
            sink.flush();

        }

        /**
         * This method handles {@link JoinTypeEnum} values other than
         * {@link JoinTypeEnum#Normal}. {@link PipelineOp.Annotations#LAST_PASS}
         * evaluation MUST be requested for any other than a
         * {@link JoinTypeEnum#Normal}.
         */
        private void doLastPass(
                final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer) {
            
            switch (state.getJoinType()) {
            case Normal:
                /*
                 * Nothing to do.
                 */
                break;
            case Optional:
            case NotExists: {
                /*
                 * Output the optional solutions.
                 */

                // where to write the optional solutions.
                final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = sink2 == null ? unsyncBuffer
                        : new UnsyncLocalOutputBuffer<IBindingSet>(
                                op.getChunkCapacity(), sink2);

                state.outputOptionals(unsyncBuffer2);

                unsyncBuffer2.flush();
                if (sink2 != null)
                    sink2.flush();

                break;
            }
            case Exists: {
                /*
                 * Output the join set.
                 * 
                 * Note: This has special hooks to support (NOT) EXISTS
                 * graph patterns, which must bind the "ASK_VAR" depending
                 * on whether or not the graph pattern is satisified.
                 */
                final IVariable<?> askVar = state.getAskVar();
                // askVar := true
                state.outputJoinSet(unsyncBuffer);
                if (askVar != null) {
                    // askVar := false;
                    state.outputOptionals(unsyncBuffer);
                }
                break;
            }
            default:
                throw new AssertionError();
            }

        }

    } // class ChunkTask

}
