/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 30, 2011
 */

package com.bigdata.bop.join;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.HTreeNamedSubqueryOp;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.htree.HTree;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Operator joins a solution set into the pipeline. The solution set must be be
 * constructed by a {@link HTreeNamedSubqueryOp} or a {@link HTreeHashIndexOp}. While this
 * JOIN requires the RHS {@link HTree} to be fully materialized, evaluation of
 * the LHS source solutions is pipelined. Parallel evaluation of source chunks
 * is permitted, but the RHS {@link HTree} must have been checkpointed before
 * this operator begins evaluation (the read-only {@link HTree} is thread-safe
 * for concurrent readers).
 * 
 * <h2>Handling OPTIONAL</h2>
 * 
 * {@link PipelineOp.Annotations#LAST_PASS} evaluation MUST be requested for an
 * OPTIONAL JOIN because we must have ALL solutions on hand in order to decide
 * which solutions did not join. If the JOIN is OPTIONAL, then solutions which
 * join will be output for each source chunk but the "optional" solutions will
 * not be reported until ALL source chunks have been processed.
 * 
 * @see HTreeHashJoinUtility
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: NamedSubqueryIncludeOp.java 5178 2011-09-12 19:09:23Z
 *          thompsonbry $
 */
public class HTreeSolutionSetHashJoinOp extends PipelineOp {

//    static private final transient Logger log = Logger
//            .getLogger(HTreeSolutionSetHashJoinOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The {@link NamedSolutionSetRef} used to locate the {@link HTree}
         * having the data for the named solution set. The query UUID must be
         * extracted and used to lookup the {@link IRunningQuery} to which the
         * solution set was attached. The {@link HTree} is then resolved against
         * the {@link IQueryAttributes} on that {@link IRunningQuery}.
         * 
         * @see NamedSolutionSetRef
         * @see HTreeNamedSubqueryOp.Annotations#NAMED_SET_REF
         */
        final String NAMED_SET_REF = HTreeNamedSubqueryOp.Annotations.NAMED_SET_REF;
        
        /**
         * Constraints to be applied by the join (in addition to any associated
         * with the {@link HTreeHashJoinUtility} state in the
         * {@link #NAMED_SET_REF}).
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
         */
        final String RELEASE = HTreeSolutionSetHashJoinOp.class + ".release";

        final boolean DEFAULT_RELEASE = true;
        
    }

    /**
     * Deep copy constructor.
     */
    public HTreeSolutionSetHashJoinOp(HTreeSolutionSetHashJoinOp op) {

        super(op);
        
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public HTreeSolutionSetHashJoinOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        if (isRelease() && !isLastPassRequested()) {
            /*
             * In order to release the hash index, this operator needs to be
             * notified when no more source solutions will become available.
             */
            throw new IllegalArgumentException(Annotations.RELEASE
                    + " requires " + Annotations.LAST_PASS);
        }
        
        // The RHS HTree annotation must be specified.
        getRequiredProperty(Annotations.NAMED_SET_REF);
        
    }

    public HTreeSolutionSetHashJoinOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    /**
     * @see Annotations#RELEASE
     */
    public boolean isRelease() {
        
        return getProperty(Annotations.RELEASE,Annotations.DEFAULT_RELEASE);
        
    }
    
    public BaseJoinStats newStats() {

        return new BaseJoinStats();

    }

    @Override
    public FutureTask<Void> eval(BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask<IBindingSet>(context, this));
        
    }

    /**
     * Task executing on the node.
     */
    private static class ChunkTask<E> implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final HTreeSolutionSetHashJoinOp op;

        private final boolean release;
        
        private final BaseJoinStats stats;

        private final IBlockingBuffer<IBindingSet[]> sink;
        
        private final IBlockingBuffer<IBindingSet[]> sink2;

        private final IConstraint[] constraints;
        
        private final HTreeHashJoinUtility state;

        public ChunkTask(final BOpContext<IBindingSet> context,
                final HTreeSolutionSetHashJoinOp op) {

            this.context = context;

            this.stats = (BaseJoinStats) context.getStats();

            this.release = op.getProperty(Annotations.RELEASE,
                    Annotations.DEFAULT_RELEASE);

            this.sink = context.getSink();

            this.sink2 = context.getSink2();

            this.op = op;

            // The name of the attribute used to discover the solution set.
            final NamedSolutionSetRef namedSetRef = (NamedSolutionSetRef) op
                    .getRequiredProperty(Annotations.NAMED_SET_REF);

            // Lookup the attributes for the query on which we will hang the
            // solution set.
            final IQueryAttributes attrs = context
                    .getQueryAttributes(namedSetRef.queryId);

            state = (HTreeHashJoinUtility) attrs.get(namedSetRef);

            if (state == null) {
                
                // The solution set was not found!
                
                throw new RuntimeException("Not found: " + namedSetRef);
                
            }

            if (state.isOptional() && !op.isLastPassRequested()) {

                /*
                 * An optional join requires that we observe all solutions
                 * before we report "optional" solutions so we can identify
                 * those which do not join.
                 */

                throw new UnsupportedOperationException(
                        JoinAnnotations.OPTIONAL + " requires "
                                + Annotations.LAST_PASS);
            
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
                     * zero, we can release the HTree.
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

            final ICloseableIterator<IBindingSet> leftItr = new Dechunkerator<IBindingSet>(
                    context.getSource());

            state.hashJoin2(leftItr, unsyncBuffer, true/* leftIsPipeline */,
                    constraints);

            if (state.isOptional() && context.isLastInvocation()) {

                // where to write the optional solutions.
                final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = sink2 == null ? unsyncBuffer
                        : new UnsyncLocalOutputBuffer<IBindingSet>(
                                op.getChunkCapacity(), sink2);

                state.outputOptionals(unsyncBuffer2);

                unsyncBuffer2.flush();
                if (sink2 != null)
                    sink2.flush();

            }

            unsyncBuffer.flush();
            sink.flush();

        }
        
    } // class ChunkTask

}
